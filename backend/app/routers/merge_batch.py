"""合并批次接口：分批键索引、合并批次结果、中断恢复、迟到更新与 CDC 交接。

所有接口通过 ``X-Data-Scope`` 请求头声明业务归属；越权访问由数据归属守卫拒绝。
"""
from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.schemas import ActionResult
from app.services.data_scope import DataScopeError, resolve_scope
from app.services.merge_batch import merge_batch_service

router = APIRouter(prefix="/api/merge_batch", tags=["合并批次"])


def _scope(request: Request) -> str:
    return resolve_scope(request)


@router.get("/runs")
def list_runs(request: Request, target_table: str | None = Query(default=None)) -> dict:
    """查询当前归属可见的合并批次运行与其批次结果。"""
    try:
        scope = _scope(request)
        runs = merge_batch_service.list_runs(scope, target_table=target_table)
        return {"items": runs, "total": len(runs), "scope": scope}
    except DataScopeError as exc:
        return {"items": [], "total": 0, "scope": None, "error": str(exc), "ok": False}


@router.get("/runs/{run_id}")
def get_run(request: Request, run_id: int) -> ActionResult:
    """读取单次合并运行及其全部批次结果（含键范围与待重算集合）。"""
    try:
        run = merge_batch_service.get_run(_scope(request), run_id)
        if run is None:
            return ActionResult(ok=False, message=f"合并批次运行 {run_id} 不存在")
        batches = merge_batch_service.list_batches(_scope(request), run_id)
        return ActionResult(ok=True, message="合并批次结果读取成功",
                            entry={"run": run, "batches": batches})
    except DataScopeError as exc:
        return ActionResult(ok=False, message=str(exc))


@router.post("/plan", response_model=ActionResult)
def plan_run(request: Request, payload: dict) -> ActionResult:
    """构建分批键索引并生成合并批次结果。"""
    try:
        run, message = merge_batch_service.plan_run(
            _scope(request),
            upsert_id=int(payload.get("upsert_id", 0)),
            key_count=int(payload.get("key_count", 0)),
            batch_size=int(payload.get("batch_size", 0)),
            cdc_config_id=payload.get("cdc_config_id"),
        )
        if run is None:
            return ActionResult(ok=False, message=message)
        batches = merge_batch_service.list_batches(_scope(request), int(run["id"]))
        return ActionResult(ok=True, message=message, entry={"run": run, "batches": batches})
    except DataScopeError as exc:
        return ActionResult(ok=False, message=str(exc))
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="upsert_id、key_count、batch_size 必须是整数")


@router.post("/runs/{run_id}/batches/{batch_no}/run", response_model=ActionResult)
def run_batch(
    request: Request,
    run_id: int,
    batch_no: int,
    fail_after_key: int | None = Query(default=None, description="模拟写入中断：处理N个键后中断"),
) -> ActionResult:
    """运行单个合并批次，只处理该批次键范围内的订单记录。"""
    try:
        batch, message = merge_batch_service.run_batch(
            _scope(request), run_id, batch_no, fail_after_key=fail_after_key)
        if batch is None:
            return ActionResult(ok=False, message=message)
        return ActionResult(ok=batch["status"] != "已中断", message=message, entry=batch)
    except DataScopeError as exc:
        return ActionResult(ok=False, message=str(exc))


@router.post("/runs/{run_id}/resume", response_model=ActionResult)
def resume_run(request: Request, run_id: int) -> ActionResult:
    """写入中断后恢复：只重算未完成批次，已发布键范围保持不变。"""
    try:
        run, message, resumed = merge_batch_service.resume_run(_scope(request), run_id)
        if run is None:
            return ActionResult(ok=False, message=message)
        batches = merge_batch_service.list_batches(_scope(request), run_id)
        return ActionResult(ok=True, message=message,
                            entry={"run": run, "resumed_batches": resumed, "batches": batches})
    except DataScopeError as exc:
        return ActionResult(ok=False, message=str(exc))


@router.post("/runs/{run_id}/late-update", response_model=ActionResult)
def late_update(
    request: Request,
    run_id: int,
    payload: dict,
) -> ActionResult:
    """在批次边界补报一个键，确认只进入一个可写批次。"""
    try:
        key = int(payload.get("key", 0))
        policy = str(payload.get("conflict_policy", "keep_owner"))
        batch, message = merge_batch_service.apply_late_update(
            _scope(request), run_id, key, conflict_policy=policy)
        if batch is None:
            return ActionResult(ok=False, message=message)
        return ActionResult(ok=True, message=message, entry=batch)
    except DataScopeError as exc:
        return ActionResult(ok=False, message=str(exc))
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="key 必须是整数")


@router.post("/runs/{run_id}/handoff/{cdc_config_id}", response_model=ActionResult)
def handoff(request: Request, run_id: int, cdc_config_id: int) -> ActionResult:
    """把合并批次结果从增量合并交付给 CDC 捕获，并保留当前版本。"""
    try:
        handoff_record, message = merge_batch_service.handoff_to_cdc(
            _scope(request), run_id, cdc_config_id)
        if handoff_record is None:
            return ActionResult(ok=False, message=message)
        return ActionResult(ok=True, message=message, entry=handoff_record)
    except DataScopeError as exc:
        return ActionResult(ok=False, message=str(exc))


@router.get("/handoffs")
def list_handoffs(request: Request) -> dict:
    """合并批次交接结果（共享内容，只读）。"""
    scope = _scope(request)
    items = merge_batch_service.list_handoffs(scope)
    return {"items": items, "total": len(items), "readonly": True, "scope": scope}


@router.get("/runs/{run_id}/orders")
def list_order_records(request: Request, run_id: int) -> dict:
    """查询合并写入的订单记录及版本（关联记录刷新后保持原记录、仅升版本）。"""
    try:
        scope = _scope(request)
        if merge_batch_service.get_run(scope, run_id) is None:
            return {"items": [], "total": 0, "ok": False, "message": f"合并批次运行 {run_id} 不存在"}
        items = merge_batch_service.list_order_records(scope, run_id)
        return {"items": items, "total": len(items)}
    except DataScopeError as exc:
        return {"items": [], "total": 0, "ok": False, "error": str(exc)}
