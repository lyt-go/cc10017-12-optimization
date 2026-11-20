"""CDC捕获 时间线接口：返回作业时间序列结构。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.change_capture import ChangeCaptureService
from app.services.data_scope import DataScopeError, resolve_scope
from app.services.merge_batch import merge_batch_service

router = APIRouter(prefix="/api/change_capture", tags=["CDC捕获"])
service = ChangeCaptureService()
LIST_FIELDS = ["配置编号", "源数据库", "源表名", "快照模式", "捕获列", "变更序号", "增量阶段", "目标主题"]
STATUSES = ["草稿", "待校验", "运行中", "已停止"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=change_capture.csv"})


@router.get("/handoffs")
def list_handoffs(request: Request) -> dict[str, Any]:
    """CDC 侧只读查看增量合并交付过来的合并批次交接结果。

    必须声明在 /{entry_id} 之前，否则 handoffs 会被当成 entry_id 匹配而校验失败。
    """
    scope = resolve_scope(request)
    items = merge_batch_service.list_handoffs(scope)
    return {"items": items, "total": len(items), "readonly": True, "scope": scope}


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"CDC配置 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="CDC配置已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.post("/{entry_id}/confirm-handoff/{run_id}")
def confirm_handoff(request: Request, entry_id: int, run_id: int) -> ActionResult:
    """CDC 捕获核对本批键范围对应的增量来源（共享内容只读）。"""
    try:
        confirmation, message = service.confirm_merge_handoff(
            resolve_scope(request), entry_id, run_id)
        if confirmation is None:
            return ActionResult(ok=False, message=message)
        return ActionResult(ok=True, message=message, entry=confirmation)
    except DataScopeError as exc:
        return ActionResult(ok=False, message=str(exc))


@router.get("/{entry_id}/handoff-confirmations")
def list_confirmations(request: Request, entry_id: int) -> dict[str, Any]:
    """CDC 对合并批次来源的核对记录。"""
    try:
        items = service.list_handoff_confirmations(resolve_scope(request), entry_id)
        return {"items": items, "total": len(items)}
    except DataScopeError as exc:
        return {"items": [], "total": 0, "ok": False, "error": str(exc)}
