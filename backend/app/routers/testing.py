"""测试辅助接口：仅在非生产环境重置内存仓库，供端到端验收隔离用例状态。"""
from __future__ import annotations

from fastapi import APIRouter

from app.config import settings
from app.schemas import ActionResult
from app.seed import SEED_ROWS
from app.store import store

router = APIRouter(prefix="/api/testing", tags=["测试辅助"])


@router.post("/reset", response_model=ActionResult)
def reset_store() -> ActionResult:
    """把内存仓库恢复到种子数据；生产环境拒绝执行。"""
    if settings.env != "local":
        return ActionResult(ok=False, message="仅本地测试环境允许重置数据仓库")
    store._tables.clear()
    for name, rows in SEED_ROWS.items():
        store._tables[name] = [dict(row) for row in rows]
    return ActionResult(ok=True, message="数据仓库已重置为种子状态")
