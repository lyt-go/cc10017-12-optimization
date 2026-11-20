"""业务数据归属守卫：每个操作者只能查看/写入本业务归属域的数据。

- 业务归属域按目标表划分（orders / users / payments），请求头 ``X-Data-Scope`` 声明当前归属。
- 跨归属访问本域表直接拒绝（读/写都拒绝）。
- 跨模块共享产物（增量合并 → CDC 捕获的合并批次交接结果）对所有归属只读开放，禁止改写。
"""
from __future__ import annotations

from fastapi import Request

# 归属域 -> 该域负责的业务表
SCOPE_TABLES: dict[str, str] = {
    "orders": "orders",
    "users": "users",
    "payments": "payments",
}

DEFAULT_SCOPE = "orders"

# 跨模块共享产物表：只允许读，任何归属都不能写
SHARED_READONLY_TABLES = {"merge_cdc_handoffs"}


class DataScopeError(PermissionError):
    """越权访问或对只读共享内容发起写入时抛出。"""


def resolve_scope(request: Request) -> str:
    """从请求头解析当前业务归属，缺省落在订单域。"""
    scope = (request.headers.get("X-Data-Scope") or DEFAULT_SCOPE).strip()
    if scope not in SCOPE_TABLES:
        raise DataScopeError(f"未知业务归属「{scope}」，可选：{'、'.join(SCOPE_TABLES)}")
    return scope


def assert_table_scope(scope: str, target_table: str | None, *, write: bool) -> None:
    """校验当前归属能否访问某张业务表。"""
    if not target_table:
        raise DataScopeError("缺少目标表，无法判定业务归属")
    if scope not in SCOPE_TABLES:
        raise DataScopeError(f"未知业务归属「{scope}」")
    if target_table != SCOPE_TABLES[scope]:
        verb = "写入" if write else "查看"
        raise DataScopeError(
            f"当前业务归属为 {scope}，无权{verb} {target_table} 的数据，越权访问已拒绝"
        )


def assert_artifact_scope(scope: str, artifact_table: str, *, write: bool) -> None:
    """校验产物表访问：共享交接结果只读，其余产物跟随业务归属。"""
    if artifact_table in SHARED_READONLY_TABLES:
        if write:
            raise DataScopeError("合并批次交接结果是跨模块共享内容，仅支持只读访问")
        return
    # 非共享产物由调用方按目标表再做归属校验
    if scope not in SCOPE_TABLES:
        raise DataScopeError(f"未知业务归属「{scope}」")
