"""合并批次服务：按分批键索引切分键范围，逐批生成合并批次结果。

设计要点（对应大批量订单合并的优化诉求）：

1. 分批键索引：构建合并策略时按合并键区间切批，每批只负责自己范围内的键，
   不再反复扫描整表，内存占用只与单批大小相关。
2. 合并批次结果：每个批次保留批次键范围（lower_key/upper_key）与待重算集合
   recalc_keys，迟到更新只重算命中的批次，不触发整表重算。
3. 中断恢复：批次只允许修改属于本批次键范围的订单记录，最后成功位置
   last_success_key 随批次持久化；恢复时跳过已发布批次，只重算未完成批次，
   已发布键范围保持不变，避免重复写入。
4. 边界冲突：同一业务键的迟到更新落在相邻批次边界上时，keep_owner 保留原
   发布批次、latest_batch 按最新批次重新归属；冲突结果会让合并批次结果产生
   不同版本（version 递增），但一个键始终只能由一个批次发布结果。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.data_scope import assert_table_scope
from app.store import store

MODULE = "upsert_merge"
RUNS_TABLE = "merge_batch_runs"
BATCH_TABLE = "merge_batch_results"
ORDER_TABLE = "merge_order_records"
HANDOFF_TABLE = "merge_cdc_handoffs"

BATCH_STATUSES = ["待执行", "运行中", "已中断", "已发布"]
RUN_STATUSES = ["待运行", "运行中", "已中断", "已完成"]
CONFLICT_POLICIES = ["keep_owner", "latest_batch"]


def _now() -> str:
    return datetime.now().isoformat()


class MergeBatchService:
    """合并批次的规划、执行、恢复、迟到更新与 CDC 交接。"""

    # ───────────────────────────────
    # 查询
    # ───────────────────────────────

    def list_runs(self, scope: str, *, target_table: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(RUNS_TABLE)
        if target_table:
            assert_table_scope(scope, target_table, write=False)
            rows = [r for r in rows if r.get("target_table") == target_table]
        else:
            # 无显式表名时按归属过滤，保证只能查看本业务归属的数据
            from app.services.data_scope import SCOPE_TABLES

            owned = SCOPE_TABLES.get(scope, "")
            rows = [r for r in rows if r.get("target_table") == owned]
        return rows

    def get_run(self, scope: str, run_id: int) -> dict[str, Any] | None:
        run = self._find_run(run_id)
        if run is not None:
            assert_table_scope(scope, run.get("target_table"), write=False)
        return run

    def list_batches(self, scope: str, run_id: int) -> list[dict[str, Any]]:
        run = self.get_run(scope, run_id)
        if run is None:
            return []
        return [b for b in store.rows(BATCH_TABLE) if b.get("run_id") == run_id]

    # ───────────────────────────────
    # 1. 构建分批键索引并生成合并批次结果
    # ───────────────────────────────

    def plan_run(
        self,
        scope: str,
        *,
        upsert_id: int,
        key_count: int,
        batch_size: int,
        cdc_config_id: int | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """按合并策略构建分批键索引，为每个键范围生成一条待执行批次结果。"""
        policy = store.find(MODULE, upsert_id)
        if policy is None:
            return None, f"合并策略 {upsert_id} 不存在或已归档"
        target_table = policy.get("目标表名")
        assert_table_scope(scope, target_table, write=True)
        if key_count <= 0 or batch_size <= 0:
            return None, "键总数与每批大小必须为正整数"

        merge_key = policy.get("合并键列", "id")
        runs = store.rows(RUNS_TABLE)
        run_id = max((int(r.get("id", 0)) for r in runs), default=0) + 1
        run_code = f"MBR-{target_table}-{run_id:04d}"
        batch_rows = store.rows(BATCH_TABLE)
        next_batch_id = max((int(b.get("id", 0)) for b in batch_rows), default=0) + 1

        planned_batches: list[dict[str, Any]] = []
        lower = 1
        batch_index = 0
        while lower <= key_count:
            upper = min(lower + batch_size - 1, key_count)
            batch = {
                "id": next_batch_id + batch_index,
                "run_id": run_id,
                "batch_no": batch_index,
                "merge_key_column": merge_key,
                "key_range": {"lower_key": lower, "upper_key": upper},
                "recalc_keys": [],
                "conflict_keys": [],
                "status": "待执行",
                "version": 1,
                "last_success_key": None,
                "published_keys": [],
                "cdc_config_id": cdc_config_id,
                "updated_at": _now(),
            }
            planned_batches.append(batch)
            lower = upper + 1
            batch_index += 1
        batch_rows.extend(planned_batches)

        run = {
            "id": run_id,
            "run_code": run_code,
            "upsert_id": upsert_id,
            "target_table": target_table,
            "merge_key_column": merge_key,
            "key_count": key_count,
            "batch_size": batch_size,
            "status": "待运行",
            "version": 1,
            "cdc_config_id": cdc_config_id,
            "created_at": _now(),
            "updated_at": _now(),
        }
        runs.append(run)
        return run, f"已生成 {len(planned_batches)} 个合并批次"

    # ───────────────────────────────
    # 2. 分批运行：每批只处理自己负责的键
    # ───────────────────────────────

    def run_batch(
        self,
        scope: str,
        run_id: int,
        batch_no: int,
        *,
        fail_after_key: int | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """执行单个合并批次；fail_after_key 用于模拟写入中断。"""
        run = self._find_run(run_id)
        if run is None:
            return None, f"合并批次运行 {run_id} 不存在"
        assert_table_scope(scope, run.get("target_table"), write=True)
        batch = self._find_batch(run_id, batch_no)
        if batch is None:
            return None, f"批次 {batch_no} 不存在"
        if batch["status"] == "已发布":
            # 已发布键范围不允许重复写入
            return batch, "批次已发布，跳过重复写入"
        if batch["status"] == "运行中":
            return None, f"批次 {batch_no} 正在运行中"

        lower = int(batch["key_range"]["lower_key"])
        upper = int(batch["key_range"]["upper_key"])
        start = lower if batch["last_success_key"] is None else int(batch["last_success_key"]) + 1

        batch["status"] = "运行中"
        run["status"] = "运行中"
        batch["updated_at"] = _now()
        run["updated_at"] = _now()

        # 待处理键 = 尚未写入的区间尾段 + 待重算集合（幂等 upsert，重复执行不产生重复行）
        pending = list(range(start, upper + 1))
        for key in batch.get("recalc_keys", []):
            if lower <= int(key) <= upper and int(key) not in pending:
                pending.append(int(key))

        interrupted = False
        processed = 0
        for key in pending:
            if fail_after_key is not None and processed >= fail_after_key:
                interrupted = True
                break
            # 硬性约束：该批次只能修改属于本批次键范围的订单记录
            if not (lower <= key <= upper):
                return None, f"键 {key} 不属于批次 {batch_no} 的键范围 [{lower},{upper}]"
            self._upsert_order(batch, key, run)
            batch["last_success_key"] = max(batch["last_success_key"] or 0, key)
            if key not in batch["published_keys"]:
                batch["published_keys"].append(key)
            processed += 1

        if interrupted:
            batch["status"] = "已中断"
            run["status"] = "已中断"
            batch["updated_at"] = _now()
            run["updated_at"] = _now()
            return batch, (
                f"批次 {batch_no} 在键 {batch['last_success_key']} 后写入中断，"
                "已保留最后成功位置与已发布键范围"
            )

        batch["status"] = "已发布"
        batch["updated_at"] = _now()
        self._refresh_run_status(run)
        return batch, f"批次 {batch_no} 已发布，键范围 [{lower},{upper}]"

    # ───────────────────────────────
    # 3. 中断恢复：只重算未完成批次，保留已发布键范围
    # ───────────────────────────────

    def resume_run(self, scope: str, run_id: int) -> tuple[dict[str, Any] | None, str, list[int]]:
        """恢复合并：跳过已发布批次，从每个未完成批次的最后成功位置继续。"""
        run = self._find_run(run_id)
        if run is None:
            return None, f"合并批次运行 {run_id} 不存在", []
        assert_table_scope(scope, run.get("target_table"), write=True)

        resumed: list[int] = []
        for batch in self._batches_of(run_id):
            if batch["status"] == "已发布":
                continue  # 已发布键范围保持不变，避免重复写入
            _, message = self.run_batch(scope, run_id, int(batch["batch_no"]))
            resumed.append(int(batch["batch_no"]))
            if batch["status"] == "已中断":
                return run, f"恢复仍在批次 {batch['batch_no']} 中断：{message}", resumed
        self._refresh_run_status(run)
        return run, f"恢复完成，重算批次：{resumed or '无（全部已发布）'}", resumed

    # ───────────────────────────────
    # 4. 迟到更新：落在批次边界上的键只进入一个可写批次
    # ───────────────────────────────

    def apply_late_update(
        self,
        scope: str,
        run_id: int,
        key: int,
        *,
        conflict_policy: str = "keep_owner",
    ) -> tuple[dict[str, Any] | None, str]:
        """迟到更新：按批次边界限制重算键的所有权范围。

        同一业务键落在两个相邻批次的边界上时：
        - keep_owner：保留原键所有者，待重算集合留在原发布批次；
        - latest_batch：按最新批次重新归属，移动批次边界并由新批次升版发布。
        无论哪种取舍，一个键最终只能由一个批次发布结果。
        """
        run = self._find_run(run_id)
        if run is None:
            return None, f"合并批次运行 {run_id} 不存在"
        assert_table_scope(scope, run.get("target_table"), write=True)
        if conflict_policy not in CONFLICT_POLICIES:
            return None, f"冲突策略仅支持：{'、'.join(CONFLICT_POLICIES)}"

        boundary = self._find_batch_for_key(run_id, key)
        if boundary is None:
            return None, f"键 {key} 超出本次合并的键范围"
        owner = self._published_owner(run_id, key)
        neighbor = self._adjacent_batch(run_id, int(boundary["batch_no"]), key)
        is_boundary = neighbor is not None and key in (
            int(boundary["key_range"]["lower_key"]),
            int(boundary["key_range"]["upper_key"]),
        )

        conflict: dict[str, Any] | None = None
        # 边界键且存在可接管的相邻批次时，两种策略才会产生不同取舍
        if is_boundary and neighbor is not None:
            conflict = {
                "key": key,
                "boundary_batch_no": int(boundary["batch_no"]),
                "neighbor_batch_no": int(neighbor["batch_no"]),
                "owner_batch_no": int(owner["batch_no"]) if owner else None,
                "policy": conflict_policy,
            }
            if conflict_policy == "keep_owner":
                # 保留原键所有者：边界不动，待重算集合留在原发布批次
                target = owner or boundary
                conflict["resolved_to"] = int(target["batch_no"])
            else:
                # 按最新批次重新归属：把边界键并入相邻最新批次
                target = neighbor
                conflict["resolved_to"] = int(neighbor["batch_no"])
                self._reassign_boundary_key(boundary, neighbor, key)
                boundary["version"] += 1  # 让出键的批次升版
                boundary["updated_at"] = _now()
                neighbor["updated_at"] = _now()
                run["version"] += 1
        else:
            target = owner or boundary

        # 已发布批次收到迟到更新后回到待执行，重算发布后仍是唯一发布者
        if target["status"] == "已发布":
            target["status"] = "待执行"
        self._add_once(target["recalc_keys"], key)
        target["version"] += 1  # 重算即产生合并批次结果新版本
        target["updated_at"] = _now()
        if conflict is not None:
            self._add_once(target["conflict_keys"], key)
            run.setdefault("conflicts", []).append(conflict)

        result, message = self.run_batch(scope, run_id, int(target["batch_no"]))
        if result is None:
            return None, message
        result["_conflict"] = conflict
        self._assert_single_publisher(run_id, key)
        return result, (
            f"迟到更新键 {key} 已由批次 {target['batch_no']} 重算并发布"
            + ("（边界冲突，版本已升级）" if conflict else "")
        )

    def _reassign_boundary_key(
        self, boundary: dict[str, Any], neighbor: dict[str, Any], key: int
    ) -> None:
        """移动相邻批次的键范围边界，让最新批次成为该键的唯一所有者。"""
        b_upper = int(boundary["key_range"]["upper_key"])
        b_lower = int(boundary["key_range"]["lower_key"])
        n_upper = int(neighbor["key_range"]["upper_key"])
        n_lower = int(neighbor["key_range"]["lower_key"])

        # 邻居在右侧：边界键从当前批次上沿移到邻居下沿
        if n_lower == b_upper + 1 and key == b_upper:
            boundary["key_range"]["upper_key"] = b_upper - 1
            neighbor["key_range"]["lower_key"] = key
        # 邻居在左侧：边界键从当前批次下沿移到邻居上沿
        elif n_upper == b_lower - 1 and key == b_lower:
            boundary["key_range"]["lower_key"] = b_lower + 1
            neighbor["key_range"]["upper_key"] = key
        else:
            return

        boundary["published_keys"] = [k for k in boundary["published_keys"] if k != key]
        boundary["recalc_keys"] = [k for k in boundary["recalc_keys"] if k != key]
        self._add_once(boundary["conflict_keys"], key)
        self._add_once(neighbor["published_keys"], key)
        self._add_once(neighbor["conflict_keys"], key)
        # last_success_key 不能落在已让出的键上
        if boundary["last_success_key"] == key:
            boundary["last_success_key"] = (
                int(boundary["key_range"]["upper_key"])
                if int(boundary["key_range"]["upper_key"]) >= int(boundary["key_range"]["lower_key"])
                else None
            )

    # ───────────────────────────────
    # 5. CDC 交接：把合并批次结果交付给 CDC 捕获并保留当前版本
    # ───────────────────────────────

    def handoff_to_cdc(
        self, scope: str, run_id: int, cdc_config_id: int
    ) -> tuple[dict[str, Any] | None, str]:
        """跨模块交付：增量合并 → CDC 捕获，交接结果保留当前版本与键范围。"""
        run = self._find_run(run_id)
        if run is None:
            return None, f"合并批次运行 {run_id} 不存在"
        assert_table_scope(scope, run.get("target_table"), write=True)
        cdc = store.find("change_capture", cdc_config_id)
        if cdc is None:
            return None, f"CDC配置 {cdc_config_id} 不存在"
        if cdc.get("源表名") != run.get("target_table"):
            return None, (
                f"CDC配置 {cdc_config_id} 的源表 {cdc.get('源表名')} 与合并目标表 "
                f"{run.get('target_table')} 不一致，无法核对增量来源"
            )

        batches = self._batches_of(run_id)
        handoff = {
            "id": max((int(h.get("id", 0)) for h in store.rows(HANDOFF_TABLE)), default=0) + 1,
            "run_id": run_id,
            "run_code": run["run_code"],
            "version": run["version"],
            "target_table": run["target_table"],
            "merge_key_column": run["merge_key_column"],
            "cdc_config_id": cdc_config_id,
            "batch_key_ranges": [
                {
                    "batch_no": int(b["batch_no"]),
                    "lower_key": int(b["key_range"]["lower_key"]),
                    "upper_key": int(b["key_range"]["upper_key"]),
                    "status": b["status"],
                    "version": b["version"],
                    "recalc_keys": list(b["recalc_keys"]),
                }
                for b in batches
            ],
            "handoff_at": _now(),
        }
        store.rows(HANDOFF_TABLE).append(handoff)
        run["cdc_config_id"] = cdc_config_id
        run["updated_at"] = _now()
        return handoff, "合并批次结果已交付 CDC 捕获，当前版本已保留"

    def list_handoffs(self, scope: str) -> list[dict[str, Any]]:
        """共享交接结果只读开放，任何归属都可以查看但不能改写。"""
        return [dict(h) for h in store.rows(HANDOFF_TABLE)]

    def list_order_records(self, scope: str, run_id: int) -> list[dict[str, Any]]:
        """查询某次合并写入的订单记录：同键始终只有一条，刷新只升版本。"""
        run = self.get_run(scope, run_id)
        if run is None:
            return []
        return sorted(
            (dict(r) for r in store.rows(ORDER_TABLE) if r.get("run_id") == run_id),
            key=lambda r: int(r["merge_key"]),
        )

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def _upsert_order(self, batch: dict[str, Any], key: int, run: dict[str, Any]) -> None:
        """写入订单记录；只有键所有者批次能写，重复位置覆盖而非追加。"""
        orders = store.rows(ORDER_TABLE)
        target_table = run["target_table"]
        existing = next(
            (r for r in orders if r.get("target_table") == target_table and int(r.get("merge_key", -1)) == key),
            None,
        )
        payload = {
            "target_table": target_table,
            "merge_key": key,
            "owner_batch_id": batch["id"],
            "run_id": run["id"],
            "version": int(existing["version"]) + 1 if existing else 1,
            "updated_at": _now(),
        }
        if existing is None:
            payload["id"] = max((int(r.get("id", 0)) for r in orders), default=0) + 1
            orders.append(payload)
        else:
            # 只有键所有者批次能写：若原发布批次仍持有该键，禁止跨批次覆盖
            if int(existing.get("owner_batch_id", -1)) != batch["id"]:
                old_owner = next(
                    (b for b in self._batches_of(int(run["id"]))
                     if int(b.get("id", -1)) == int(existing.get("owner_batch_id", -1))),
                    None,
                )
                if old_owner is not None and key in old_owner.get("published_keys", []):
                    raise PermissionError(f"键 {key} 仍由批次 {old_owner['batch_no']} 发布，禁止跨批次写入")
            existing.update(payload)

    def _find_run(self, run_id: int) -> dict[str, Any] | None:
        return next((r for r in store.rows(RUNS_TABLE) if int(r.get("id", 0)) == run_id), None)

    def _batches_of(self, run_id: int) -> list[dict[str, Any]]:
        return sorted(
            (b for b in store.rows(BATCH_TABLE) if b.get("run_id") == run_id),
            key=lambda b: int(b["batch_no"]),
        )

    def _find_batch(self, run_id: int, batch_no: int) -> dict[str, Any] | None:
        return next(
            (b for b in store.rows(BATCH_TABLE)
             if b.get("run_id") == run_id and int(b.get("batch_no", -1)) == batch_no),
            None,
        )

    def _find_batch_for_key(self, run_id: int, key: int) -> dict[str, Any] | None:
        for batch in self._batches_of(run_id):
            lower = int(batch["key_range"]["lower_key"])
            upper = int(batch["key_range"]["upper_key"])
            if lower <= key <= upper:
                return batch
        return None

    def _published_owner(self, run_id: int, key: int) -> dict[str, Any] | None:
        for batch in self._batches_of(run_id):
            if key in batch.get("published_keys", []):
                return batch
        return None

    def _adjacent_batch(self, run_id: int, batch_no: int, key: int) -> dict[str, Any] | None:
        batches = self._batches_of(run_id)
        idx = next((i for i, b in enumerate(batches) if int(b["batch_no"]) == batch_no), -1)
        if idx < 0:
            return None
        cur = batches[idx]
        if key == int(cur["key_range"]["upper_key"]) and idx + 1 < len(batches):
            return batches[idx + 1]
        if key == int(cur["key_range"]["lower_key"]) and idx - 1 >= 0:
            return batches[idx - 1]
        return None

    def _refresh_run_status(self, run: dict[str, Any]) -> None:
        statuses = [b["status"] for b in self._batches_of(int(run["id"]))]
        if statuses and all(s == "已发布" for s in statuses):
            run["status"] = "已完成"
        elif any(s == "已中断" for s in statuses):
            run["status"] = "已中断"
        elif any(s in ("运行中", "待执行") for s in statuses):
            run["status"] = "运行中"
        run["updated_at"] = _now()

    def _assert_single_publisher(self, run_id: int, key: int) -> None:
        owners = [
            int(b["batch_no"])
            for b in self._batches_of(run_id)
            if key in b.get("published_keys", [])
        ]
        if len(owners) > 1:
            raise RuntimeError(f"键 {key} 同时被批次 {owners} 发布，违反单发布者约束")

    @staticmethod
    def _add_once(values: list[int], key: int) -> None:
        if key not in values:
            values.append(key)


merge_batch_service = MergeBatchService()
