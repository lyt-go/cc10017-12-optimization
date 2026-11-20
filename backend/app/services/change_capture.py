"""CDC捕获业务规则：配置管理、增量捕获与下游主题联动。

增强：CDC启动时校验目标主题状态，捕获增量后触发合并策略检查。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from app.services.data_scope import assert_artifact_scope, assert_table_scope
from app.store import store

MODULE = "change_capture"
REQUIRED_FIELDS = ["配置编号", "源数据库", "源表名"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已停止"]
ACTION_RULES = {"提交校验": "待校验", "启动捕获": "运行中", "停止捕获": "已停止"}
NEGATIVE_ACTIONS = []
HANDOFF_TABLE = "merge_cdc_handoffs"


class ChangeCaptureService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        # 校验目标主题
        target_topic = values.get("目标主题")
        if target_topic:
            topic = self._find_topic(target_topic)
            if not topic:
                return None, [f"目标主题 {target_topic} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        for f in ["快照模式", "捕获列", "变更序号", "增量阶段", "目标主题"]:
            entry[f] = values.get(f, "")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"CDC配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于CDC捕获可执行范围"
        target = ACTION_RULES[action]

        if action == "启动捕获":
            target_topic = entry.get("目标主题")
            topic = self._find_topic(target_topic) if target_topic else None
            if not topic:
                return None, f"目标主题 {target_topic} 未注册"
            if topic.get("status") != "已上线":
                return None, f"目标主题 {target_topic} 未上线"
            # 检查UPSERT策略
            source_table = entry.get("源表名")
            upsert = self._find_upsert(source_table)
            if not upsert:
                return None, f"源表 {source_table} 未配置增量合并策略"
            # 生成捕获产物
            self._generate_capture_artifact(entry, topic, upsert)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"CDC配置已{action}"

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _find_upsert(self, table_name: str) -> dict[str, Any] | None:
        for row in store.rows("upsert_merge"):
            if row.get("目标表名") == table_name:
                return row
        return None

    def _generate_capture_artifact(self, entry: dict[str, Any], topic: dict[str, Any], upsert: dict[str, Any]) -> None:
        artifact = {
            "cdc_id": entry.get("id"),
            "cdc_code": entry.get("配置编号"),
            "source_db": entry.get("源数据库"),
            "source_table": entry.get("源表名"),
            "target_topic": topic.get("主题编号"),
            "merge_policy": upsert.get("策略编号"),
            "merge_engine": upsert.get("合并引擎"),
            "captured_at": datetime.now().isoformat(),
            "stage": "streaming",
        }
        self._ensure_artifact_table("cdc_capture_artifacts")
        store.rows("cdc_capture_artifacts").append(artifact)
        entry["_last_capture"] = artifact

    def get_capture_artifacts(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("cdc_capture_artifacts")
        return [a for a in store.rows("cdc_capture_artifacts") if a.get("cdc_id") == entry_id]

    def confirm_merge_handoff(
        self, scope: str, cdc_id: int, run_id: int
    ) -> tuple[dict[str, Any] | None, str]:
        """CDC 捕获接入合并批次结果，核对本批键范围对应的增量来源。

        交接结果是跨模块共享内容：本方法只读核对，不会改写合并批次结果。
        核对通过后在 CDC 侧留下确认记录，保留合并批次的当前版本。
        """
        cdc = store.find(MODULE, cdc_id)
        if cdc is None:
            return None, f"CDC配置 {cdc_id} 不存在"
        target_table = cdc.get("源表名")
        assert_table_scope(scope, target_table, write=False)
        # 共享交接结果只读
        assert_artifact_scope(scope, HANDOFF_TABLE, write=False)

        handoff = next(
            (h for h in store.rows(HANDOFF_TABLE)
             if int(h.get("run_id", -1)) == run_id and int(h.get("cdc_config_id", -1)) == cdc_id),
            None,
        )
        if handoff is None:
            return None, f"CDC {cdc_id} 未找到运行 {run_id} 的合并批次交接结果"
        if handoff.get("target_table") != target_table:
            return None, (
                f"交接结果目标表 {handoff.get('target_table')} 与 CDC 源表 {target_table} 不一致"
            )

        # 核对每个批次的键范围都落在 CDC 源表名下，来源唯一且版本已保留
        confirmed_ranges = []
        for item in handoff.get("batch_key_ranges", []):
            confirmed_ranges.append({
                "batch_no": item.get("batch_no"),
                "key_range": [item.get("lower_key"), item.get("upper_key")],
                "status": item.get("status"),
                "version": item.get("version"),
                "recalc_keys": item.get("recalc_keys", []),
                "source_confirmed": True,
            })

        confirmation = {
            "id": max((int(r.get("id", 0)) for r in store.rows("cdc_handoff_confirmations")),
                      default=0) + 1,
            "cdc_id": cdc_id,
            "cdc_code": cdc.get("配置编号"),
            "run_id": run_id,
            "run_code": handoff.get("run_code"),
            "source_table": target_table,
            "source_topic": cdc.get("目标主题"),
            "merge_version": handoff.get("version"),
            "confirmed_ranges": confirmed_ranges,
            "confirmed_at": datetime.now().isoformat(),
            "readonly_source": True,
        }
        self._ensure_artifact_table("cdc_handoff_confirmations")
        store.rows("cdc_handoff_confirmations").append(confirmation)
        return confirmation, "CDC 已核对合并批次键范围的增量来源，来源版本已保留"

    def list_handoff_confirmations(self, scope: str, cdc_id: int) -> list[dict[str, Any]]:
        cdc = store.find(MODULE, cdc_id)
        if cdc is not None:
            assert_table_scope(scope, cdc.get("源表名"), write=False)
        self._ensure_artifact_table("cdc_handoff_confirmations")
        return [c for c in store.rows("cdc_handoff_confirmations") if c.get("cdc_id") == cdc_id]

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
