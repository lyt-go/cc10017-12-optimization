"""合并批次结果测试：覆盖分批键索引、中断恢复、迟到更新边界冲突、CDC交接、数据归属。

验收主线（对应需求）：
1. 优化构建合并策略的分批键索引并生成合并批次结果，批次结果保留键范围与待重算集合；
2. 写入中断后只重算未完批次，已发布键范围保持不变，避免重复写入；
3. 批次边界补报一个键时只进入一个可写批次，两种冲突取舍产生不同版本；
4. CDC 捕获接入合并批次结果并核对本批键范围的增量来源；
5. 跨模块交接结果只读，越权访问被拒绝；
6. 关联订单记录刷新后保持原记录（同一条记录升版本而非重复写入）。
"""
from __future__ import annotations

import unittest

from app.pipeline_engine import pipeline_engine
from app.services.change_capture import ChangeCaptureService
from app.services.data_scope import DataScopeError
from app.services.merge_batch import (
    HANDOFF_TABLE,
    ORDER_TABLE,
    MergeBatchService,
)
from app.store import store

SCOPE_ORDERS = "orders"
SCOPE_USERS = "users"
SCOPE_PAYMENTS = "payments"


class MergeBatchTestBase(unittest.TestCase):
    def setUp(self) -> None:
        # 每个用例在全新的内存仓库上运行，避免批次自增 id 互相干扰
        store._tables.clear()
        from app.seed import SEED_ROWS

        for name, rows in SEED_ROWS.items():
            store._tables[name] = [dict(row) for row in rows]
        self.service = MergeBatchService()
        self.cdc_service = ChangeCaptureService()

    def _plan_orders_run(self, key_count: int = 10, batch_size: int = 5,
                         cdc_config_id: int | None = None) -> dict:
        run, message = self.service.plan_run(
            SCOPE_ORDERS, upsert_id=1, key_count=key_count,
            batch_size=batch_size, cdc_config_id=cdc_config_id)
        self.assertIsNotNone(run, message)
        return run

    def _publish_all(self, run_id: int) -> None:
        for batch in self.service.list_batches(SCOPE_ORDERS, run_id):
            result, message = self.service.run_batch(
                SCOPE_ORDERS, run_id, int(batch["batch_no"]))
            self.assertIsNotNone(result, message)
            self.assertEqual(result["status"], "已发布")


class TestMergeBatchPlanning(MergeBatchTestBase):
    """分批键索引与合并批次结果结构。"""

    def test_plan_builds_key_range_index(self) -> None:
        run = self._plan_orders_run(key_count=10, batch_size=5)
        batches = self.service.list_batches(SCOPE_ORDERS, int(run["id"]))
        self.assertEqual(len(batches), 2)
        self.assertEqual([b["key_range"] for b in batches], [
            {"lower_key": 1, "upper_key": 5},
            {"lower_key": 6, "upper_key": 10},
        ])
        # 合并批次结果保留批次键范围与待重算集合
        for batch in batches:
            self.assertIn("recalc_keys", batch)
            self.assertEqual(batch["recalc_keys"], [])
            self.assertEqual(batch["status"], "待执行")
            self.assertEqual(batch["version"], 1)
        # 尾部不足一批时也要成批
        run2 = self._plan_orders_run(key_count=7, batch_size=3)
        self.assertEqual(len(self.service.list_batches(SCOPE_ORDERS, int(run2["id"]))), 3)

    def test_each_batch_only_writes_own_key_range(self) -> None:
        run = self._plan_orders_run(key_count=10, batch_size=5)
        run_id = int(run["id"])
        self.service.run_batch(SCOPE_ORDERS, run_id, 0)
        orders = store.rows(ORDER_TABLE)
        keys = sorted(int(r["merge_key"]) for r in orders)
        self.assertEqual(keys, [1, 2, 3, 4, 5])
        # 第二批还没跑，6-10 的订单记录不应存在
        self.assertEqual(len(orders), 5)
        self.service.run_batch(SCOPE_ORDERS, run_id, 1)
        self.assertEqual(len(store.rows(ORDER_TABLE)), 10)

    def test_invalid_plan_rejected(self) -> None:
        run, _ = self.service.plan_run(
            SCOPE_ORDERS, upsert_id=1, key_count=0, batch_size=5)
        self.assertIsNone(run)


class TestMergeBatchInterruption(MergeBatchTestBase):
    """写入中断与恢复：最后成功位置驱动，只重算未完批次。"""

    def test_interrupt_then_resume_skips_published_batches(self) -> None:
        run = self._plan_orders_run(key_count=10, batch_size=5)
        run_id = int(run["id"])
        # 第一批完整发布
        b0, _ = self.service.run_batch(SCOPE_ORDERS, run_id, 0)
        self.assertEqual(b0["status"], "已发布")
        published_before = list(b0["published_keys"])
        # 第二批处理 2 个键后中断
        b1, message = self.service.run_batch(
            SCOPE_ORDERS, run_id, 1, fail_after_key=2)
        self.assertEqual(b1["status"], "已中断")
        self.assertIn("中断", message)
        self.assertEqual(b1["last_success_key"], 7)
        self.assertEqual(self.service.get_run(SCOPE_ORDERS, run_id)["status"], "已中断")

        # 恢复：只重算未完成批次
        resumed_run, resume_msg, resumed = self.service.resume_run(SCOPE_ORDERS, run_id)
        self.assertEqual(resumed, [1])
        self.assertIn("重算批次", resume_msg)
        # 已发布键范围保持不变，且没有重复写入
        b0_after = self.service.list_batches(SCOPE_ORDERS, run_id)[0]
        self.assertEqual(b0_after["published_keys"], published_before)
        self.assertEqual(b0_after["version"], 1)
        b1_after = self.service.list_batches(SCOPE_ORDERS, run_id)[1]
        self.assertEqual(b1_after["status"], "已发布")
        self.assertEqual(b1_after["last_success_key"], 10)
        self.assertEqual(resumed_run["status"], "已完成")
        # 10 个键仍然只有 10 条订单记录，避免重复写入
        self.assertEqual(len(store.rows(ORDER_TABLE)), 10)

    def test_resume_without_interruption_is_noop(self) -> None:
        run = self._plan_orders_run(key_count=6, batch_size=3)
        run_id = int(run["id"])
        self._publish_all(run_id)
        _, message, resumed = self.service.resume_run(SCOPE_ORDERS, run_id)
        self.assertEqual(resumed, [])
        self.assertIn("无", message)


class TestLateUpdateBoundary(MergeBatchTestBase):
    """迟到更新落在相邻批次边界：两种取舍产生不同版本，但单键单发布者。"""

    def setUp(self) -> None:
        super().setUp()
        self.run = self._plan_orders_run(key_count=10, batch_size=5)
        self.run_id = int(self.run["id"])
        self._publish_all(self.run_id)

    def test_non_boundary_late_update_stays_in_owner_batch(self) -> None:
        batch, message = self.service.apply_late_update(
            SCOPE_ORDERS, self.run_id, 3)  # 批次0内部键
        self.assertIsNotNone(batch, message)
        self.assertEqual(int(batch["batch_no"]), 0)
        self.assertIn(3, batch["recalc_keys"])
        self.assertEqual(batch["version"], 2)
        self.assertIsNone(batch["_conflict"])
        self._assert_one_publisher(3)

    def test_boundary_keep_owner_keeps_recalc_in_original_batch(self) -> None:
        # 键5是批次0的上沿，相邻批次1在右侧
        batch, message = self.service.apply_late_update(
            SCOPE_ORDERS, self.run_id, 5, conflict_policy="keep_owner")
        self.assertIsNotNone(batch, message)
        self.assertEqual(int(batch["batch_no"]), 0)  # 原键所有者
        self.assertIn(5, batch["recalc_keys"])
        self.assertEqual(batch["key_range"], {"lower_key": 1, "upper_key": 5})
        self.assertEqual(batch["version"], 2)
        # 批次1完全不受影响，仍是 v1
        neighbor = self.service.list_batches(SCOPE_ORDERS, self.run_id)[1]
        self.assertEqual(neighbor["version"], 1)
        self._assert_one_publisher(5)

    def test_boundary_latest_batch_reassigns_and_bumps_version(self) -> None:
        batch, message = self.service.apply_late_update(
            SCOPE_ORDERS, self.run_id, 5, conflict_policy="latest_batch")
        self.assertIsNotNone(batch, message)
        # 待重算集合落在新批次（最新批次1）
        self.assertEqual(int(batch["batch_no"]), 1)
        self.assertIn(5, batch["recalc_keys"])
        self.assertEqual(batch["key_range"]["lower_key"], 5)
        self.assertEqual(batch["version"], 2)
        old = self.service.list_batches(SCOPE_ORDERS, self.run_id)[0]
        self.assertEqual(old["key_range"], {"lower_key": 1, "upper_key": 4})
        self.assertNotIn(5, old["published_keys"])
        self.assertEqual(old["version"], 2)  # 让出键的批次也升版
        # 运行整体版本随重新归属升级
        self.assertGreaterEqual(self.service.get_run(SCOPE_ORDERS, self.run_id)["version"], 2)
        # 一个键只能由一个批次发布结果
        self._assert_one_publisher(5)

    def test_two_policies_produce_different_versions(self) -> None:
        """同一冲突点两种取舍，合并批次结果版本归属不同。"""
        keep_run = self._plan_orders_run(key_count=10, batch_size=5)
        keep_id = int(keep_run["id"])
        self._publish_all(keep_id)
        keep_batch, _ = self.service.apply_late_update(
            SCOPE_ORDERS, keep_id, 5, conflict_policy="keep_owner")

        latest_run = self._plan_orders_run(key_count=10, batch_size=5)
        latest_id = int(latest_run["id"])
        self._publish_all(latest_id)
        latest_batch, _ = self.service.apply_late_update(
            SCOPE_ORDERS, latest_id, 5, conflict_policy="latest_batch")

        self.assertNotEqual(int(keep_batch["batch_no"]), int(latest_batch["batch_no"]))
        # keep_owner：键5 仍在范围 [1,5]；latest_batch：键5 已并入 [5,10]
        self.assertEqual(keep_batch["key_range"]["upper_key"], 5)
        self.assertEqual(latest_batch["key_range"]["lower_key"], 5)

    def test_late_update_out_of_range_rejected(self) -> None:
        batch, _ = self.service.apply_late_update(SCOPE_ORDERS, self.run_id, 99)
        self.assertIsNone(batch)

    def _assert_one_publisher(self, key: int) -> None:
        owners = [
            int(b["batch_no"]) for b in self.service.list_batches(SCOPE_ORDERS, self.run_id)
            if key in b.get("published_keys", [])
        ]
        self.assertEqual(len(owners), 1, f"键 {key} 存在多个发布批次 {owners}")


class TestOrderRecordVersioning(MergeBatchTestBase):
    """关联记录刷新后保持原记录：同键 upsert 升版本而非追加。"""

    def test_refresh_keeps_single_record_with_new_version(self) -> None:
        run = self._plan_orders_run(key_count=5, batch_size=5)
        run_id = int(run["id"])
        self._publish_all(run_id)
        before = [r for r in store.rows(ORDER_TABLE) if int(r["merge_key"]) == 3]
        self.assertEqual(len(before), 1)
        self.assertEqual(before[0]["version"], 1)

        self.service.apply_late_update(SCOPE_ORDERS, run_id, 3)
        after = [r for r in store.rows(ORDER_TABLE) if int(r["merge_key"]) == 3]
        self.assertEqual(len(after), 1)  # 仍是同一条记录
        self.assertEqual(after[0]["id"], before[0]["id"])
        self.assertEqual(after[0]["version"], 2)  # 刷新后版本升级


class TestCdcHandoff(MergeBatchTestBase):
    """CDC 捕获接入合并批次结果并核对增量来源。"""

    def test_handoff_delivers_ranges_with_version(self) -> None:
        run = self._plan_orders_run(key_count=10, batch_size=5, cdc_config_id=1)
        run_id = int(run["id"])
        self._publish_all(run_id)
        handoff, message = self.service.handoff_to_cdc(SCOPE_ORDERS, run_id, 1)
        self.assertIsNotNone(handoff, message)
        self.assertEqual(handoff["target_table"], "orders")
        self.assertEqual(handoff["version"], run["version"])
        ranges = handoff["batch_key_ranges"]
        self.assertEqual([(r["lower_key"], r["upper_key"]) for r in ranges], [(1, 5), (6, 10)])
        self.assertTrue(all(r["status"] == "已发布" for r in ranges))

    def test_cdc_confirms_source_for_each_batch_range(self) -> None:
        run = self._plan_orders_run(key_count=10, batch_size=5, cdc_config_id=1)
        run_id = int(run["id"])
        self._publish_all(run_id)
        self.service.handoff_to_cdc(SCOPE_ORDERS, run_id, 1)
        confirmation, message = self.cdc_service.confirm_merge_handoff(
            SCOPE_ORDERS, 1, run_id)
        self.assertIsNotNone(confirmation, message)
        self.assertEqual(confirmation["source_table"], "orders")
        self.assertEqual(len(confirmation["confirmed_ranges"]), 2)
        self.assertTrue(all(r["source_confirmed"] for r in confirmation["confirmed_ranges"]))
        # CDC 侧可重复只读查看
        again = self.cdc_service.list_handoff_confirmations(SCOPE_ORDERS, 1)
        self.assertEqual(len(again), 1)

    def test_handoff_rejects_table_mismatch(self) -> None:
        run = self._plan_orders_run(key_count=5, batch_size=5)
        # CDC 2 对应 users 表，不能接 orders 的合并结果
        handoff, message = self.service.handoff_to_cdc(
            SCOPE_ORDERS, int(run["id"]), 2)
        self.assertIsNone(handoff)
        self.assertIn("不一致", message)

    def test_shared_handoff_is_readonly(self) -> None:
        run = self._plan_orders_run(key_count=5, batch_size=5, cdc_config_id=1)
        run_id = int(run["id"])
        self._publish_all(run_id)
        self.service.handoff_to_cdc(SCOPE_ORDERS, run_id, 1)
        # 任何归属都能读
        self.assertEqual(len(self.service.list_handoffs(SCOPE_USERS)), 1)
        # 直接对共享表写入会被守卫拒绝
        from app.services.data_scope import assert_artifact_scope

        with self.assertRaises(DataScopeError):
            assert_artifact_scope(SCOPE_ORDERS, HANDOFF_TABLE, write=True)


class TestDataScopeGuard(MergeBatchTestBase):
    """只能查看本业务归属的数据，越权访问被拒绝。"""

    def test_cross_scope_plan_denied(self) -> None:
        # users 归属不能操作 orders 的合并策略（id=1 对应 orders）
        with self.assertRaises(DataScopeError):
            self.service.plan_run(SCOPE_USERS, upsert_id=1, key_count=5, batch_size=5)

    def test_cross_scope_read_denied(self) -> None:
        run = self._plan_orders_run(key_count=5, batch_size=5)
        with self.assertRaises(DataScopeError):
            self.service.get_run(SCOPE_PAYMENTS, int(run["id"]))

    def test_list_runs_filters_by_scope(self) -> None:
        self._plan_orders_run(key_count=5, batch_size=5)
        orders_runs = self.service.list_runs(SCOPE_ORDERS)
        users_runs = self.service.list_runs(SCOPE_USERS)
        self.assertEqual(len(orders_runs), 1)
        self.assertEqual(users_runs, [])

    def test_unknown_scope_rejected(self) -> None:
        with self.assertRaises(DataScopeError):
            self.service.plan_run("unknown-scope", upsert_id=1, key_count=5, batch_size=5)


class TestPipelineIntegration(MergeBatchTestBase):
    """管道引擎：CDC→增量合并链路接入合并批次结果。"""

    def test_cdc_upsert_simulation_produces_batches(self) -> None:
        result = pipeline_engine.run_cdc_upsert_simulation(1)
        self.assertTrue(result["ok"])
        self.assertEqual(result["batch_count"], 3)  # 9 个键 / 每批 3 个
        self.assertEqual(result["batch_key_ranges"], [[1, 3], [4, 6], [7, 9]])
        # 交接结果已交付 CDC
        handoffs = store.rows(HANDOFF_TABLE)
        self.assertEqual(len(handoffs), 1)
        self.assertEqual(handoffs[0]["cdc_config_id"], 1)
        # 旧产物结构保持兼容
        self.assertEqual(result["merged_count"], 9)
        self.assertGreaterEqual(len(store.rows("upsert_merge_artifacts")), 9)
        # 引擎可继续完成 CDC 来源核对
        confirmation, _ = self.cdc_service.confirm_merge_handoff(
            SCOPE_ORDERS, 1, result["run_id"])
        self.assertIsNotNone(confirmation)


if __name__ == "__main__":
    unittest.main()
