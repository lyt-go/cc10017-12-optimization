# 实时数据入湖平台

面向流式数据接入、实时转换、增量合并与湖上物化查询的实时数据入湖治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 主题注册 | `topic_registry` | 数据主题 | 主题编号、主题名称、消息格式 |
| 流式计算 | `flink_job` | 流式任务 | 任务编号、消费主题、转换逻辑 |
| 消息接入 | `kafka_connector` | 接入配置 | 配置编号、集群地址、消费组名 |
| Schema演进 | `schema_evolve` | 演进规则 | 规则编号、关联主题、新增字段 |
| 增量合并 | `upsert_merge` | 合并策略 | 策略编号、目标表名、合并键列 |
| 合并批次 | `merge_batch` | 合并批次结果 | 批次键范围、待重算集合、批次版本 |
| 湖上分区 | `lake_partition` | 分区定义 | 分区编号、目标表名、分区列名 |
| 流式质量 | `stream_quality` | 质量规则 | 规则编号、监控主题、校验类型 |
| Iceberg管理 | `iceberg_table` | 表元数据 | 表编号、表名称、存储格式 |
| 水位追踪 | `watermark_track` | 水位标记 | 标记编号、数据主题、分区编号 |
| 死信处理 | `dead_letter` | 死信记录 | 记录编号、原始主题、原始分区 |
| 物化查询 | `materialized_view` | 物化视图 | 视图编号、查询SQL、基表名 |
| CDC捕获 | `change_capture` | CDC配置 | 配置编号、源数据库、源表名 |
| 合并清理 | `compact_policy` | 清理策略 | 策略编号、目标表名、触发条件 |
| 流式血缘 | `stream_lineage` | 血缘记录 | 记录编号、上游主题、下游表名 |
| 流式补数 | `backfill_stream` | 补数任务 | 任务编号、目标表名、起始偏移 |
| 下沉分发 | `sink_dispatch` | 分发规则 | 规则编号、湖表名、目标系统 |
| 数据生命周期 | `ttl_policy` | 生命周期 | 策略编号、目标表名、热数据保留 |
| 实时指标 | `realtime_metrics` | 指标定义 | 指标编号、指标名称、聚合窗口 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 合并批次结果（订单表大批量合并优化）

针对大批量变更反复扫描整表、内存占用上升、迟到更新触发整表重算的问题，合并批次链路
（`backend/app/services/merge_batch.py`，前端「合并批次」页）按分批键索引组织合并：

1. **分批键索引**：构建合并策略时按合并键区间切批，每批只处理自己键范围内的订单记录，
   内存占用只与单批大小相关；合并批次结果保留 `key_range`（批次键范围）与
   `recalc_keys`（待重算集合）。
2. **中断恢复**：批次只允许修改属于本批键范围的记录，`last_success_key` 记录最后成功位置、
   `published_keys` 保留已发布键范围；恢复时跳过已发布批次，只重算未完批次，避免重复写入。
3. **迟到更新与边界冲突**：批次边界补报的键只进入一个可写批次。`keep_owner` 保留原键
   所有者、`latest_batch` 按最新批次移动边界重新归属；两种取舍让待重算集合落在不同批次、
   产生不同版本（version 递增），但一个键始终只有一个批次发布结果。
4. **CDC 交接**：合并批次结果经 `/api/merge_batch/runs/{id}/handoff/{cdc_id}` 交付 CDC
   捕获并保留当前版本，CDC 侧通过 `confirm-handoff` 只读核对每批键范围的增量来源。

### 数据归属与只读共享

- 请求头 `X-Data-Scope`（orders/users/payments，前端顶栏切换，localStorage 持久化）声明
  业务归属；守卫在 `app/services/data_scope.py`，越权读/写直接拒绝。
- 跨模块交接结果 `merge_cdc_handoffs` 对所有归属只读开放，任何写入都被拒绝。
- 本地端到端用例通过 `POST /api/testing/reset` 把内存仓库恢复到种子状态（仅 `env=local`）。

### 主要接口

| 接口 | 说明 |
| --- | --- |
| `POST /api/merge_batch/plan` | 构建分批键索引并生成合并批次结果 |
| `POST /api/merge_batch/runs/{id}/batches/{n}/run` | 运行单个批次（`fail_after_key` 可模拟中断） |
| `POST /api/merge_batch/runs/{id}/resume` | 中断恢复，只重算未完批次 |
| `POST /api/merge_batch/runs/{id}/late-update` | 迟到更新，`conflict_policy` 决定边界冲突取舍 |
| `POST /api/merge_batch/runs/{id}/handoff/{cdc_id}` | 交付 CDC 并保留当前版本 |
| `GET /api/change_capture/handoffs` | CDC 侧只读查看交接结果 |
| `POST /api/change_capture/{id}/confirm-handoff/{run_id}` | CDC 核对键范围增量来源 |

领域：数据工程
子方向：数仓与数据湖
