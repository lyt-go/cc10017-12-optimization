<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">实时数据入湖平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向流式数据接入、实时转换、增量合并与湖上物化查询的实时数据入湖治理平台。</span>
        <span class="head-user">
          <label class="scope-switch">
            业务归属：
            <select
              :value="store.scope"
              data-testid="scope-switch"
              @change="onScopeChange(($event.target as HTMLSelectElement).value)"
            >
              <option v-for="item in scopeOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
            </select>
          </label>
          当前值班：{{ store.operator }} · {{ store.shiftLabel }}
        </span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
import { useRouter } from 'vue-router'

import { useSessionStore, DATA_SCOPES } from '@/stores/session'

const store = useSessionStore()
const router = useRouter()
const scopeOptions = DATA_SCOPES

function onScopeChange(scope: string) {
  store.switchScope(scope)
  // 归属切换后回到概览，避免旧归属页面继续展示无权查看的数据
  void router.push('/')
}

const navItems = [{ label: "运营概览", path: "/" }, { label: "主题注册", path: "/topic_registry" }, { label: "流式计算", path: "/flink_job" }, { label: "消息接入", path: "/kafka_connector" }, { label: "Schema演进", path: "/schema_evolve" }, { label: "增量合并", path: "/upsert_merge" }, { label: "合并批次", path: "/merge_batch" }, { label: "湖上分区", path: "/lake_partition" }, { label: "流式质量", path: "/stream_quality" }, { label: "Iceberg管理", path: "/iceberg_table" }, { label: "水位追踪", path: "/watermark_track" }, { label: "死信处理", path: "/dead_letter" }, { label: "物化查询", path: "/materialized_view" }, { label: "CDC捕获", path: "/change_capture" }, { label: "合并清理", path: "/compact_policy" }, { label: "流式血缘", path: "/stream_lineage" }, { label: "流式补数", path: "/backfill_stream" }, { label: "下沉分发", path: "/sink_dispatch" }, { label: "数据生命周期", path: "/ttl_policy" }, { label: "实时指标", path: "/realtime_metrics" }]
</script>

<style scoped>
.scope-switch { font-size: 13px; margin-right: 12px; }
.scope-switch select { padding: 2px 6px; border-radius: 6px; border: 1px solid var(--border); }
</style>
