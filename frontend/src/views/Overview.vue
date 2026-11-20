<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title" data-testid="artifact-title">数据产物（含合并批次结果）</h3>
    <table class="data-table" data-testid="artifact-table">
      <thead><tr><th>产物</th><th>数量</th></tr></thead>
      <tbody>
        <tr v-for="(count, name) in artifacts" :key="name" :data-testid="`artifact-${name}`">
          <td>{{ artifactLabels[name] || name }}</td>
          <td>{{ count }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
  artifacts: Record<string, number>
}

const artifactLabels: Record<string, string> = {
  merge_batch_runs: '合并批次运行',
  merge_batch_results: '合并批次结果',
  merge_order_records: '合并订单记录',
  merge_cdc_handoffs: 'CDC交接结果（只读）',
  cdc_handoff_confirmations: 'CDC来源核对记录',
  upsert_merge_artifacts: '增量合并产物',
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])
const artifacts = ref<Record<string, number>>({})

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
    artifacts.value = payload.artifacts ?? {}
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "主题注册", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式计算", "created": 0, "pending": 0, "abnormal": 0}, {"name": "消息接入", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Schema演进", "created": 0, "pending": 0, "abnormal": 0}, {"name": "增量合并", "created": 0, "pending": 0, "abnormal": 0}, {"name": "湖上分区", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式质量", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Iceberg管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "水位追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "死信处理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "物化查询", "created": 0, "pending": 0, "abnormal": 0}, {"name": "CDC捕获", "created": 0, "pending": 0, "abnormal": 0}, {"name": "合并清理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式血缘", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式补数", "created": 0, "pending": 0, "abnormal": 0}, {"name": "下沉分发", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据生命周期", "created": 0, "pending": 0, "abnormal": 0}, {"name": "实时指标", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>

<style scoped>
.section-title { margin: 18px 0 8px; font-size: 15px; }
</style>
