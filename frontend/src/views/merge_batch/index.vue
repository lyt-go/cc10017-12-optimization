<template>
  <section class="page" data-module="merge_batch">
    <header class="page-head">
      <div>
        <h2>合并批次工作台</h2>
        <p class="page-desc">
          按分批键索引切分键范围，每批只处理自己负责的键；合并批次结果保留批次键范围与待重算集合，
          中断恢复只重算未完批次，迟到更新按批次边界限制重算键所有权。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-plan" type="button" @click="openPlan">构建分批键索引</button>
        <button class="btn" data-testid="reload" type="button" @click="reload">刷新工作台</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="card in stats" :key="card.label" class="stat-card" :data-testid="`stat-${card.key}`">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <div v-if="errorMessage" class="error-banner" data-testid="scope-error">{{ errorMessage }}</div>

    <form v-if="showPlan" class="modal-backdrop" data-testid="plan-modal" @submit.prevent="submitPlan">
      <div class="modal">
        <h3>构建合并策略的分批键索引</h3>
        <label class="create-field">
          <span>合并策略 id（orders 域默认 UPSE-0001 = 1）</span>
          <input v-model.number="planForm.upsert_id" data-testid="plan-upsert" type="number" min="1" required />
        </label>
        <label class="create-field">
          <span>键总数</span>
          <input v-model.number="planForm.key_count" data-testid="plan-key-count" type="number" min="1" required />
        </label>
        <label class="create-field">
          <span>每批键数</span>
          <input v-model.number="planForm.batch_size" data-testid="plan-batch-size" type="number" min="1" required />
        </label>
        <label class="create-field">
          <span>关联 CDC 配置 id（可选）</span>
          <input v-model.number="planForm.cdc_config_id" data-testid="plan-cdc" type="number" min="1" />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showPlan = false">取消</button>
          <button class="btn primary" data-testid="submit-plan" type="submit">生成合并批次</button>
        </div>
      </div>
    </form>

    <table class="data-table" data-testid="run-table">
      <thead>
        <tr>
          <th>运行编号</th><th>目标表</th><th>合并键</th><th>键总数/批大小</th>
          <th>状态</th><th>版本</th><th>CDC</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="run in runs" :key="run.id" :data-testid="`run-${run.id}`" :data-status="run.status">
          <td>{{ run.run_code }}</td>
          <td>{{ run.target_table }}</td>
          <td>{{ run.merge_key_column }}</td>
          <td>{{ run.key_count }} / {{ run.batch_size }}</td>
          <td>{{ run.status }}</td>
          <td :data-testid="`run-version-${run.id}`">v{{ run.version }}</td>
          <td>{{ run.cdc_config_id ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`view-${run.id}`" @click="selectRun(run.id)">查看批次</button>
            <button class="link" type="button" :data-testid="`resume-${run.id}`" @click="resume(run.id)">中断恢复</button>
          </td>
        </tr>
        <tr v-if="!runs.length">
          <td colspan="8" class="empty-state">当前归属暂无合并批次运行，可先构建分批键索引</td>
        </tr>
      </tbody>
    </table>

    <template v-if="selectedRun">
      <h3 class="section-title">合并批次结果 · {{ selectedRun.run_code }}（{{ selectedRun.status }}，v{{ selectedRun.version }}）</h3>
      <table class="data-table" data-testid="batch-table">
        <thead>
          <tr>
            <th>批次</th><th>批次键范围</th><th>状态</th><th>版本</th>
            <th>最后成功位置</th><th>待重算集合</th><th>冲突键</th><th>已发布键数</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in batches" :key="batch.id" :data-testid="`batch-${batch.batch_no}`" :data-status="batch.status">
            <td>#{{ batch.batch_no }}</td>
            <td :data-testid="`range-${batch.batch_no}`">[{{ batch.key_range.lower_key }}, {{ batch.key_range.upper_key }}]</td>
            <td>{{ batch.status }}</td>
            <td :data-testid="`version-${batch.batch_no}`">v{{ batch.version }}</td>
            <td>{{ batch.last_success_key ?? '—' }}</td>
            <td :data-testid="`recalc-${batch.batch_no}`">{{ batch.recalc_keys.join(',') || '—' }}</td>
            <td>{{ batch.conflict_keys.join(',') || '—' }}</td>
            <td>{{ batch.published_keys.length }}</td>
            <td class="row-actions">
              <button
                v-if="batch.status !== '已发布'"
                class="link" type="button"
                :data-testid="`run-batch-${batch.batch_no}`"
                @click="runBatch(batch.batch_no)"
              >运行批次</button>
              <button
                v-if="batch.status === '待执行'"
                class="link" type="button"
                :data-testid="`interrupt-${batch.batch_no}`"
                @click="runBatch(batch.batch_no, 2)"
              >模拟中断(2键)</button>
              <button class="link" type="button" :data-testid="`late-${batch.batch_no}`" @click="openLate(batch)">迟到补报</button>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="action-grid">
        <form class="inline-form" data-testid="late-form" @submit.prevent="submitLate">
          <strong>批次边界补报一个键（只进入一个可写批次）</strong>
          <label><span>业务键</span>
            <input v-model.number="lateForm.key" data-testid="late-key" type="number" min="1" required />
          </label>
          <label><span>边界冲突取舍</span>
            <select v-model="lateForm.conflict_policy" data-testid="late-policy">
              <option value="keep_owner">保留原键所有者 (keep_owner)</option>
              <option value="latest_batch">按最新批次重新归属 (latest_batch)</option>
            </select>
          </label>
          <button class="btn primary" data-testid="submit-late" type="submit">提交迟到更新</button>
        </form>

        <div class="inline-form">
          <strong>CDC 捕获核对增量来源</strong>
          <label><span>CDC 配置 id</span>
            <input v-model.number="handoffCdcId" data-testid="handoff-cdc" type="number" min="1" />
          </label>
          <div class="page-actions">
            <button class="btn primary" data-testid="handoff" type="button" @click="handoff">交付CDC并核对</button>
          </div>
        </div>
      </div>

      <h3 class="section-title">关联订单记录（刷新后保持原记录，仅版本升级）</h3>
      <table class="data-table" data-testid="order-table">
        <thead><tr><th>记录 id</th><th>业务键</th><th>归属批次 id</th><th>版本</th><th>更新时间</th></tr></thead>
        <tbody>
          <tr v-for="order in orders" :key="order.id" :data-testid="`order-${order.merge_key}`">
            <td>{{ order.id }}</td>
            <td>{{ order.merge_key }}</td>
            <td>{{ order.owner_batch_id }}</td>
            <td :data-testid="`order-version-${order.merge_key}`">v{{ order.version }}</td>
            <td>{{ order.updated_at }}</td>
          </tr>
          <tr v-if="!orders.length"><td colspan="5" class="empty-state">暂无订单记录</td></tr>
        </tbody>
      </table>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'
import { useSessionStore } from '@/stores/session'

type Run = {
  id: number
  run_code: string
  target_table: string
  merge_key_column: string
  key_count: number
  batch_size: number
  status: string
  version: number
  cdc_config_id: number | null
}

type Batch = {
  id: number
  batch_no: number
  key_range: { lower_key: number; upper_key: number }
  status: string
  version: number
  last_success_key: number | null
  recalc_keys: number[]
  conflict_keys: number[]
  published_keys: number[]
}

type OrderRecord = {
  id: number
  merge_key: number
  owner_batch_id: number
  version: number
  updated_at: string
}

const session = useSessionStore()
const runs = ref<Run[]>([])
const selectedRun = ref<Run | null>(null)
const batches = ref<Batch[]>([])
const orders = ref<OrderRecord[]>([])
const errorMessage = ref('')
const showPlan = ref(false)
const planForm = ref({ upsert_id: 1, key_count: 10, batch_size: 5, cdc_config_id: 1 })
const lateForm = ref({ key: 1, conflict_policy: 'keep_owner' })
const handoffCdcId = ref(1)

const stats = computed(() => [
  { key: 'runs', label: '合并运行数', value: runs.value.length },
  { key: 'published', label: '已发布批次', value: batches.value.filter((b) => b.status === '已发布').length },
  { key: 'recalc', label: '待重算键数', value: batches.value.reduce((sum, b) => sum + b.recalc_keys.length, 0) },
  { key: 'scope', label: '当前业务归属', value: session.scope },
])

function openPlan() {
  errorMessage.value = ''
  showPlan.value = true
}

async function submitPlan() {
  errorMessage.value = ''
  try {
    const response = await request('/api/merge_batch/plan', {
      method: 'POST',
      body: JSON.stringify(planForm.value),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    showPlan.value = false
    await reload()
    await selectRun(payload.entry.run.id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分批键索引构建失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const response = await request('/api/merge_batch/runs')
    const payload = await response.json()
    if (payload.ok === false) throw new Error(payload.error)
    runs.value = payload.items ?? []
    if (selectedRun.value && !runs.value.some((r) => r.id === selectedRun.value?.id)) {
      selectedRun.value = null
      batches.value = []
      orders.value = []
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '合并批次运行读取失败'
  }
}

async function selectRun(runId: number) {
  errorMessage.value = ''
  try {
    const [runResp, orderResp] = await Promise.all([
      request(`/api/merge_batch/runs/${runId}`),
      request(`/api/merge_batch/runs/${runId}/orders`),
    ])
    const runPayload = await runResp.json()
    if (!runPayload.ok) throw new Error(runPayload.message)
    selectedRun.value = runPayload.entry.run
    batches.value = runPayload.entry.batches
    const orderPayload = await orderResp.json()
    orders.value = orderPayload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '合并批次结果读取失败'
  }
}

async function runBatch(batchNo: number, failAfter?: number) {
  if (!selectedRun.value) return
  errorMessage.value = ''
  const query = failAfter ? `?fail_after_key=${failAfter}` : ''
  try {
    const response = await request(
      `/api/merge_batch/runs/${selectedRun.value.id}/batches/${batchNo}/run${query}`,
      { method: 'POST' },
    )
    const payload = await response.json()
    if (payload.ok === false && !payload.message.includes('中断')) throw new Error(payload.message)
    errorMessage.value = payload.message.includes('中断') ? payload.message : ''
    await selectRun(selectedRun.value.id)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次运行失败'
  }
}

async function resume(runId: number) {
  errorMessage.value = ''
  try {
    const response = await request(`/api/merge_batch/runs/${runId}/resume`, { method: 'POST' })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    if (selectedRun.value?.id === runId) await selectRun(runId)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '中断恢复失败'
  }
}

function openLate(batch: Batch) {
  lateForm.value.key = batch.key_range.upper_key
  lateForm.value.conflict_policy = 'keep_owner'
}

async function submitLate() {
  if (!selectedRun.value) return
  errorMessage.value = ''
  try {
    const response = await request(
      `/api/merge_batch/runs/${selectedRun.value.id}/late-update`,
      { method: 'POST', body: JSON.stringify(lateForm.value) },
    )
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    await selectRun(selectedRun.value.id)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迟到更新失败'
  }
}

async function handoff() {
  if (!selectedRun.value) return
  errorMessage.value = ''
  const runId = selectedRun.value.id
  try {
    const response = await request(
      `/api/merge_batch/runs/${runId}/handoff/${handoffCdcId.value}`,
      { method: 'POST' },
    )
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message)
    const confirmResp = await request(
      `/api/change_capture/${handoffCdcId.value}/confirm-handoff/${runId}`,
      { method: 'POST' },
    )
    const confirmPayload = await confirmResp.json()
    if (!confirmPayload.ok) throw new Error(confirmPayload.message)
    errorMessage.value = `${payload.message}；${confirmPayload.message}`
    await selectRun(runId)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'CDC 交接失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.section-title { margin: 18px 0 8px; font-size: 15px; }
.action-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin: 12px 0; }
.inline-form { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; display: flex; flex-direction: column; gap: 8px; }
.inline-form label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.inline-form input, .inline-form select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.error-banner { background: #fef3f2; border: 1px solid #fda29b; color: #b42318; border-radius: 6px; padding: 8px 12px; margin-bottom: 12px; font-size: 13px; }
</style>
