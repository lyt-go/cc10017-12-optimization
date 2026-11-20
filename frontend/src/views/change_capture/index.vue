<template>
  <section class="page" data-module="change_capture">
    <header class="page-head">
      <div><h2>CDC捕获</h2><p class="page-desc">维护数据库CDC的日志捕获、快照与增量切换配置，为CDC捕获提供可验证的数据工程流程。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记CDC配置</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <section class="handoff-panel" data-testid="handoff-panel">
      <h3>合并批次来源核对（增量合并 → CDC 捕获，共享内容只读）</h3>
      <p v-if="handoffError" class="error-text" data-testid="handoff-error">{{ handoffError }}</p>
      <table class="data-table">
        <thead>
          <tr><th>交接编号</th><th>合并运行</th><th>版本</th><th>目标表</th><th>批次键范围 / 状态</th><th>交付时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="handoff in handoffs" :key="handoff.id" :data-testid="`handoff-${handoff.id}`">
            <td>#{{ handoff.id }}</td>
            <td>{{ handoff.run_code }}</td>
            <td :data-testid="`handoff-version-${handoff.id}`">v{{ handoff.version }}</td>
            <td>{{ handoff.target_table }}</td>
            <td>
              <span v-for="r in handoff.batch_key_ranges" :key="r.batch_no" class="range-chip">
                #{{ r.batch_no }} [{{ r.lower_key }},{{ r.upper_key }}] {{ r.status }} v{{ r.version }}
                <template v-if="r.recalc_keys && r.recalc_keys.length"> 重算:{{ r.recalc_keys.join('|') }}</template>
              </span>
            </td>
            <td>{{ handoff.handoff_at }}</td>
          </tr>
          <tr v-if="!handoffs.length"><td colspan="6" class="empty-state">暂无合并批次交接结果</td></tr>
        </tbody>
      </table>
      <p class="readonly-tip">交接结果只读展示，CDC 侧确认记录通过合并批次工作台的「交付CDC并核对」生成。</p>
    </section>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记CDC配置</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/change_capture'
const columns = ["配置编号", "源数据库", "源表名", "快照模式", "捕获列", "变更序号", "增量阶段", "目标主题"]
const actions = ["提交校验", "启动捕获", "停止捕获"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
const handoffs = ref<any[]>([])
const handoffError = ref('')
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
  await loadHandoffs()
}
async function loadHandoffs() {
  handoffError.value = ''
  try {
    const res = await request(`${ENDPOINT}/handoffs`)
    const data = await res.json()
    if (data.ok === false) throw new Error(data.error)
    handoffs.value = data.items || []
  } catch (e: any) { handoffError.value = e.message || '合并批次交接结果读取失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
<style scoped>
.handoff-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.handoff-panel h3 { margin: 0 0 8px; font-size: 14px; }
.range-chip { display: inline-block; background: #f0f5ff; border: 1px solid #c7d7fe; border-radius: 4px; padding: 2px 6px; margin: 2px 4px 2px 0; font-size: 12px; }
.readonly-tip { color: var(--muted); font-size: 12px; margin: 8px 0 0; }
</style>
