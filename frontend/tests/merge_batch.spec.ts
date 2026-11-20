import { expect, test } from '@playwright/test'

/**
 * 合并批次结果验收：
 * - 增量合并页分批运行订单变更，CDC 捕获页核对来源；
 * - 批次边界补报一个键，只进入一个可写批次；
 * - 越权访问被拒绝，共享内容保持只读；
 * - 工作台同步显示合并批次结果，关联记录刷新后保持原版本。
 */
const BASE = process.env.E2E_BASE_URL || 'http://127.0.0.1:5173'

async function planRun(page: import('@playwright/test').Page, keyCount: number, batchSize: number) {
  await page.getByTestId('open-plan').click()
  await expect(page.getByTestId('plan-modal')).toBeVisible()
  await page.getByTestId('plan-key-count').fill(String(keyCount))
  await page.getByTestId('plan-batch-size').fill(String(batchSize))
  await page.getByTestId('submit-plan').click()
  await expect(page.getByTestId('plan-modal')).toHaveCount(0)
  await expect(page.getByTestId('batch-table')).toBeVisible()
}

test.describe('合并批次工作台', () => {
  test.beforeEach(async ({ page, request }) => {
    const errors: string[] = []
    page.on('pageerror', (e) => errors.push(e.message))
    page.on('response', (r) => {
      if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`)
    })
    test.info().attach('errors', { body: errors.join('\n') })
    // 每个用例从干净的种子状态开始，避免跨用例批次/运行累积干扰断言
    const resetResp = await request.post('/api/testing/reset')
    expect(resetResp.ok()).toBeTruthy()
    await page.goto(`${BASE}/merge_batch`)
    await expect(page.getByTestId('run-table')).toBeVisible()
  })

  test('分批运行、中断恢复：只重算未完批次并保留已发布键范围', async ({ page }) => {
    await planRun(page, 10, 5)

    // 两个批次的键范围明确
    await expect(page.getByTestId('range-0')).toHaveText('[1, 5]')
    await expect(page.getByTestId('range-1')).toHaveText('[6, 10]')

    // 第一批完整发布
    await page.getByTestId('run-batch-0').click()
    await expect(page.getByTestId('batch-0')).toHaveAttribute('data-status', '已发布')

    // 第二批模拟中断（处理 2 个键）
    await page.getByTestId('interrupt-1').click()
    await expect(page.getByTestId('batch-1')).toHaveAttribute('data-status', '已中断')
    const runRow = page.locator('tr[data-testid^="run-"]').first()
    await expect(runRow).toHaveAttribute('data-status', '已中断')

    // 恢复：只重算未完批次
    await page.getByRole('button', { name: '中断恢复' }).first().click()
    await expect(page.getByTestId('batch-1')).toHaveAttribute('data-status', '已发布')
    await expect(runRow).toHaveAttribute('data-status', '已完成')

    // 10 个键对应 10 条订单记录，无重复写入
    await expect(page.getByTestId('order-table').locator('tbody tr')).toHaveCount(10)
    // 已发布批次版本保持 v1
    await expect(page.getByTestId('version-0')).toHaveText('v1')
  })

  test('批次边界补报一个键：只进入一个可写批次，两种取舍版本不同', async ({ page }) => {
    await planRun(page, 10, 5)
    await page.getByTestId('run-batch-0').click()
    await page.getByTestId('run-batch-1').click()

    // keep_owner：键 5 留在原批次 0
    await page.getByTestId('late-key').fill('5')
    await page.getByTestId('late-policy').selectOption('keep_owner')
    await page.getByTestId('submit-late').click()
    await expect(page.getByTestId('recalc-0')).toContainText('5')
    await expect(page.getByTestId('version-0')).toHaveText('v2')
    await expect(page.getByTestId('range-0')).toHaveText('[1, 5]')

    // 关联订单记录刷新后仍是同一条，仅版本升级
    const orderRow = page.getByTestId('order-5')
    await expect(orderRow).toBeVisible()
    await expect(orderRow.getByTestId('order-version-5')).toHaveText('v2')

    // latest_batch：键 5 重新归属到批次 1，边界移动
    await page.getByTestId('late-key').fill('5')
    await page.getByTestId('late-policy').selectOption('latest_batch')
    await page.getByTestId('submit-late').click()
    await expect(page.getByTestId('range-0')).toHaveText('[1, 4]')
    await expect(page.getByTestId('range-1')).toContainText('[5,')
    await expect(page.getByTestId('recalc-1')).toContainText('5')

    // 一个键只能由一个批次发布：订单记录仍然只有一条键 5
    await expect(page.getByTestId('order-5')).toHaveCount(1)
  })

  test('CDC 交接保留当前版本，CDC 页只读核对来源', async ({ page }) => {
    await planRun(page, 10, 5)
    await page.getByTestId('run-batch-0').click()
    await page.getByTestId('run-batch-1').click()
    const runVersionBefore = await page.locator('[data-testid^="run-version-"]').first().innerText()

    await page.getByTestId('handoff-cdc').fill('1')
    await page.getByTestId('handoff').click()

    // CDC 捕获页核对来源（共享内容只读）
    await page.goto(`${BASE}/change_capture`)
    const handoffRow = page.getByTestId('handoff-panel').locator('tbody tr').first()
    await expect(handoffRow).toBeVisible()
    await expect(handoffRow.getByTestId(/handoff-version-\d+/)).toHaveText(runVersionBefore)
    await expect(handoffRow).toContainText('[1,5]')
    await expect(handoffRow).toContainText('[6,10]')
    await expect(page.getByText('交接结果只读展示')).toBeVisible()
  })

  test('越权访问被拒绝：切换归属后看不到其他域的合并运行', async ({ page }) => {
    // orders 域先建一个运行
    await planRun(page, 5, 5)
    await expect(page.getByTestId('run-table').locator('tbody tr')).toHaveCount(1)

    // 切到 users 域：列表为空（后端按归属过滤）
    await page.getByTestId('scope-switch').selectOption('users')
    await page.goto(`${BASE}/merge_batch`)
    await expect(page.getByTestId('run-table').locator('.empty-state')).toBeVisible()
    await expect(page.getByTestId('stat-scope').locator('strong')).toHaveText('users')

    // 切回 orders 域数据仍在，版本保持
    await page.getByTestId('scope-switch').selectOption('orders')
    await page.goto(`${BASE}/merge_batch`)
    await expect(page.getByTestId('run-table').locator('tbody tr')).toHaveCount(1)
  })

  test('概览同步显示合并批次结果', async ({ page }) => {
    await planRun(page, 6, 3)
    await page.goto(`${BASE}/`)
    await expect(page.getByTestId('artifact-merge_batch_runs')).toBeVisible()
    const batchCountText = await page.getByTestId('artifact-merge_batch_results').locator('td').nth(1).innerText()
    expect(Number(batchCountText.trim())).toBeGreaterThanOrEqual(2)
    await expect(page.getByTestId('artifact-merge_cdc_handoffs')).toBeVisible()
    await expect(page.getByTestId('artifact-merge_order_records')).toBeVisible()
  })
})
