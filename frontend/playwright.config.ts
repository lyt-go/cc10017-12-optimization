import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './tests',
  timeout: 300_000,
  expect: { timeout: 10_000 },
  // 后端是单一有状态内存仓库，测试间通过 /api/testing/reset 重置共享数据；
  // 并行 worker 会让复位接口互相干扰，因此端到端用例固定串行执行。
  workers: 1,
  use: {
    baseURL: process.env.E2E_BASE_URL || 'http://127.0.0.1:5173',
    browserName: 'chromium',
    headless: true,
    actionTimeout: 10_000,
  },
  reporter: 'line',
})
