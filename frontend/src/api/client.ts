/** 统一请求封装：拼后端地址、抛网络错误、给页脚留一句可读的说明。 */
const API_BASE = import.meta.env.VITE_API_BASE ?? ''

const SCOPE_STORAGE_KEY = 'data-scope'
const DEFAULT_SCOPE = 'orders'

function readStoredScope(): string {
  try {
    return localStorage.getItem(SCOPE_STORAGE_KEY) || DEFAULT_SCOPE
  } catch {
    return DEFAULT_SCOPE
  }
}

/** 当前业务归属（数据归属守卫按 X-Data-Scope 限制可见数据），刷新页面后保持。 */
let currentScope = readStoredScope()

export function setDataScope(scope: string) {
  currentScope = scope
  try {
    localStorage.setItem(SCOPE_STORAGE_KEY, scope)
  } catch {
    /* 隐私模式等场景忽略持久化失败 */
  }
}

export function getDataScope() {
  return currentScope
}

export function request(path: string, init?: RequestInit): Promise<Response> {
  const url = path.startsWith('http') ? path : `${API_BASE}${path}`
  const headers = new Headers(init?.headers)
  headers.set('Content-Type', 'application/json')
  headers.set('X-Data-Scope', currentScope)
  return fetch(url, {
    ...init,
    headers,
  }).catch((error: unknown) => {
    const detail = error instanceof Error ? error.message : '请求未送达'
    throw new Error(`接口请求失败：${detail}`)
  })
}

export async function fetchJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    throw new Error(`接口返回 ${response.status}，数据未更新`)
  }
  return (await response.json()) as T
}
