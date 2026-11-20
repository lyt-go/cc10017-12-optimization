import { defineStore } from 'pinia'

import { getDataScope, setDataScope } from '@/api/client'

/** 业务归属域 -> 负责的业务表，与后端 data_scope 守卫保持一致。 */
export const DATA_SCOPES = [
  { value: 'orders', label: '订单域 (orders)' },
  { value: 'users', label: '用户域 (users)' },
  { value: 'payments', label: '支付域 (payments)' },
] as const

export const useSessionStore = defineStore('session', {
  state: () => ({
    operator: '值班管理员',
    shiftLabel: '白班 08:00-20:00',
    scope: getDataScope(),
    scopeLabel: '订单域 (orders)',
  }),
  getters: {
    canOperate: (state) => state.operator.length > 0,
  },
  actions: {
    setShift(label: string) {
      this.shiftLabel = label
    },
    switchScope(scope: string) {
      const matched = DATA_SCOPES.find((item) => item.value === scope)
      if (!matched) return
      this.scope = matched.value
      this.scopeLabel = matched.label
      setDataScope(matched.value)
    },
  },
})
