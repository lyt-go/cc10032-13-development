/** 批次分片批量登记：已启用流式源清单 + 多区间一次提交。 */
import { request } from '@/api/client'

export type EnabledSource = {
  id: number
  源标识: string
  Topic名称: string
  status: string
  [field: string]: string | number
}

export type SliceIntervalInput = {
  批次编号: string
  分片起始值: number
  分片结束值: number
}

export type IntervalResult = {
  批次编号: string
  ok: boolean
  编号?: string
  entry_id?: number
  重投?: boolean
  reason?: string
  起始值?: number
  结束值?: number
  冲突范围?: {
    起始值: number
    结束值: number
    冲突批次?: string
    冲突编号?: string
  }
}

export type BatchRegisterResult = {
  ok: boolean
  reason?: string
  来源?: string
  源表名称?: string
  分片键列?: string
  确认数?: number
  冲突数?: number
  结果?: IntervalResult[]
  连续覆盖?: { 起始值: number; 结束值: number }[]
}

export async function fetchEnabledSources(): Promise<EnabledSource[]> {
  const response = await request('/api/batch_slice/enabled-sources')
  if (!response.ok) throw new Error('已启用流式源清单读取失败')
  const payload = (await response.json()) as { items: EnabledSource[] }
  return payload.items
}

export async function batchRegister(payload: {
  source_id: number
  源表名称: string
  分片键列: string
  intervals: SliceIntervalInput[]
}): Promise<BatchRegisterResult> {
  const response = await request('/api/batch_slice/batch-register', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return (await response.json()) as BatchRegisterResult
}
