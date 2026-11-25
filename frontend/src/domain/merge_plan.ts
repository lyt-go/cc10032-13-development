/** 合并覆盖清单：batch_slice 批量登记的区间覆盖，merge_plan 按范围采用。 */
import { request } from '@/api/client'

export type Range = { 起始值: number; 结束值: number }

export type CoverageInterval = Range & {
  编号: string
  批次编号: string
  来源?: string
  entry_id?: number
}

export type AdoptionRecord = {
  计划编号?: string
  ok: boolean
  reason: string
  选择范围: Range
  缺口: Range[]
  可读范围: Range | null
  ts?: number
}

export type CoverageManifest = {
  ok: boolean
  源表名称: string
  分片键列: string
  已登记区间: CoverageInterval[]
  连续覆盖: Range[]
  采用日志: AdoptionRecord[]
  选择范围?: Range
  缺口?: Range[]
  可读范围?: Range | null
  可整体采用?: boolean
}

export type AdoptResult = {
  ok: boolean
  reason?: string
  计划编号?: string
  源表名称?: string
  分片键列?: string
  选择范围?: Range
  缺口?: Range[]
  可读范围?: Range | null
  可整体采用?: boolean
}

export async function fetchCoverageManifest(
  sourceTable: string,
  keyColumn: string,
  start?: number,
  end?: number,
): Promise<CoverageManifest> {
  const query = new URLSearchParams({ source_table: sourceTable, key_column: keyColumn })
  if (start !== undefined && end !== undefined) {
    query.set('start', String(start))
    query.set('end', String(end))
  }
  const response = await request(`/api/merge_plan/coverage-manifest?${query}`)
  if (!response.ok) throw new Error('合并覆盖清单读取失败')
  return (await response.json()) as CoverageManifest
}

export async function adoptCoverage(
  planId: number,
  start: number,
  end: number,
): Promise<AdoptResult> {
  const response = await request(`/api/merge_plan/${planId}/adopt-coverage`, {
    method: 'POST',
    body: JSON.stringify({ 选择起始值: start, 选择结束值: end }),
  })
  return (await response.json()) as AdoptResult
}
