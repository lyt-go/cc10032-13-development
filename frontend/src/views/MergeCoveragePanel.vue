<template>
  <section class="panel" data-testid="merge-coverage-panel">
    <header class="panel-head">
      <h3>合并覆盖清单</h3>
      <p class="panel-desc">
        批次分片批量登记的区间覆盖同步到这里；合并只在选定范围没有未解释缺口时才采用它。
      </p>
    </header>

    <form class="analysis-form" @submit.prevent="query">
      <label>
        <span>合并计划</span>
        <select v-model.number="planId" data-testid="coverage-plan" required>
          <option v-for="plan in plans" :key="plan.id" :value="plan.id">
            {{ plan.计划编号 }}（{{ plan.批式来源 }} / {{ plan.合并键列 }}）
          </option>
        </select>
      </label>
      <label>
        <span>选择起始值</span>
        <input v-model.number="selStart" data-testid="coverage-start" type="number" required />
      </label>
      <label>
        <span>选择结束值</span>
        <input v-model.number="selEnd" data-testid="coverage-end" type="number" required />
      </label>
      <button class="btn" data-testid="coverage-query" type="submit">查询覆盖</button>
      <button class="btn primary" data-testid="coverage-adopt" type="button" :disabled="adopting" @click="adopt">
        {{ adopting ? '采用中…' : '采用覆盖' }}
      </button>
    </form>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>
    <p v-if="adoptMessage" class="adopt-message" :data-ok="String(adoptOk)">{{ adoptMessage }}</p>

    <div v-if="manifest" class="panel-result" data-testid="coverage-result">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">连续覆盖</span>
          <strong class="stat-value coverage-value">
            <span v-if="!manifest.连续覆盖.length">—</span>
            <span v-for="seg in manifest.连续覆盖" :key="`${seg.起始值}-${seg.结束值}`" class="tag">
              [{{ seg.起始值 }}, {{ seg.结束值 }})
            </span>
          </strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">未解释缺口</span>
          <strong class="stat-value coverage-value">
            <span v-if="!manifest.缺口 || !manifest.缺口.length">无</span>
            <span v-for="gap in manifest.缺口" :key="`${gap.起始值}-${gap.结束值}`" class="tag conflict">
              [{{ gap.起始值 }}, {{ gap.结束值 }})
            </span>
          </strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">合并开始可读的范围</span>
          <strong class="stat-value coverage-value">
            <span v-if="manifest.可读范围">[{{ manifest.可读范围.起始值 }}, {{ manifest.可读范围.结束值 }})</span>
            <span v-else>—</span>
          </strong>
        </article>
      </div>

      <table class="data-table">
        <thead>
          <tr>
            <th>登记编号</th>
            <th>批次编号</th>
            <th>区间</th>
            <th>来源</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in manifest.已登记区间" :key="item.编号">
            <td>{{ item.编号 }}</td>
            <td>{{ item.批次编号 }}</td>
            <td>[{{ item.起始值 }}, {{ item.结束值 }})</td>
            <td>{{ item.来源 ?? '—' }}</td>
          </tr>
          <tr v-if="!manifest.已登记区间.length">
            <td colspan="4" class="empty-state">该范围还没有已登记的分片区间</td>
          </tr>
        </tbody>
      </table>

      <h4 class="log-title">采用日志（含此前的失败原因）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>计划编号</th>
            <th>选择范围</th>
            <th>结果</th>
            <th>失败原因</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(record, index) in manifest.采用日志" :key="index" :data-ok="String(record.ok)">
            <td>{{ record.计划编号 ?? '—' }}</td>
            <td>[{{ record.选择范围.起始值 }}, {{ record.选择范围.结束值 }})</td>
            <td>
              <span class="tag" :class="record.ok ? 'ok' : 'conflict'">
                {{ record.ok ? '已采用' : '已拒绝' }}
              </span>
            </td>
            <td>{{ record.reason || '—' }}</td>
          </tr>
          <tr v-if="!manifest.采用日志.length">
            <td colspan="4" class="empty-state">还没有采用记录</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import {
  adoptCoverage,
  fetchCoverageManifest,
  type CoverageManifest,
} from '@/domain/merge_plan'
import type { EntryRow } from '@/data/types'

const plans = ref<EntryRow[]>([])
const planId = ref(0)
const selStart = ref(0)
const selEnd = ref(40)
const manifest = ref<CoverageManifest | null>(null)
const adopting = ref(false)
const errorMessage = ref('')
const adoptMessage = ref('')
const adoptOk = ref(false)

const currentPlan = computed(() => plans.value.find((plan) => Number(plan.id) === planId.value))

async function query() {
  errorMessage.value = ''
  adoptMessage.value = ''
  const plan = currentPlan.value
  if (!plan) return
  try {
    manifest.value = await fetchCoverageManifest(
      String(plan.批式来源),
      String(plan.合并键列),
      Number(selStart.value),
      Number(selEnd.value),
    )
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '合并覆盖清单读取失败'
  }
}

async function adopt() {
  adopting.value = true
  errorMessage.value = ''
  adoptMessage.value = ''
  try {
    const result = await adoptCoverage(planId.value, Number(selStart.value), Number(selEnd.value))
    adoptOk.value = result.ok
    adoptMessage.value = result.ok
      ? `已采用：合并开始可读的范围 [${result.可读范围?.起始值}, ${result.可读范围?.结束值})`
      : `拒绝采用：${result.reason ?? '存在未解释缺口'}`
    await query()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采用失败'
  } finally {
    adopting.value = false
  }
}

onMounted(async () => {
  try {
    const payload = await listEntries('merge_plan')
    plans.value = payload.items
    if (plans.value.length) {
      planId.value = Number(plans.value[0].id)
      await query()
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '合并计划读取失败'
  }
})
</script>
