<template>
  <section class="panel" data-testid="batch-register-panel">
    <header class="panel-head">
      <h3>批量登记分片</h3>
      <p class="panel-desc">
        基于已启用流式源清单，以源表和键列确定范围，多选区间一次提交；逐条给出登记编号或冲突范围，相邻区间自动拼接。
      </p>
    </header>

    <form class="analysis-form" @submit.prevent="submit">
      <label>
        <span>流式来源（已启用）</span>
        <select v-model.number="sourceId" data-testid="batch-source" required>
          <option v-for="source in sources" :key="source.id" :value="source.id">
            {{ source.源标识 }}（{{ source.Topic名称 }}）
          </option>
        </select>
      </label>
      <label>
        <span>源表名称</span>
        <input v-model.trim="sourceTable" data-testid="batch-table" placeholder="如 ods_trade_ticks" required />
      </label>
      <label>
        <span>分片键列</span>
        <input v-model.trim="keyColumn" data-testid="batch-key" placeholder="如 trade_id" required />
      </label>
      <button class="btn primary" data-testid="batch-submit" type="submit" :disabled="submitting">
        {{ submitting ? '提交中…' : '批量登记' }}
      </button>
    </form>

    <table class="data-table interval-table">
      <thead>
        <tr>
          <th>批次编号</th>
          <th>分片起始值</th>
          <th>分片结束值</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, index) in intervals" :key="index">
          <td><input v-model.trim="row.批次编号" :data-testid="`interval-no-${index}`" placeholder="BATC-" /></td>
          <td><input v-model.number="row.分片起始值" :data-testid="`interval-start-${index}`" type="number" /></td>
          <td><input v-model.number="row.分片结束值" :data-testid="`interval-end-${index}`" type="number" /></td>
          <td>
            <button class="link" type="button" :disabled="intervals.length <= 1" @click="removeInterval(index)">
              移除
            </button>
          </td>
        </tr>
      </tbody>
    </table>
    <div class="panel-actions">
      <button class="btn ghost" data-testid="add-interval" type="button" @click="addInterval">添加区间</button>
    </div>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <div v-if="result" class="panel-result" data-testid="batch-result">
      <p class="result-summary">
        确认 {{ result.确认数 }} 段，冲突 {{ result.冲突数 }} 段；连续覆盖：
        <span v-for="seg in result.连续覆盖" :key="`${seg.起始值}-${seg.结束值}`" class="tag">
          [{{ seg.起始值 }}, {{ seg.结束值 }})
        </span>
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>批次编号</th>
            <th>区间</th>
            <th>登记结果</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in result.结果" :key="item.批次编号" :data-ok="String(item.ok)">
            <td>{{ item.批次编号 }}</td>
            <td>[{{ item.起始值 }}, {{ item.结束值 }})</td>
            <td>
              <span v-if="item.ok" class="tag ok">
                编号 {{ item.编号 }}<template v-if="item.重投">（重投，返回原结果）</template>
              </span>
              <span v-else class="tag conflict">
                冲突范围 [{{ item.冲突范围?.起始值 }}, {{ item.冲突范围?.结束值 }})
                <template v-if="item.冲突范围?.冲突批次">（与 {{ item.冲突范围.冲突批次 }}）</template>
                ：{{ item.reason }}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  batchRegister,
  fetchEnabledSources,
  type BatchRegisterResult,
  type EnabledSource,
  type SliceIntervalInput,
} from '@/domain/batch_slice'

const emit = defineEmits<{ registered: [] }>()

const sources = ref<EnabledSource[]>([])
const sourceId = ref(0)
const sourceTable = ref('')
const keyColumn = ref('')
const intervals = ref<SliceIntervalInput[]>([
  { 批次编号: '', 分片起始值: 0, 分片结束值: 10 },
])
const submitting = ref(false)
const errorMessage = ref('')
const result = ref<BatchRegisterResult | null>(null)

function addInterval() {
  const last = intervals.value[intervals.value.length - 1]
  intervals.value.push({
    批次编号: '',
    分片起始值: last ? Number(last.分片结束值) || 0 : 0,
    分片结束值: last ? (Number(last.分片结束值) || 0) + 10 : 10,
  })
}

function removeInterval(index: number) {
  intervals.value.splice(index, 1)
}

async function submit() {
  submitting.value = true
  errorMessage.value = ''
  result.value = null
  try {
    const payload = await batchRegister({
      source_id: sourceId.value,
      源表名称: sourceTable.value,
      分片键列: keyColumn.value,
      intervals: intervals.value.map((row) => ({
        批次编号: row.批次编号,
        分片起始值: Number(row.分片起始值),
        分片结束值: Number(row.分片结束值),
      })),
    })
    if (!payload.ok) {
      errorMessage.value = payload.reason ?? '批量登记失败'
      return
    }
    result.value = payload
    emit('registered')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量登记失败'
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  try {
    sources.value = await fetchEnabledSources()
    if (sources.value.length) sourceId.value = sources.value[0].id
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '已启用流式源清单读取失败'
  }
})
</script>
