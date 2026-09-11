<template>
  <div>
    <div class="header">
      <h1>时序数据监控平台 · Time Series DB</h1>
      <div class="stats">
        <div class="stat"><div class="num">{{ metricsList.length }}</div><div class="label">指标</div></div>
        <div class="stat"><div class="num">{{ totalPoints.toLocaleString() }}</div><div class="label">数据点</div></div>
        <div class="stat">
          <div class="num" :style="{ color: live ? '#66bb6a' : '#7a869e' }">●</div>
          <div class="label">{{ live ? '实时采集中' : '已暂停' }}</div>
        </div>
      </div>
    </div>

    <div class="toolbar">
      <div class="group">
        <label>实例</label>
        <select v-model="instance" @change="onQuery">
          <option v-for="i in instances" :key="i" :value="i">{{ i || '全部' }}</option>
        </select>
      </div>
      <div class="group">
        <label>指标</label>
        <div class="metric-chips">
          <span
            v-for="m in metricNames"
            :key="m"
            class="chip"
            :class="{ on: selected.includes(m) }"
            @click="toggleMetric(m)"
          >{{ m }}</span>
        </div>
      </div>
    </div>

    <div class="toolbar">
      <div class="group">
        <label>时间范围</label>
        <button v-for="r in ranges" :key="r.sec" :class="{ active: rangeSec === r.sec }" @click="setRange(r.sec)">
          {{ r.label }}
        </button>
      </div>
      <div class="group">
        <label>聚合</label>
        <button v-for="a in ['min','max','avg','sum']" :key="a" :class="{ active: agg === a }" @click="setAgg(a)">
          {{ a.toUpperCase() }}
        </button>
      </div>
      <div class="group">
        <button :class="{ active: live }" @click="toggleLive">{{ live ? '暂停实时' : '开启实时' }}</button>
        <button class="primary" @click="onQuery">刷新查询</button>
      </div>
    </div>

    <div class="chart-card">
      <h3>多指标对比 · 降采样曲线（聚合: {{ agg.toUpperCase() }} / 桶: {{ queryMeta.bucket }}s ·
        数据源: {{ queryMeta.source === 'hourly' ? '小时预聚合表' : '原始分区表' }} ·
        耗时: {{ queryMeta.elapsed }}ms · 异常点: {{ anomalyCount }}）</h3>
      <div ref="historyChart" class="chart chart-tall"></div>
    </div>

    <div class="chart-card">
      <div class="card-head">
        <h3>异常事件 · 按时间接近度与实例关联性自动聚类（可见 {{ visibleEvents.length }} / 共 {{ anomalyEvents.length }} 个 · 聚类间隔: {{ eventGap }}s）</h3>
        <button class="reset-zoom" @click="resetZoom">重置缩放</button>
      </div>
      <div v-if="visibleEvents.length" class="event-list">
        <div
          v-for="e in visibleEvents"
          :key="e.id"
          class="event-item"
          :class="{ active: activeEventId === e.id }"
          @click="focusEvent(e)"
        >
          <div class="event-head">
            <span class="event-id">{{ e.id }}</span>
            <span class="event-time">{{ fmtTs(e.start_ts) }} ~ {{ fmtTs(e.end_ts) }}</span>
            <span class="event-dur">持续 {{ fmtDur(e.duration_sec) }}</span>
          </div>
          <div class="event-body">
            <span>指标: {{ e.metrics.join(', ') }}</span>
            <span>影响范围: {{ e.instances.join(', ') || '-' }}</span>
            <span>异常点: {{ e.point_count }}</span>
            <span>峰值 Z: {{ e.max_zscore }}</span>
          </div>
        </div>
      </div>
      <div v-else class="event-empty">{{ anomalyEvents.length ? '当前可视范围内无异常事件' : '该时间范围内无异常事件' }}</div>
    </div>

    <div class="chart-card">
      <h3>实时曲线（最近 {{ liveWindow }} 秒原始点）· 异常点以红色标注</h3>
      <div ref="liveChart" class="chart"></div>
      <div class="meta">
        <span><span class="legend-dot" style="background:#ff5252"></span>异常点 (滑动窗口 Z-Score &gt; {{ anomalyThreshold }})</span>
        <span>异常总数: <span class="hl">{{ anomalyCount }}</span></span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'

const API = ''

const metricsList = ref([])
const totalPoints = ref(0)
const instances = ref([''])
const instance = ref('')
const metricNames = ref([])
const selected = ref(['cpu.usage', 'mem.usage'])
const ranges = [
  { sec: 900, label: '15分钟' },
  { sec: 3600, label: '1小时' },
  { sec: 21600, label: '6小时' },
  { sec: 86400, label: '24小时' },
  { sec: 172800, label: '2天' },
]
const rangeSec = ref(21600)
const agg = ref('avg')
const live = ref(true)
const liveWindow = 300
const anomalyThreshold = 3.0
const anomalyCount = ref(0)
const anomalyEvents = ref([])
const eventGap = ref('-')
// 面板 <-> 曲线双向联动状态
const activeEventId = ref(null)   // 当前选中(点击定位)的事件
const visStart = ref(0)           // 历史曲线当前可视窗口起点(毫秒)
const visEnd = ref(0)             // 历史曲线当前可视窗口终点(毫秒)
let queryStartMs = 0              // 本次历史查询的完整范围(毫秒)
let queryEndMs = 0

// 曲线可视范围过滤面板: 只列出与可视窗口有交集的事件
const visibleEvents = computed(() =>
  anomalyEvents.value.filter(e => e.end_ts >= visStart.value && e.start_ts <= visEnd.value)
)

const queryMeta = reactive({ bucket: '-', source: 'raw', elapsed: '-' })

const historyChart = ref(null)
const liveChart = ref(null)
let histInst = null
let liveInst = null
let liveTimer = null
let liveAnomalyTimer = null

const COLORS = ['#4fc3f7', '#ffb74d', '#81c784', '#ba68c8', '#f06292', '#4dd0e1']

async function fetchJson(url) {
  const r = await fetch(API + url)
  return r.json()
}

async function loadMetrics() {
  const rows = await fetchJson('/api/metrics')
  metricsList.value = rows
  totalPoints.value = rows.reduce((s, r) => s + Number(r.points || 0), 0)
  const instSet = [...new Set(rows.map(r => r.instance))]
  instances.value = ['', ...instSet]
  const names = [...new Set(rows.map(r => r.name))]
  metricNames.value = names
  if (!selected.value.some(s => names.includes(s)) && names.length) {
    selected.value = names.slice(0, 2)
  }
}

function toggleMetric(m) {
  const i = selected.value.indexOf(m)
  if (i >= 0) selected.value.splice(i, 1)
  else selected.value.push(m)
  onQuery()
}
function setRange(s) { rangeSec.value = s; onQuery() }
function setAgg(a) { agg.value = a; onQuery() }
function toggleLive() { live.value = !live.value; scheduleLive() }

// ---------- 历史查询: 多指标对比 + 降采样 ----------
async function onQuery() {
  if (!selected.value.length) { anomalyEvents.value = []; histInst.setOption({ series: [] }); return }
  const end = Math.floor(Date.now() / 1000)
  const start = end - rangeSec.value
  // 记录完整查询范围并重置联动状态(新查询后曲线回到全量视图)
  queryStartMs = start * 1000
  queryEndMs = end * 1000
  visStart.value = queryStartMs
  visEnd.value = queryEndMs
  activeEventId.value = null
  const q = new URLSearchParams({
    metrics: selected.value.join(','),
    instance: instance.value,
    start: String(start), end: String(end),
    agg: agg.value,
  })
  const data = await fetchJson('/api/query?' + q.toString())
  queryMeta.bucket = data.bucket_seconds
  queryMeta.source = data.source
  queryMeta.elapsed = data.elapsed_ms

  const series = Object.entries(data.series || {}).map(([name, pts], idx) => ({
    name,
    type: 'line',
    smooth: true,
    showSymbol: false,
    lineStyle: { width: 2 },
    itemStyle: { color: COLORS[idx % COLORS.length] },
    data: pts.map(p => [p.ts * 1000, p.value]),
    connectNulls: true,
  }))

  // 对第一个选中指标做异常点检测, 在历史曲线上以红色散点标注
  const anomalies = await fetchAnomalies(selected.value[0], start, end)
  anomalyCount.value = anomalies.length
  if (anomalies.length) {
    series.push({
      name: '异常点',
      type: 'scatter',
      symbolSize: 11,
      itemStyle: { color: '#ff5252', borderColor: '#fff', borderWidth: 1 },
      data: anomalies.map(a => [a.ts, a.value]),
      z: 10,
    })
  }

  // 异常事件聚类: 在曲线上以色带标出每次故障的起止区间
  const events = await fetchEvents(start, end)
  anomalyEvents.value = events
  if (events.length && series.length) {
    series[0].markArea = {
      silent: true,
      itemStyle: { color: 'rgba(255, 82, 82, 0.10)' },
      label: { color: '#ff8a80', fontSize: 10 },
      data: events.map(e => [{ name: e.id, xAxis: e.start_ts }, { xAxis: e.end_ts }]),
    }
  }

  histInst.setOption(buildBaseOption(false, series), true)
}

async function fetchEvents(start, end) {
  const q = new URLSearchParams({
    metrics: selected.value.join(','),
    instance: instance.value,
    start: String(start), end: String(end),
    threshold: String(anomalyThreshold),
  })
  const data = await fetchJson('/api/anomaly-events?' + q.toString())
  eventGap.value = data.gap_seconds ?? '-'
  return data.events || []
}

function fmtTs(ms) {
  const d = new Date(ms)
  return d.toLocaleString('zh-CN', { hour12: false })
}
function fmtDur(sec) {
  if (sec < 60) return `${Math.round(sec)}秒`
  if (sec < 3600) return `${(sec / 60).toFixed(1)}分钟`
  return `${(sec / 3600).toFixed(1)}小时`
}

// ---------- 面板 <-> 曲线双向联动 ----------
// 面板驱动曲线: 点击事件, 历史曲线缩放到该事件时间范围(前后留上下文)
function focusEvent(e) {
  activeEventId.value = e.id
  const pad = Math.max((e.end_ts - e.start_ts) * 0.3, 120_000)
  histInst.dispatchAction({
    type: 'dataZoom',
    startValue: Math.max(e.start_ts - pad, queryStartMs),
    endValue: Math.min(e.end_ts + pad, queryEndMs),
  })
}

function resetZoom() {
  activeEventId.value = null
  histInst.dispatchAction({ type: 'dataZoom', startValue: queryStartMs, endValue: queryEndMs })
}

// 曲线驱动面板: 用户在图上缩放(滑块/滚轮/程序触发)后, 同步当前可视窗口
function syncVisibleWindow() {
  const dz = histInst.getOption()?.dataZoom?.[0]
  if (!dz) return
  let s = dz.startValue, t = dz.endValue
  if (s == null || t == null) {
    // 未显式设置起止值时按百分比换算
    const span = queryEndMs - queryStartMs
    s = queryStartMs + ((dz.start ?? 0) / 100) * span
    t = queryStartMs + ((dz.end ?? 100) / 100) * span
  }
  visStart.value = s
  visEnd.value = t
}

async function fetchAnomalies(metric, start, end) {
  if (!metric) return []
  const q = new URLSearchParams({
    metric, instance: instance.value,
    start: String(start), end: String(end),
    threshold: String(anomalyThreshold),
  })
  const data = await fetchJson('/api/anomalies?' + q.toString())
  return data.anomalies || []
}

// ---------- 实时曲线 + 异常点标注 ----------
async function refreshLive() {
  if (!selected.value.length) return
  const q = new URLSearchParams({
    metrics: selected.value.join(','),
    instance: instance.value,
    window: String(liveWindow),
  })
  const data = await fetchJson('/api/latest?' + q.toString())

  const series = Object.entries(data.series || {}).map(([name, pts], idx) => ({
    name,
    type: 'line',
    smooth: true,
    showSymbol: false,
    lineStyle: { width: 2 },
    itemStyle: { color: COLORS[idx % COLORS.length] },
    data: pts.map(p => [p.ts, p.value]),
    connectNulls: true,
  }))
  liveInst.setOption(buildBaseOption(true, series), { replaceMerge: ['series'] })
}

async function refreshAnomalies() {
  // 对第一个选中指标做异常点标注
  const metric = selected.value[0]
  if (!metric) return
  const end = Math.floor(Date.now() / 1000)
  const start = end - liveWindow
  const q = new URLSearchParams({ metric, instance: instance.value, start: String(start), end: String(end), threshold: String(anomalyThreshold) })
  const data = await fetchJson('/api/anomalies?' + q.toString())
  const anomalies = data.anomalies || []
  anomalyCount.value = anomalies.length
  const scatter = {
    name: '异常点',
    type: 'scatter',
    symbolSize: 12,
    itemStyle: { color: '#ff5252', borderColor: '#fff', borderWidth: 1 },
    data: anomalies.map(a => [a.ts, a.value]),
    z: 10,
    tooltip: {
      formatter: p => `异常点<br/>值: ${p.value[1].toFixed(2)}<br/>${new Date(p.value[0]).toLocaleTimeString()}`,
    },
  }
  // 追加异常点散点序列(不清空已有曲线)
  const opt = liveInst.getOption()
  liveInst.setOption({ series: [...opt.series.filter(s => s.name !== '异常点'), scatter] })
}

function buildBaseOption(isLive, series) {
  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#1e2940',
      borderColor: '#31405e',
      textStyle: { color: '#d7dde8' },
    },
    legend: {
      data: series.map(s => s.name),
      textStyle: { color: '#8b96ad' },
      top: 0,
    },
    grid: { left: 56, right: 24, top: 40, bottom: 56 },
    xAxis: {
      type: 'time',
      axisLine: { lineStyle: { color: '#31405e' } },
      axisLabel: { color: '#7a869e' },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLine: { lineStyle: { color: '#31405e' } },
      axisLabel: { color: '#7a869e' },
      splitLine: { lineStyle: { color: '#1d273b' } },
    },
    dataZoom: isLive
      ? [{ type: 'inside' }, { type: 'slider', height: 18, bottom: 12, borderColor: '#31405e', fillerColor: 'rgba(79,195,247,0.15)' }]
      : [{ type: 'inside' }, { type: 'slider', height: 18, bottom: 12, borderColor: '#31405e', fillerColor: 'rgba(79,195,247,0.15)' }],
    series,
  }
}

function scheduleLive() {
  clearInterval(liveTimer)
  clearInterval(liveAnomalyTimer)
  if (live.value) {
    liveTimer = setInterval(refreshLive, 2000)
    liveAnomalyTimer = setInterval(refreshAnomalies, 5000)
    refreshLive()
    refreshAnomalies()
  }
}

onMounted(async () => {
  await nextTick()
  histInst = echarts.init(historyChart.value, 'dark')
  liveInst = echarts.init(liveChart.value, 'dark')
  // 曲线缩放(滑块/滚轮/点击事件定位)时同步可视窗口, 驱动面板过滤
  histInst.on('datazoom', syncVisibleWindow)
  window.addEventListener('resize', () => { histInst.resize(); liveInst.resize() })
  await loadMetrics()
  await onQuery()
  scheduleLive()
  // 指标列表每 10 秒刷新一次统计
  setInterval(loadMetrics, 10000)
})

onBeforeUnmount(() => {
  clearInterval(liveTimer)
  clearInterval(liveAnomalyTimer)
})
</script>
