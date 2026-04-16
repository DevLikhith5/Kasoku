import { useState, useEffect, useRef, useCallback } from 'react'
import { motion } from 'framer-motion'
import {
  LineChart,
  Line,
  XAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { Activity, Play, Loader2 } from 'lucide-react'

interface MetricCardData {
  label: string
  value: string
}

interface MetricPoint {
  time: string
  getRate: number
  putRate: number
  memBytes: number
  diskBytes: number
  keys: number
  activeNodes: number
  pendingHints: number
  goroutines: number
  heapAlloc: number
}

export function MetricsView({ apiBase }: { apiBase: string }) {
  const [metrics, setMetrics] = useState<MetricCardData[]>([])
  const [rawMetrics, setRawMetrics] = useState<Record<string, number>>({})
  const [chartData, setChartData] = useState<MetricPoint[]>([])
  const [loading, setLoading] = useState(true)
  const [serverDown, setServerDown] = useState(false)
  const [benchmarking, setBenchmarking] = useState(false)
  const [benchmarkPhase, setBenchmarkPhase] = useState('')
  const [benchmarkResults, setBenchmarkResults] = useState<string>('')

  const prevRef = useRef<Record<string, number>>({})

  const api = (path: string) => {
    // Use Vite proxy to avoid CORS
    return path
  }

  const runAllBenchmarks = useCallback(async () => {
    setBenchmarking(true)
    setBenchmarkPhase('write')
    setBenchmarkResults('Running network benchmark (pressure-tool)...')
    
    try {
      const res = await fetch(api('/api/v1/benchmark/network?workers=30&duration=10'), {
        method: 'POST',
      })
      if (!res.ok) throw new Error('Benchmark failed')
      
      const data = await res.json()
      setBenchmarkPhase('done')
      
      // Parse the pressure-tool output to extract key numbers
      const output = data.data.pressure_output || ''
      
      // Extract numbers
      const writes = output.match(/Writes[\s\S]*?avg\s+(\d+)/)?.[1] || 'N/A'
      const reads = output.match(/Reads[\s\S]*?avg\s+(\d+)/)?.[1] || 'N/A'
      const total = output.match(/Total[\s\S]*?(\d+)\s+ops\/sec/)?.[1] || 'N/A'
      const latency = output.match(/Latency p50[\s\S]*?([\d.]+ms)/)?.[1] || 'N/A'
      
      setBenchmarkResults(`${data.data.mode === 'cluster' ? '3-Node Cluster' : 'Single-Node'} Benchmark:

Nodes: ${data.data.nodes}
Workers: ${data.data.workers}
Duration: ${data.data.duration_sec}s

Writes:  ${writes} ops/sec
Reads:  ${reads} ops/sec  
Total: ${total} ops/sec

p50 Latency: ${latency}

See full output below.`)
    } catch (e) {
      setBenchmarkPhase('error')
      setBenchmarkResults(`Error: ${e instanceof Error ? e.message : 'Failed to run benchmark'}

Run manually:
./pressure-tool -workers=60 -write-duration=10s -read-duration=10s -batch-get=1`)
    }
    
    setBenchmarking(false)
  }, [api])

  useEffect(() => {
    let cancelled = false

    const fetchMetrics = async () => {
      try {
        const res = await fetch(api('/metrics'))
        if (!res.ok) {
          if (!cancelled) setServerDown(true)
          return
        }
        const text = await res.text()

        // Parse Prometheus text format
        const parsed: Record<string, number> = {}
        for (const line of text.split('\n')) {
          if (!line || line.startsWith('#')) continue
          // More lenient matching - split on last space
          const parts = line.trim().split(/\s+/)
          if (parts.length >= 2) {
            const key = parts[0]
            const val = parseFloat(parts[parts.length - 1])
            if (!isNaN(val)) {
              parsed[key] = val
            }
          }
        }

        if (!cancelled) {
          setServerDown(false)
          setRawMetrics(parsed)

          const goroutines = parsed['go_goroutines'] || 0
          const heapAlloc = parsed['go_memstats_heap_alloc_bytes'] || 0

          // Try to get engine metrics from /api/v1/node fallback
          let nodeData: any = null
          try {
            const nodeRes = await fetch(api('/api/v1/node'))
            if (nodeRes.ok) {
              const d = await nodeRes.json()
              nodeData = d.data
            }
          } catch { /* ignore */ }

          const keyCount = nodeData?.stats?.KeyCount ?? parsed['kasoku_storage_engine_keys_total'] ?? 0
          const diskBytes = nodeData?.stats?.DiskBytes ?? parsed['kasoku_storage_engine_bytes{type="disk"}'] ?? 0
          const memBytes = nodeData?.stats?.MemBytes ?? parsed['kasoku_storage_engine_bytes{type="memory"}'] ?? 0

          const activeNodes = parsed['kasoku_cluster_nodes_active'] ?? 1
          const pendingHints = parsed['kasoku_cluster_pending_hints'] ?? 0

          // Try different label orderings since Prometheus sorts labels alphabetically
          const getReqs = 
            parsed['kasoku_requests_total{operation="get",status="success"}'] ||
            parsed['kasoku_requests_total{status="success",operation="get"}'] || 0
          const putReqs = 
            parsed['kasoku_requests_total{operation="put",status="success"}'] ||
            parsed['kasoku_requests_total{status="success",operation="put"}'] || 0

          const prevGets = prevRef.current['gets'] ?? getReqs
          const prevPuts = prevRef.current['puts'] ?? putReqs

          prevRef.current['gets'] = getReqs
          prevRef.current['puts'] = putReqs

          // Polling every 5 seconds, so operations per second is delta / 5.
          // On first fetch (prev == current), rate will be 0. Use raw values for initial display.
          const getDelta = getReqs - prevGets
          const putDelta = putReqs - prevPuts
          const getRate = getDelta >= 0 ? getDelta / 5 : getReqs / 5
          const putRate = putDelta >= 0 ? putDelta / 5 : putReqs / 5

          const cards: MetricCardData[] = [
            { label: 'Keys', value: keyCount.toLocaleString() },
            { label: 'Disk Used', value: diskBytes > 1024 * 1024 ? `${(diskBytes / (1024 * 1024)).toFixed(1)} MB` : `${(diskBytes / 1024).toFixed(0)} KB` },
            { label: 'MemTable', value: memBytes > 1024 * 1024 ? `${(memBytes / (1024 * 1024)).toFixed(1)} MB` : `${memBytes} B` },
            { label: 'Cluster Nodes', value: String(activeNodes) },
            { label: 'Goroutines', value: goroutines.toFixed(0) },
            { label: 'Heap Alloc', value: heapAlloc > 1024 * 1024 ? `${(heapAlloc / (1024 * 1024)).toFixed(1)} MB` : `${(heapAlloc / 1024).toFixed(0)} KB` },
          ]

          setMetrics(cards)

          const newPoint: MetricPoint = {
            time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            getRate,
            putRate,
            memBytes,
            diskBytes,
            keys: keyCount,
            activeNodes,
            pendingHints,
            goroutines,
            heapAlloc,
          }

          setChartData(prev => [...prev, newPoint].slice(-60)) // Rolling window of 5 minutes (60 * 5s)
        }
      } catch {
        if (!cancelled) setServerDown(true)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    fetchMetrics()
    const interval = setInterval(fetchMetrics, 5000)
    return () => { cancelled = true; clearInterval(interval) }
  }, [apiBase])

  if (loading) {
    return (
      <div className="metrics">
        <div className="metrics-header">
          <h1 className="metrics-title">Metrics</h1>
          <p className="metrics-subtitle">Real-time performance data from the Go runtime and LSM engine.</p>
        </div>
        <div className="metrics-empty">Loading metrics…</div>
      </div>
    )
  }

  if (serverDown) {
    return (
      <div className="metrics">
        <div className="metrics-header">
          <h1 className="metrics-title">Metrics</h1>
          <p className="metrics-subtitle">Real-time performance data from the Go runtime and LSM engine.</p>
        </div>
        <div className="metrics-offline-card">
          <Activity size={24} />
          <div className="metrics-offline-body">
            <h3>Server Unreachable</h3>
            <p>
              The Kasoku server appears to be offline. Start it with:
            </p>
            <code className="metrics-offline-cmd">KASOKU_CONFIG=kasoku.yaml ./kasoku-server</code>
            <p className="metrics-offline-hint">
              Metrics are exposed at <code>/metrics</code> in Prometheus format.
            </p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="metrics">
      <div className="metrics-header">
        <h1 className="metrics-title">Metrics & Benchmark</h1>
        <p className="metrics-subtitle">
          Real-time performance data from the Go runtime and LSM engine.
        </p>
        
        <div className="benchmark-buttons" style={{ display: 'flex', gap: '12px', marginTop: '16px', marginBottom: '16px' }}>
          <button 
            className="btn btn-primary" 
            onClick={runAllBenchmarks}
            disabled={benchmarking}
            style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: '180px' }}
          >
            {benchmarking ? (
              <>
                <Loader2 className="spin" size={16} style={{ animation: 'spin 1s linear infinite' }} />
                {benchmarkPhase === 'init' && 'Starting...'}
                {benchmarkPhase === 'write' && 'Writes...'}
                {benchmarkPhase === 'read' && 'Reads...'}
                {benchmarkPhase === 'batch' && 'Batch...'}
                {benchmarkPhase === 'done' && 'Done!'}
                {benchmarkPhase === 'error' && 'Error'}
              </>
            ) : (
              <>
                <Play size={16} />
                Run Full Benchmark
              </>
            )}
          </button>
        </div>
        
        {benchmarkResults && (
          <pre className="benchmark-results" style={{ background: 'var(--bg-secondary)', padding: '16px', borderRadius: '8px', fontSize: '12px', marginBottom: '16px', whiteSpace: 'pre-wrap' }}>
            {benchmarkResults}
          </pre>
        )}
      </div>

      {metrics.length > 0 && (
        <div className="metrics-cards">
          {metrics.map((metric, i) => (
            <motion.div
              key={metric.label}
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.05 }}
              className="metric-card"
            >
              <span className="metric-label">{metric.label}</span>
              <span className="metric-value">{metric.value}</span>
            </motion.div>
          ))}
        </div>
      )}

      {chartData.length > 0 && (
        <div className="metrics-stages">
          {/* Stage 1: API & Network */}
          <div className="metrics-stage">
            <h2 className="metrics-stage-title">Stage 1: API &amp; Network</h2>
            <div className="metrics-charts">
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.1 }} className="metrics-chart">
                <h3>Throughput (Ops/sec)</h3>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={chartData}>
                    <XAxis dataKey="time" tick={{ fontSize: 10, fill: 'var(--text-muted)', fontFamily: 'var(--font-sans)' }} axisLine={false} tickLine={false} />
                    <Tooltip contentStyle={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '8px', fontSize: '12px', fontFamily: 'var(--font-sans)', boxShadow: 'var(--shadow-lg)' }} />
                    <Line type="monotone" dataKey="getRate" stroke="#10b981" strokeWidth={1.5} dot={false} name="GET Rate" isAnimationActive={false} />
                    <Line type="monotone" dataKey="putRate" stroke="#e11d5a" strokeWidth={1.5} dot={false} name="PUT Rate" isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </motion.div>
            </div>
          </div>

          {/* Stage 2: LSM Storage Engine */}
          <div className="metrics-stage">
            <h2 className="metrics-stage-title">Stage 2: LSM Storage Engine</h2>
            <div className="metrics-charts">
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.2 }} className="metrics-chart">
                <h3>Active Keys</h3>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={chartData}>
                    <XAxis dataKey="time" tick={{ fontSize: 10, fill: 'var(--text-muted)', fontFamily: 'var(--font-sans)' }} axisLine={false} tickLine={false} />
                    <Tooltip contentStyle={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '8px', fontSize: '12px', fontFamily: 'var(--font-sans)', boxShadow: 'var(--shadow-lg)' }} />
                    <Line type="monotone" dataKey="keys" stroke="#a855f7" strokeWidth={1.5} dot={false} name="Keys" isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </motion.div>
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.3 }} className="metrics-chart">
                <h3>Storage Size (Bytes)</h3>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={chartData}>
                    <XAxis dataKey="time" tick={{ fontSize: 10, fill: 'var(--text-muted)', fontFamily: 'var(--font-sans)' }} axisLine={false} tickLine={false} />
                    <Tooltip contentStyle={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '8px', fontSize: '12px', fontFamily: 'var(--font-sans)', boxShadow: 'var(--shadow-lg)' }} />
                    <Line type="monotone" dataKey="memBytes" stroke="#3b82f6" strokeWidth={1.5} dot={false} name="MemTable" isAnimationActive={false} />
                    <Line type="monotone" dataKey="diskBytes" stroke="#f59e0b" strokeWidth={1.5} dot={false} name="Disk" isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </motion.div>
            </div>
          </div>

          {/* Stage 3: Cluster Operations */}
          <div className="metrics-stage">
            <h2 className="metrics-stage-title">Stage 3: Cluster Operations</h2>
            <div className="metrics-charts">
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.4 }} className="metrics-chart">
                <h3>Membership &amp; Gossip</h3>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={chartData}>
                    <XAxis dataKey="time" tick={{ fontSize: 10, fill: 'var(--text-muted)', fontFamily: 'var(--font-sans)' }} axisLine={false} tickLine={false} />
                    <Tooltip contentStyle={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '8px', fontSize: '12px', fontFamily: 'var(--font-sans)', boxShadow: 'var(--shadow-lg)' }} />
                    <Line type="monotone" dataKey="activeNodes" stroke="#10b981" strokeWidth={1.5} dot={false} name="Active Nodes" isAnimationActive={false} />
                    <Line type="monotone" dataKey="pendingHints" stroke="#ef4444" strokeWidth={1.5} dot={false} name="Pending Hints" isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </motion.div>
            </div>
          </div>

          {/* Stage 4: Go Runtime */}
          <div className="metrics-stage">
            <h2 className="metrics-stage-title">Stage 4: Go Runtime</h2>
            <div className="metrics-charts">
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.5 }} className="metrics-chart">
                <h3>Goroutines</h3>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={chartData}>
                    <XAxis dataKey="time" tick={{ fontSize: 10, fill: 'var(--text-muted)', fontFamily: 'var(--font-sans)' }} axisLine={false} tickLine={false} />
                    <Tooltip contentStyle={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '8px', fontSize: '12px', fontFamily: 'var(--font-sans)', boxShadow: 'var(--shadow-lg)' }} />
                    <Line type="monotone" dataKey="goroutines" stroke="#e11d5a" strokeWidth={1.5} dot={false} name="Goroutines" isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </motion.div>

              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.6 }} className="metrics-chart">
                <h3>Heap Alloc (Bytes)</h3>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={chartData}>
                    <XAxis dataKey="time" tick={{ fontSize: 10, fill: 'var(--text-muted)', fontFamily: 'var(--font-sans)' }} axisLine={false} tickLine={false} />
                    <Tooltip contentStyle={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: '8px', fontSize: '12px', fontFamily: 'var(--font-sans)', boxShadow: 'var(--shadow-lg)' }} />
                    <Line type="monotone" dataKey="heapAlloc" stroke="#a1a1aa" strokeWidth={1.5} dot={false} name="Heap Alloc" isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </motion.div>
            </div>
          </div>
        </div>
      )}

      {Object.keys(rawMetrics).length > 0 && (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.7 }} className="metrics-raw">
          <h3>All Metrics</h3>
          <details className="metrics-raw-details">
            <summary>Expand raw Prometheus metrics</summary>
            <pre className="metrics-raw-pre">
              {Object.entries(rawMetrics).sort((a, b) => a[0].localeCompare(b[0])).map(([k, v]) => `${k}  ${v}`).join('\n')}
            </pre>
          </details>
        </motion.div>
      )}
    </div>
  )
}
