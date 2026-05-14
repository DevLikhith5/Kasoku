import { useState, useEffect, useRef, useCallback } from 'react'
import { motion } from 'framer-motion'
import {
  LineChart, Line, ResponsiveContainer
} from 'recharts'
import {
  Activity, AlertTriangle, CheckCircle, XCircle,
  Database, Server, Zap, Clock, ExternalLink,
  TrendingUp, TrendingDown, Minus
} from 'lucide-react'

interface TimePoint {
  time: string
  value: number
}

interface MetricCardProps {
  label: string
  value: string
  delta?: 'up' | 'down' | 'neutral'
  deltaValue?: string
  icon: React.ReactNode
  color?: string
  alert?: boolean
}

function Sparkline({ data, color = '#10b981', height = 40 }: { data: TimePoint[]; color?: string; height?: number }) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={data}>
        <Line
          type="monotone"
          dataKey="value"
          stroke={color}
          strokeWidth={1.5}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}

function MetricCard({ label, value, delta, deltaValue, icon, color = '#10b981', alert }: MetricCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      className={`metric-card ${alert ? 'metric-card-alert' : ''}`}
      style={{ borderLeft: `3px solid ${color}` }}
    >
      <div className="metric-card-header">
        <span className="metric-card-icon" style={{ color }}>{icon}</span>
        {alert && <AlertTriangle size={14} color="#ef4444" />}
      </div>
      <span className="metric-card-value">{value}</span>
      <span className="metric-card-label">{label}</span>
      {delta && (
        <span className={`metric-card-delta ${delta}`}>
          {delta === 'up' ? <TrendingUp size={12} /> : delta === 'down' ? <TrendingDown size={12} /> : <Minus size={12} />}
          {deltaValue}
        </span>
      )}
    </motion.div>
  )
}

interface AlertRule {
  name: string
  severity: 'critical' | 'warning' | 'info'
  state: 'firing' | 'pending' | 'resolved'
  message: string
}

export function ObservabilityPanel({ apiBase }: { apiBase: string }) {
  const [loading, setLoading] = useState(true)
  const [serverDown, setServerDown] = useState(false)
  const [rawMetrics, setRawMetrics] = useState<Record<string, number>>({})
  const [history, setHistory] = useState<Record<string, TimePoint[]>>({})
  const [alerts, setAlerts] = useState<AlertRule[]>([])
  const prevRef = useRef<Record<string, number>>({})

  const api = (path: string) => path

  const deriveAlerts = useCallback((parsed: Record<string, number>): AlertRule[] => {
    const derived: AlertRule[] = []
    const getTotal = parsed['kasoku_requests_total{operation="get",status="success"}'] || 0
    const putTotal = parsed['kasoku_requests_total{operation="put",status="success"}'] || 0
    const getErrors = parsed['kasoku_requests_total{operation="get",status="error"}'] || 0
    const putErrors = parsed['kasoku_requests_total{operation="put",status="error"}'] || 0
    const totalReqs = getTotal + putTotal
    const totalErrors = getErrors + putErrors

    if (totalReqs > 0 && totalErrors / totalReqs > 0.01) {
      derived.push({
        name: 'KasokuHighErrorRate',
        severity: 'warning',
        state: 'firing',
        message: `Error rate is ${((totalErrors / totalReqs) * 100).toFixed(1)}%`,
      })
    }

    const nodes = parsed['kasoku_cluster_nodes_active'] ?? 1
    if (nodes < 2) {
      derived.push({
        name: 'KasokuClusterNodeCountLow',
        severity: 'critical',
        state: 'firing',
        message: `Only ${nodes} node(s) active`,
      })
    }

    const hints = parsed['kasoku_cluster_pending_hints'] ?? 0
    if (hints > 100) {
      derived.push({
        name: 'KasokuHighPendingHints',
        severity: 'warning',
        state: 'firing',
        message: `${hints} pending hinted handoffs`,
      })
    }

    const goroutines = parsed['go_goroutines'] || 0
    if (goroutines > 5000) {
      derived.push({
        name: 'KasokuHighGoroutineCount',
        severity: 'warning',
        state: 'firing',
        message: `${goroutines} goroutines running`,
      })
    }

    return derived
  }, [])

  const pushHistory = useCallback((key: string, value: number) => {
    setHistory(prev => {
      const arr = prev[key] ? [...prev[key]] : []
      const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
      arr.push({ time, value })
      return { ...prev, [key]: arr.slice(-30) }
    })
  }, [])

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
        const parsed: Record<string, number> = {}
        for (const line of text.split('\n')) {
          if (!line || line.startsWith('#')) continue
          const parts = line.trim().split(/\s+/)
          if (parts.length >= 2) {
            const key = parts[0]
            const val = parseFloat(parts[parts.length - 1])
            if (!isNaN(val)) parsed[key] = val
          }
        }

        if (!cancelled) {
          setServerDown(false)
          setRawMetrics(parsed)
          setAlerts(deriveAlerts(parsed))

          // Push to history for sparklines
          const getReqs = parsed['kasoku_requests_total{operation="get",status="success"}'] || 0
          const putReqs = parsed['kasoku_requests_total{operation="put",status="success"}'] || 0
          const prevGets = prevRef.current['gets'] ?? getReqs
          const prevPuts = prevRef.current['puts'] ?? putReqs
          prevRef.current['gets'] = getReqs
          prevRef.current['puts'] = putReqs
          pushHistory('getRate', Math.max(0, (getReqs - prevGets) / 5))
          pushHistory('putRate', Math.max(0, (putReqs - prevPuts) / 5))
          pushHistory('goroutines', parsed['go_goroutines'] || 0)
          pushHistory('heapAlloc', parsed['go_memstats_heap_alloc_bytes'] || 0)
          pushHistory('keys', parsed['kasoku_storage_engine_keys_total'] || 0)
          pushHistory('pendingHints', parsed['kasoku_cluster_pending_hints'] || 0)
          pushHistory('activeNodes', parsed['kasoku_cluster_nodes_active'] || 1)
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
  }, [apiBase, deriveAlerts, pushHistory])

  if (loading) {
    return (
      <div className="metrics">
        <div className="metrics-header">
          <h1 className="metrics-title">Observability</h1>
          <p className="metrics-subtitle">Real-time health, metrics, and alerting.</p>
        </div>
        <div className="metrics-empty">Loading observability data…</div>
      </div>
    )
  }

  if (serverDown) {
    return (
      <div className="metrics">
        <div className="metrics-header">
          <h1 className="metrics-title">Observability</h1>
          <p className="metrics-subtitle">Real-time health, metrics, and alerting.</p>
        </div>
        <div className="metrics-offline-card">
          <Activity size={24} />
          <div className="metrics-offline-body">
            <h3>Server Unreachable</h3>
            <p>Start the server with metrics enabled to see observability data.</p>
            <code className="metrics-offline-cmd">KASOKU_CONFIG=kasoku.yaml ./kasoku-server</code>
          </div>
        </div>
      </div>
    )
  }

  const firingCritical = alerts.filter(a => a.state === 'firing' && a.severity === 'critical').length
  const firingWarning = alerts.filter(a => a.state === 'firing' && a.severity === 'warning').length
  const overallHealth = firingCritical > 0 ? 'critical' : firingWarning > 0 ? 'warning' : 'healthy'

  const getRateHist = history['getRate'] || []
  const putRateHist = history['putRate'] || []
  const goroutineHist = history['goroutines'] || []
  const heapHist = history['heapAlloc'] || []
  const keysHist = history['keys'] || []
  const hintsHist = history['pendingHints'] || []
  const nodesHist = history['activeNodes'] || []

  const totalReqs = (rawMetrics['kasoku_requests_total{operation="get",status="success"}'] || 0)
    + (rawMetrics['kasoku_requests_total{operation="put",status="success"}'] || 0)
  const totalErrors = (rawMetrics['kasoku_requests_total{operation="get",status="error"}'] || 0)
    + (rawMetrics['kasoku_requests_total{operation="put",status="error"}'] || 0)
  const availability = totalReqs > 0 ? ((1 - totalErrors / totalReqs) * 100).toFixed(3) : '100.000'

  return (
    <div className="metrics">
      <div className="metrics-header">
        <h1 className="metrics-title">Observability</h1>
        <p className="metrics-subtitle">Real-time health, metrics, SLOs, and external integrations.</p>
      </div>

      {/* Health Banner */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className={`health-banner health-banner--${overallHealth}`}
      >
        {overallHealth === 'healthy' && <CheckCircle size={20} />}
        {overallHealth === 'warning' && <AlertTriangle size={20} />}
        {overallHealth === 'critical' && <XCircle size={20} />}
        <span>
          {overallHealth === 'healthy' && 'All systems operational'}
          {overallHealth === 'warning' && `${firingWarning} warning(s) active`}
          {overallHealth === 'critical' && `${firingCritical} critical alert(s) firing`}
        </span>
      </motion.div>

      {/* Alerts Section */}
      {alerts.length > 0 && (
        <div className="alerts-section">
          <h3>Active Alerts</h3>
          {alerts.map((alert, i) => (
            <motion.div
              key={alert.name + i}
              initial={{ opacity: 0, x: -8 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: i * 0.05 }}
              className={`alert-row alert-row--${alert.severity}`}
            >
              {alert.severity === 'critical' ? <XCircle size={14} /> : <AlertTriangle size={14} />}
              <span className="alert-name">{alert.name}</span>
              <span className="alert-msg">{alert.message}</span>
              <span className={`alert-badge alert-badge--${alert.severity}`}>{alert.severity}</span>
            </motion.div>
          ))}
        </div>
      )}

      {/* Top Metric Cards */}
      <div className="metrics-cards" style={{ marginTop: 16 }}>
        <MetricCard
          label="Availability"
          value={`${availability}%`}
          icon={<CheckCircle size={16} />}
          color={parseFloat(availability) >= 99.9 ? '#10b981' : parseFloat(availability) >= 99 ? '#f59e0b' : '#ef4444'}
          alert={parseFloat(availability) < 99}
        />
        <MetricCard
          label="Keys"
          value={String(Math.round(rawMetrics['kasoku_storage_engine_keys_total'] || 0)).toLocaleString()}
          icon={<Database size={16} />}
          color="#a855f7"
        />
        <MetricCard
          label="Cluster Nodes"
          value={String(Math.round(rawMetrics['kasoku_cluster_nodes_active'] || 1))}
          icon={<Server size={16} />}
          color="#3b82f6"
          alert={(rawMetrics['kasoku_cluster_nodes_active'] || 1) < 2}
        />
        <MetricCard
          label="Pending Hints"
          value={String(Math.round(rawMetrics['kasoku_cluster_pending_hints'] || 0))}
          icon={<Clock size={16} />}
          color="#f59e0b"
          alert={(rawMetrics['kasoku_cluster_pending_hints'] || 0) > 100}
        />
        <MetricCard
          label="Goroutines"
          value={String(Math.round(rawMetrics['go_goroutines'] || 0))}
          icon={<Zap size={16} />}
          color="#e11d5a"
          alert={(rawMetrics['go_goroutines'] || 0) > 5000}
        />
        <MetricCard
          label="Heap Alloc"
          value={`${((rawMetrics['go_memstats_heap_alloc_bytes'] || 0) / (1024 * 1024)).toFixed(1)} MB`}
          icon={<Activity size={16} />}
          color="#6366f1"
        />
      </div>

      {/* Sparkline Charts */}
      <div className="metrics-stages">
        {/* Request Rate */}
        <div className="metrics-stage">
          <h2 className="metrics-stage-title">Request Rate</h2>
          <div className="metrics-charts">
            <div className="metrics-chart">
              <h3>GET Rate (ops/sec)</h3>
              <Sparkline data={getRateHist} color="#10b981" height={80} />
            </div>
            <div className="metrics-chart">
              <h3>PUT Rate (ops/sec)</h3>
              <Sparkline data={putRateHist} color="#e11d5a" height={80} />
            </div>
          </div>
        </div>

        {/* LSM Engine */}
        <div className="metrics-stage">
          <h2 className="metrics-stage-title">LSM Engine</h2>
          <div className="metrics-charts">
            <div className="metrics-chart">
              <h3>Active Keys</h3>
              <Sparkline data={keysHist} color="#a855f7" height={80} />
            </div>
            <div className="metrics-chart">
              <h3>L0 SSTables</h3>
              <Sparkline data={history['l0SSTables'] || []} color="#f59e0b" height={80} />
            </div>
          </div>
        </div>

        {/* Cluster Health */}
        <div className="metrics-stage">
          <h2 className="metrics-stage-title">Cluster Health</h2>
          <div className="metrics-charts">
            <div className="metrics-chart">
              <h3>Active Nodes</h3>
              <Sparkline data={nodesHist} color="#10b981" height={80} />
            </div>
            <div className="metrics-chart">
              <h3>Pending Hints</h3>
              <Sparkline data={hintsHist} color="#ef4444" height={80} />
            </div>
          </div>
        </div>

        {/* Go Runtime */}
        <div className="metrics-stage">
          <h2 className="metrics-stage-title">Go Runtime</h2>
          <div className="metrics-charts">
            <div className="metrics-chart">
              <h3>Goroutines</h3>
              <Sparkline data={goroutineHist} color="#e11d5a" height={80} />
            </div>
            <div className="metrics-chart">
              <h3>Heap Alloc (MB)</h3>
              <Sparkline data={heapHist.map(h => ({ time: h.time, value: h.value / (1024 * 1024) }))} color="#6366f1" height={80} />
            </div>
          </div>
        </div>
      </div>

      {/* External Links */}
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.4 }}
        className="external-links"
        style={{ marginTop: 24 }}
      >
        <h3 style={{ marginBottom: 12 }}>External Monitoring</h3>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          <a href="http://localhost:3000/d/kasoku-overview" target="_blank" rel="noopener noreferrer" className="external-link-card">
            <BarChartIcon size={16} />
            <span>Grafana</span>
            <ExternalLink size={12} />
          </a>
          <a href="http://localhost:9090" target="_blank" rel="noopener noreferrer" className="external-link-card">
            <Activity size={16} />
            <span>Prometheus</span>
            <ExternalLink size={12} />
          </a>
          <a href="http://localhost:16686" target="_blank" rel="noopener noreferrer" className="external-link-card">
            <Zap size={16} />
            <span>Jaeger Traces</span>
            <ExternalLink size={12} />
          </a>
          <a href="http://localhost:9093" target="_blank" rel="noopener noreferrer" className="external-link-card">
            <AlertTriangle size={16} />
            <span>Alertmanager</span>
            <ExternalLink size={12} />
          </a>
        </div>
      </motion.div>

      {/* Raw Metrics Toggle */}
      {Object.keys(rawMetrics).length > 0 && (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.6 }} className="metrics-raw">
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

// Alias for BarChart icon to avoid conflict with recharts
function BarChartIcon({ size }: { size: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="20" x2="12" y2="10" />
      <line x1="18" y1="20" x2="18" y2="4" />
      <line x1="6" y1="20" x2="6" y2="16" />
    </svg>
  )
}
