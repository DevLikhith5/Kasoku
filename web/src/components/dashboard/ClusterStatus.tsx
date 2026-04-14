import { useState, useEffect, useMemo } from 'react'
import { motion } from 'framer-motion'
import { CircleDot, KeyRound, Layers, Server } from 'lucide-react'
import { HashRing, type KeyRoute } from '../../lib/hashRing'

interface NodeInfo {
  id: string
  addr: string
  status: 'alive' | 'suspected' | 'dead'
  phi: number
}

const VNODE_COUNT = 150
const REPLICA_COUNT = 3

export function ClusterStatus({ apiBase }: { apiBase: string }) {
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [clusterDisabled, setClusterDisabled] = useState(false)
  const [testKey, setTestKey] = useState('')
  const [activeTab, setActiveTab] = useState<'ring' | 'distribution'>('ring')

  const api = (path: string) => {
    if (apiBase.startsWith('http')) return `${apiBase}${path}`
    return path
  }

  // Build hash ring from REAL nodes only
  const ring = useMemo(() => {
    const r = new HashRing(VNODE_COUNT)
    nodes.forEach((n) => r.addNode(n.id))
    return r
  }, [nodes])

  // Compute key routing
  const route = useMemo<KeyRoute | null>(() => {
    if (!testKey.trim() || ring.realNodeCount === 0) return null
    return ring.routeKey(testKey.trim(), Math.min(REPLICA_COUNT, ring.realNodeCount))
  }, [testKey, ring])

  // VNode distribution stats
  const distribution = useMemo(() => {
    const dist = ring.distribution()
    return Array.from(dist.entries()).map(([nodeId, count]) => ({
      nodeId,
      vnodeCount: count,
      percentage: ring.totalVNodes > 0 ? ((count / ring.totalVNodes) * 100).toFixed(1) : '0',
    }))
  }, [ring])

  useEffect(() => {
    let cancelled = false

    const fetchCluster = async () => {
      try {
        const res = await fetch(api('/api/v1/cluster/status'))

        if (res.status === 404) {
          // Cluster mode disabled — fall back to single-node info
          if (!cancelled) {
            setClusterDisabled(true)
            try {
              const nodeRes = await fetch(api('/api/v1/node'))
              if (nodeRes.ok && !cancelled) {
                const data = await nodeRes.json()
                const nd = data.data
                setNodes([{
                  id: nd.node_id || nd.addr || 'node-1',
                  addr: nd.addr || nd.node_id || 'http://localhost:9000',
                  status: 'alive',
                  phi: 0,
                }])
              }
            } catch { /* ignore */ }
          }
        } else if (res.ok) {
          const data = await res.json()
          if (!cancelled) {
            const raw = data.data || data

            // API returns: { nodes: string[], ring_distribution: {[url]: pct}, node_id, node_addr }
            const nodeUrls: string[] = raw.nodes || []
            const dist: Record<string, number> = raw.ring_distribution || {}

            if (nodeUrls.length > 0) {
              const parsed: NodeInfo[] = nodeUrls.map((url: string) => ({
                id: url,
                addr: url,
                // All nodes in the ring are considered alive; phi is not yet exposed by the API
                status: 'alive' as const,
                phi: dist[url] ?? 0,
              }))
              setNodes(parsed)
            } else {
              // Fallback: try members array format
              const members = raw.members || []
              const parsed: NodeInfo[] = members.map((m: any) => ({
                id: m.node_id || m.id || 'unknown',
                addr: m.addr || m.address || m.node_addr || '',
                status: (m.status === 'alive' || m.healthy === true) ? 'alive' : m.status === 'suspected' ? 'suspected' : 'dead',
                phi: m.phi || 0,
              }))
              setNodes(parsed)
            }
          }
        }
      } catch {
        if (!cancelled) setClusterDisabled(true)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    fetchCluster()
    const interval = setInterval(fetchCluster, 5000) // poll every 5s
    return () => { cancelled = true; clearInterval(interval) }
  }, [apiBase])

  if (loading) {
    return (
      <div className="cluster">
        <div className="cluster-header">
          <h1 className="cluster-title">Cluster Status</h1>
          <p className="cluster-subtitle">Masterless peer-to-peer topology with gossip-based membership.</p>
        </div>
        <div className="cluster-loading">Loading cluster state…</div>
      </div>
    )
  }

  return (
    <div className="cluster">
      <div className="cluster-header">
        <h1 className="cluster-title">Cluster Status</h1>
        <p className="cluster-subtitle">
          Consistent hash ring with {VNODE_COUNT} virtual nodes per real node.
          {clusterDisabled && nodes.length <= 1 && (
            <span className="cluster-mode-badge">Single-Node Mode</span>
          )}
        </p>
      </div>

      {/* Key routing test */}
      <div className="ring-test-key">
        <div className="ring-test-key-icon">
          <KeyRound size={16} />
        </div>
        <input
          className="ring-test-input"
          placeholder="Enter a key to see its routing (e.g. user:1001)"
          value={testKey}
          onChange={(e) => setTestKey(e.target.value)}
        />
        {route && ring.realNodeCount > 0 && (
          <motion.div
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            className="ring-route-info"
          >
            <div className="ring-route-hash">
              <span className="ring-route-label">CRC32 Hash</span>
              <code>{route.hashPosition >>> 0}</code>
            </div>
            <div className="ring-route-primary">
              <span className="ring-route-label">Primary VNode</span>
              <code>{route.vnode.nodeId}#vnode{route.vnode.index}</code>
              <span className="ring-route-vnode-pos">{route.vnode.position}</span>
            </div>
            <div className="ring-route-replicas">
              <span className="ring-route-label">Replicas</span>
              {route.replicas.map((r) => (
                <span key={r} className="ring-route-replica-badge">{r}</span>
              ))}
            </div>
          </motion.div>
        )}
      </div>

      {/* Empty state when no nodes */}
      {ring.realNodeCount === 0 && (
        <div className="metrics-offline-card">
          <Server size={24} />
          <div className="metrics-offline-body">
            <h3>No Nodes Detected</h3>
            <p>
              The cluster appears to have no active members. Make sure your Kasoku server is running with cluster mode enabled.
            </p>
            <code className="metrics-offline-cmd">KASOKU_CONFIG=kasoku.yaml ./kasoku-server</code>
          </div>
        </div>
      )}

      {ring.realNodeCount > 0 && (
        <>
          {/* Tab selector */}
          <div className="ring-tabs">
            <button
              className={`ring-tab ${activeTab === 'ring' ? 'active' : ''}`}
              onClick={() => setActiveTab('ring')}
            >
              <CircleDot size={15} /> Hash Ring
            </button>
            <button
              className={`ring-tab ${activeTab === 'distribution' ? 'active' : ''}`}
              onClick={() => setActiveTab('distribution')}
            >
              <Layers size={15} /> VNode Distribution
            </button>
          </div>

          {activeTab === 'ring' && (
            <div className="cluster-ring-viz">
              <HashRingVisual ring={ring} highlightedRoute={route} />
            </div>
          )}

          {activeTab === 'distribution' && (
            <div className="cluster-distribution">
              <div className="distribution-header">
                <h2>VNode Distribution Across Nodes</h2>
                <span className="distribution-stats">
                  {ring.totalVNodes} vnodes · {ring.realNodeCount} node{ring.realNodeCount !== 1 ? 's' : ''} · {VNODE_COUNT} vnodes/node
                </span>
              </div>
              <div className="distribution-bars">
                {distribution.map((d) => (
                  <div key={d.nodeId} className="distribution-bar-row">
                    <div className="distribution-bar-label">
                      <code>{d.nodeId}</code>
                      <span>{d.vnodeCount} vnodes ({d.percentage}%)</span>
                    </div>
                    <div className="distribution-bar-track">
                      <div
                        className="distribution-bar-fill"
                        style={{ width: `${d.percentage}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Node table */}
          <div className="cluster-table">
            <h2>Member List</h2>
            <table>
              <thead>
                <tr>
                  <th>Node ID</th>
                  <th>Address</th>
                  <th>Status</th>
                  <th>Distribution</th>
                </tr>
              </thead>
              <tbody>
                {nodes.map((node) => (
                  <tr key={node.id}>
                    <td><code>{node.id}</code></td>
                    <td>{node.addr}</td>
                    <td><span className={`status-badge ${node.status}`}>{node.status}</span></td>
                    <td>{node.phi.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

/**
 * Renders the hash ring as an SVG circle with vnodes and nodes.
 */
function HashRingVisual({
  ring,
  highlightedRoute,
}: {
  ring: HashRing
  highlightedRoute: KeyRoute | null
}) {
  const cx = 200
  const cy = 200
  const radius = 170
  const vnodeRadius = 185
  const nodeRadius = 155

  // Sample vnodes for display (too many to render all)
  const sampledVnodes = ring.sampledVnodes(300)

  // Compute angle for a position on the ring (0..2^32 maps to 0..360)
  const posToAngle = (pos: number) => {
    const fraction = pos / 2 ** 32
    return fraction * 2 * Math.PI - Math.PI / 2
  }

  // Unique node colors
  const nodeColors: Record<string, string> = {}
  const palette = ['#e11d5a', '#f43f5e', '#fb7185', '#fda4af', '#10b981', '#f59e0b', '#3b82f6', '#a855f7']
  const uniqueNodes = Array.from(ring.nodes)
  uniqueNodes.forEach((id, i) => {
    nodeColors[id] = palette[i % palette.length]
  })

  // Highlighted key position
  const keyAngle = highlightedRoute ? posToAngle(highlightedRoute.hashPosition) : null

  return (
    <div className="hash-ring-container">
      <svg viewBox="0 0 400 400" className="hash-ring-svg">
        {/* Ring circle */}
        <circle
          cx={cx}
          cy={cy}
          r={radius}
          fill="none"
          stroke="var(--border)"
          strokeWidth="1.5"
          strokeDasharray="4 4"
        />

        {/* Vnode markers */}
        {sampledVnodes.map((vnode) => {
          const angle = posToAngle(vnode.position)
          const x = cx + vnodeRadius * Math.cos(angle)
          const y = cy + vnodeRadius * Math.sin(angle)
          const color = nodeColors[vnode.nodeId] || '#a8a29e'
          const isHighlighted = highlightedRoute?.vnode.index === vnode.index &&
            highlightedRoute?.vnode.nodeId === vnode.nodeId

          return (
            <g key={`${vnode.nodeId}-${vnode.index}`}>
              <circle
                cx={x}
                cy={y}
                r={isHighlighted ? 4 : 2}
                fill={isHighlighted ? '#ef4444' : color}
                opacity={isHighlighted ? 1 : 0.5}
              />
              {isHighlighted && (
                <circle
                  cx={x}
                  cy={y}
                  r={8}
                  fill="none"
                  stroke="#ef4444"
                  strokeWidth="1"
                  opacity={0.6}
                />
              )}
            </g>
          )
        })}

        {/* Real node labels on inner ring */}
        {Array.from(ring.nodes).map((nodeId) => {
          const dist = ring.distribution()
          const count = dist.get(nodeId) || 0
          const nodeVnodes = ring.vnodes.filter((v: { nodeId: string }) => v.nodeId === nodeId)
          const avgPos = nodeVnodes.reduce((s: number, v: { position: number }) => s + v.position, 0) / nodeVnodes.length
          const angle = posToAngle(avgPos)
          const x = cx + nodeRadius * Math.cos(angle)
          const y = cy + nodeRadius * Math.sin(angle)

          return (
            <g key={`node-${nodeId}`}>
              <circle
                cx={x}
                cy={y}
                r={14}
                fill="var(--bg-subtle)"
                stroke={nodeColors[nodeId]}
                strokeWidth="2"
              />
              <text
                x={x}
                y={y}
                textAnchor="middle"
                dominantBaseline="central"
                fill="var(--text-primary)"
                fontSize="9"
                fontFamily="var(--font-mono)"
                fontWeight="600"
              >
                {nodeId.replace('node-', 'n')}
              </text>
              <text
                x={x}
                y={y + 22}
                textAnchor="middle"
                dominantBaseline="central"
                fill="var(--text-muted)"
                fontSize="7"
                fontFamily="var(--font-sans)"
              >
                {count} vn
              </text>
            </g>
          )
        })}

        {/* Key position indicator */}
        {keyAngle !== null && highlightedRoute && (() => {
          const x = cx + radius * Math.cos(keyAngle)
          const y = cy + radius * Math.sin(keyAngle)
          return (
            <g>
              <line
                x1={cx}
                y1={cy}
                x2={x}
                y2={y}
                stroke="#ef4444"
                strokeWidth="1"
                strokeDasharray="3 2"
                opacity={0.5}
              />
              <circle
                cx={x}
                cy={y}
                r={6}
                fill="#ef4444"
              />
              <text
                x={x}
                y={y - 14}
                textAnchor="middle"
                dominantBaseline="central"
                fill="#ef4444"
                fontSize="9"
                fontFamily="var(--font-mono)"
                fontWeight="600"
              >
                {highlightedRoute.key}
              </text>
            </g>
          )
        })()}

        {/* Center info */}
        <text
          x={cx}
          y={cy - 8}
          textAnchor="middle"
          dominantBaseline="central"
          fill="var(--text-muted)"
          fontSize="10"
          fontFamily="var(--font-sans)"
        >
          {ring.realNodeCount} node{ring.realNodeCount !== 1 ? 's' : ''}
        </text>
        <text
          x={cx}
          y={cy + 8}
          textAnchor="middle"
          dominantBaseline="central"
          fill="var(--text-muted)"
          fontSize="10"
          fontFamily="var(--font-mono)"
        >
          {ring.totalVNodes} vnodes
        </text>
      </svg>

      {/* Legend */}
      <div className="hash-ring-legend">
        <div className="hash-ring-legend-title">Nodes</div>
        {uniqueNodes.map((nodeId: string) => (
          <div key={nodeId} className="hash-ring-legend-item">
            <span className="hash-ring-legend-dot" style={{ background: nodeColors[nodeId] }} />
            <code>{nodeId}</code>
          </div>
        ))}
      </div>
    </div>
  )
}
