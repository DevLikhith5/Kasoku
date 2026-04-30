import { useState, useEffect } from 'react'
import { FileCode, Server } from 'lucide-react'

interface ConfigSection {
  section: string
  fields: { key: string; label: string; value: string }[]
}

export function SettingsPanel({ apiBase }: { apiBase: string }) {
  const [sections, setSections] = useState<ConfigSection[]>([])
  const [loading, setLoading] = useState(true)
  const [serverDown, setServerDown] = useState(false)

  const api = (path: string) => {
    if (apiBase.startsWith('http')) return `${apiBase}${path}`
    return path
  }

  useEffect(() => {
    let cancelled = false

    const fetchConfig = async () => {
      try {
        const nodeRes = await fetch(api('/api/v1/node'))
        if (!nodeRes.ok) {
          if (!cancelled) setServerDown(true)
          return
        }

        let nd: any = null
        try {
          const body = await nodeRes.text()
          if (!body.trim()) {
            if (!cancelled) setServerDown(true)
            return
          }
          const parsed = JSON.parse(body)
          nd = parsed.data
        } catch {
          if (!cancelled) setServerDown(true)
          return
        }

        if (cancelled || !nd) return

        const nodeAddr = nd.addr || 'http://localhost:9000'
        const nodeId = nd.node_id || 'this-node'
        const stats = nd.stats || {}

        const secs: ConfigSection[] = [
          {
            section: 'Node',
            fields: [
              { key: 'node_id', label: 'Node ID', value: nodeId },
              { key: 'addr', label: 'Address', value: nodeAddr },
            ],
          },
          {
            section: 'Storage Engine',
            fields: [
              { key: 'stats.key_count', label: 'Key Count', value: (stats.KeyCount ?? 0).toLocaleString() },
              { key: 'stats.disk_bytes', label: 'Disk Bytes', value: (stats.DiskBytes ?? 0).toLocaleString() },
              { key: 'stats.mem_bytes', label: 'MemTable Bytes', value: (stats.MemBytes ?? 0).toLocaleString() },
              { key: 'stats.bloom_fp_rate', label: 'Bloom FP Rate', value: String(stats.BloomFPRate ?? 'N/A') },
            ],
          },
        ]

        // Try cluster status
        try {
          const statusRes = await fetch(api('/api/v1/cluster/status'))
          if (statusRes.ok) {
            const statusRaw = await statusRes.json()
            const data = statusRaw.data || statusRaw
            const nodeCount = data.node_count || (data.nodes || []).length
            const rf = data.replication_factor || 3
            secs.push({
              section: 'Cluster',
              fields: [
                { key: 'cluster.mode', label: 'Cluster Mode', value: 'Enabled' },
                { key: 'cluster.nodes', label: 'Node Count', value: String(nodeCount) },
                { key: 'cluster.replication_factor', label: 'Replication Factor', value: String(rf) },
                { key: 'cluster.node_addr', label: 'This Node', value: data.node_addr || 'N/A' },
              ],
            })
          } else {
            secs.push({
              section: 'Cluster',
              fields: [{ key: 'cluster.mode', label: 'Cluster Mode', value: 'Disabled (single-node)' }],
            })
          }
        } catch {
          secs.push({
            section: 'Cluster',
            fields: [{ key: 'cluster.mode', label: 'Cluster Mode', value: 'Disabled (single-node)' }],
          })
        }

        // Try to get all node details (excluding this node to avoid duplication)
        try {
          const statusRes = await fetch(api('/api/v1/cluster/status'))
          if (statusRes.ok) {
            const statusRaw = await statusRes.json()
            const data = statusRaw.data || statusRaw
            const nodeDetails = data.node_details || []
            
            // Filter out this node (already shown in Node section)
            // Compare against both node_id and addr to handle any inconsistencies
            const otherNodes = nodeDetails.filter((n: any) => 
              n.node_id !== nodeId && n.node_id !== nodeAddr && 
              n.addr !== nodeId && n.addr !== nodeAddr
            )
            
            if (otherNodes.length > 0) {
              secs.push({
                section: 'Other Nodes',
                fields: otherNodes.map((n: any) => ({
                  key: `node_${n.node_id}`,
                  label: n.node_id,
                  value: n.alive 
                    ? `Keys: ${n.stats?.key_count ?? 0}, Disk: ${((n.stats?.disk_bytes ?? 0) / 1024 / 1024).toFixed(1)}MB`
                    : `OFFLINE: ${n.error || 'unreachable'}`,
                })),
              })
            }
          }
        } catch {
          // Ignore - node details are optional
        }

        setSections(secs)
      } catch {
        if (!cancelled) setServerDown(true)
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    fetchConfig()
    return () => { cancelled = true }
  }, [apiBase])

  if (loading) {
    return (
      <div className="settings">
        <div className="settings-header">
          <h1 className="settings-title">Configuration</h1>
          <p className="settings-subtitle">Current runtime configuration and engine state.</p>
        </div>
        <div className="metrics-empty">Loading configuration…</div>
      </div>
    )
  }

  if (serverDown) {
    return (
      <div className="settings">
        <div className="settings-header">
          <h1 className="settings-title">Configuration</h1>
          <p className="settings-subtitle">Current runtime configuration and engine state.</p>
        </div>
        <div className="metrics-offline-card">
          <Server size={24} />
          <div className="metrics-offline-body">
            <h3>Server Unreachable</h3>
            <p>
              The Kasoku server appears to be offline. Start it with:
            </p>
            <code className="metrics-offline-cmd">KASOKU_CONFIG=kasoku.yaml ./kasoku-server</code>
            <p className="metrics-offline-hint">
              Runtime config is fetched from <code>/api/v1/node</code>.
            </p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="settings">
      <div className="settings-header">
        <h1 className="settings-title">Configuration</h1>
        <p className="settings-subtitle">
          Current runtime configuration and engine state.
        </p>
      </div>

      {sections.map((section) => (
        <div key={section.section} className="settings-section">
          <h2 className="settings-section-title">{section.section}</h2>
          <div className="settings-grid">
            {section.fields.map((field) => (
              <div key={field.key} className="setting-card">
                <div className="setting-header">
                  <label className="setting-label">{field.label}</label>
                  <code className="setting-key">{field.key}</code>
                </div>
                <div className="setting-value">
                  <code>{field.value}</code>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}

      <div className="settings-yaml-ref">
        <FileCode size={16} />
        <span>Defined in <code>kasoku.yaml</code></span>
      </div>
    </div>
  )
}
