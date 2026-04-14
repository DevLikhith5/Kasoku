import { useState, useCallback, useMemo } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { KeyRound, ArrowRight, Server } from 'lucide-react'
import { HashRing, type KeyRoute } from '../../lib/hashRing'

type Operation = 'put' | 'get' | 'delete' | 'scan' | 'keys'

interface OpResult {
  type: 'success' | 'error' | 'info'
  message: string
  data?: any
}

const VNODE_COUNT = 150
const REPLICA_COUNT = 3

export function KVOperations({ apiBase }: { apiBase: string }) {
  const [operation, setOperation] = useState<Operation>('put')
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [prefix, setPrefix] = useState('')
  const [result, setResult] = useState<OpResult | null>(null)
  const [loading, setLoading] = useState(false)

  // Build a local hash ring matching the server for key routing visualization
  const ring = useMemo(() => {
    const r = new HashRing(VNODE_COUNT)
    r.addNode('node-1')
    return r
  }, [])

  // Compute routing for the current key
  const keyRoute: KeyRoute | null = useMemo(() => {
    if (operation === 'scan' || operation === 'keys') return null
    if (!key.trim()) return null
    return ring.routeKey(key.trim(), REPLICA_COUNT)
  }, [key, operation, ring])

  const api = (path: string) => {
    if (apiBase.startsWith('http')) return `${apiBase}${path}`
    return path
  }

  const execute = useCallback(async () => {
    setLoading(true)
    setResult(null)

    try {
      let url: string
      let method = 'GET'
      let body: any = null

      switch (operation) {
        case 'put':
          url = `${api(`/api/v1/put/${encodeURIComponent(key)}`)}`
          method = 'PUT'
          body = { value }
          break
        case 'get':
          url = `${api(`/api/v1/get/${encodeURIComponent(key)}`)}`
          break
        case 'delete':
          url = `${api(`/api/v1/delete/${encodeURIComponent(key)}`)}`
          method = 'DELETE'
          break
        case 'scan':
          url = `${api(`/api/v1/scan?prefix=${encodeURIComponent(prefix)}`)}`
          break
        case 'keys':
          url = `${api('/api/v1/keys')}`
          break
      }

      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined,
      })

      const data = await res.json()

      if (res.ok) {
        setResult({
          type: 'success',
          message: `${operation.toUpperCase()} successful`,
          data,
        })
      } else {
        setResult({
          type: 'error',
          message: data.error || `Request failed with ${res.status}`,
        })
      }
    } catch (err: any) {
      setResult({
        type: 'error',
        message: `Connection error: ${err.message}. Is the Kasoku server running?`,
      })
    } finally {
      setLoading(false)
    }
  }, [operation, key, value, prefix, api])

  const operations: { id: Operation; label: string }[] = [
    { id: 'put', label: 'PUT' },
    { id: 'get', label: 'GET' },
    { id: 'delete', label: 'DELETE' },
    { id: 'scan', label: 'SCAN' },
    { id: 'keys', label: 'KEYS' },
  ]

  return (
    <div className="kv-ops">
      <div className="kv-ops-header">
        <h1 className="kv-ops-title">Key-Value Store</h1>
        <p className="kv-ops-subtitle">
          Interact with the LSM engine. Keys are routed through the consistent hash ring.
        </p>
      </div>

      {/* Operation selector */}
      <div className="kv-op-selector">
        {operations.map((op) => (
          <button
            key={op.id}
            className={`kv-op-btn ${operation === op.id ? 'active' : ''}`}
            onClick={() => setOperation(op.id)}
          >
            {op.label}
          </button>
        ))}
      </div>

      {/* Input form */}
      <div className="kv-form">
        {operation === 'put' && (
          <>
            <div className="kv-field">
              <label className="kv-field-label">Key</label>
              <input
                value={key}
                onChange={(e) => setKey(e.target.value)}
                placeholder="e.g. user:1001"
                className="kv-input"
                onKeyDown={(e) => e.key === 'Enter' && execute()}
              />
            </div>
            <div className="kv-field">
              <label className="kv-field-label">Value</label>
              <textarea
                value={value}
                onChange={(e) => setValue(e.target.value)}
                placeholder="Enter value..."
                rows={3}
                className="kv-textarea"
              />
            </div>
          </>
        )}

        {(operation === 'get' || operation === 'delete') && (
          <div className="kv-field">
            <label className="kv-field-label">Key</label>
            <input
              value={key}
              onChange={(e) => setKey(e.target.value)}
              placeholder="e.g. user:1001"
              className="kv-input"
              onKeyDown={(e) => e.key === 'Enter' && execute()}
            />
          </div>
        )}

        {operation === 'scan' && (
          <div className="kv-field">
            <label className="kv-field-label">Prefix</label>
            <input
              value={prefix}
              onChange={(e) => setPrefix(e.target.value)}
              placeholder="e.g. user:"
              className="kv-input"
              onKeyDown={(e) => e.key === 'Enter' && execute()}
            />
          </div>
        )}
      </div>

      {/* Execute button */}
      <button
        className="kv-execute"
        onClick={execute}
        disabled={loading}
      >
        {loading ? 'Executing...' : `Execute ${operation.toUpperCase()}`}
      </button>

      {/* Key routing visualization */}
      <AnimatePresence>
        {keyRoute && operation !== 'scan' && operation !== 'keys' && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.25 }}
            className="kv-routing"
          >
            <div className="kv-routing-header">
              <KeyRound size={14} />
              <span>Key Routing</span>
            </div>

            <div className="kv-routing-flow">
              {/* Step 1: Key hash */}
              <div className="kv-routing-step">
                <div className="kv-routing-step-icon hash">
                  <span>#</span>
                </div>
                <div className="kv-routing-step-body">
                  <span className="kv-routing-step-label">CRC32 Hash</span>
                  <code className="kv-routing-step-value">{keyRoute.hashPosition >>> 0}</code>
                </div>
              </div>

              <ArrowRight size={14} className="kv-routing-arrow" />

              {/* Step 2: Primary vnode */}
              <div className="kv-routing-step">
                <div className="kv-routing-step-icon vnode">
                  <span>V</span>
                </div>
                <div className="kv-routing-step-body">
                  <span className="kv-routing-step-label">Primary VNode</span>
                  <code className="kv-routing-step-value">
                    {keyRoute.vnode.nodeId}#vnode{keyRoute.vnode.index}
                  </code>
                </div>
              </div>

              <ArrowRight size={14} className="kv-routing-arrow" />

              {/* Step 3: Replica nodes */}
              <div className="kv-routing-step replicas">
                <div className="kv-routing-step-icon replica">
                  <Server size={14} />
                </div>
                <div className="kv-routing-step-body">
                  <span className="kv-routing-step-label">
                    Replicas (N={REPLICA_COUNT}, W={REPLICA_COUNT})
                  </span>
                  <div className="kv-routing-replicas">
                    {keyRoute.replicas.map((r, i) => (
                      <span key={r} className="kv-routing-replica-tag">
                        <span className="kv-routing-replica-index">{i + 1}</span>
                        {r}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Result */}
      {result && (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          className={`kv-result ${result.type}`}
        >
          <div className="kv-result-header">
            <span className="kv-result-icon">
              {result.type === 'success' ? '✓' : result.type === 'error' ? '✕' : 'ℹ'}
            </span>
            <span className="kv-result-message">{result.message}</span>
          </div>
          {result.data && (
            <pre className="kv-result-data">
              {JSON.stringify(result.data, null, 2)}
            </pre>
          )}
        </motion.div>
      )}
    </div>
  )
}
