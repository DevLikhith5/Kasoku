import { createContext, useContext, useState, useEffect, useCallback, type ReactNode } from 'react'

export interface RingNode {
  id: string    // nodeId as returned by backend (full URL, e.g. "http://localhost:9000")
  addr: string  // same as id — backend uses URL as node identity
}

export interface RingState {
  nodes: RingNode[]
  count: number
  replication: number
  loading: boolean
  error: string | null
  isClusterMode: boolean
  selectedNode: string | null   // null = "all nodes" / use default proxy target
}

interface RingContextValue extends RingState {
  refetch: () => Promise<void>
  api: (path: string) => string
  setSelectedNode: (nodeId: string | null) => void
}

const RingContext = createContext<RingContextValue | null>(null)
export { RingContext }

export function useRingContext() {
  const ctx = useContext(RingContext)
  if (!ctx) throw new Error('useRingContext must be used within RingProvider')
  return ctx
}

interface RingProviderProps {
  apiBase: string
  children: ReactNode
}

export function RingProvider({ apiBase, children }: RingProviderProps) {
  const [nodes, setNodes] = useState<RingNode[]>([])
  const [count, setCount] = useState(0)
  const [replication, setReplication] = useState(3)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [isClusterMode, setIsClusterMode] = useState(false)
  const [selectedNode, setSelectedNode] = useState<string | null>(null)

  const api = useCallback((path: string) => {
    if (apiBase.startsWith('http')) return `${apiBase}${path}`
    return path
  }, [apiBase])

  const fetchRing = useCallback(async () => {
    try {
      setError(null)
      const res = await fetch(api('/ring'))

      if (!res.ok) {
        throw new Error(`Failed to fetch ring state: ${res.status}`)
      }

      const json = await res.json()
      const data = json.data

      const parsedNodes: RingNode[] = (data.nodes || []).map((id: string) => ({
        id,
        addr: id, // Backend uses nodeAddr as node ID (full URL)
      }))

      setNodes(parsedNodes)
      setCount(data.count ?? parsedNodes.length)
      setReplication(data.replication ?? 3)
      setIsClusterMode(parsedNodes.length > 1)
      setLoading(false)
    } catch (err: any) {
      setError(err.message || 'Failed to fetch ring state')
      setLoading(false)
    }
  }, [api])

  useEffect(() => {
    fetchRing()
    const interval = setInterval(fetchRing, 5000)
    return () => clearInterval(interval)
  }, [fetchRing])

  const value: RingContextValue = {
    nodes,
    count,
    replication,
    loading,
    error,
    isClusterMode,
    selectedNode,
    setSelectedNode,
    refetch: fetchRing,
    api,
  }

  return <RingContext.Provider value={value}>{children}</RingContext.Provider>
}
