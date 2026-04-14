import CRC32 from 'crc-32'

/**
 * Hash ring implementation matching Kasoku's Go ring.go exactly.
 * Uses CRC32-IEEE (same as Go's crc32.ChecksumIEEE).
 */

export interface VNode {
  position: number // 0..2^32-1
  nodeId: string
  index: number // 0..vnodeCount-1
}

export interface KeyRoute {
  hashPosition: number
  vnode: VNode
  replicas: string[]
  key: string
}

export class HashRing {
  vnodes: VNode[] = []
  nodes = new Set<string>()
  vnodeCount: number

  constructor(vnodeCount: number = 150) {
    this.vnodeCount = vnodeCount
  }

  private hash(key: string): number {
    // crc32.str returns signed int32, convert to unsigned
    return CRC32.str(key) >>> 0
  }

  addNode(nodeId: string): void {
    if (this.nodes.has(nodeId)) return
    this.nodes.add(nodeId)

    for (let i = 0; i < this.vnodeCount; i++) {
      const vnodeKey = `${nodeId}#vnode${i}`
      const pos = this.hash(vnodeKey)
      this.vnodes.push({ position: pos, nodeId, index: i })
    }

    this.vnodes.sort((a, b) => a.position - b.position)
  }

  removeNode(nodeId: string): void {
    if (!this.nodes.has(nodeId)) return
    this.nodes.delete(nodeId)
    this.vnodes = this.vnodes.filter((v) => v.nodeId !== nodeId)
  }

  /**
   * Find the vnode responsible for a key (successor on the ring).
   */
  getNode(key: string): string | null {
    if (this.vnodes.length === 0) return null
    const pos = this.hash(key)
    const idx = this.search(pos)
    return this.vnodes[idx].nodeId
  }

  /**
   * Get n replica nodes for a key.
   */
  getNodes(key: string, n: number): string[] {
    if (this.nodes.size === 0) return []
    const maxN = Math.min(n, this.nodes.size)

    const pos = this.hash(key)
    const startIdx = this.search(pos)

    const seen = new Set<string>()
    const result: string[] = []

    for (let offset = 0; result.length < maxN; offset++) {
      const idx = (startIdx + offset) % this.vnodes.length
      const nodeId = this.vnodes[idx].nodeId
      if (!seen.has(nodeId)) {
        seen.add(nodeId)
        result.push(nodeId)
      }
    }

    return result
  }

  /**
   * Get full routing info for a key.
   */
  routeKey(key: string, replicationFactor: number = 3): KeyRoute {
    const hashPosition = this.hash(key)
    const startIdx = this.search(hashPosition)
    const vnode = this.vnodes[startIdx]
    const replicas = this.getNodes(key, replicationFactor)

    return { hashPosition, vnode, replicas, key }
  }

  /**
   * Search for the first vnode with position >= pos (binary search).
   */
  private search(pos: number): number {
    let lo = 0
    let hi = this.vnodes.length

    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.vnodes[mid].position >= pos) {
        hi = mid
      } else {
        lo = mid + 1
      }
    }

    return lo % this.vnodes.length
  }

  /**
   * Get distribution of vnodes per node.
   */
  distribution(): Map<string, number> {
    const counts = new Map<string, number>()
    for (const v of this.vnodes) {
      counts.set(v.nodeId, (counts.get(v.nodeId) || 0) + 1)
    }
    return counts
  }

  /**
   * Get the percentage range each vnode covers on the ring.
   */
  vnodeRanges(): Array<{ vnode: VNode; start: number; end: number; range: number }> {
    if (this.vnodes.length === 0) return []

    const maxPos = 2 ** 32
    return this.vnodes.map((vnode, i) => {
      const prev = this.vnodes[(i - 1 + this.vnodes.length) % this.vnodes.length]
      let range = vnode.position - prev.position
      if (range < 0) range += maxPos
      return {
        vnode,
        start: prev.position,
        end: vnode.position,
        range,
      }
    })
  }

  /**
   * Get a sampled subset of vnodes for visualization (too many to render all).
   */
  sampledVnodes(maxSamples: number = 200): VNode[] {
    if (this.vnodes.length <= maxSamples) return this.vnodes
    const step = Math.ceil(this.vnodes.length / maxSamples)
    return this.vnodes.filter((_, i) => i % step === 0)
  }

  get totalVNodes(): number {
    return this.vnodes.length
  }

  get realNodeCount(): number {
    return this.nodes.size
  }
}
