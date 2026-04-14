import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import { ArrowLeft, BookOpen, Terminal, Database, Network, ShieldCheck, Code2, Zap } from 'lucide-react'

const sections = [
  { id: 'overview', title: 'Philosophy', icon: <BookOpen size={16} strokeWidth={2} /> },
  { id: 'installation', title: 'Installation', icon: <Terminal size={16} strokeWidth={2} /> },
  { id: 'storage', title: 'Storage Engine', icon: <Database size={16} strokeWidth={2} /> },
  { id: 'cluster', title: 'Cluster Layer', icon: <Network size={16} strokeWidth={2} /> },
  { id: 'replication', title: 'Replication', icon: <ShieldCheck size={16} strokeWidth={2} /> },
  { id: 'cli', title: 'CLI & Shell', icon: <Code2 size={16} strokeWidth={2} /> },
  { id: 'benchmarks', title: 'Performance', icon: <Zap size={16} strokeWidth={2} /> },
]



export function Docs() {
  const [activeSection, setActiveSection] = useState('overview')

  const scrollTo = (id: string) => {
    const el = document.getElementById(id)
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }

  useEffect(() => {
    const scrollContainer = document.querySelector('.docs-main-scroll')
    if (!scrollContainer) return

    const handleScroll = () => {
      const { scrollTop, scrollHeight, clientHeight } = scrollContainer
      if (scrollTop + clientHeight >= scrollHeight - 50) {
        setActiveSection(sections[sections.length - 1].id)
      }
    }

    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            setActiveSection(entry.target.id)
          }
        })
      },
      { 
        root: scrollContainer,
        threshold: 0.1, 
        rootMargin: '-10% 0% -50% 0%' 
      }
    )

    sections.forEach((s) => {
      const el = document.getElementById(s.id)
      if (el) observer.observe(el)
    })

    scrollContainer.addEventListener('scroll', handleScroll)

    return () => {
      observer.disconnect()
      scrollContainer.removeEventListener('scroll', handleScroll)
    }
  }, [])

  return (
    <div className="docs-page">
      <header className="docs-header">
        <div className="docs-header-left">
          <Link to="/" className="nav-logo">
            <img src="/logo.svg" alt="kasoku" className="nav-logo-img" />
          </Link>
        </div>
        <div className="docs-header-right">
          <Link to="/dashboard" className="nav-cta">Dashboard</Link>
        </div>
      </header>

      <div className="docs-layout">
        <aside className="docs-sidebar">
          <div className="docs-nav-group">
            <span className="docs-nav-title">Kasoku Manual</span>
            {sections.map(s => (
              <button 
                key={s.id} 
                onClick={() => scrollTo(s.id)} 
                className={`docs-nav-link ${activeSection === s.id ? 'active' : ''}`}
                style={{ border: 'none', background: 'none', textAlign: 'left', width: '100%', cursor: 'pointer', outline: 'none' }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <span style={{ opacity: activeSection === s.id ? 1 : 0.6, color: activeSection === s.id ? 'var(--accent)' : 'inherit' }}>{s.icon}</span>
                  {s.title}
                </div>
              </button>
            ))}
          </div>

          <Link to="/" className="docs-nav-link" style={{ marginTop: 'auto', display: 'flex', alignItems: 'center', gap: '8px', opacity: 0.8 }}>
            <ArrowLeft size={16} /> Back to Landing
          </Link>
        </aside>

        <main className="docs-main-scroll">
          <div className="docs-hero">
            <motion.h1
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6 }}
            >
              The Kasoku Geeta.
            </motion.h1>
            <motion.p
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: 0.1 }}
            >
               An industrial-grade guide to the masterless, distributed LSM key-value engine. 
               Built for heavy write workloads and eventual consistency at scale.
            </motion.p>
          </div>

          <div className="docs-content-area" style={{ paddingTop: 0 }}>
            {/* Overview Section */}
            <motion.section 
              id="overview" 
              className="docs-section"
              initial={{ opacity: 0 }}
              whileInView={{ opacity: 1 }}
              viewport={{ once: true }}
            >
              <h2>
                <div className="docs-icon"><BookOpen size={18} strokeWidth={2} /></div>
                Philosophy & Architecture
              </h2>
              <p>
                Kasoku is built on the principle of <strong>symmetric architecture</strong>. Unlike traditional 
                Leader/Follower (Primary/Secondary) systems where a single node handles all writes, Kasoku has no single point of failure and no leader election overhead. 
              </p>
              <p>
                By utilizing a <strong>Leaderless (Dynamo-style)</strong> model, any node can act as a coordinator for any request. This design prioritizes <strong>Write Availability</strong> and horizontal scalability, making it ideal for high-throughput, globally distributed workloads.
              </p>
              <div className="docs-code-block" style={{ background: 'var(--bg-subtle)', borderRadius: '12px' }}>
                <pre style={{ color: 'var(--text-muted)', fontSize: '11px' }}>
{`Client Request -> Any Node (Coordinator)
                      |
        +-------------+-------------+
        |             |             |
   Local WAL     Replica A     Replica B
   Local Mem     Remote RPC    Remote RPC
        |             |             |
        +-------------+-------------+
                      |
              Quorum Ack (W=2)`}
                </pre>
              </div>
            </motion.section>

            {/* Installation Section */}
            <section id="installation" className="docs-section">
              <h2>
                <div className="docs-icon"><Terminal size={18} strokeWidth={2} /></div>
                Installation
              </h2>
              <p>Kasoku is written in pure Go and requires version 1.25+. It has zero external C-bindings or runtime dependencies.</p>
              <div className="docs-code-block">
                <pre>go get github.com/DevLikhith5/kasoku</pre>
              </div>
            </section>

            {/* Storage Engine Section */}
            <section id="storage" className="docs-section">
              <h2>
                <div className="docs-icon"><Database size={18} strokeWidth={2} /></div>
                LSM Storage Engine
              </h2>
              <p>
                The storage engine is an optimized Log-Structured Merge-Tree (LSM). Unlike B-Trees which use in-place updates, LSM engines turn random writes into sequential I/O by appending updates to a log and merging them later. This design allows Kasoku to saturate disk throughput even on high-latency storage media.
              </p>
              
              <h3>The Write Path</h3>
              <ol style={{ color: 'var(--text-secondary)', paddingLeft: '20px', marginBottom: '24px' }}>
                <li style={{ marginBottom: '8px' }}>Append entry to <strong>Write-Ahead Log (WAL)</strong> for durability.</li>
                <li style={{ marginBottom: '8px' }}>Insert into in-memory <strong>MemTable</strong> (implemented as a lock-free Skip List).</li>
                <li style={{ marginBottom: '8px' }}>When MemTable is full (default 64MB), freeze and flush to an immutable <strong>L0 SSTable</strong>.</li>
                <li>Background <strong>Compaction</strong> threads merge SSTables to eliminate tombstones and merge duplicate keys across levels.</li>
              </ol>

              <h3>SSTable Compaction</h3>
              <p>
                Kasoku supports <strong>Leveled Compaction</strong>. SSTables in Level 0 have overlapping key ranges. Once Level 0 reaches a threshold, it merges into Level 1. From Level 1 onwards, SSTables are partitioned into non-overlapping key ranges, minimizing the "Read Amplification" inherent in LSM systems.
              </p>

              <h3>The Read Path</h3>
              <p>Reads check the <strong>MemTable</strong> first, then immutable memtables. If not found, it queries <strong>SSTables</strong> from newest to oldest. Each SSTable consults a <strong>Bloom Filter</strong> first to avoid unnecessary disk seeks.</p>
              
              <div className="docs-table-wrap">
                <table className="docs-table">
                  <thead>
                    <tr>
                      <th>Feature</th>
                      <th>Implementation Detail</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>MemTable</td>
                      <td>Probabilistic Skip List (Thread-safe)</td>
                    </tr>
                    <tr>
                      <td>SSTables</td>
                      <td>Sorted, Snappy-compressed data blocks</td>
                    </tr>
                    <tr>
                      <td>Bloom Filters</td>
                      <td>Per-SSTable, default 1% FP rate</td>
                    </tr>
                    <tr>
                        <td>WAL Sync</td>
                        <td>Configurable: Per-write or async interval</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>

            {/* Cluster Section */}
            <section id="cluster" className="docs-section">
              <h2>
                <div className="docs-icon"><Network size={18} strokeWidth={2} /></div>
                Cluster Layer
              </h2>
              <p>
                The distribution layer handles partitioning and membership using a fully decentralized P2P model.
              </p>
              
              <h3>Consistent Hashing & vNodes</h3>
              <p>Kasoku uses a <strong>CRC32 Consistent Hash Ring</strong>. Each physical node is mapped to 150 <strong>Virtual Nodes (vNodes)</strong>. vNodes allow for more granular data distribution and significantly faster cluster rebalancing when a node joins or leaves.</p>

              <h3>Gossip & Failure Detection</h3>
              <p>Membership and cluster state are propagated via a <strong>SWIM-inspired Gossip Protocol</strong>. This decentralized approach ensures that every node eventually reaches the same view of the cluster without a central registry.</p>
              
              <p>To handle network jitter, we use a <strong>Phi Accrual Failure Detector</strong>. Instead of a binary "Up/Down" state, it outputs a probability of failure. This allows the system to remain resilient to transient network partitions while quickly reacting to permanent node failures.</p>
            </section>

            {/* Replication Section */}
            <section id="replication" className="docs-section">
              <h2>
                <div className="docs-icon"><ShieldCheck size={18} strokeWidth={2} /></div>
                Quorum & Consistency
              </h2>
              <p>Kasoku follows a tunable <strong>N/W/R Quorum model</strong> to balance latency and consistency.</p>
              
              <div className="docs-table-wrap">
                <table className="docs-table">
                  <thead>
                    <tr>
                      <th>Param</th>
                      <th>Default</th>
                      <th>Meaning</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>N</td>
                      <td>3</td>
                      <td>Number of replicas per key</td>
                    </tr>
                    <tr>
                      <td>W</td>
                      <td>2</td>
                      <td>Successful writes required for ACK</td>
                    </tr>
                    <tr>
                      <td>R</td>
                      <td>2</td>
                      <td>Successful reads required for success</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <p style={{ fontStyle: 'italic', fontSize: '14px' }}>Requirement: W + R &gt; N (ensures at least one overlapping replica).</p>

              <h3>Anti-Entropy (Merkle Trees)</h3>
              <p>To heal data divergence during prolonged network partitions, Kasoku runs a background <strong>SHA-256 Merkle Tree</strong> reconciliation loop. It compares tree hashes with neighbors to identify and sync only the missing key-ranges, saving significant bandwidth.</p>

              <h3>Hinted Handoff</h3>
              <p>If a target node is down during a write request, the coordinator stores a <strong>Hint</strong> locally. Once the target node recovers, the coordinator "hands off" the hint to replay the missed write, ensuring eventually consistent cluster state.</p>

              <h3>Read Repair</h3>
              <p>During a Read Quorum (R), if Kasoku detects that one replica is out-of-date compared to others (based on timestamps), it automatically triggers a background <strong>Read Repair</strong> to update the stale node with the latest value.</p>
            </section>

            {/* CLI Section */}
            <section id="cli" className="docs-section">
              <h2>
                <div className="docs-icon"><Code2 size={18} strokeWidth={2} /></div>
                CLI & Interactive Shell
              </h2>
              <p>The <code>kvctl</code> tool is the primary way to interact with a Kasoku cluster.</p>
              <div className="docs-code-block">
                <pre>{`# Basic Operations
./kvctl put user:1001 '{"name": "Alice"}'
./kvctl get user:1001 -o json
./kvctl stats

# Interactive Shell
./kvctl shell
kasoku > SET user:1 "Bob"
kasoku > GET user:1
kasoku > INFO`}</pre>
              </div>
            </section>

            {/* Benchmarks Section */}
            <section id="benchmarks" className="docs-section">
              <h2>
                <div className="docs-icon"><Zap size={18} strokeWidth={2} /></div>
                Performance Benchmarks
              </h2>
              <p>Benchmarks executed on Apple M1 (ARM64). Sequential write speed is heavily influenced by WAL fsync policy.</p>
              
              <div className="docs-table-wrap">
                <table className="docs-table">
                   <thead>
                    <tr>
                      <th>Operation</th>
                      <th>Config</th>
                      <th>Result</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>Write (Sync)</td>
                      <td>Fsync on write</td>
                      <td>~267 ops/sec</td>
                    </tr>
                    <tr>
                      <td>Write (Async)</td>
                      <td>100ms interval</td>
                      <td>3.5M ops/sec</td>
                    </tr>
                    <tr>
                      <td>Read Seq</td>
                      <td>MemTable + Cache</td>
                      <td>7.3M ops/sec</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>
          </div>
        </main>
      </div>
    </div>
  )
}

