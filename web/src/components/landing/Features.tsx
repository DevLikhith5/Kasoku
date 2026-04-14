import { motion, useScroll, useTransform } from 'framer-motion'
import {
  Database,
  Network,
  Repeat,
  ShieldCheck,
  Activity,
  GitBranch,
} from 'lucide-react'
import type { Variants } from 'framer-motion'
import { useRef } from 'react'

const features = [
  {
    icon: Database,
    title: 'LSM-Tree Storage',
    description: 'Write-optimized log-structured merge tree with background compaction, achieving millions of writes per second.',
    tags: ['WAL', 'SSTables', 'Skip List'],
  },
  {
    icon: Network,
    title: 'Masterless Cluster',
    description: 'Fully peer-to-peer topology with consistent hashing. Every node accepts reads and writes—no leader election.',
    tags: ['CRC32 Ring', '150 Vnodes', 'Symmetric'],
  },
  {
    icon: Repeat,
    title: 'Gossip Protocol',
    description: 'Epidemic membership protocol achieving eventual consistency across all nodes in O(log N) rounds.',
    tags: ['O(log N)', 'Self-healing', 'No Registry'],
  },
  {
    icon: ShieldCheck,
    title: 'Quorum Replication',
    description: 'Configurable N/W/R quorum model ensuring read-your-writes consistency with overlapping read/write sets.',
    tags: ['N=3', 'W=2', 'R=2'],
  },
  {
    icon: Activity,
    title: 'Phi Accrual Detection',
    description: 'Adaptive failure detection based on heartbeat statistical distribution, not fixed timeouts.',
    tags: ['Adaptive', 'Statistical', 'Low False Positives'],
  },
  {
    icon: GitBranch,
    title: 'Merkle Anti-Entropy',
    description: 'SHA-256 Merkle trees for efficient data reconciliation, synchronizing only divergent keys.',
    tags: ['SHA-256', 'O(K log N)', 'Bandwidth Efficient'],
  },
]

const containerVariants: Variants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { staggerChildren: 0.08 },
  },
}

const itemVariants: Variants = {
  hidden: { opacity: 0, y: 16 },
  visible: { opacity: 1, y: 0, transition: { duration: 0.5, ease: [0.22, 1, 0.36, 1] as const } },
}

export function Features() {
  const ref = useRef(null)
  const { scrollYProgress } = useScroll({
    target: ref,
    offset: ['start end', 'end start'],
  })

  const y = useTransform(scrollYProgress, [0, 1], [40, -40])

  return (
    <section className="features" id="features" ref={ref}>
      <div className="features-inner">
        <motion.div
          style={{ y }}
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true, margin: '-80px' }}
          transition={{ duration: 0.6 }}
          className="features-header"
        >
          <h2 className="features-title">Core Capabilities</h2>
          <p className="features-subtitle">
            Every layer engineered for resilience and performance.
          </p>
        </motion.div>

        <motion.div
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-60px' }}
          className="features-grid"
        >
          {features.map((feature) => {
            const Icon = feature.icon
            return (
              <motion.div key={feature.title} variants={itemVariants} className="feature-card">
                <div className="feature-icon">
                  <Icon size={18} strokeWidth={1.8} />
                </div>
                <h3 className="feature-name">{feature.title}</h3>
                <p className="feature-desc">{feature.description}</p>
                <div className="feature-tags">
                  {feature.tags.map((tag) => (
                    <span key={tag} className="feature-tag">
                      {tag}
                    </span>
                  ))}
                </div>
              </motion.div>
            )
          })}
        </motion.div>
      </div>
    </section>
  )
}
