import { motion, useScroll, useTransform } from 'framer-motion'
import { useRef } from 'react'
import { BarChart, Bar, XAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

const singleNodeData = [
  { name: 'Writes', value: 118620, fill: '#e11d5a', displayValue: '118K' },
  { name: 'Reads (Single-Key)', value: 308552, fill: '#f43f5e', displayValue: '308K' },
  { name: 'Reads (Batch)', value: 435000, fill: '#fb7185', displayValue: '435K' },
]

const clusterData = [
  { name: 'Writes', value: 24765, fill: '#e11d5a', displayValue: '24K' },
  { name: 'Reads (Single-Key)', value: 27970, fill: '#f43f5e', displayValue: '28K' },
  { name: 'Reads (Batch)', value: 261799, fill: '#fb7185', displayValue: '262K' },
]

const MIN_BAR_HEIGHT = 4 // Minimum visible bar height in pixels

const CustomBar = (props: any) => {
  const { x, y, width, height, fill, payload } = props

  const displayHeight = Math.max(height, MIN_BAR_HEIGHT)
  const displayY = y - (displayHeight - height)

  return (
    <g>
      <rect
        x={x}
        y={displayY}
        width={width}
        height={displayHeight}
        fill={fill}
        rx={6}
        ry={6}
      />
      {height < MIN_BAR_HEIGHT && (
        <text
          x={x + width / 2}
          y={displayY - 8}
          textAnchor="middle"
          fill="var(--text-muted)"
          fontSize="10"
          fontFamily="var(--font-mono)"
        >
          {payload.displayValue}
        </text>
      )}
    </g>
  )
}

const CustomTooltip = ({ active, payload }: any) => {
  if (active && payload?.[0]) {
    return (
      <div className="chart-tooltip">
        <span className="chart-tooltip-label">{payload[0].payload.name}</span>
        <span className="chart-tooltip-value">{payload[0].value.toLocaleString()} ops/sec</span>
      </div>
    )
  }
  return null
}

export function Benchmarks() {
  const ref = useRef(null)
  const { scrollYProgress } = useScroll({
    target: ref,
    offset: ['start end', 'end start'],
  })

  const y = useTransform(scrollYProgress, [0, 1], [40, -40])

  return (
    <section className="benchmarks" id="benchmarks" ref={ref}>
      <div className="benchmarks-inner">
        <motion.div
          style={{ y }}
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true, margin: '-80px' }}
          transition={{ duration: 0.6 }}
          className="benchmarks-header"
        >
          <h2 className="benchmarks-title">Performance Benchmarks</h2>
          <p className="benchmarks-subtitle">
            Apple M1 · 8-core · pressure load testing tool
          </p>
        </motion.div>

        <div className="benchmarks-grid">
          {/* Single Node chart */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ delay: 0.1 }}
            className="benchmark-card"
          >
            <h3 className="benchmark-card-title">Single Node (KeyCache)</h3>
            <div className="benchmark-chart">
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={singleNodeData} margin={{ top: 24, right: 8, bottom: 0, left: 0 }}>
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11, fontFamily: 'var(--font-sans)', fill: 'var(--text-muted)' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip content={<CustomTooltip />} cursor={{ fill: 'transparent' }} />
                  <Bar
                    dataKey="value"
                    shape={<CustomBar />}
                    barSize={48}
                  >
                    {singleNodeData.map((entry, i) => (
                      <Cell key={i} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </motion.div>

          {/* Cluster chart */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ delay: 0.2 }}
            className="benchmark-card"
          >
            <h3 className="benchmark-card-title">3-Node Cluster (RF=3)</h3>
            <div className="benchmark-chart">
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={clusterData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11, fontFamily: 'var(--font-sans)', fill: 'var(--text-muted)' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip content={<CustomTooltip />} cursor={{ fill: 'transparent' }} />
                  <Bar dataKey="value" radius={[6, 6, 0, 0]} barSize={48}>
                    {clusterData.map((entry, i) => (
                      <Cell key={i} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </motion.div>
        </div>

        {/* Key metrics row */}
        <motion.div
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ delay: 0.3 }}
          className="benchmark-highlights"
        >
          <div className="benchmark-highlight">
            <span className="benchmark-highlight-value">427K</span>
            <span className="benchmark-highlight-label">Total ops/sec (single node)</span>
          </div>
          <div className="benchmark-highlight">
            <span className="benchmark-highlight-value">118K</span>
            <span className="benchmark-highlight-label">Writes/sec (single node)</span>
          </div>
          <div className="benchmark-highlight">
            <span className="benchmark-highlight-value">308K</span>
            <span className="benchmark-highlight-label">Reads/sec (single node)</span>
          </div>
        </motion.div>
      </div>
    </section>
  )
}
