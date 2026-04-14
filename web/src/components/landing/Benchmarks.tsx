import { motion, useScroll, useTransform } from 'framer-motion'
import { useRef } from 'react'
import { BarChart, Bar, XAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

const writeData = [
  { name: 'WAL Sync', value: 267, fill: '#52525b', displayValue: '267' },
  { name: 'WAL 100ms', value: 3516847, fill: '#e11d5a', displayValue: '3.5M' },
  { name: 'MemTable Put', value: 2432594, fill: '#f43f5e', displayValue: '2.4M' },
]

const readData = [
  { name: 'Sequential', value: 7280403, fill: '#e11d5a', displayValue: '7.3M' },
  { name: 'Random', value: 5996497, fill: '#f43f5e', displayValue: '6.0M' },
  { name: 'Concurrent', value: 7093032, fill: '#fb7185', displayValue: '7.1M' },
]

const MIN_BAR_HEIGHT = 4 // Minimum visible bar height in pixels

const CustomBar = (props: any) => {
  const { x, y, width, height, fill, payload } = props

  // Ensure minimum bar height for visibility
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
      {/* Show value label for very small bars */}
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
            Apple M1 · 8-core · Go benchmarks with allocation tracking
          </p>
        </motion.div>

        <div className="benchmarks-grid">
          {/* Write chart */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ delay: 0.1 }}
            className="benchmark-card"
          >
            <h3 className="benchmark-card-title">Write Throughput</h3>
            <div className="benchmark-chart">
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={writeData} margin={{ top: 24, right: 8, bottom: 0, left: 0 }}>
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
                    {writeData.map((entry, i) => (
                      <Cell key={i} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </motion.div>

          {/* Read chart */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ delay: 0.2 }}
            className="benchmark-card"
          >
            <h3 className="benchmark-card-title">Read Throughput</h3>
            <div className="benchmark-chart">
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={readData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11, fontFamily: 'var(--font-sans)', fill: 'var(--text-muted)' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip content={<CustomTooltip />} cursor={{ fill: 'transparent' }} />
                  <Bar dataKey="value" radius={[6, 6, 0, 0]} barSize={48}>
                    {readData.map((entry, i) => (
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
            <span className="benchmark-highlight-value">13,000×</span>
            <span className="benchmark-highlight-label">WAL throughput improvement with background sync</span>
          </div>
          <div className="benchmark-highlight">
            <span className="benchmark-highlight-value">174ns</span>
            <span className="benchmark-highlight-label">Sequential read latency</span>
          </div>
          <div className="benchmark-highlight">
            <span className="benchmark-highlight-value">556×</span>
            <span className="benchmark-highlight-label">Mixed workload improvement</span>
          </div>
        </motion.div>
      </div>
    </section>
  )
}
