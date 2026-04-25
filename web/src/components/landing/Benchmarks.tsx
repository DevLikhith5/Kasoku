import { motion, useScroll, useTransform } from 'framer-motion'
import { useRef } from 'react'
import { BarChart, Bar, XAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

const httpClusterData = [
  { name: 'Writes', value: 140000, fill: '#e11d5a', displayValue: '140K' },
  { name: 'Reads', value: 30000, fill: '#f43f5e', displayValue: '30K' },
  { name: 'Total', value: 170000, fill: '#fb7185', displayValue: '170K' },
]

const grpcClusterData = [
  { name: 'Writes', value: 720000, fill: '#e11d5a', displayValue: '720K' },
  { name: 'Reads', value: 1150000, fill: '#f43f5e', displayValue: '1.15M' },
  { name: 'Total', value: 870000, fill: '#fb7185', displayValue: '870K' },
]

const MIN_BAR_HEIGHT = 4

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
          <h2 className="benchmarks-title">3-Node Cluster Performance</h2>
          <p className="benchmarks-subtitle">
            Apple M1 · 8-core · gRPC with connection pooling · RF=3
          </p>
        </motion.div>

        <div className="benchmarks-grid">
          {/* HTTP Cluster */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ delay: 0.1 }}
            className="benchmark-card"
          >
            <h3 className="benchmark-card-title">HTTP (baseline)</h3>
            <div className="benchmark-chart">
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={httpClusterData} margin={{ top: 24, right: 8, bottom: 0, left: 0 }}>
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
                    {httpClusterData.map((entry, i) => (
                      <Cell key={i} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </motion.div>

          {/* gRPC Cluster */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ delay: 0.15 }}
            className="benchmark-card"
          >
            <h3 className="benchmark-card-title">gRPC <span style={{ color: '#22c55e', fontSize: '0.7em' }}>5x faster</span></h3>
            <div className="benchmark-chart">
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={grpcClusterData} margin={{ top: 24, right: 8, bottom: 0, left: 0 }}>
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
                    {grpcClusterData.map((entry, i) => (
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
            <span className="benchmark-highlight-value">870K</span>
            <span className="benchmark-highlight-label">Cluster ops/sec (gRPC)</span>
          </div>
          <div className="benchmark-highlight">
            <span className="benchmark-highlight-value">38x</span>
            <span className="benchmark-highlight-label">Read speedup with gRPC</span>
          </div>
          <div className="benchmark-highlight">
            <span className="benchmark-highlight-value">RF=3</span>
            <span className="benchmark-highlight-label">Replication factor</span>
          </div>
        </motion.div>
      </div>
    </section>
  )
}