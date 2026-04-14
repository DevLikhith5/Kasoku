import { useState, useRef, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'

interface FlowStep {
  id: string
  label: string
  detail: string
  short: string
}

const writeFlow: FlowStep[] = [
  { id: 'client', label: 'Client Request', short: 'Request', detail: 'PUT /api/v1/put' },
  { id: 'coordinator', label: 'Coordinator', short: 'Coordinator', detail: 'Hash ring selects replicas' },
  { id: 'wal', label: 'WAL Append', short: 'WAL', detail: 'Durability guarantee' },
  { id: 'memtable', label: 'MemTable', short: 'MemTable', detail: 'In-memory skip list insert' },
  { id: 'replicate', label: 'Replica Write', short: 'Replicate', detail: 'W=2 quorum replication' },
  { id: 'ack', label: 'Acknowledge', short: 'Ack', detail: 'Response to client' },
]

const readFlow: FlowStep[] = [
  { id: 'client', label: 'Client Request', short: 'Request', detail: 'GET /api/v1/get' },
  { id: 'coordinator', label: 'Coordinator', short: 'Coordinator', detail: 'Resolve key to ring position' },
  { id: 'memtable', label: 'MemTable Check', short: 'MemTable', detail: 'Active + immutable tables' },
  { id: 'bloom', label: 'Bloom Filter', short: 'Bloom', detail: 'Skip absent SSTables' },
  { id: 'sstable', label: 'SSTable Lookup', short: 'SSTable', detail: 'Block cache + disk read' },
  { id: 'quorum', label: 'Quorum Read', short: 'Quorum', detail: 'R=2, return latest version' },
]

export function ArchitectureViz() {
  const [activeFlow, setActiveFlow] = useState<'write' | 'read'>('write')
  const [activeStep, setActiveStep] = useState(0)
  const sectionRef = useRef<HTMLElement>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const flow = activeFlow === 'write' ? writeFlow : readFlow

  const clearAnim = () => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current)
      intervalRef.current = null
    }
  }

  const playAnimation = () => {
    clearAnim()
    setActiveStep(0)
    let step = 0
    intervalRef.current = setInterval(() => {
      step += 1
      if (step >= flow.length) {
        step = 0
        setActiveStep(0)
      } else {
        setActiveStep(step)
      }
    }, 1200)
  }

  // Auto-play when scrolled into view
  useEffect(() => {
    const el = sectionRef.current
    if (!el) return
    const obs = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          playAnimation()
        } else {
          clearAnim()
        }
      },
      { threshold: 0.3 }
    )
    obs.observe(el)
    return () => { obs.disconnect(); clearAnim() }
  }, [flow])

  // Reset on flow switch
  useEffect(() => {
    clearAnim()
    setActiveStep(0)
    if (sectionRef.current) {
      const obs = new IntersectionObserver(
        ([entry]) => {
          if (entry.isIntersecting) playAnimation()
        },
        { threshold: 0.3 }
      )
      obs.observe(sectionRef.current)
      return () => obs.disconnect()
    }
  }, [activeFlow])

  return (
    <section className="architecture" id="architecture" ref={sectionRef}>
      <div className="architecture-inner">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: '-80px' }}
          transition={{ duration: 0.6 }}
          className="architecture-header"
        >
          <h2 className="architecture-title">Request Flow</h2>
          <p className="architecture-subtitle">
            Trace how a request travels through the system.
          </p>
        </motion.div>

        <div className="architecture-visual">
          {/* Flow selector */}
          <div className="flow-selector">
            <button
              className={`flow-btn ${activeFlow === 'write' ? 'active' : ''}`}
              onClick={() => setActiveFlow('write')}
            >
              Write Path
            </button>
            <button
              className={`flow-btn ${activeFlow === 'read' ? 'active' : ''}`}
              onClick={() => setActiveFlow('read')}
            >
              Read Path
            </button>
          </div>

          {/* Flow steps row */}
          <div className="flow-diagram">
            {flow.map((step, i) => (
              <div key={step.id} className="flow-step">
                <button
                  className={`flow-step-node ${i === activeStep ? 'active' : ''} ${i < activeStep ? 'completed' : ''}`}
                  onClick={() => setActiveStep(i)}
                >
                  <span className="flow-step-text">{step.short}</span>
                </button>
                {i < flow.length - 1 && (
                  <span className={`flow-connector ${i < activeStep ? 'completed' : ''}`} />
                )}
              </div>
            ))}
          </div>

          {/* Detail panel */}
          <AnimatePresence mode="wait">
            <motion.div
              key={`${activeFlow}-${activeStep}`}
              initial={{ opacity: 0, y: 4 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -4 }}
              transition={{ duration: 0.2 }}
              className="flow-detail"
            >
              <span className="flow-detail-id">{flow[activeStep].label}</span>
              <p className="flow-detail-text">{flow[activeStep].detail}</p>
            </motion.div>
          </AnimatePresence>
        </div>
      </div>
    </section>
  )
}
