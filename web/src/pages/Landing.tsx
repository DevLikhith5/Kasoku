import { useState, useMemo, useRef } from 'react'
import { motion, useScroll, useSpring, useTransform } from 'framer-motion'
import { Link } from 'react-router-dom'
import { Hero } from '../components/landing/Hero'
import { Features } from '../components/landing/Features'
import { ArchitectureViz } from '../components/landing/ArchitectureViz'
import { Benchmarks } from '../components/landing/Benchmarks'
import { Footer } from '../components/landing/Footer'
import { HashRing, type KeyRoute } from '../lib/hashRing'

export function Landing() {
  const containerRef = useRef(null)
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const [demoKey, setDemoKey] = useState('user:1001')

  const ring = useMemo(() => {
    const r = new HashRing(150)
    r.addNode('node-1')
    r.addNode('node-2')
    r.addNode('node-3')
    return r
  }, [])

  const keyRoute: KeyRoute | null = useMemo(() => {
    if (!demoKey.trim()) return null
    return ring.routeKey(demoKey.trim(), 3)
  }, [demoKey, ring])

  const { scrollYProgress } = useScroll()
  const scaleX = useSpring(scrollYProgress, {
    stiffness: 100,
    damping: 30,
    restDelta: 0.001
  })

  const yBG1 = useTransform(scrollYProgress, [0, 1], [0, 600])
  const yBG2 = useTransform(scrollYProgress, [0, 1], [0, -400])

  return (
    <div className="landing" ref={containerRef}>
      <motion.div className="scroll-progress" style={{ scaleX }} />
      
      <motion.div className="parallax-blob-1" style={{ y: yBG1 }} />
      <motion.div className="parallax-blob-2" style={{ y: yBG2 }} />

      <nav className="nav">
        <div className="nav-inner">
          <a href="#top" className="nav-logo">
            <img src="/logo.svg" alt="kasoku" className="nav-logo-img" />
          </a>
          <div className="nav-links">
            <a href="#features" className="nav-link-underline">Features</a>
            <a href="#architecture" className="nav-link-underline">Architecture</a>
            <a href="#benchmarks" className="nav-link-underline">Benchmarks</a>
            <Link to="/docs" className="nav-link-underline">Docs</Link>
            <Link to="/dashboard" className="nav-cta">Dashboard</Link>
          </div>
          <button
            className="nav-mobile-toggle"
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            aria-label="Toggle menu"
          >
            <span />
            <span />
            <span />
          </button>
        </div>
      </nav>

      {mobileMenuOpen && (
        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -10 }}
          className="nav-mobile-menu"
        >
          <a href="#features" onClick={() => setMobileMenuOpen(false)}>Features</a>
          <a href="#architecture" onClick={() => setMobileMenuOpen(false)}>Architecture</a>
          <a href="#benchmarks" onClick={() => setMobileMenuOpen(false)}>Benchmarks</a>
          <Link to="/docs" onClick={() => setMobileMenuOpen(false)}>Docs</Link>
          <Link to="/dashboard" onClick={() => setMobileMenuOpen(false)}>Dashboard</Link>
        </motion.div>
      )}

      <Hero />

      {/* Hash Ring Demo */}
      <motion.section
        className="hash-ring-demo"
        initial={{ opacity: 0, y: 30 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true, margin: '-60px' }}
        transition={{ duration: 0.6, ease: [0.22, 1, 0.36, 1] }}
      >
        <div className="hash-ring-inner">
          <h2 className="hash-ring-title">Try the Consistent Hash Ring</h2>
          <p className="hash-ring-subtitle">
            Enter any key to see how it's routed through the ring to the correct vNode and replicas.
          </p>

          <motion.div
            className="hash-ring-input-wrap"
            whileFocus={{ scale: 1.01 }}
            transition={{ duration: 0.2 }}
          >
            <label className="hash-ring-label">Key</label>
            <input
              value={demoKey}
              onChange={(e) => setDemoKey(e.target.value)}
              placeholder="e.g. user:1001"
              className="hash-ring-input-field"
            />
          </motion.div>

          {keyRoute && (
            <motion.div
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              className="hash-ring-routing-card"
            >
              <div className="hash-ring-routing-header">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                  <path d="M8 1a7 7 0 100 14A7 7 0 008 1zm2.5 7.5a1 1 0 01-1.293.954L8 8.207V4a1 1 0 112 0v4.207l.793.793a1 1 0 01-.293 1.414z" fill="currentColor" opacity="0.6" />
                  <circle cx="8" cy="10" r="1.5" fill="currentColor" />
                </svg>
                <span>Key Routing</span>
              </div>

              <div className="hash-ring-routing-flow">
                <motion.div
                  className="hash-ring-step-card"
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.05 }}
                  whileHover={{ scale: 1.03, y: -2 }}
                >
                  <div className="hash-ring-step-icon pink">
                    <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
                      <text x="3" y="14" fontFamily="monospace" fontSize="14" fontWeight="700" fill="white">Σ</text>
                    </svg>
                  </div>
                  <div className="hash-ring-step-content">
                    <span className="hash-ring-step-label">CRC32 Hash</span>
                    <code className="hash-ring-step-value">{keyRoute.hashPosition >>> 0}</code>
                  </div>
                </motion.div>

                <motion.svg
                  className="hash-ring-connector"
                  width="24" height="24" viewBox="0 0 24 24" fill="none"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 0.5 }}
                  transition={{ delay: 0.1 }}
                >
                  <path d="M5 12h14M13 6l6 6-6 6" stroke="var(--text-muted)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                </motion.svg>

                <motion.div
                  className="hash-ring-step-card"
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.15 }}
                  whileHover={{ scale: 1.03, y: -2 }}
                >
                  <div className="hash-ring-step-icon pink">
                    <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
                      <text x="4" y="14" fontFamily="monospace" fontSize="13" fontWeight="700" fill="white">V</text>
                    </svg>
                  </div>
                  <div className="hash-ring-step-content">
                    <span className="hash-ring-step-label">Primary VNode</span>
                    <code className="hash-ring-step-value">
                      {keyRoute.vnode.nodeId}#vnode{keyRoute.vnode.index}
                    </code>
                  </div>
                </motion.div>

                <motion.svg
                  className="hash-ring-connector"
                  width="24" height="24" viewBox="0 0 24 24" fill="none"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 0.5 }}
                  transition={{ delay: 0.2 }}
                >
                  <path d="M5 12h14M13 6l6 6-6 6" stroke="var(--text-muted)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                </motion.svg>

                <motion.div
                  className="hash-ring-replicas-card"
                  initial={{ opacity: 0, x: 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: 0.25 }}
                  whileHover={{ scale: 1.02, y: -2 }}
                >
                  <span className="hash-ring-step-label replicas-label">Replicas (N=3, W=3)</span>
                  <div className="hash-ring-replica-list">
                    {keyRoute.replicas.map((r, i) => (
                      <motion.div
                        key={r}
                        className="hash-ring-replica-item"
                        initial={{ opacity: 0, x: 20 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: 0.3 + i * 0.08 }}
                        whileHover={{ scale: 1.05, x: 4 }}
                      >
                        <span className="hash-ring-replica-number">{i + 1}</span>
                        <code className="hash-ring-replica-name">{r}</code>
                      </motion.div>
                    ))}
                  </div>
                </motion.div>
              </div>
            </motion.div>
          )}
        </div>
      </motion.section>

      <Features />
      <ArchitectureViz />
      <Benchmarks />
      <Footer />
    </div>
  )
}
