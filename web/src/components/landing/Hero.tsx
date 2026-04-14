import { motion, useScroll, useTransform } from 'framer-motion'
import { Link } from 'react-router-dom'
import { useRef } from 'react'

export function Hero() {
  const containerRef = useRef<HTMLElement>(null)
  const { scrollYProgress } = useScroll({
    target: containerRef,
    offset: ['start start', 'end start'],
  })

  const y1 = useTransform(scrollYProgress, [0, 1], [0, 150])
  const y2 = useTransform(scrollYProgress, [0, 1], [0, -100])
  const opacity = useTransform(scrollYProgress, [0, 0.5], [1, 0])
  const scale = useTransform(scrollYProgress, [0, 1], [1, 1.1])

  return (
    <section className="hero" id="top" ref={containerRef}>
      <motion.div className="hero-bg" style={{ y: y1, scale }}>
        <div className="hero-grid" />
        <div className="hero-glow" />
      </motion.div>

      {/* Decorative Parallax Element */}
      <motion.div 
        className="hero-floating-node"
        style={{ y: y2, opacity }}
      />

      <motion.div 
        className="hero-content"
        style={{ opacity }}
      >
        <motion.h1
          initial={{ opacity: 0, y: 24 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.1, ease: [0.22, 1, 0.36, 1] }}
          className="hero-title"
        >
          Built for speed.
          <br />
          Designed for scale.
        </motion.h1>

        <motion.p
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2, ease: [0.22, 1, 0.36, 1] }}
          className="hero-description"
        >
          A distributed, highly available key-value storage engine built on a custom
          LSM-Tree with Dynamo-style replication. Written in Go. No external dependencies.
        </motion.p>

        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.3, ease: [0.22, 1, 0.36, 1] }}
          className="hero-actions"
        >
          <Link to="/dashboard" className="btn btn-primary">
            Open Dashboard
          </Link>
          <a href="#architecture" className="btn btn-secondary">
            See Architecture
          </a>
        </motion.div>

        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.8, delay: 0.5 }}
          className="hero-stats"
        >
          <div className="hero-stat">
            <div className="hero-stat-value">7.3M</div>
            <div className="hero-stat-label">Reads/sec</div>
          </div>
          <div className="hero-stat-divider" />
          <div className="hero-stat">
            <div className="hero-stat-value">174ns</div>
            <div className="hero-stat-label">Read Latency</div>
          </div>
          <div className="hero-stat-divider" />
          <div className="hero-stat">
            <div className="hero-stat-value">N=3</div>
            <div className="hero-stat-label">Replication</div>
          </div>
        </motion.div>
      </motion.div>

      <div className="hero-scroll">
        <span>Scroll to explore</span>
        <div className="hero-scroll-line" />
      </div>
    </section>
  )
}
