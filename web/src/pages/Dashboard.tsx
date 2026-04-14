import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Database, Network, BarChart3, Settings, Book } from 'lucide-react'
import { Link } from 'react-router-dom'
import { KVOperations } from '../components/dashboard/KVOperations'
import { ClusterStatus } from '../components/dashboard/ClusterStatus'
import { MetricsView } from '../components/dashboard/MetricsView'
import { SettingsPanel } from '../components/dashboard/SettingsPanel'

const API_BASE = import.meta.env.VITE_API_URL || ''

interface NavItem {
  id: string
  label: string
  icon: typeof Database
}

const NAV_ITEMS: NavItem[] = [
  { id: 'operations', label: 'Key-Value Store', icon: Database },
  { id: 'cluster', label: 'Cluster', icon: Network },
  { id: 'metrics', label: 'Metrics', icon: BarChart3 },
  { id: 'settings', label: 'Settings', icon: Settings },
]

export function Dashboard() {
  const [activeNav, setActiveNav] = useState('operations')
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)

  return (
    <div className="dashboard">
      <aside className={`dashboard-sidebar ${sidebarCollapsed ? 'collapsed' : ''}`}>
        <div className="sidebar-header">
          <button
            className="sidebar-logo"
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
          >
            <img src="/logo-icon.svg" alt="kasoku" className="sidebar-logo-img" />
            {!sidebarCollapsed && (
              <span className="sidebar-logo-text">kasoku</span>
            )}
          </button>
        </div>

        <nav className="sidebar-nav">
          {NAV_ITEMS.map((item) => {
            const Icon = item.icon
            return (
              <button
                key={item.id}
                className={`sidebar-nav-item ${activeNav === item.id ? 'active' : ''}`}
                onClick={() => setActiveNav(item.id)}
                title={item.label}
              >
                <span className="sidebar-nav-icon">
                  <Icon size={18} strokeWidth={1.8} />
                </span>
                {!sidebarCollapsed && (
                  <span className="sidebar-nav-label">{item.label}</span>
                )}
              </button>
            )
          })}
          
          <div style={{ marginTop: 'auto', paddingTop: '20px', borderTop: '1px solid var(--border)' }}>
            <Link 
              to="/docs" 
              className="sidebar-nav-item" 
              style={{ color: 'var(--text-muted)' }}
              title="Documentation"
            >
              <span className="sidebar-nav-icon">
                <Book size={18} strokeWidth={1.8} />
              </span>
              {!sidebarCollapsed && (
                <span className="sidebar-nav-label">Docs</span>
              )}
            </Link>
          </div>
        </nav>
      </aside>

      <main className={`dashboard-main ${sidebarCollapsed ? 'collapsed' : ''}`}>
        <AnimatePresence mode="wait">
          <motion.div
            key={activeNav}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.2 }}
            className="dashboard-content"
          >
            {activeNav === 'operations' && <KVOperations apiBase={API_BASE} />}
            {activeNav === 'cluster' && <ClusterStatus apiBase={API_BASE} />}
            {activeNav === 'metrics' && <MetricsView apiBase={API_BASE} />}
            {activeNav === 'settings' && <SettingsPanel apiBase={API_BASE} />}
          </motion.div>
        </AnimatePresence>
      </main>
    </div>
  )
}
