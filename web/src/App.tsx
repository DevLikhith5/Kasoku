import { Routes, Route } from 'react-router-dom'
import './App.css'
import { Landing } from './pages/Landing'
import { Dashboard } from './pages/Dashboard'
import { Docs } from './pages/Docs'

function App() {
  return (
    <Routes>
      <Route path="/" element={<Landing />} />
      <Route path="/docs" element={<Docs />} />
      <Route path="/dashboard/*" element={<Dashboard />} />
    </Routes>
  )
}

export default App
