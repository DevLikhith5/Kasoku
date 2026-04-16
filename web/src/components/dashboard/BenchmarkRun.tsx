import { useState } from 'react'
import { Play, Loader2 } from 'lucide-react'

export function BenchmarkRun({ apiBase }: { apiBase: string }) {
  const [running, setRunning] = useState(false)
  const [results, setResults] = useState<any>(null)

  const api = (path: string) => {
    if (apiBase.startsWith('http')) return `${apiBase}${path}`
    return path
  }

  const runBenchmark = async (type: 'write' | 'read' | 'batch') => {
    setRunning(true)
    setResults(null)

    try {
      const res = await fetch(api(`/api/v1/benchmark?type=${type}`), {
        method: 'POST',
      })
      const data = await res.json()
      setResults(data)
    } catch (e) {
      setResults({ error: 'Benchmark endpoint not implemented' })
    } finally {
      setRunning(false)
    }
  }

  return (
    <div className="benchmark-run">
      <h3>Run Benchmark</h3>
      
      <div className="benchmark-buttons">
        <button
          className="btn btn-primary"
          onClick={() => runBenchmark('write')}
          disabled={running}
        >
          <Play size={16} />
          Writes
        </button>
        
        <button
          className="btn btn-primary"
          onClick={() => runBenchmark('read')}
          disabled={running}
        >
          <Play size={16} />
          Single-Key Reads
        </button>
        
        <button
          className="btn btn-primary"
          onClick={() => runBenchmark('batch')}
          disabled={running}
        >
          <Play size={16} />
          Batch Reads
        </button>
      </div>

      {running && (
        <div className="benchmark-loading">
          <Loader2 className="spin" size={20} />
          Running benchmark...
        </div>
      )}

      {results && (
        <div className="benchmark-results">
          {results.error ? (
            <p className="error">{results.error}</p>
          ) : (
            <pre>{JSON.stringify(results, null, 2)}</pre>
          )}
        </div>
      )}
    </div>
  )
}