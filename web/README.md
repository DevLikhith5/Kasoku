# Kasoku Web Dashboard

Real-time monitoring and management interface for the Kasoku distributed key-value store.

## Features

- **Cluster Status**: Live view of all nodes, their health, and ring membership
- **Metrics Dashboard**: Operations/sec, latency percentiles, memory usage
- **KV Operations**: Put, get, delete, and scan keys directly from the UI
- **Benchmark Viewer**: Visual comparison of single-node vs cluster performance

## Getting Started

```bash
cd web
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173) (or the port shown in terminal).

## Development

```bash
# Development with HMR
npm run dev

# Production build
npm run build

# Preview production build
npm run preview
```

## Tech Stack

- React 18 + TypeScript
- Vite (build tool)
- Recharts (charts)
- Framer Motion (animations)
- Lucide React (icons)
- Tailwind CSS (styling)