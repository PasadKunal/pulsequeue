import { useEffect, useState, useRef } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'

const API_BASE = ""

interface DataPoint {
  windowStart: string
  p50: number
  p95: number
  p99: number
  errorRate: number
}

interface ServiceMetrics {
  service: string
  p50: number
  p95: number
  p99: number
  errorRate: number
  totalEvents: number
  windowStart: string
  history: DataPoint[]
}

interface SSEPayload {
  timestamp: string
  services: ServiceMetrics[]
}

const formatTime = (iso: string) => {
  const d = new Date(iso)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
}

const formatPct = (v: number) => `${(v * 100).toFixed(1)}%`

function MetricCard({ label, value, unit = 'ms' }: { label: string; value: number; unit?: string }) {
  return (
    <div style={{ background: '#1e2130', borderRadius: 8, padding: '12px 16px', minWidth: 120 }}>
      <div style={{ fontSize: 11, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 600, color: '#e2e8f0' }}>{value}<span style={{ fontSize: 12, color: '#64748b', marginLeft: 4 }}>{unit}</span></div>
    </div>
  )
}

function ServicePanel({ metrics }: { metrics: ServiceMetrics }) {
  const chartData = metrics.history.map(d => ({
    time: formatTime(d.windowStart),
    p50: d.p50,
    p95: d.p95,
    p99: d.p99,
  }))

  return (
    <div style={{ background: '#161b27', borderRadius: 12, padding: 20, marginBottom: 20, border: '1px solid #1e2a3a' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <h2 style={{ fontSize: 16, fontWeight: 600, color: '#e2e8f0' }}>{metrics.service}</h2>
          <div style={{ fontSize: 12, color: '#64748b', marginTop: 2 }}>
            last window: {formatTime(metrics.windowStart)} &middot; {metrics.totalEvents} events
          </div>
        </div>
        <div style={{ display: 'flex', gap: 10 }}>
          <MetricCard label="p50" value={metrics.p50} />
          <MetricCard label="p95" value={metrics.p95} />
          <MetricCard label="p99" value={metrics.p99} />
          <MetricCard label="error rate" value={parseFloat((metrics.errorRate * 100).toFixed(1))} unit="%" />
        </div>
      </div>

      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2a3a" />
          <XAxis dataKey="time" tick={{ fill: '#64748b', fontSize: 11 }} />
          <YAxis tick={{ fill: '#64748b', fontSize: 11 }} unit="ms" />
          <Tooltip
            contentStyle={{ background: '#1e2130', border: '1px solid #1e2a3a', borderRadius: 8 }}
            labelStyle={{ color: '#94a3b8' }}
          />
          <Legend wrapperStyle={{ fontSize: 12, color: '#94a3b8' }} />
          <Line type="monotone" dataKey="p50" stroke="#3b82f6" strokeWidth={2} dot={false} name="p50" />
          <Line type="monotone" dataKey="p95" stroke="#f59e0b" strokeWidth={2} dot={false} name="p95" />
          <Line type="monotone" dataKey="p99" stroke="#ef4444" strokeWidth={2} dot={false} name="p99" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export default function App() {
  const [services, setServices] = useState<ServiceMetrics[]>([])
  const [connected, setConnected] = useState(false)
  const [lastUpdate, setLastUpdate] = useState<string>('')
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    const connect = () => {
      const es = new EventSource(`${API_BASE}/v1/dashboard/stream`)
      esRef.current = es

      es.addEventListener('metrics', (e: MessageEvent) => {
        const payload: SSEPayload = JSON.parse(e.data)
        setServices(payload.services)
        setLastUpdate(new Date(payload.timestamp).toLocaleTimeString())
        setConnected(true)
      })

      es.onerror = () => {
        setConnected(false)
        es.close()
        setTimeout(connect, 3000)
      }
    }

    connect()
    return () => esRef.current?.close()
  }, [])

  return (
    <div style={{ maxWidth: 1100, margin: '0 auto', padding: '24px 20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 700, color: '#e2e8f0' }}>PulseQueue</h1>
          <div style={{ fontSize: 13, color: '#64748b', marginTop: 2 }}>real-time service latency dashboard</div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{
            width: 8, height: 8, borderRadius: '50%',
            background: connected ? '#22c55e' : '#ef4444',
            boxShadow: connected ? '0 0 6px #22c55e' : 'none'
          }} />
          <span style={{ fontSize: 12, color: '#64748b' }}>
            {connected ? `live - updated ${lastUpdate}` : 'connecting...'}
          </span>
        </div>
      </div>

      {services.length === 0 ? (
        <div style={{ textAlign: 'center', color: '#64748b', paddingTop: 80 }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>...</div>
          <div>waiting for data</div>
        </div>
      ) : (
        services.map(s => <ServicePanel key={s.service} metrics={s} />)
      )}
    </div>
  )
}
