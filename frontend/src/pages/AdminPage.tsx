import { useEffect, useState } from 'react'
import { portalApi } from '../api/portalApi'
import type { ColetaLog, ScheduledJob } from '../types'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'

type ColetaTipo =
  | 'completa'
  | 'noticias'
  | 'instagram'
  | 'trending'
  | 'trending_twitter'
  | 'trending_google'
  | 'socials'
  | 'social_mentions'
  | 'processual_tse'

export function AdminPage() {
  const [jobs, setJobs] = useState<ScheduledJob[] | null>(null)
  const [logs, setLogs] = useState<ColetaLog[] | null>(null)
  const [tipo, setTipo] = useState<ColetaTipo>('completa')
  const [dryRun, setDryRun] = useState(false)
  const [running, setRunning] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [lastMsg, setLastMsg] = useState<string | null>(null)

  async function load() {
    setError(null)
    try {
      const [j, l] = await Promise.all([portalApi.getColetaJobs(), portalApi.getColetaLogs(50)])
      setJobs(j)
      setLogs(l)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar admin')
    }
  }

  useEffect(() => {
    void load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function runColeta() {
    setRunning(true)
    setLastMsg(null)
    setError(null)
    try {
      const res = await portalApi.executarColeta(tipo, tipo === 'socials' ? dryRun : false)
      setLastMsg(res.mensagem)
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao iniciar coleta')
    } finally {
      setRunning(false)
    }
  }

  if (error) return <ErrorState message={error} onRetry={load} />
  if (!jobs || !logs) return <Loading label="Carregando admin…" />

  return (
    <div className="stack gap-16">
      <section className="hero">
        <h1>Admin</h1>
        <p className="muted">Executar coleta manual e acompanhar jobs/logs.</p>
      </section>

      <section className="card">
        <div className="card-title">Executar coleta manual</div>
        <div className="row gap-12 wrap">
          <label className="field">
            <span className="muted">Tipo</span>
            <select className="input" value={tipo} onChange={(e) => setTipo(e.target.value as ColetaTipo)}>
              <option value="completa">completa</option>
              <option value="noticias">noticias</option>
              <option value="instagram">instagram</option>
              <option value="trending">trending (política + Twitter + Google)</option>
              <option value="trending_twitter">trending_twitter (Twitter/X)</option>
              <option value="trending_google">trending_google (Google)</option>
              <option value="socials">socials</option>
              <option value="social_mentions">social_mentions</option>
              <option value="processual_tse">processual_tse</option>
            </select>
          </label>

          {tipo === 'socials' ? (
            <div className="field">
              <span className="muted">Dry-run</span>
              <label className="row gap-8">
                <input type="checkbox" checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} />
                <span className="muted">não grava no banco</span>
              </label>
            </div>
          ) : null}

          <button type="button" className="btn" onClick={() => void runColeta()} disabled={running}>
            {running ? 'Iniciando…' : 'Executar'}
          </button>

          <button type="button" className="btn secondary" onClick={() => void load()} disabled={running}>
            Atualizar
          </button>
        </div>

        {lastMsg ? <div className="muted">Resposta: {lastMsg}</div> : null}
      </section>

      <section className="grid two">
        <div className="card">
          <div className="card-title">Jobs agendados</div>
          <div className="list">
            {jobs.map((j) => (
              <div key={j.id} className="list-row">
                <div className="grow">
                  <div className="item-title">{j.name}</div>
                  <div className="muted">id: {j.id}</div>
                </div>
                <div className="pill">{j.next_run ? formatDateTime(j.next_run) : '—'}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="card">
          <div className="card-title">Logs recentes</div>
          <div className="list">
            {logs.map((l) => (
              <div key={l.id} className="list-row">
                <div className="grow">
                  <div className="item-title">
                    {l.tipo_coleta} • <span className={`status ${l.status}`}>{l.status}</span>
                  </div>
                  <div className="muted">
                    {l.mensagem ?? '—'} {typeof l.registros_coletados === 'number' ? `• ${l.registros_coletados} registros` : ''}
                  </div>
                </div>
                <div className="pill">{l.iniciado_em ? formatDateTime(l.iniciado_em) : '—'}</div>
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  )
}

function formatDateTime(iso: string) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return new Intl.DateTimeFormat('pt-BR', { dateStyle: 'short', timeStyle: 'short' }).format(d)
}

