import { useEffect, useState } from 'react'
import { portalApi } from '../api/portalApi'
import type { ColetaLog } from '../types'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'

export function AdminPage() {
  const [logs, setLogs] = useState<ColetaLog[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function load() {
    setError(null)
    try {
      const l = await portalApi.getColetaLogs(100)
      setLogs(l)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar logs')
    }
  }

  useEffect(() => {
    void load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  if (error) return <ErrorState message={error} onRetry={load} />
  if (!logs) return <Loading label="Carregando logs…" />

  return (
    <div className="stack gap-16">
      <section className="hero">
        <h1>Admin</h1>
        <p className="muted">Visualização dos logs de coleta. As coletas são executadas automaticamente no servidor Python.</p>
      </section>

      <section className="card">
        <div className="row space-between gap-12 wrap">
          <div>
            <div className="card-title">Servidor de Coletas</div>
            <p className="muted" style={{ marginTop: 4 }}>
              Os jobs de coleta rodam em servidor separado (Digital Ocean).
              <br />
              Para executar coletas manuais, acesse o servidor Python diretamente.
            </p>
          </div>
          <button type="button" className="btn secondary" onClick={() => void load()}>
            Atualizar logs
          </button>
        </div>
      </section>

      <section className="card">
        <div className="card-title">Logs de Coleta ({logs.length} registros)</div>
        <div className="list" style={{ maxHeight: 600, overflowY: 'auto' }}>
          {logs.length === 0 ? (
            <div className="muted" style={{ padding: 16 }}>Nenhum log encontrado.</div>
          ) : (
            logs.map((l) => (
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
            ))
          )}
        </div>
      </section>

      <section className="card">
        <div className="card-title">Jobs Agendados no Servidor</div>
        <p className="muted" style={{ marginTop: 4 }}>
          Os jobs são gerenciados pelo APScheduler no servidor Python:
        </p>
        <div className="list" style={{ marginTop: 12 }}>
          <div className="list-row">
            <div className="grow">
              <div className="item-title">Coleta de Notícias</div>
              <div className="muted">Google News + NewsAPI</div>
            </div>
            <div className="pill">06:00</div>
          </div>
          <div className="list-row">
            <div className="grow">
              <div className="item-title">Coleta Instagram</div>
              <div className="muted">Posts mais engajados</div>
            </div>
            <div className="pill">06:45</div>
          </div>
          <div className="list-row">
            <div className="grow">
              <div className="item-title">Coleta Menções Sociais</div>
              <div className="muted">BlueSky, Google Trends</div>
            </div>
            <div className="pill">07:00</div>
          </div>
          <div className="list-row">
            <div className="grow">
              <div className="item-title">Coleta Trending Topics</div>
              <div className="muted">Política + Twitter + Google</div>
            </div>
            <div className="pill">08:00</div>
          </div>
          <div className="list-row">
            <div className="grow">
              <div className="item-title">Limpeza de Dados</div>
              <div className="muted">Remove dados antigos</div>
            </div>
            <div className="pill">08:15</div>
          </div>
          <div className="list-row">
            <div className="grow">
              <div className="item-title">Coleta Processual TSE</div>
              <div className="muted">Candidaturas, doações, filiações</div>
            </div>
            <div className="pill">Dom 03:00</div>
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

