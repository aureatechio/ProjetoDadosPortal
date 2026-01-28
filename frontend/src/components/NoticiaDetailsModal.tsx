import { useEffect, useState } from 'react'
import type { Noticia } from '../types'
import { portalApi } from '../api/portalApi'
import { Loading } from './Loading'
import { ErrorState } from './ErrorState'

type Analise = {
  noticia_id: string
  pontos: {
    pesos: Record<string, number>
    scores: Record<string, number>
    contribuicoes: Record<string, number>
    relevancia_calculada: number
    detalhes: Record<string, unknown>
  }
  resumo_tecnico: string | null
  porque_pontuou: string[]
  hipoteses: string[]
  alertas: string[]
}

export function NoticiaDetailsModal({
  open,
  noticia,
  onClose,
}: {
  open: boolean
  noticia: Noticia | null
  onClose: () => void
}) {
  const [data, setData] = useState<Analise | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open || !noticia) return
    let alive = true
    setData(null)
    setError(null)
    portalApi
      .getNoticiaAnalise(noticia.id)
      .then((d) => {
        if (!alive) return
        setData(d)
      })
      .catch((e) => {
        if (!alive) return
        setError(e instanceof Error ? e.message : 'Falha ao carregar análise')
      })
    return () => {
      alive = false
    }
  }, [open, noticia])

  useEffect(() => {
    if (!open) return
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [open, onClose])

  if (!open || !noticia) return null

  return (
    <div
      className="modal-backdrop"
      role="dialog"
      aria-modal="true"
      aria-label="Detalhes da notícia"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose()
      }}
    >
      <div className="modal">
        <div className="row space-between gap-12 wrap">
          <div className="stack gap-8">
            <div className="card-title">Análise da notícia</div>
            <div className="muted">{noticia.titulo}</div>
          </div>
          <div className="row gap-12 wrap">
            <button type="button" className="btn" onClick={onClose}>
              Fechar
            </button>
          </div>
        </div>

        <div style={{ marginTop: 12 }}>
          {error ? (
            <ErrorState message={error} />
          ) : !data ? (
            <Loading label="Gerando/Carregando análise…" />
          ) : (
            <div className="stack gap-16">
              <section className="card">
                <div className="card-title">Pontos (scores)</div>
                <div className="grid two" style={{ marginTop: 10 }}>
                  <ScoreRow label="Recência" score={data.pontos.scores.recencia} peso={data.pontos.pesos.recencia} contrib={data.pontos.contribuicoes.recencia} />
                  <ScoreRow label="Menção" score={data.pontos.scores.mencao} peso={data.pontos.pesos.mencao} contrib={data.pontos.contribuicoes.mencao} />
                  <ScoreRow label="Fonte" score={data.pontos.scores.fonte} peso={data.pontos.pesos.fonte} contrib={data.pontos.contribuicoes.fonte} />
                  <ScoreRow label="Engajamento" score={data.pontos.scores.engajamento} peso={data.pontos.pesos.engajamento} contrib={data.pontos.contribuicoes.engajamento} />
                </div>

                <div className="row gap-12 wrap" style={{ marginTop: 10 }}>
                  <span className="pill">relevância total (DB): {fmt(data.pontos.scores.relevancia_total)}</span>
                  <span className="pill">relevância (calculada): {fmt(data.pontos.relevancia_calculada)}</span>
                  <a className="pill" href={noticia.url} target="_blank" rel="noreferrer">
                    abrir fonte
                  </a>
                </div>
              </section>

              <section className="card">
                <div className="card-title">Resumo técnico</div>
                {data.resumo_tecnico ? (
                  <ul className="ul">
                    {data.resumo_tecnico
                      .split('\n')
                      .map((l) => l.trim())
                      .filter(Boolean)
                      .map((l, i) => (
                        <li key={i}>{l.replace(/^-+\s?/, '')}</li>
                      ))}
                  </ul>
                ) : (
                  <div className="muted">Sem resumo (OpenAI não configurado ou falhou).</div>
                )}
              </section>

              {data.porque_pontuou.length ? (
                <section className="card">
                  <div className="card-title">Por que pontuou</div>
                  <ul className="ul">
                    {data.porque_pontuou.map((t, i) => (
                      <li key={i}>{t}</li>
                    ))}
                  </ul>
                </section>
              ) : null}

              {data.hipoteses.length ? (
                <section className="card">
                  <div className="card-title">Hipóteses / próximos passos</div>
                  <ul className="ul">
                    {data.hipoteses.map((t, i) => (
                      <li key={i}>{t}</li>
                    ))}
                  </ul>
                </section>
              ) : null}

              {data.alertas.length ? (
                <section className="card">
                  <div className="card-title">Alertas</div>
                  <ul className="ul">
                    {data.alertas.map((t, i) => (
                      <li key={i}>{t}</li>
                    ))}
                  </ul>
                </section>
              ) : null}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function ScoreRow({ label, score, peso, contrib }: { label: string; score: number; peso: number; contrib: number }) {
  return (
    <div className="score-row">
      <div className="row space-between gap-12">
        <div className="item-title">{label}</div>
        <span className="pill">
          score: {fmt(score)} • peso: {Math.round(peso * 100)}% • contrib: {fmt(contrib)}
        </span>
      </div>
      <div className="bar">
        <div className="bar-fill" style={{ width: `${clampPct(score)}%` }} />
      </div>
    </div>
  )
}

function clampPct(v: number) {
  const n = Number.isFinite(v) ? v : 0
  return Math.max(0, Math.min(100, n))
}

function fmt(v: number) {
  return (Number.isFinite(v) ? v : 0).toFixed(1)
}

