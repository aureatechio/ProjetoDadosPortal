import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { portalApi } from '../api/portalApi'
import type { Politico, SocialMention } from '../types'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'
import { SocialMentionCard } from '../components/SocialMentionCard'

const PLATAFORMAS = ['bluesky', 'google_trends', 'google_search', 'twitter'] as const

export function PoliticoMencoesPage() {
  const { id } = useParams()
  const politicoId = Number(id)
  const [politico, setPolitico] = useState<Politico | null>(null)
  const [mentions, setMentions] = useState<SocialMention[] | null>(null)
  const [limit, setLimit] = useState(50)
  const [plataforma, setPlataforma] = useState<string>('') // '' = todas
  const [error, setError] = useState<string | null>(null)

  async function load() {
    setError(null)
    try {
      const [p, m] = await Promise.all([
        portalApi.getPolitico(politicoId),
        portalApi.getSocialMentionsPolitico(politicoId, plataforma || null, limit),
      ])
      setPolitico(p)
      setMentions(m)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar menções')
    }
  }

  useEffect(() => {
    if (!Number.isFinite(politicoId)) return
    void load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [politicoId, limit, plataforma])

  const grouped = useMemo(() => {
    const rows = mentions ?? []
    const map = new Map<string, SocialMention[]>()
    for (const m of rows) {
      const key = m.assunto ?? 'Outro'
      map.set(key, [...(map.get(key) ?? []), m])
    }
    return [...map.entries()].sort((a, b) => b[1].length - a[1].length)
  }, [mentions])

  if (!Number.isFinite(politicoId)) return <ErrorState title="ID inválido" message="Político não encontrado." />
  if (error) return <ErrorState message={error} onRetry={load} />
  if (!politico || !mentions) return <Loading label="Carregando menções…" />

  return (
    <div className="stack gap-16">
      <section className="hero">
        <div className="row space-between gap-12 wrap">
          <div>
            <h1>Menções sociais</h1>
            <p className="muted">{politico.name} • ordenadas por engajamento</p>
          </div>
          <Link className="btn" to={`/politicos/${politico.id}`}>
            Voltar ao resumo
          </Link>
        </div>

        <div className="row gap-12 wrap">
          <label className="field">
            <span className="muted">Plataforma</span>
            <select className="input" value={plataforma} onChange={(e) => setPlataforma(e.target.value)}>
              <option value="">todas</option>
              {PLATAFORMAS.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span className="muted">Limite</span>
            <input
              className="input"
              type="number"
              min={1}
              max={200}
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
            />
          </label>
        </div>
      </section>

      <section className="stack gap-12">
        {mentions.length ? (
          grouped.map(([assunto, rows]) => (
            <div key={assunto} className="stack gap-12">
              <div className="row space-between gap-12 wrap">
                <h2>{assunto}</h2>
                <span className="pill">{rows.length} menções</span>
              </div>
              {rows.map((m) => (
                <SocialMentionCard key={m.id} m={m} />
              ))}
            </div>
          ))
        ) : (
          <div className="muted">Sem menções coletadas ainda.</div>
        )}
      </section>
    </div>
  )
}

