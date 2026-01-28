import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { portalApi } from '../api/portalApi'
import type { InstagramPost, Politico } from '../types'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'
import { InstagramCard } from '../components/InstagramCard'

export function PoliticoInstagramPage() {
  const { id } = useParams()
  const politicoId = Number(id)
  const [politico, setPolitico] = useState<Politico | null>(null)
  const [posts, setPosts] = useState<InstagramPost[] | null>(null)
  const [limit, setLimit] = useState(20)
  const [error, setError] = useState<string | null>(null)

  async function load() {
    setError(null)
    try {
      const [p, ig] = await Promise.all([portalApi.getPolitico(politicoId), portalApi.getInstagramPolitico(politicoId, limit)])
      setPolitico(p)
      setPosts(ig)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar Instagram')
    }
  }

  useEffect(() => {
    if (!Number.isFinite(politicoId)) return
    void load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [politicoId, limit])

  if (!Number.isFinite(politicoId)) return <ErrorState title="ID inválido" message="Político não encontrado." />
  if (error) return <ErrorState message={error} onRetry={load} />
  if (!politico || !posts) return <Loading label="Carregando Instagram…" />

  return (
    <div className="stack gap-16">
      <section className="hero">
        <div className="row space-between gap-12 wrap">
          <div>
            <h1>Instagram</h1>
            <p className="muted">{politico.name} • ordenado por engajamento</p>
          </div>
          <Link className="btn" to={`/politicos/${politico.id}`}>
            Voltar ao resumo
          </Link>
        </div>

        <div className="row gap-12 wrap">
          <label className="field">
            <span className="muted">Limite</span>
            <input
              className="input"
              type="number"
              min={1}
              max={50}
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
            />
          </label>
        </div>
      </section>

      <section className="grid auto">
        {posts.length ? posts.map((p) => <InstagramCard key={p.id} post={p} />) : <div className="muted">Sem posts.</div>}
      </section>
    </div>
  )
}

