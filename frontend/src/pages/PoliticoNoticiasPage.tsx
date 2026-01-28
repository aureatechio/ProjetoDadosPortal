import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { portalApi } from '../api/portalApi'
import type { Noticia, Politico } from '../types'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'
import { NoticiaCard } from '../components/NoticiaCard'
import { NoticiaDetailsModal } from '../components/NoticiaDetailsModal'

export function PoliticoNoticiasPage() {
  const { id } = useParams()
  const politicoId = Number(id)
  const [politico, setPolitico] = useState<Politico | null>(null)
  const [noticias, setNoticias] = useState<Noticia[] | null>(null)
  const [minScore, setMinScore] = useState(30)
  const [limit, setLimit] = useState(30)
  const [error, setError] = useState<string | null>(null)
  const [selectedNoticia, setSelectedNoticia] = useState<Noticia | null>(null)

  async function load() {
    setError(null)
    try {
      const [p, n] = await Promise.all([
        portalApi.getPolitico(politicoId),
        portalApi.getNoticiasPolitico(politicoId, limit, minScore),
      ])
      setPolitico(p)
      setNoticias(n)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar notícias')
    }
  }

  useEffect(() => {
    if (!Number.isFinite(politicoId)) return
    void load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [politicoId, minScore, limit])

  if (!Number.isFinite(politicoId)) return <ErrorState title="ID inválido" message="Político não encontrado." />
  if (error) return <ErrorState message={error} onRetry={load} />
  if (!politico || !noticias) return <Loading label="Carregando notícias…" />

  return (
    <div className="stack gap-16">
      <NoticiaDetailsModal open={!!selectedNoticia} noticia={selectedNoticia} onClose={() => setSelectedNoticia(null)} />
      <section className="hero">
        <div className="row space-between gap-12 wrap">
          <div>
            <h1>Notícias</h1>
            <p className="muted">
              {politico.name} • ordenadas por relevância
            </p>
          </div>
          <Link className="btn" to={`/politicos/${politico.id}`}>
            Voltar ao resumo
          </Link>
        </div>

        <div className="row gap-12 wrap">
          <label className="field">
            <span className="muted">Score mínimo</span>
            <input
              className="input"
              type="number"
              min={0}
              max={100}
              value={minScore}
              onChange={(e) => setMinScore(Number(e.target.value))}
            />
          </label>
          <label className="field">
            <span className="muted">Limite</span>
            <input
              className="input"
              type="number"
              min={1}
              max={100}
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
            />
          </label>
        </div>
      </section>

      <section className="stack gap-12">
        {noticias.length ? (
          noticias.map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
        ) : (
          <div className="muted">Nenhuma notícia encontrada com esse filtro.</div>
        )}
      </section>
    </div>
  )
}

