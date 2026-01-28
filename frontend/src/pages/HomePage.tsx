import { useEffect, useMemo, useState } from 'react'
import { portalApi } from '../api/portalApi'
import type { Noticia, Politico, TrendingTopic } from '../types'
import { PoliticoCard } from '../components/PoliticoCard'
import { TrendingList } from '../components/TrendingList'
import { NoticiaCard } from '../components/NoticiaCard'
import { NoticiaDetailsModal } from '../components/NoticiaDetailsModal'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'

export function HomePage() {
  const [politicos, setPoliticos] = useState<Politico[] | null>(null)
  const [trendingPolitica, setTrendingPolitica] = useState<TrendingTopic[] | null>(null)
  const [trendingTwitter, setTrendingTwitter] = useState<TrendingTopic[] | null>(null)
  const [trendingGoogle, setTrendingGoogle] = useState<TrendingTopic[] | null>(null)
  const [noticiasGerais, setNoticiasGerais] = useState<Noticia[] | null>(null)
  const [q, setQ] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [selectedNoticia, setSelectedNoticia] = useState<Noticia | null>(null)

  async function load() {
    setError(null)
    try {
      const [p, tp, tt, tg, n] = await Promise.all([
        portalApi.getPoliticos(),
        portalApi.getTrending('politica'),
        portalApi.getTrending('twitter'),
        portalApi.getTrending('google'),
        portalApi.getNoticiasPolitica(),
      ])
      setPoliticos(p.filter((x) => x.usar_diretoriaja === true))
      setTrendingPolitica(tp)
      setTrendingTwitter(tt)
      setTrendingGoogle(tg)
      setNoticiasGerais(n)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar dados')
    }
  }

  useEffect(() => {
    void load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const politicosFiltrados = useMemo(() => {
    if (!politicos) return []
    const term = q.trim().toLowerCase()
    if (!term) return politicos
    return politicos.filter((p) => p.name.toLowerCase().includes(term))
  }, [politicos, q])

  if (error) return <ErrorState message={error} onRetry={load} />
  if (!politicos || !trendingPolitica || !trendingTwitter || !trendingGoogle || !noticiasGerais) return <Loading label="Carregando painel…" />

  return (
    <div className="stack gap-16">
      <NoticiaDetailsModal open={!!selectedNoticia} noticia={selectedNoticia} onClose={() => setSelectedNoticia(null)} />
      <section className="hero">
        <h1>Painel</h1>
        <p className="muted">
          Visualização das informações coletadas (notícias por relevância, Instagram e trending topics).
        </p>
      </section>

      <section className="grid three">
        <TrendingList topics={trendingPolitica.slice(0, 10)} title="Trending Política" />
        <TrendingList topics={trendingTwitter.slice(0, 10)} title="Trending Twitter/X" />
        <TrendingList topics={trendingGoogle.slice(0, 10)} title="Trending Google" />
      </section>

      <section className="grid one">
        <div className="card">
          <div className="card-title">Notícias políticas gerais</div>
          <div className="stack gap-12">
            {noticiasGerais.slice(0, 6).map((n) => (
              <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />
            ))}
          </div>
        </div>
      </section>

      <section className="stack gap-12">
        <div className="row space-between gap-12">
          <h2>Políticos</h2>
          <input
            className="input"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Buscar por nome…"
            aria-label="Buscar por nome"
          />
        </div>
        <div className="grid three">
          {politicosFiltrados.map((p) => (
            <PoliticoCard key={p.id} politico={p} />
          ))}
        </div>
      </section>
    </div>
  )
}

