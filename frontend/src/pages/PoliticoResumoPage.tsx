import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { portalApi } from '../api/portalApi'
import type {
  Noticia,
  PoliticoAssuntosResponse,
  PoliticoResumo,
  InstagramPost,
  SocialMention,
  ConcorrenteResumo,
} from '../types'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'
import { NoticiaCard } from '../components/NoticiaCard'
import { NoticiaDetailsModal } from '../components/NoticiaDetailsModal'
import { InstagramCard } from '../components/InstagramCard'
import { AssuntosCard } from '../components/AssuntosCard'
import { SocialMentionCard } from '../components/SocialMentionCard'
import { ConcorrenteCard } from '../components/ConcorrenteCard'

export function PoliticoResumoPage() {
  const { id } = useParams()
  const politicoId = Number(id)
  const [resumo, setResumo] = useState<PoliticoResumo | null>(null)
  const [assuntos, setAssuntos] = useState<PoliticoAssuntosResponse | null>(null)
  const [noticiasEstado, setNoticiasEstado] = useState<Noticia[] | null>(null)
  const [noticiasBrasil, setNoticiasBrasil] = useState<Noticia[] | null>(null)
  const [todasNoticias, setTodasNoticias] = useState<Noticia[] | null>(null)
  const [todosInstagram, setTodosInstagram] = useState<InstagramPost[] | null>(null)
  const [mencoes, setMencoes] = useState<SocialMention[] | null>(null)
  const [concorrentesResumo, setConcorrentesResumo] = useState<ConcorrenteResumo[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [selectedNoticia, setSelectedNoticia] = useState<Noticia | null>(null)

  async function load() {
    setError(null)
    try {
      const data = await portalApi.getResumo(politicoId)
      setResumo(data)

      // Extras (não bloqueiam o resumo)
      const estado = data?.politico?.estado ?? null
      await Promise.all([
        portalApi
          .getAssuntosPolitico(politicoId, 10)
          .then((r) => setAssuntos(r))
          .catch(() => setAssuntos(null)),
        estado
          ? portalApi
              .getNoticiasEstado(estado, 10)
              .then((n) => setNoticiasEstado(n))
              .catch(() => setNoticiasEstado(null))
          : Promise.resolve().then(() => setNoticiasEstado([])),
        portalApi
          .getNoticiasPolitica()
          .then((n) => setNoticiasBrasil(n.slice(0, 10)))
          .catch(() => setNoticiasBrasil(null)),
        // Carregar todas as notícias do político
        portalApi
          .getNoticiasPolitico(politicoId, 30)
          .then((n) => setTodasNoticias(n))
          .catch(() => setTodasNoticias(null)),
        // Carregar todos os posts do Instagram
        portalApi
          .getInstagramPolitico(politicoId, 20)
          .then((p) => setTodosInstagram(p))
          .catch(() => setTodosInstagram(null)),
        // Carregar menções sociais
        portalApi
          .getSocialMentionsPolitico(politicoId, null, 20)
          .then((m) => setMencoes(m))
          .catch(() => setMencoes(null)),
        // Carregar resumo dos concorrentes com notícias diversificadas
        portalApi
          .getResumoConcorrentes(politicoId, 5)
          .then((c) => setConcorrentesResumo(c))
          .catch(() => setConcorrentesResumo(null)),
      ])
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar resumo')
    }
  }

  useEffect(() => {
    if (!Number.isFinite(politicoId)) return
    void load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [politicoId])

  if (!Number.isFinite(politicoId)) return <ErrorState title="ID inválido" message="Político não encontrado." />
  if (error) return <ErrorState message={error} onRetry={load} />
  if (!resumo) return <Loading label="Carregando resumo…" />

  const { politico } = resumo

  return (
    <div className="stack gap-16">
      <NoticiaDetailsModal open={!!selectedNoticia} noticia={selectedNoticia} onClose={() => setSelectedNoticia(null)} />
      
      {/* Header do Político */}
      <section className="hero">
        <div className="row space-between gap-12 wrap">
          <div className="row gap-16 align-center">
            {politico.image && (
              <img 
                src={politico.image} 
                alt={politico.name} 
                style={{ width: 100, height: 100, borderRadius: '50%', objectFit: 'cover' }} 
              />
            )}
            <div>
              <h1>{politico.name}</h1>
              <p className="muted">
                {politico.funcao && <strong>{politico.funcao} • </strong>}
                {[politico.cidade, politico.estado].filter(Boolean).join(' • ') || politico.description || '—'}
              </p>
              <div className="row gap-8 meta wrap" style={{ marginTop: 8 }}>
                {politico.instagram_username && (
                  <a href={`https://instagram.com/${politico.instagram_username}`} target="_blank" rel="noopener noreferrer" className="pill">
                    @{politico.instagram_username}
                  </a>
                )}
                {politico.twitter_username && (
                  <a href={`https://twitter.com/${politico.twitter_username}`} target="_blank" rel="noopener noreferrer" className="pill">
                    X: @{politico.twitter_username}
                  </a>
                )}
              </div>
            </div>
          </div>
          <div className="row gap-12 wrap">
            <div className="stat">
              <div className="stat-value">{resumo.total_noticias}</div>
              <div className="muted">notícias</div>
            </div>
            <div className="stat">
              <div className="stat-value">{resumo.total_posts_instagram}</div>
              <div className="muted">posts IG</div>
            </div>
            <div className="stat">
              <div className="stat-value">{resumo.total_mencoes ?? mencoes?.length ?? 0}</div>
              <div className="muted">menções</div>
            </div>
          </div>
        </div>
        <div className="row gap-8 wrap" style={{ marginTop: 12 }}>
          <Link className="pill" to={`/politicos/${politico.id}/processual`}>
            Consulta Processual →
          </Link>
        </div>
      </section>

      {/* Assuntos em Alta */}
      <section>
        <h2>Assuntos em alta</h2>
        {assuntos?.assuntos?.length ? (
          <AssuntosCard assuntos={assuntos.assuntos} />
        ) : (
          <div className="muted">Sem assuntos agregados no momento.</div>
        )}
      </section>

      {/* Card de Concorrentes (resumo) */}
      <section>
        <h2>Concorrentes</h2>
        <div className="card stack gap-12">
          <div className="row space-between wrap gap-12">
            <div className="stack gap-4">
              <div className="card-title">Visão geral</div>
              <div className="muted" style={{ fontSize: '0.9rem' }}>
                Dados consolidados dos concorrentes monitorados
              </div>
            </div>
            <a className="btn" href="#concorrentes">
              Ver detalhes
            </a>
          </div>

          <div className="row gap-12 wrap">
            <div className="stat" style={{ flex: 1 }}>
              <div className="stat-value">{concorrentesResumo?.length ?? resumo.concorrentes.length ?? 0}</div>
              <div className="muted">concorrentes</div>
            </div>
            <div className="stat" style={{ flex: 1 }}>
              <div className="stat-value">
                {concorrentesResumo?.reduce((acc, c) => acc + (c.total_noticias || 0), 0) ?? 0}
              </div>
              <div className="muted">notícias</div>
            </div>
            <div className="stat" style={{ flex: 1 }}>
              <div className="stat-value">
                {concorrentesResumo?.reduce((acc, c) => acc + (c.total_instagram || 0), 0) ?? 0}
              </div>
              <div className="muted">posts IG</div>
            </div>
          </div>

          {concorrentesResumo?.length ? (
            <div className="row gap-8 wrap">
              {concorrentesResumo.slice(0, 6).map((c) => (
                <Link key={c.politico.id} to={`/politicos/${c.politico.id}`} className="pill">
                  {c.politico.name}
                </Link>
              ))}
            </div>
          ) : (
            <div className="muted">Carregando dados dos concorrentes...</div>
          )}
        </div>
      </section>

      {/* Notícias do Político */}
      <section>
        <h2>Notícias sobre {politico.name}</h2>
        {todasNoticias?.length ? (
          <div className="grid two">
            {todasNoticias.map((n) => (
              <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />
            ))}
          </div>
        ) : (
          <div className="muted">Sem notícias para este político.</div>
        )}
      </section>

      {/* Posts do Instagram */}
      <section>
        <h2>Posts do Instagram</h2>
        {todosInstagram?.length ? (
          <div className="grid three">
            {todosInstagram.map((p) => (
              <InstagramCard key={p.id} post={p} />
            ))}
          </div>
        ) : resumo.top_instagram.length ? (
          <div className="grid three">
            {resumo.top_instagram.map((p) => (
              <InstagramCard key={p.id} post={p} />
            ))}
          </div>
        ) : (
          <div className="muted">Sem posts coletados do Instagram.</div>
        )}
      </section>

      {/* Menções Sociais */}
      <section>
        <h2>Menções em Redes Sociais</h2>
        {mencoes?.length ? (
          <div className="grid two">
            {mencoes.map((m) => (
              <SocialMentionCard key={m.id} mention={m} />
            ))}
          </div>
        ) : (
          <div className="muted">Sem menções coletadas.</div>
        )}
      </section>

      {/* Concorrentes com dados completos */}
      {concorrentesResumo && concorrentesResumo.length > 0 && (
        <section id="concorrentes">
          <h2>Concorrentes</h2>
          <p className="muted" style={{ marginBottom: 16 }}>
            Acompanhe as notícias e dados dos principais concorrentes
          </p>
          <div className="grid two">
            {concorrentesResumo.map((c) => (
              <ConcorrenteCard 
                key={c.politico.id} 
                concorrente={c} 
                onNoticiaOpen={setSelectedNoticia}
              />
            ))}
          </div>
        </section>
      )}

      {/* Fallback para concorrentes sem dados completos */}
      {(!concorrentesResumo || concorrentesResumo.length === 0) && resumo.concorrentes.length > 0 && (
        <section>
          <h2>Concorrentes</h2>
          <div className="muted">Carregando dados dos concorrentes...</div>
        </section>
      )}

      {/* Notícias do Estado e da Capital */}
      <section className="grid two">
        <div className="stack gap-12">
          <h2>Notícias do Estado ({politico.estado || '—'})</h2>
          <p className="muted" style={{ marginTop: -8, fontSize: '0.85rem' }}>Governo, Assembleia Legislativa, Política Estadual</p>
          {resumo.noticias_estado?.length ? (
            resumo.noticias_estado.map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
          ) : noticiasEstado?.length ? (
            noticiasEstado.slice(0, 3).map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
          ) : (
            <div className="muted">Sem notícias do estado.</div>
          )}
        </div>

        <div className="stack gap-12">
          <h2>Notícias da Capital ({resumo.noticias_capital?.[0]?.cidade || politico.estado || '—'})</h2>
          <p className="muted" style={{ marginTop: -8, fontSize: '0.85rem' }}>Prefeitura, Câmara Municipal, Política Municipal</p>
          {resumo.noticias_capital?.length ? (
            resumo.noticias_capital.map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
          ) : resumo.noticias_cidade.length ? (
            resumo.noticias_cidade.slice(0, 3).map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
          ) : (
            <div className="muted">Sem notícias da capital.</div>
          )}
        </div>
      </section>

      {/* Notícias do Brasil */}
      <section>
        <h2>Notícias Políticas do Brasil</h2>
        {noticiasBrasil?.length ? (
          <div className="grid two">
            {noticiasBrasil.map((n) => (
              <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />
            ))}
          </div>
        ) : (
          <div className="muted">Sem notícias gerais.</div>
        )}
      </section>
    </div>
  )
}

