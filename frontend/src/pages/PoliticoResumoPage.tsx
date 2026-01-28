import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { portalApi } from '../api/portalApi'
import type { Noticia, PoliticoAssuntosResponse, PoliticoResumo } from '../types'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'
import { NoticiaCard } from '../components/NoticiaCard'
import { NoticiaDetailsModal } from '../components/NoticiaDetailsModal'
import { InstagramCard } from '../components/InstagramCard'
import { PoliticoCard } from '../components/PoliticoCard'
import { AssuntosCard } from '../components/AssuntosCard'

export function PoliticoResumoPage() {
  const { id } = useParams()
  const politicoId = Number(id)
  const [resumo, setResumo] = useState<PoliticoResumo | null>(null)
  const [assuntos, setAssuntos] = useState<PoliticoAssuntosResponse | null>(null)
  const [noticiasEstado, setNoticiasEstado] = useState<Noticia[] | null>(null)
  const [noticiasBrasil, setNoticiasBrasil] = useState<Noticia[] | null>(null)
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
              .getNoticiasEstado(estado, 6)
              .then((n) => setNoticiasEstado(n))
              .catch(() => setNoticiasEstado(null))
          : Promise.resolve().then(() => setNoticiasEstado([])),
        portalApi
          .getNoticiasPolitica()
          .then((n) => setNoticiasBrasil(n.slice(0, 6)))
          .catch(() => setNoticiasBrasil(null)),
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
      <section className="hero">
        <div className="row space-between gap-12 wrap">
          <div>
            <h1>{politico.name}</h1>
            <p className="muted">
              {[politico.cidade, politico.estado].filter(Boolean).join(' • ') || politico.description || '—'}
            </p>
            <div className="row gap-8 meta wrap">
              {politico.instagram_username ? <span className="pill">@{politico.instagram_username}</span> : null}
              {politico.twitter_username ? <span className="pill">X: @{politico.twitter_username}</span> : null}
            </div>
          </div>
          <div className="row gap-12 wrap">
            <div className="stat">
              <div className="stat-value">{resumo.total_noticias}</div>
              <div className="muted">notícias (7d)</div>
            </div>
            <div className="stat">
              <div className="stat-value">{resumo.total_posts_instagram}</div>
              <div className="muted">posts IG (30d)</div>
            </div>
          </div>
        </div>

        <div className="row gap-12 wrap">
          <Link className="btn" to={`/politicos/${politico.id}/noticias`}>
            Ver todas as notícias
          </Link>
          <Link className="btn" to={`/politicos/${politico.id}/instagram`}>
            Ver posts do Instagram
          </Link>
          <Link className="btn" to={`/politicos/${politico.id}/mencoes`}>
            Ver menções sociais
          </Link>
          <Link className="btn" to={`/politicos/${politico.id}/processual`}>
            Consulta Processual
          </Link>
        </div>
      </section>

      <section className="grid two">
        <div className="stack gap-12">
          <h2>Assuntos em alta</h2>
          {assuntos ? <AssuntosCard assuntos={assuntos.assuntos} /> : <div className="muted">Sem dados de assuntos.</div>}
        </div>
        <div className="stack gap-12">
          <h2>Notícias do Brasil</h2>
          {noticiasBrasil ? (
            noticiasBrasil.length ? (
              noticiasBrasil.map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
            ) : (
              <div className="muted">Sem notícias gerais.</div>
            )
          ) : (
            <div className="muted">Carregando/sem acesso às notícias gerais.</div>
          )}
        </div>
      </section>

      <section className="grid two">
        <div className="stack gap-12">
          <div className="row space-between gap-12">
            <h2>Top notícias</h2>
            <Link to={`/politicos/${politico.id}/noticias`} className="link">
              ver mais
            </Link>
          </div>
          {resumo.top_noticias.length ? (
            resumo.top_noticias.map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
          ) : (
            <div className="muted">Sem notícias para este político.</div>
          )}
        </div>

        <div className="stack gap-12">
          <div className="row space-between gap-12">
            <h2>Top Instagram</h2>
            <Link to={`/politicos/${politico.id}/instagram`} className="link">
              ver mais
            </Link>
          </div>
          {resumo.top_instagram.length ? (
            resumo.top_instagram.map((p) => <InstagramCard key={p.id} post={p} />)
          ) : (
            <div className="muted">Sem posts coletados do Instagram.</div>
          )}
        </div>
      </section>

      <section className="grid two">
        <div className="stack gap-12">
          <h2>Concorrentes</h2>
          {resumo.concorrentes.length ? (
            <div className="grid two">
              {resumo.concorrentes.map((c) => (
                <PoliticoCard key={c.id} politico={c} />
              ))}
            </div>
          ) : (
            <div className="muted">Nenhum concorrente configurado.</div>
          )}
        </div>

        <div className="stack gap-12">
          <h2>Notícias do estado</h2>
          {noticiasEstado ? (
            noticiasEstado.length ? (
              noticiasEstado.map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
            ) : (
              <div className="muted">Sem notícias do estado (ou estado não cadastrado).</div>
            )
          ) : (
            <div className="muted">Carregando/sem acesso às notícias do estado.</div>
          )}

          <div style={{ marginTop: 12 }} />

          <h2>Notícias da cidade</h2>
          {resumo.noticias_cidade.length ? (
            resumo.noticias_cidade.map((n) => <NoticiaCard key={n.id} noticia={n} onOpen={setSelectedNoticia} />)
          ) : (
            <div className="muted">Sem notícias locais (ou cidade não cadastrada).</div>
          )}
        </div>
      </section>
    </div>
  )
}

