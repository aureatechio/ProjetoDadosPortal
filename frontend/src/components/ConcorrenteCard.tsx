import { Link } from 'react-router-dom'
import type { ConcorrenteResumo, Noticia } from '../types'
import { NoticiaCard } from './NoticiaCard'

interface ConcorrenteCardProps {
  concorrente: ConcorrenteResumo
  onNoticiaOpen?: (n: Noticia) => void
}

export function ConcorrenteCard({ concorrente, onNoticiaOpen }: ConcorrenteCardProps) {
  const { politico, noticias, total_noticias, total_instagram } = concorrente

  return (
    <div className="card stack gap-12">
      {/* Header do Concorrente */}
      <div className="row gap-12 align-center">
        {politico.image && (
          <img
            src={politico.image}
            alt={politico.name}
            style={{ width: 64, height: 64, borderRadius: '50%', objectFit: 'cover' }}
          />
        )}
        <div className="grow">
          <Link to={`/politicos/${politico.id}`} className="card-title link">
            {politico.name}
          </Link>
          <p className="muted" style={{ fontSize: '0.9rem', marginTop: 4 }}>
            {politico.funcao && <strong>{politico.funcao}</strong>}
            {politico.funcao && (politico.cidade || politico.estado) && ' • '}
            {[politico.cidade, politico.estado].filter(Boolean).join(' • ')}
          </p>
        </div>
      </div>

      {/* Métricas */}
      <div className="row gap-12 wrap">
        <div className="stat" style={{ flex: 1 }}>
          <div className="stat-value">{total_noticias}</div>
          <div className="muted" style={{ fontSize: '0.75rem' }}>notícias</div>
        </div>
        <div className="stat" style={{ flex: 1 }}>
          <div className="stat-value">{total_instagram}</div>
          <div className="muted" style={{ fontSize: '0.75rem' }}>posts IG</div>
        </div>
      </div>

      {/* Redes Sociais */}
      {(politico.instagram_username || politico.twitter_username) && (
        <div className="row gap-8 wrap">
          {politico.instagram_username && (
            <a
              href={`https://instagram.com/${politico.instagram_username}`}
              target="_blank"
              rel="noopener noreferrer"
              className="pill"
              style={{ fontSize: '0.8rem' }}
            >
              @{politico.instagram_username}
            </a>
          )}
          {politico.twitter_username && (
            <a
              href={`https://twitter.com/${politico.twitter_username}`}
              target="_blank"
              rel="noopener noreferrer"
              className="pill"
              style={{ fontSize: '0.8rem' }}
            >
              X: @{politico.twitter_username}
            </a>
          )}
        </div>
      )}

      {/* Notícias do Concorrente */}
      {noticias.length > 0 && (
        <div className="stack gap-8">
          <h4 style={{ margin: 0, fontSize: '0.95rem', color: 'var(--text-muted)' }}>
            Últimas notícias
          </h4>
          <div className="stack gap-8">
            {noticias.slice(0, 3).map((noticia) => (
              <NoticiaCard key={noticia.id} noticia={noticia} onOpen={onNoticiaOpen} />
            ))}
          </div>
          {noticias.length > 3 && (
            <Link
              to={`/politicos/${politico.id}/noticias`}
              className="muted"
              style={{ fontSize: '0.85rem', textAlign: 'center' }}
            >
              Ver mais {total_noticias - 3} notícias →
            </Link>
          )}
        </div>
      )}

      {noticias.length === 0 && (
        <div className="muted" style={{ fontSize: '0.85rem', textAlign: 'center' }}>
          Sem notícias recentes do concorrente.
        </div>
      )}

      {/* Link para perfil completo */}
      <Link to={`/politicos/${politico.id}`} className="btn" style={{ textAlign: 'center' }}>
        Ver perfil completo
      </Link>
    </div>
  )
}
