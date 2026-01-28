import type { InstagramPost } from '../types'

// URL base da API
const API_BASE = 'http://localhost:8000'

// Função para gerar URL do proxy de imagem
function getProxyImageUrl(originalUrl: string | null | undefined): string | null {
  if (!originalUrl) return null
  return `${API_BASE}/proxy/image?url=${encodeURIComponent(originalUrl)}`
}

// Formatar números grandes
function formatNumber(num: number | null | undefined): string {
  if (num === null || num === undefined) return '0'
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M'
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K'
  return num.toString()
}

export function InstagramCard({ post }: { post: InstagramPost }) {
  const score = typeof post.engagement_score === 'number' ? post.engagement_score : null
  // Usa media_url primeiro, depois thumbnail_url como fallback
  const originalImageUrl = post.media_url || post.thumbnail_url
  // URL do proxy para contornar proteção de hotlinking
  const imageUrl = getProxyImageUrl(originalImageUrl)
  // Monta a URL do post
  const postUrl = post.post_url || (post.post_shortcode ? `https://www.instagram.com/p/${post.post_shortcode}/` : null)
  // Texto do post (caption ou conteudo)
  const texto = post.caption || post.conteudo
  // Plataforma
  const plataforma = post.plataforma || 'instagram'

  return (
    <div className="card" style={{ overflow: 'hidden' }}>
      {/* Imagem do Post */}
      <a 
        href={postUrl || '#'} 
        target="_blank" 
        rel="noopener noreferrer" 
        style={{ display: 'block', margin: -16, marginBottom: 0 }}
      >
        {imageUrl ? (
          <img 
            src={imageUrl} 
            alt="Post" 
            loading="lazy" 
            style={{ 
              width: '100%', 
              height: 280, 
              objectFit: 'cover',
              display: 'block'
            }} 
            onError={(e) => {
              // Fallback se a imagem falhar
              const target = e.target as HTMLImageElement
              target.style.display = 'none'
              target.parentElement!.innerHTML = '<div style="height: 150px; display: flex; align-items: center; justify-content: center; background: #f5f5f5; color: #999;">Imagem indisponível</div>'
            }}
          />
        ) : (
          <div style={{ 
            height: 150, 
            display: 'flex', 
            alignItems: 'center', 
            justifyContent: 'center', 
            background: '#f5f5f5',
            color: '#999'
          }}>
            Sem imagem
          </div>
        )}
      </a>

      {/* Conteúdo */}
      <div style={{ padding: '16px 0 0 0' }}>
        {/* Header com plataforma e score */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ 
            textTransform: 'capitalize', 
            fontWeight: 600, 
            fontSize: 14,
            color: plataforma === 'instagram' ? '#E1306C' : '#333'
          }}>
            {plataforma}
          </span>
          {score !== null && (
            <span style={{ 
              background: '#10b981', 
              color: 'white', 
              padding: '2px 8px', 
              borderRadius: 12, 
              fontSize: 12,
              fontWeight: 600
            }}>
              {formatNumber(score)}
            </span>
          )}
        </div>

        {/* Métricas */}
        <div style={{ 
          display: 'flex', 
          gap: 16, 
          marginBottom: 12,
          fontSize: 13,
          color: '#666'
        }}>
          <span>❤️ {formatNumber(post.likes)}</span>
          <span>💬 {formatNumber(post.comments)}</span>
          {post.shares ? <span>🔄 {formatNumber(post.shares)}</span> : null}
          {post.views ? <span>👁️ {formatNumber(post.views)}</span> : null}
        </div>

        {/* Data */}
        {post.posted_at && (
          <div style={{ fontSize: 12, color: '#999', marginBottom: 8 }}>
            {formatDateTime(post.posted_at)}
          </div>
        )}

        {/* Legenda */}
        {texto ? (
          <div style={{ 
            fontSize: 13, 
            color: '#444', 
            lineHeight: 1.4,
            maxHeight: 80,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            marginBottom: 12
          }}>
            {texto}
          </div>
        ) : null}

        {/* Ações */}
        <div style={{ 
          display: 'flex', 
          gap: 8, 
          flexWrap: 'wrap',
          paddingTop: 8,
          borderTop: '1px solid #eee'
        }}>
          {postUrl && (
            <>
              <a 
                href={postUrl} 
                target="_blank" 
                rel="noopener noreferrer" 
                style={{ 
                  fontSize: 12, 
                  color: '#3b82f6',
                  textDecoration: 'none'
                }}
              >
                Abrir post ↗
              </a>
              <button 
                style={{ 
                  fontSize: 12, 
                  padding: '2px 8px',
                  background: '#f3f4f6',
                  border: 'none',
                  borderRadius: 4,
                  cursor: 'pointer'
                }}
                onClick={() => {
                  navigator.clipboard.writeText(postUrl)
                  alert('Link copiado!')
                }}
              >
                Copiar link
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  )
}

function formatDateTime(iso: string) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return new Intl.DateTimeFormat('pt-BR', { dateStyle: 'short', timeStyle: 'short' }).format(d)
}

