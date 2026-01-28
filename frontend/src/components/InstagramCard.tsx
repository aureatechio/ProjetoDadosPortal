import type { InstagramPost } from '../types'

export function InstagramCard({ post }: { post: InstagramPost }) {
  const score = typeof post.engagement_score === 'number' ? post.engagement_score : null

  return (
    <div className="card">
      {post.thumbnail_url ? (
        <div className="media">
          <img src={post.thumbnail_url} alt="" loading="lazy" />
        </div>
      ) : (
        <div className="media placeholder" aria-hidden="true">
          Sem imagem
        </div>
      )}

      <div className="stack gap-12" style={{ marginTop: 12 }}>
        <div className="row space-between gap-12">
          <div className="card-title">Instagram</div>
          {score !== null ? <span className="score">{score.toFixed(1)}</span> : null}
        </div>

        <div className="row gap-8 meta wrap">
          <span className="pill">likes: {post.likes ?? 0}</span>
          <span className="pill">comentários: {post.comments ?? 0}</span>
          {post.posted_at ? <span className="pill">{formatDateTime(post.posted_at)}</span> : null}
          <span className="pill">id: {post.post_shortcode}</span>
        </div>

        {post.caption ? (
          <div className="muted line-clamp-5">{post.caption}</div>
        ) : (
          <div className="muted">Sem legenda.</div>
        )}
      </div>
    </div>
  )
}

function formatDateTime(iso: string) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return new Intl.DateTimeFormat('pt-BR', { dateStyle: 'short', timeStyle: 'short' }).format(d)
}

