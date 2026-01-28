import type { SocialMention } from '../types'

export function SocialMentionCard({ m }: { m: SocialMention }) {
  const when = m.posted_at ?? m.collected_at ?? null
  const dateLabel = when ? formatDateTime(when) : null
  const score = typeof m.engagement_score === 'number' ? m.engagement_score : null
  const sentimento = m.sentimento ?? null

  return (
    <div className="card">
      <div className="row space-between gap-12 wrap">
        <div className="stack gap-6">
          <div className="card-title">{m.assunto ?? 'Menção'}</div>
          <div className="row gap-8 meta wrap">
            <span className="pill">{m.plataforma}</span>
            {sentimento ? <span className="pill">sentimento: {sentimento}</span> : null}
            {dateLabel ? <span className="pill">{dateLabel}</span> : null}
            {score !== null ? <span className="pill">engajamento: {score.toFixed(0)}</span> : null}
          </div>
        </div>
        {m.url ? (
          <a className="btn secondary" href={m.url} target="_blank" rel="noreferrer">
            Abrir
          </a>
        ) : null}
      </div>

      {m.assunto_detalhe ? <div className="muted" style={{ marginTop: 10 }}>{m.assunto_detalhe}</div> : null}
      {m.conteudo ? <div style={{ marginTop: 10, whiteSpace: 'pre-wrap' }}>{m.conteudo}</div> : null}

      <div className="row gap-8 meta wrap" style={{ marginTop: 10 }}>
        {m.autor ? <span className="pill">{m.autor}</span> : null}
        {m.autor_username ? <span className="pill">@{m.autor_username}</span> : null}
        {typeof m.likes === 'number' ? <span className="pill">likes: {m.likes}</span> : null}
        {typeof m.reposts === 'number' ? <span className="pill">reposts: {m.reposts}</span> : null}
        {typeof m.replies === 'number' ? <span className="pill">replies: {m.replies}</span> : null}
      </div>
    </div>
  )
}

function formatDateTime(iso: string) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return new Intl.DateTimeFormat('pt-BR', { dateStyle: 'short', timeStyle: 'short' }).format(d)
}

