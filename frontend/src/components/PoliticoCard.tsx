import { Link } from 'react-router-dom'
import type { Politico } from '../types'

export function PoliticoCard({ politico }: { politico: Politico }) {
  return (
    <Link to={`/politicos/${politico.id}`} className="card clickable">
      <div className="row gap-12">
        <div className="avatar" aria-hidden="true">
          {politico.image ? <img src={politico.image} alt="" /> : <span>{initials(politico.name)}</span>}
        </div>
        <div className="grow">
          <div className="card-title">{politico.name}</div>
          <div className="muted line-clamp-2">
            {politico.funcao ?? ([politico.cidade, politico.estado].filter(Boolean).join(' • ') || '—')}
          </div>
          <div className="meta">
            {politico.instagram_username ? <span className="pill">Instagram: @{politico.instagram_username}</span> : null}
            {politico.cidade ? <span className="pill">{politico.cidade}</span> : null}
          </div>
        </div>
      </div>
    </Link>
  )
}

function initials(name: string) {
  const parts = name.trim().split(/\s+/).slice(0, 2)
  return parts.map((p) => p[0]?.toUpperCase()).join('')
}

