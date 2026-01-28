import { Link } from 'react-router-dom'

export function NotFoundPage() {
  return (
    <div className="center">
      <div className="card">
        <div className="card-title">Página não encontrada</div>
        <div className="muted">O endereço acessado não existe.</div>
        <div className="card-actions">
          <Link className="btn" to="/">
            Ir para início
          </Link>
        </div>
      </div>
    </div>
  )
}

