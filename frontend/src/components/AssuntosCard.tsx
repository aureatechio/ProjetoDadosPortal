import type { AssuntoStats } from '../types'

export function AssuntosCard({ assuntos }: { assuntos: AssuntoStats[] }) {
  if (!assuntos.length) return <div className="muted">Sem assuntos agregados no momento.</div>

  return (
    <div className="card">
      <div className="card-title">Assuntos em alta</div>
      <div className="list" style={{ marginTop: 10 }}>
        {assuntos.map((a) => (
          <div key={a.assunto} className="list-row">
            <div className="grow">
              <div className="item-title">{a.assunto}</div>
              <div className="muted">
                {a.total_mencoes} menções • sentimento: {a.sentimento_predominante}
                {typeof a.engagement_total === 'number' ? ` • engajamento: ${Math.round(a.engagement_total)}` : ''}
              </div>
              {a.exemplo ? <div className="muted" style={{ marginTop: 6 }}>{a.exemplo}</div> : null}
            </div>
            <span className="pill">
              +{a.mencoes_positivas} / ={a.mencoes_neutras} / -{a.mencoes_negativas}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

