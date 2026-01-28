import type { Noticia } from '../types'

export function NoticiaCard({ noticia, onOpen }: { noticia: Noticia; onOpen?: (n: Noticia) => void }) {
  const dateLabel = noticia.publicado_em ? formatDateTime(noticia.publicado_em) : null
  const score = typeof noticia.relevancia_total === 'number' ? noticia.relevancia_total : null

  const Inner = (
    <div className="row gap-12">
      {noticia.imagem_url ? (
        <div className="thumb" aria-hidden="true">
          <img src={noticia.imagem_url} alt="" />
        </div>
      ) : null}

      <div className="grow">
        <div className="row space-between gap-12">
          <div className="card-title">{noticia.titulo}</div>
          {score !== null ? <span className="score">{score.toFixed(1)}</span> : null}
        </div>
        {noticia.descricao ? <div className="muted line-clamp-3">{noticia.descricao}</div> : null}

        <div className="row gap-8 meta wrap">
          {noticia.fonte_nome ? <span className="pill">{noticia.fonte_nome}</span> : null}
          {dateLabel ? <span className="pill">{dateLabel}</span> : null}
          {noticia.tipo ? <span className="pill">tipo: {noticia.tipo}</span> : null}
          {noticia.tipo === 'estado' && noticia.estado ? <span className="pill">estado: {noticia.estado}</span> : null}
          <span className="pill">clique para ver pontos</span>
        </div>
      </div>
    </div>
  )

  return (
    <button type="button" className="card clickable card-button" onClick={() => onOpen?.(noticia)}>
      {Inner}
    </button>
  )
}

function formatDateTime(iso: string) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return new Intl.DateTimeFormat('pt-BR', { dateStyle: 'short', timeStyle: 'short' }).format(d)
}

