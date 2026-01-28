export function ErrorState({
  title = 'Erro ao carregar',
  message,
  onRetry,
}: {
  title?: string
  message?: string
  onRetry?: () => void
}) {
  return (
    <div className="card error">
      <div className="card-title">{title}</div>
      {message ? <div className="muted">{message}</div> : null}
      {onRetry ? (
        <div className="card-actions">
          <button type="button" className="btn" onClick={onRetry}>
            Tentar novamente
          </button>
        </div>
      ) : null}
    </div>
  )
}

