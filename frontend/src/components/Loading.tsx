export function Loading({ label = 'Carregando…' }: { label?: string }) {
  return (
    <div className="center">
      <div className="spinner" aria-hidden="true" />
      <div className="muted">{label}</div>
    </div>
  )
}

