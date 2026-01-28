import type { TrendingTopic } from '../types'

interface TrendingListProps {
  topics: TrendingTopic[]
  title?: string
}

export function TrendingList({ topics, title = 'Trending topics' }: TrendingListProps) {
  if (!topics.length) return <div className="muted">Sem dados de trending no momento.</div>

  return (
    <div className="card">
      <div className="card-title">{title}</div>
      <div className="list">
        {topics.map((t) => (
          <div key={`${t.rank}-${t.title}-${t.category}`} className="list-row">
            <div className="rank">{t.rank}</div>
            <div className="grow">
              <div className="item-title">{t.title}</div>
              {t.subtitle ? <div className="muted">{t.subtitle}</div> : null}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

