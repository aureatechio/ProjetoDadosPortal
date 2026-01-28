-- Tabela de insights de Twitter/X para concorrentes (snapshot diário)
-- Armazena:
-- - followers_count (via Apify / scraping)
-- - top_mentions (top 3 menções mais engajadas no Twitter/X; opcionalmente também gravadas em public.social_mentions)
--
-- Como aplicar:
-- 1) Supabase SQL Editor: cole e rode este SQL
-- 2) (Opcional) Supabase CLI migrations: crie migration e aplique

create table if not exists public.concorrente_twitter_insights (
  id bigserial primary key,

  -- referência ao registro da tabela public.concorrentes
  concorrente_id uuid not null references public.concorrentes(id) on delete cascade,

  -- referência ao "politico" do concorrente (quando existir), para join com social_mentions
  politico_id uuid null references public.politico(uuid) on delete set null,

  -- opcional: id inteiro (facilita debug/joins no app)
  concorrente_politico_int_id integer null,

  twitter_username text null,
  followers_count integer null,

  -- top 3 menções mais engajadas (lista de objetos no formato da tabela social_mentions)
  top_mentions jsonb not null default '[]'::jsonb,

  -- janela usada para selecionar menções (ex.: 7 dias)
  mentions_window_days integer not null default 7,

  -- controle de snapshot
  computed_date date not null default (now()::date),
  computed_at timestamptz not null default now(),

  source text not null default 'apify+supabase'
);

-- 1 snapshot por concorrente por dia por janela (evita duplicar)
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'concorrente_twitter_insights_unique_snapshot'
  ) then
    alter table public.concorrente_twitter_insights
      add constraint concorrente_twitter_insights_unique_snapshot
      unique (concorrente_id, mentions_window_days, computed_date);
  end if;
end $$;

create index if not exists concorrente_twitter_insights_concorrente_idx
  on public.concorrente_twitter_insights (concorrente_id);

create index if not exists concorrente_twitter_insights_politico_idx
  on public.concorrente_twitter_insights (politico_id);

create index if not exists concorrente_twitter_insights_computed_at_idx
  on public.concorrente_twitter_insights (computed_at desc);

