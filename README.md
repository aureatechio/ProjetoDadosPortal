# Portal de Dados Políticos

Sistema de coleta automática de dados de políticos brasileiros, incluindo notícias, posts do Instagram e trending topics políticos.

## Funcionalidades

- **Coleta de Notícias**: Google News + NewsAPI com sistema de relevância multi-fator
- **Instagram Scraping**: Posts mais engajados dos políticos
- **Trending Topics**: Identificação automática de tópicos políticos em alta
- **Sistema de Relevância**: Ranking de notícias baseado em recência, menção direta, fonte confiável e engajamento
- **Agendamento Automático**: Coleta diária configurável via APScheduler

## Instalação

### 1. Clone o projeto e instale dependências

```bash
cd ProjetoDadosPortal
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou: venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### 2. Configure as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
# Supabase (obrigatório)
SUPABASE_URL=https://seu-projeto.supabase.co
SUPABASE_KEY=sua-anon-key

# Apify (opcional, apenas para scripts de busca por redes sociais via Apify)
# IMPORTANTE: não commitar tokens. Se você expôs um token em chat/log, rotacione no Apify.
APIFY_TOKEN=

# NewsAPI (opcional, mas recomendado)
NEWSAPI_KEY=sua-chave-newsapi

# OpenAI (opcional, para resumo por IA de comentários)
# IMPORTANTE: não commitar tokens.
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o-mini

# Instagram (opcional - para sessão logada)
INSTAGRAM_USERNAME=
INSTAGRAM_PASSWORD=

# Configurações de Coleta
COLETA_HORARIO=06:00
COLETA_TIMEZONE=America/Sao_Paulo

# Limites
MAX_NOTICIAS_POR_POLITICO=20
MAX_POSTS_INSTAGRAM=10
DIAS_RETENCAO_NOTICIAS=7
DIAS_RETENCAO_INSTAGRAM=30
```

### Script: preencher Instagram/Twitter via Apify (1 por 1)

Esse script busca o próximo registro da tabela `politico` que esteja faltando `instagram_username` **ou** `twitter_username` e tenta preencher via Apify.

```bash
python scripts/fill_socials_from_apify.py --limit 10
```

Para gravar no Supabase (cuidado: faz update):

```bash
python scripts/fill_socials_from_apify.py --limit 100 --apply
```

### Script: coletar menções do Twitter/X via Apify

Coleta menções do Twitter/X sobre políticos com `usar_diretoriaja = TRUE`. Busca tweets que mencionam o nome do político ou seu @username.

```bash
# Dry-run (apenas simula, não grava)
python scripts/collect_twitter_mentions_apify.py

# Grava no Supabase
python scripts/collect_twitter_mentions_apify.py --apply

# Opções disponíveis:
python scripts/collect_twitter_mentions_apify.py --apply \
    --limit-politicos 10 \
    --limit-tweets 50 \
    --search-mode latest \
    --days-back 7 \
    --min-engagement 10

# Processar apenas IDs específicos
python scripts/collect_twitter_mentions_apify.py --apply --only-ids 15,16,17
```

Parâmetros:
- `--apply`: Grava no Supabase (sem isso, apenas simula)
- `--limit-politicos`: Máximo de políticos a processar (default: 50)
- `--limit-tweets`: Máximo de tweets por político (default: 50)
- `--search-mode`: `top` (mais relevantes) ou `latest` (mais recentes)
- `--days-back`: Quantos dias para trás buscar (default: 7)
- `--min-engagement`: Engajamento mínimo para salvar (default: 0)
- `--skip-if-recent`: Pula político se já tiver N menções nas últimas 24h

### 3. Execute a aplicação

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Acesse a documentação da API em: http://localhost:8000/docs

## Sistema de Relevância

O sistema usa 4 fatores para calcular a relevância das notícias:

| Fator | Peso | Descrição |
|-------|------|-----------|
| **Menção Direta** | 35% | Nome do político no título (+50 pontos) ou conteúdo |
| **Recência** | 25% | Decai 2 pontos por hora desde publicação |
| **Fonte** | 25% | Fontes premium (G1, Folha) = peso 2.0 |
| **Engajamento** | 15% | Baseado em compartilhamentos |

### Hierarquia de Fontes

- **Nacional Premium** (peso 2.0): G1, Folha, Estadão, CNN Brasil
- **Nacional Standard** (peso 1.5-1.8): UOL, Terra, R7, Poder360
- **Regional** (peso 1.3-1.6): Metrópoles, portais estaduais
- **Local** (peso 1.0-1.2): Portais locais da cidade

## Endpoints da API

### Notícias

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/politicos/{id}/noticias` | Notícias do político por relevância |
| GET | `/politicos/{id}/noticias/top` | Top 5 notícias mais relevantes |
| GET | `/politicos/{id}/concorrentes/noticias` | Notícias dos concorrentes |
| GET | `/noticias/cidade/{cidade}` | Notícias locais por relevância |
| GET | `/noticias/politica` | Notícias políticas gerais |

### Instagram

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/politicos/{id}/instagram` | Posts mais engajados |
| GET | `/politicos/{id}/instagram/stats` | Estatísticas de engajamento |

### Trending e Admin

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/trending` | Trending topics políticos |
| GET | `/fontes` | Lista fontes com pesos |
| PUT | `/fontes/{id}/peso` | Atualiza peso de fonte |
| POST | `/coleta/executar` | Executa coleta manual |
| GET | `/coleta/logs` | Logs das coletas |
| GET | `/coleta/jobs` | Jobs agendados |

## Cronograma de Coleta Diária

| Horário | Tarefa |
|---------|--------|
| 06:00 | Coleta de notícias (Google News + NewsAPI) |
| 06:45 | Coleta do Instagram (top posts engajados) |
| 08:00 | Atualização de trending topics |
| 08:15 | Limpeza de dados antigos |

## Estrutura do Projeto

```
ProjetoDadosPortal/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI + Scheduler
│   ├── config.py            # Configurações
│   ├── database.py          # Cliente Supabase
│   ├── relevance/           # Módulo de Relevância
│   │   ├── engine.py        # Motor de cálculo
│   │   ├── weights.py       # Pesos configuráveis
│   │   └── analyzer.py      # Análise de menções
│   ├── collectors/          # Coletores de dados
│   │   ├── news_google.py   # Google News
│   │   ├── news_api.py      # NewsAPI
│   │   ├── news_aggregator.py
│   │   ├── instagram.py     # Instaloader
│   │   └── trending.py      # Trending topics
│   ├── models/
│   │   └── schemas.py       # Pydantic models
│   └── scheduler/
│       └── jobs.py          # Jobs de coleta
├── requirements.txt
├── frontend/                 # Interface web (React + Vite)
└── README.md
```

## Frontend (interface web)

O frontend fica em `frontend/` e consome esta API FastAPI.

```bash
cd frontend
npm install
npm run dev
```

Configuração da URL da API via `VITE_API_URL` (veja `frontend/env.example`).

## Tabelas do Banco de Dados

- `politico` - Políticos cadastrados (com instagram_username, twitter_username, usar_diretoriaja, cidade, estado)
- `politico_concorrentes` - Relacionamento de concorrentes
- `instagram_posts` - Posts do Instagram com engajamento
- `social_media_posts` - Posts das redes sociais do próprio político (Instagram, Twitter)
- `social_mentions` - Menções aos políticos em redes sociais (Twitter/X, Bluesky, Google Trends)
- `concorrente_twitter_insights` - Snapshot diário de Twitter/X para concorrentes (seguidores + top 3 menções)
- `noticias` - Notícias com scores de relevância
- `fontes_noticias` - Fontes com pesos de confiabilidade
- `portal_trending_topics` - Trending topics políticos
- `coleta_logs` - Logs de execução das coletas

## Rotina: insights de Twitter/X para concorrentes

1) Crie a tabela no Supabase usando:

- `scripts/sql/create_concorrente_twitter_insights.sql`

2) Rode o coletor (dry-run / aplicar):

```bash
python scripts/collect_concorrentes_twitter_insights.py
python scripts/collect_concorrentes_twitter_insights.py --apply
```

3) Rotina pronta (wrapper):

```bash
python scripts/routine_concorrentes_twitter_insights.py
```

4) Endpoint da API (para consumo no frontend):

- `GET /politicos/{id}/concorrentes/twitter_insights?days_back=7`

## Limites de API

- **NewsAPI**: 100 requisições/dia (plano gratuito)
- **Google News**: Sem limite oficial (use delays de 2s)
- **Instagram**: Rate limit variável (use sessão logada)

## Licença

MIT
