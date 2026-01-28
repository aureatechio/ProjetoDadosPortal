# Frontend — Portal de Dados Políticos

Interface web para visualizar os dados coletados pela API FastAPI deste repositório:

- Lista de políticos
- Resumo por político (top notícias, top Instagram, concorrentes, notícias da cidade)
- Notícias por relevância (com filtro de score mínimo)
- Posts do Instagram por engajamento
- Admin (executar coleta manual, ver jobs e logs)

## Requisitos

- Node.js 20 (funciona com 20.x; este projeto usa Vite 5 para compatibilidade)
- API rodando em `http://localhost:8000`

## Configuração

Crie um arquivo `.env` (ou exporte variável no shell) com a URL da API:

```
VITE_API_URL=http://localhost:8000
```

Obs.: por limitação do ambiente, foi incluído um exemplo em `env.example`.

## Rodar em desenvolvimento

```bash
cd frontend
npm install
npm run dev
```

Abra: http://localhost:5173
