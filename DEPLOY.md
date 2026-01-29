# Deploy (Vercel + DigitalOcean + GitHub)

Este repositório tem:
- **Frontend**: `frontend/` (React + Vite) → recomendado deploy na **Vercel**
- **Backend**: raiz do repo (FastAPI) → recomendado deploy na **DigitalOcean App Platform** via `Dockerfile` e `.do/app.yaml`

## 1) Deploy do frontend na Vercel (via GitHub)

1. No painel da Vercel, clique em **New Project** e selecione este repositório do GitHub.
2. Em **Root Directory**, selecione `frontend/`.
3. Garanta:
   - **Build Command**: `npm run build`
   - **Output Directory**: `dist`
4. Configure as variáveis (Project → Settings → Environment Variables):
   - `VITE_SUPABASE_URL`
   - `VITE_SUPABASE_ANON_KEY`
   - (opcional) `VITE_API_URL` se você for consumir a API FastAPI no frontend
5. Deploy.

Nota: o arquivo `frontend/vercel.json` já inclui rewrite para SPA, necessário para rotas do React Router.

## 2) Deploy do backend na DigitalOcean App Platform (via GitHub)

O spec do App Platform já existe em `.do/app.yaml` e está configurado com `deploy_on_push: true`.

### Opção A (mais simples): criar pelo painel

1. No painel da DigitalOcean: **Create → Apps**.
2. Conecte o GitHub e selecione este repositório/branch.
3. Na detecção, escolha **Dockerfile** (raiz) e confirme a porta `8000`.
4. Configure as variáveis/segredos (App → Settings → Environment Variables):
   - `SUPABASE_URL` (secret)
   - `SUPABASE_KEY` (secret)
   - `NEWSAPI_KEY` (secret, opcional)
   - `OPENAI_API_KEY` (secret, opcional)
   - `APIFY_TOKEN` (secret, opcional)
   - `COLETA_HORARIO` (ex: `06:00`)
   - `COLETA_TIMEZONE` (ex: `America/Sao_Paulo`)
5. Deploy.

### Opção B (automatizada): criar via `doctl` usando `.do/app.yaml`

Pré-requisito: `doctl` autenticado.

```bash
doctl apps create --spec .do/app.yaml
```

Depois, a cada push na `main`, o App Platform pode redeployar automaticamente (se `deploy_on_push: true` estiver habilitado).

## 3) GitHub (CI)

Este repo agora tem um workflow em `.github/workflows/ci.yml` que roda:
- build do frontend (`npm ci` + `npm run build`)
- sanity-check do backend (`pip install` + `python -m compileall`)

