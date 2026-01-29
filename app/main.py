"""
API FastAPI para o Portal de Dados Políticos.
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
from io import BytesIO
import base64

from app import __version__
from app.config import settings
from app.database import db
from app.scheduler.jobs import (
    start_scheduler, 
    shutdown_scheduler, 
    get_scheduled_jobs,
    executar_coleta_manual
)
from app.models.schemas import (
    Noticia,
    InstagramPost,
    InstagramStats,
    FonteNoticia,
    FonteUpdate,
    TrendingTopic,
    ColetaLog,
    HealthResponse,
    ColetaExecutarResponse,
    ProcessoJudicialResponse,
    DoacaoEleitoralResponse,
    FiliacaoPartidariaeResponse,
    CandidaturaResponse,
    ConsultaProcessualRequest,
    ConsultaProcessualResponse,
    PoliticoResumoProcessual
)
from app.ai.noticias import calcular_pontos, gerar_resumo_tecnico

# Importa collectors de consulta processual
from app.collectors.tse_dados_abertos import tse_collector
from app.collectors.tse_divulgacand import divulgacand_collector
from app.collectors.tjsp_esaj import tjsp_collector
from app.collectors.trf3_consulta import trf3_collector
from app.collectors.doe_sp import doe_sp_collector

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia ciclo de vida da aplicação"""
    # Startup
    logger.info("Iniciando Portal de Dados Políticos...")
    start_scheduler()
    yield
    # Shutdown
    logger.info("Encerrando aplicação...")
    shutdown_scheduler()


# Cria aplicação FastAPI
app = FastAPI(
    title="Portal de Dados Políticos",
    description="API para coleta e consulta de dados de políticos brasileiros",
    version=__version__,
    lifespan=lifespan
)

# Configura CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== HEALTH ====================

@app.get("/", response_model=HealthResponse)
async def health_check():
    """Health check da API"""
    return HealthResponse(
        status="ok",
        version=__version__,
        timestamp=datetime.now(timezone.utc)
    )


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check alternativo"""
    return await health_check()


# ==================== NOTÍCIAS ====================

@app.get("/politicos/{politico_id}/noticias", response_model=List[dict])
async def get_noticias_politico(
    politico_id: int,
    limit: int = Query(default=20, ge=1, le=100),
    min_score: float = Query(default=0, ge=0, le=100),
    diversificar: bool = Query(default=True, description="Diversificar notícias por fonte/canal")
):
    """
    Retorna notícias de um político ordenadas por relevância.
    
    - **politico_id**: ID do político
    - **limit**: Número máximo de notícias (padrão: 20)
    - **min_score**: Score mínimo de relevância (padrão: 0)
    - **diversificar**: Se true, diversifica as notícias por fonte/canal (padrão: true)
    """
    noticias = db.get_noticias_politico(politico_id, limit, min_score, diversificar_fontes=diversificar)
    return noticias


@app.get("/politicos/{politico_id}/noticias/top", response_model=List[dict])
async def get_top_noticias_politico(
    politico_id: int,
    limit: int = Query(default=5, ge=1, le=20)
):
    """
    Retorna as notícias mais relevantes de um político.
    
    - **politico_id**: ID do político
    - **limit**: Número de notícias (padrão: 5)
    """
    noticias = db.get_noticias_politico(politico_id, limit, min_score=50)
    return noticias


@app.get("/politicos/{politico_id}/concorrentes/noticias", response_model=List[dict])
async def get_noticias_concorrentes(
    politico_id: int,
    limit: int = Query(default=10, ge=1, le=50)
):
    """
    Retorna notícias dos concorrentes de um político.
    
    - **politico_id**: ID do político
    - **limit**: Número máximo de notícias por concorrente
    """
    concorrentes = db.get_concorrentes(politico_id)
    
    if not concorrentes:
        return []
    
    todas_noticias = []
    for concorrente in concorrentes:
        noticias = db.get_noticias_politico(concorrente["id"], limit, diversificar_fontes=True)
        todas_noticias.extend(noticias)
    
    # Ordena por relevância
    todas_noticias.sort(key=lambda x: x.get("relevancia_total", 0), reverse=True)
    
    return todas_noticias[:limit * len(concorrentes)]


@app.get("/politicos/{politico_id}/concorrentes/resumo", response_model=List[dict])
async def get_resumo_concorrentes(
    politico_id: int,
    limit_noticias: int = Query(default=5, ge=1, le=20)
):
    """
    Retorna resumo completo dos concorrentes de um político, incluindo
    dados básicos e notícias diversificadas de cada um.
    
    - **politico_id**: ID do político
    - **limit_noticias**: Número máximo de notícias por concorrente (padrão: 5)
    """
    concorrentes = db.get_concorrentes(politico_id)
    
    if not concorrentes:
        return []
    
    resultado = []
    for concorrente in concorrentes:
        concorrente_id = concorrente["id"]
        
        # Busca notícias diversificadas do concorrente
        noticias = db.get_noticias_politico(
            concorrente_id, 
            limit=limit_noticias, 
            min_score=0,
            diversificar_fontes=True
        )
        
        # Busca posts do Instagram (tabela unificada primeiro, fallback para legada)
        instagram = db.get_social_media_posts(concorrente_id, "instagram", limit=3)
        if not instagram:
            instagram = db.get_instagram_posts(concorrente_id, limit=3)
        
        # Monta resumo do concorrente
        resumo_concorrente = {
            "politico": concorrente,
            "noticias": noticias,
            "total_noticias": db.count_noticias_politico(concorrente_id),
            "instagram": instagram,
            "total_instagram": db.count_instagram_posts(concorrente_id),
        }
        
        resultado.append(resumo_concorrente)
    
    return resultado


@app.get("/politicos/{politico_id}/concorrentes/twitter_insights", response_model=List[dict])
async def get_concorrentes_twitter_insights(
    politico_id: int,
    days_back: int = Query(default=7, ge=1, le=30),
):
    """
    Retorna insights de Twitter/X para os concorrentes de um político:
    - followers_count (snapshot mais recente em `concorrente_twitter_insights`)
    - top 3 menções mais engajadas no Twitter/X (via snapshot; fallback em `social_mentions`)
    """
    return db.get_concorrentes_twitter_insights(politico_id, days_back=days_back)


@app.get("/noticias/cidade/{cidade}", response_model=List[dict])
async def get_noticias_cidade(
    cidade: str,
    limit: int = Query(default=20, ge=1, le=100)
):
    """
    Retorna notícias de uma cidade ordenadas por relevância.
    
    - **cidade**: Nome da cidade
    - **limit**: Número máximo de notícias
    """
    noticias = db.get_noticias_cidade(cidade, limit)
    return noticias


@app.get("/noticias/politica", response_model=List[dict])
async def get_noticias_politicas():
    """
    Retorna notícias políticas gerais ordenadas por relevância.
    """
    noticias = db.get_noticias_gerais(limit=30)
    return noticias


@app.get("/noticias/estado/{estado}", response_model=List[dict])
async def get_noticias_estado(
    estado: str,
    limit: int = Query(default=30, ge=1, le=100)
):
    """
    Retorna notícias políticas de um estado (tipo=estado) ordenadas por relevância.
    
    - **estado**: Sigla do estado (ex: SP, RJ)
    - **limit**: Número máximo de notícias
    """
    noticias = db.get_noticias_estado(estado.upper(), limit=limit)
    return noticias


@app.get("/noticias/capital/{estado}", response_model=List[dict])
async def get_noticias_capital(
    estado: str,
    limit: int = Query(default=3, ge=1, le=20)
):
    """
    Retorna notícias da capital de um estado ordenadas por relevância.
    
    - **estado**: Sigla do estado (ex: SP, RJ)
    - **limit**: Número máximo de notícias (padrão: 3)
    """
    noticias = db.get_noticias_capital(estado.upper(), limit=limit)
    return noticias


@app.get("/noticias/capitais", response_model=dict)
async def get_noticias_todas_capitais(
    limit_por_capital: int = Query(default=3, ge=1, le=10)
):
    """
    Retorna notícias de todas as capitais agrupadas por estado.
    
    - **limit_por_capital**: Número máximo de notícias por capital (padrão: 3)
    """
    noticias = db.get_noticias_todas_capitais(limit_por_capital)
    return {
        "noticias_por_estado": noticias,
        "estados_com_noticias": list(noticias.keys()),
        "total_estados": len(noticias)
    }


@app.get("/noticias/{noticia_id}/analise", response_model=dict)
async def get_noticia_analise(noticia_id: str):
    """
    Retorna análise técnica de uma notícia:
    - breakdown de scores/pontos
    - resumo técnico (quando OpenAI estiver configurado)
    """
    noticia = db.get_noticia_by_id(noticia_id)
    if not noticia:
        raise HTTPException(status_code=404, detail="Notícia não encontrada")

    politico_nome = None
    if noticia.get("politico_id"):
        p = db.get_politico_by_id(int(noticia["politico_id"]))
        politico_nome = p.get("name") if p else None

    pontos = calcular_pontos(noticia)
    analise = gerar_resumo_tecnico(noticia, politico_nome=politico_nome)

    return {
        "noticia_id": noticia_id,
        "pontos": pontos,
        "resumo_tecnico": analise.resumo_tecnico if analise else None,
        "porque_pontuou": analise.porque_pontuou if analise else [],
        "hipoteses": analise.hipoteses if analise else [],
        "alertas": analise.alertas if analise else (["OpenAI não configurado ou falhou; exibindo apenas breakdown de pontos."] if not analise else []),
    }


# ==================== REDES SOCIAIS ====================

@app.get("/politicos/{politico_id}/social", response_model=List[dict])
async def get_social_posts(
    politico_id: int,
    plataforma: Optional[str] = Query(default=None, regex="^(instagram|twitter|facebook|tiktok|youtube)$"),
    limit: int = Query(default=10, ge=1, le=50)
):
    """
    Retorna posts de redes sociais de um político ordenados por engajamento.
    
    - **politico_id**: ID do político
    - **plataforma**: Filtro por plataforma (instagram, twitter, facebook, tiktok, youtube)
    - **limit**: Número máximo de posts
    """
    posts = db.get_social_media_posts(politico_id, plataforma, limit)
    return posts


@app.get("/politicos/{politico_id}/social_mentions", response_model=List[dict])
async def get_social_mentions(
    politico_id: int,
    plataforma: Optional[str] = Query(default=None, regex="^(bluesky|twitter|google_trends|google_search)$"),
    limit: int = Query(default=8, ge=1, le=8),
):
    """
    Retorna menções (sobre o político) coletadas de redes sociais, ordenadas por engajamento.
    Por padrão, retorna apenas menções recentes (últimos 7 dias).
    
    - **politico_id**: ID do político
    - **plataforma**: Filtro por plataforma (bluesky, twitter, google_trends, google_search)
    - **limit**: Número máximo de menções (máx: 8)
    """
    # Regra do produto: retornar somente as 8 com maior engajamento
    limit = min(int(limit or 8), 8)
    mentions = db.get_social_mentions_politico(politico_id, plataforma=plataforma, limit=limit)
    return mentions


@app.get("/politicos/{politico_id}/mention_topics", response_model=List[dict])
async def get_mention_topics(politico_id: int):
    """
    Retorna agregações de tópicos (assuntos) de menções para um político.
    """
    topics = db.get_mention_topics_politico(politico_id)
    return topics


@app.get("/politicos/{politico_id}/assuntos", response_model=dict)
async def get_assuntos_politico(
    politico_id: int,
    limite: int = Query(default=10, ge=1, le=50),
):
    """
    Retorna os principais assuntos discutidos sobre um político.
    """
    politico = db.get_politico_by_id(politico_id)
    if not politico:
        raise HTTPException(status_code=404, detail="Político não encontrado")

    topics = db.get_top_assuntos_politico(politico_id, limite=limite)

    assuntos = []
    for t in topics:
        total = int(t.get("total_mencoes") or 0)
        pos = int(t.get("mencoes_positivas") or 0)
        neg = int(t.get("mencoes_negativas") or 0)
        neu = int(t.get("mencoes_neutras") or 0)
        eng = float(t.get("engagement_total") or 0)

        sentimento_pred = "neutro"
        if pos >= neg and pos >= neu:
            sentimento_pred = "positivo"
        elif neg >= pos and neg >= neu:
            sentimento_pred = "negativo"

        exemplo = None
        try:
            m = db.get_social_mentions_by_assunto(politico_id, str(t.get("assunto") or ""), limit=1)
            if m and m[0].get("assunto_detalhe"):
                exemplo = m[0].get("assunto_detalhe")
            elif m and m[0].get("conteudo"):
                exemplo = str(m[0].get("conteudo"))[:120]
        except Exception:
            exemplo = None

        assuntos.append(
            {
                "assunto": t.get("assunto"),
                "total_mencoes": total,
                "mencoes_positivas": pos,
                "mencoes_negativas": neg,
                "mencoes_neutras": neu,
                "sentimento_predominante": sentimento_pred,
                "engagement_total": eng,
                "exemplo": exemplo,
            }
        )

    return {
        "politico_id": politico_id,
        "nome": politico.get("name"),
        "periodo": "últimos registros",
        "assuntos": assuntos,
    }


@app.get("/politicos/{politico_id}/instagram", response_model=List[dict])
async def get_instagram_posts(
    politico_id: int,
    limit: int = Query(default=10, ge=1, le=50)
):
    """
    Retorna posts do Instagram de um político ordenados por engajamento.
    (Backward compatible - usa tabela unificada)
    
    - **politico_id**: ID do político
    - **limit**: Número máximo de posts
    """
    # Tenta nova tabela primeiro, fallback para antiga
    posts = db.get_social_media_posts(politico_id, "instagram", limit)
    if not posts:
        posts = db.get_instagram_posts(politico_id, limit)
    return posts


@app.get("/politicos/{politico_id}/instagram/stats", response_model=dict)
async def get_instagram_stats(politico_id: int):
    """
    Retorna estatísticas de Instagram de um político.
    
    - **politico_id**: ID do político
    """
    posts = db.get_instagram_posts(politico_id, limit=100)
    
    if not posts:
        return {
            "politico_id": politico_id,
            "total_posts": 0,
            "total_likes": 0,
            "total_comments": 0,
            "media_engagement": 0,
            "top_post": None
        }
    
    total_likes = sum(p.get("likes", 0) for p in posts)
    total_comments = sum(p.get("comments", 0) for p in posts)
    total_posts = len(posts)
    
    return {
        "politico_id": politico_id,
        "total_posts": total_posts,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "media_engagement": round((total_likes + total_comments) / total_posts, 2) if total_posts > 0 else 0,
        "top_post": posts[0] if posts else None
    }


# ==================== TRENDING ====================

@app.get("/trending", response_model=List[dict])
async def get_trending_topics(
    category: Optional[str] = Query(
        default=None, 
        regex="^(politica|twitter|google)$",
        description="Filtrar por categoria: 'politica', 'twitter', 'google', ou vazio para todos"
    )
):
    """
    Retorna os trending topics.
    
    - **category**: Filtro por categoria:
        - 'politica': Trending topics de notícias políticas
        - 'twitter': Trending topics do Twitter/X Brasil
        - 'google': Trending topics do Google Trends Brasil
        - vazio: Retorna todos
    """
    topics = db.get_trending_topics(category=category)
    return topics


# ==================== FONTES ====================

@app.get("/fontes", response_model=List[dict])
async def get_fontes():
    """
    Retorna todas as fontes de notícias com seus pesos.
    """
    fontes = db.get_fontes_ativas()
    return fontes


@app.put("/fontes/{fonte_id}/peso")
async def update_fonte_peso(fonte_id: str, update: FonteUpdate):
    """
    Atualiza o peso de confiabilidade de uma fonte.
    
    - **fonte_id**: ID da fonte
    - **peso_confiabilidade**: Novo peso (0.0 a 2.0)
    """
    success = db.update_fonte_peso(fonte_id, update.peso_confiabilidade)
    
    if not success:
        raise HTTPException(status_code=404, detail="Fonte não encontrada")
    
    return {"status": "ok", "mensagem": f"Peso atualizado para {update.peso_confiabilidade}"}


# ==================== COLETA ====================

@app.post("/coleta/executar", response_model=ColetaExecutarResponse)
async def executar_coleta(
    background_tasks: BackgroundTasks,
    tipo: str = Query(default="completa", regex="^(completa|noticias|instagram|trending|trending_twitter|trending_google|socials|social_mentions)$"),
    dry_run: bool = Query(default=False)
):
    """
    Executa coleta manual em background.
    
    - **tipo**: Tipo de coleta (completa, noticias, instagram, trending, trending_twitter, trending_google, socials, social_mentions)
    - **dry_run**: Quando tipo=socials, não grava no banco
    """
    log_id = db.log_coleta_inicio(f"manual_{tipo}")
    
    # Executa em background
    background_tasks.add_task(executar_coleta_manual, tipo, dry_run)
    
    return ColetaExecutarResponse(
        status="iniciado",
        mensagem=f"Coleta '{tipo}' iniciada em background",
        log_id=log_id
    )


@app.get("/coleta/logs", response_model=List[dict])
async def get_coleta_logs(limit: int = Query(default=50, ge=1, le=200)):
    """
    Retorna os logs das coletas mais recentes.
    
    - **limit**: Número máximo de logs
    """
    logs = db.get_logs_coleta(limit)
    return logs


@app.get("/coleta/jobs", response_model=List[dict])
async def get_jobs_agendados():
    """
    Retorna a lista de jobs agendados com próximas execuções.
    """
    jobs = get_scheduled_jobs()
    return jobs


# ==================== POLÍTICOS ====================

@app.get("/politicos", response_model=List[dict])
async def get_politicos():
    """
    Retorna apenas políticos com usar_diretoriaja = true.
    """
    politicos = db.get_politicos_diretoriaja()
    return politicos


@app.get("/politicos/{politico_id}", response_model=dict)
async def get_politico(politico_id: int):
    """
    Retorna dados de um político específico.
    
    - **politico_id**: ID do político
    """
    politico = db.get_politico_by_id(politico_id)
    
    if not politico:
        raise HTTPException(status_code=404, detail="Político não encontrado")
    
    return politico


@app.get("/politicos/{politico_id}/concorrentes", response_model=List[dict])
async def get_concorrentes(politico_id: int):
    """
    Retorna os concorrentes de um político.
    
    - **politico_id**: ID do político
    """
    concorrentes = db.get_concorrentes(politico_id)
    return concorrentes


@app.get("/politicos/{politico_id}/resumo", response_model=dict)
async def get_politico_resumo(politico_id: int):
    """
    Retorna um resumo completo de um político com todas as informações.
    
    - **politico_id**: ID do político
    """
    politico = db.get_politico_by_id(politico_id)
    
    if not politico:
        raise HTTPException(status_code=404, detail="Político não encontrado")
    
    # Coleta todos os dados
    noticias = db.get_noticias_politico(politico_id, limit=5, min_score=30)
    # Instagram: tabela unificada primeiro (compatível com endpoint /instagram)
    instagram = db.get_social_media_posts(politico_id, "instagram", limit=5)
    if not instagram:
        instagram = db.get_instagram_posts(politico_id, limit=5)
    concorrentes = db.get_concorrentes(politico_id)
    
    # Notícias da cidade do político (tipo='cidade' - busca genérica por nome da cidade)
    noticias_cidade = []
    if politico.get("cidade"):
        noticias_cidade = db.get_noticias_cidade(politico["cidade"], limit=5)
    
    # Notícias a nível de ESTADO (tipo='estado' sem cidade - governo, assembleia)
    noticias_estado = []
    if politico.get("estado"):
        noticias_estado = db.get_noticias_nivel_estado(politico["estado"], limit=3)
    
    # Notícias a nível de CIDADE/CAPITAL (tipo='cidade' com cidade preenchida - prefeitura, câmara)
    noticias_capital = []
    if politico.get("estado"):
        noticias_capital = db.get_noticias_capital(politico["estado"], limit=3)
    
    return {
        "politico": politico,
        "top_noticias": noticias,
        "top_instagram": instagram,
        "concorrentes": concorrentes,
        "noticias_cidade": noticias_cidade,
        "noticias_estado": noticias_estado,
        "noticias_capital": noticias_capital,
        "total_noticias": db.count_noticias_politico(politico_id),
        "total_posts_instagram": db.count_instagram_posts(politico_id),
        "total_mencoes": db.count_social_mentions_politico(politico_id),
    }


# ==================== CONSULTA PROCESSUAL ====================

@app.get("/politicos/{politico_id}/processos", response_model=dict)
async def get_processos_politico(
    politico_id: int,
    tribunal: Optional[str] = Query(default=None, description="Filtrar por tribunal (TJSP, TRF3, TSE)"),
    tipo: Optional[str] = Query(default=None, description="Filtrar por tipo (criminal, civel, eleitoral)"),
    status: Optional[str] = Query(default=None, description="Filtrar por status (ativo, arquivado)"),
    limit: int = Query(default=50, ge=1, le=200)
):
    """
    Retorna processos judiciais de um político.
    
    - **politico_id**: ID do político
    - **tribunal**: Filtro por tribunal
    - **tipo**: Filtro por tipo de processo
    - **status**: Filtro por status
    - **limit**: Número máximo de resultados
    """
    # Converte ID inteiro para UUID
    politico_uuid = db.get_politico_uuid(politico_id)
    if not politico_uuid:
        raise HTTPException(status_code=404, detail="Político não encontrado")
    
    query = db.client.table("processos_judiciais").select("*").eq("politico_id", politico_uuid)
    
    if tribunal:
        query = query.eq("tribunal", tribunal.upper())
    if tipo:
        query = query.eq("tipo", tipo)
    if status:
        query = query.eq("status", status)
    
    result = query.order("coletado_em", desc=True).limit(limit).execute()
    processos = result.data if result.data else []
    
    # Agrupa por tribunal e tipo
    por_tribunal = {}
    por_tipo = {}
    for p in processos:
        t = p.get("tribunal", "outros")
        por_tribunal[t] = por_tribunal.get(t, 0) + 1
        tp = p.get("tipo", "outros")
        por_tipo[tp] = por_tipo.get(tp, 0) + 1
    
    return {
        "processos": processos,
        "total": len(processos),
        "por_tribunal": por_tribunal,
        "por_tipo": por_tipo
    }


@app.get("/politicos/{politico_id}/doacoes", response_model=dict)
async def get_doacoes_politico(
    politico_id: int,
    tipo: str = Query(default="todas", regex="^(feitas|recebidas|todas)$"),
    eleicao: Optional[str] = Query(default=None, description="Filtrar por eleição (ex: 2024)"),
    limit: int = Query(default=100, ge=1, le=500)
):
    """
    Retorna doações eleitorais de um político.
    
    - **politico_id**: ID do político
    - **tipo**: Tipo de doação (feitas, recebidas, todas)
    - **eleicao**: Filtrar por eleição
    - **limit**: Número máximo de resultados
    """
    # Busca CPF do político
    politico = db.get_politico_by_id(politico_id)
    if not politico:
        raise HTTPException(status_code=404, detail="Político não encontrado")
    
    cpf = politico.get("cpf")
    doacoes = []
    
    if tipo in ["feitas", "todas"] and cpf:
        query = db.client.table("doacoes_eleitorais").select("*").eq("cpf_doador", cpf)
        if eleicao:
            query = query.eq("eleicao", eleicao)
        result = query.limit(limit).execute()
        doacoes.extend(result.data if result.data else [])
    
    if tipo in ["recebidas", "todas"] and cpf:
        query = db.client.table("doacoes_eleitorais").select("*").eq("cpf_candidato", cpf)
        if eleicao:
            query = query.eq("eleicao", eleicao)
        result = query.limit(limit).execute()
        doacoes.extend(result.data if result.data else [])
    
    # Agrupa por eleição
    por_eleicao = {}
    valor_total = 0
    for d in doacoes:
        e = d.get("eleicao", "outros")
        por_eleicao[e] = por_eleicao.get(e, 0) + 1
        valor_total += float(d.get("valor", 0) or 0)
    
    return {
        "doacoes": doacoes,
        "total": len(doacoes),
        "valor_total": round(valor_total, 2),
        "por_eleicao": por_eleicao
    }


@app.get("/politicos/{politico_id}/filiacoes", response_model=dict)
async def get_filiacoes_politico(politico_id: int):
    """
    Retorna histórico de filiações partidárias de um político.
    
    - **politico_id**: ID do político
    """
    # Converte ID inteiro para UUID
    politico_uuid = db.get_politico_uuid(politico_id)
    if not politico_uuid:
        raise HTTPException(status_code=404, detail="Político não encontrado")
    
    result = db.client.table("filiacoes_partidarias").select("*").eq("politico_id", politico_uuid).order("data_filiacao", desc=True).execute()
    filiacoes = result.data if result.data else []
    
    # Lista de partidos
    partidos = list(set(f.get("sigla_partido") or f.get("partido") for f in filiacoes if f.get("sigla_partido") or f.get("partido")))
    
    return {
        "filiacoes": filiacoes,
        "total": len(filiacoes),
        "historico_partidos": partidos
    }


@app.get("/politicos/{politico_id}/candidaturas", response_model=dict)
async def get_candidaturas_politico(
    politico_id: int,
    eleicao: Optional[str] = Query(default=None)
):
    """
    Retorna histórico de candidaturas de um político.
    
    - **politico_id**: ID do político
    - **eleicao**: Filtrar por eleição
    """
    # Converte ID inteiro para UUID
    politico_uuid = db.get_politico_uuid(politico_id)
    if not politico_uuid:
        raise HTTPException(status_code=404, detail="Político não encontrado")
    
    query = db.client.table("candidaturas").select("*").eq("politico_id", politico_uuid)
    
    if eleicao:
        query = query.eq("eleicao", eleicao)
    
    result = query.order("eleicao", desc=True).execute()
    candidaturas = result.data if result.data else []
    
    # Agrupa por eleição
    por_eleicao = {}
    for c in candidaturas:
        e = c.get("eleicao", "outros")
        por_eleicao[e] = por_eleicao.get(e, 0) + 1
    
    return {
        "candidaturas": candidaturas,
        "total": len(candidaturas),
        "por_eleicao": por_eleicao
    }


@app.post("/politicos/{politico_id}/consulta-processual", response_model=dict)
async def executar_consulta_processual(
    politico_id: int,
    background_tasks: BackgroundTasks,
    fontes: List[str] = Query(default=["TSE"], description="Fontes a consultar: TSE, TJSP, TRF3, DOE")
):
    """
    Inicia consulta processual completa para um político.
    
    - **politico_id**: ID do político
    - **fontes**: Lista de fontes a consultar
    
    NOTA: Consultas em TJSP, TRF3 e DOE requerem CAPTCHA e retornam URLs para consulta manual.
    """
    politico = db.get_politico_by_id(politico_id)
    if not politico:
        raise HTTPException(status_code=404, detail="Político não encontrado")
    
    cpf = politico.get("cpf")
    nome = politico.get("name")
    
    resultado = {
        "politico_id": politico_id,
        "nome": nome,
        "cpf": cpf[:3] + "***" + cpf[-2:] if cpf else None,
        "fontes_consultadas": [],
        "urls_pendentes": [],
        "dados_coletados": {}
    }
    
    # TSE - Dados Abertos (automático)
    if "TSE" in fontes and cpf:
        try:
            dados_tse = tse_collector.consulta_completa_cpf(cpf, politico_id)
            resultado["dados_coletados"]["TSE"] = dados_tse.get("resumo", {})
            resultado["fontes_consultadas"].append("TSE")
        except Exception as e:
            logger.error(f"Erro na consulta TSE: {e}")
            resultado["dados_coletados"]["TSE"] = {"erro": str(e)}
    
    # TSE DivulgaCandContas (automático)
    if "TSE" in fontes and nome:
        try:
            dados_divulga = divulgacand_collector.consulta_completa_candidato(
                nome=nome, uf=politico.get("estado", "SP"), politico_id=politico_id
            )
            if dados_divulga.get("candidato"):
                resultado["dados_coletados"]["TSE_DIVULGACAND"] = {
                    "candidato_encontrado": True,
                    "total_receitas": dados_divulga.get("total_receitas", 0),
                    "total_despesas": dados_divulga.get("total_despesas", 0)
                }
            resultado["fontes_consultadas"].append("TSE_DIVULGACAND")
        except Exception as e:
            logger.error(f"Erro na consulta DivulgaCandContas: {e}")
    
    # TJSP (semi-automatizado - requer CAPTCHA)
    if "TJSP" in fontes and cpf:
        dados_tjsp = tjsp_collector.buscar_todos_processos(cpf, politico_id)
        resultado["urls_pendentes"].append({
            "fonte": "TJSP",
            "urls": [
                dados_tjsp["primeiro_grau"]["url_consulta"],
                dados_tjsp["segundo_grau"]["url_consulta"]
            ],
            "instrucoes": dados_tjsp["instrucoes_gerais"]
        })
        resultado["fontes_consultadas"].append("TJSP")
    
    # TRF-3 (semi-automatizado - requer CAPTCHA)
    if "TRF3" in fontes and cpf:
        dados_trf3 = trf3_collector.consultar_por_cpf_semi_auto(cpf, politico_id)
        resultado["urls_pendentes"].append({
            "fonte": "TRF3",
            "url": dados_trf3["url_consulta"],
            "instrucoes": dados_trf3["instrucoes"]
        })
        resultado["fontes_consultadas"].append("TRF3")
    
    # DOE-SP (semi-automatizado - busca por nome)
    if "DOE" in fontes and nome:
        dados_doe = doe_sp_collector.buscar_por_nome_semi_auto(nome, politico_id)
        resultado["urls_pendentes"].append({
            "fonte": "DOE_SP",
            "url": dados_doe["url_consulta"],
            "instrucoes": dados_doe["instrucoes"]
        })
        resultado["fontes_consultadas"].append("DOE_SP")
    
    return resultado


@app.post("/processos/importar-html")
async def importar_html_resultado(
    fonte: str = Query(..., regex="^(TJSP|TRF3|DOE)$"),
    html: str = Query(..., description="HTML da página de resultado"),
    cpf: Optional[str] = Query(default=None),
    nome: Optional[str] = Query(default=None),
    politico_id: Optional[int] = Query(default=None)
):
    """
    Importa resultados de HTML de consulta manual.
    
    Use este endpoint após consultar manualmente TJSP, TRF-3 ou DOE
    e copiar o HTML da página de resultados.
    
    - **fonte**: Origem do HTML (TJSP, TRF3, DOE)
    - **html**: Conteúdo HTML da página de resultados
    - **cpf**: CPF consultado (opcional)
    - **nome**: Nome consultado (opcional)
    - **politico_id**: ID do político (opcional)
    """
    processos = []
    
    if fonte == "TJSP":
        processos = tjsp_collector.processar_html_resultado(html, cpf, politico_id)
    elif fonte == "TRF3":
        processos = trf3_collector.processar_html_resultado(html, cpf, politico_id)
    elif fonte == "DOE":
        processos = doe_sp_collector.processar_html_resultado(html, nome, politico_id)
    
    return {
        "fonte": fonte,
        "processos_importados": len(processos),
        "processos": processos
    }


@app.get("/politicos/{politico_id}/resumo-processual", response_model=dict)
async def get_resumo_processual(politico_id: int):
    """
    Retorna resumo processual completo de um político.
    
    - **politico_id**: ID do político
    """
    politico = db.get_politico_by_id(politico_id)
    if not politico:
        raise HTTPException(status_code=404, detail="Político não encontrado")
    
    cpf = politico.get("cpf")
    politico_uuid = politico.get("uuid")  # Obtém o UUID do político
    
    # Busca processos usando UUID
    processos = []
    processos_ativos = []
    if politico_uuid:
        processos_result = db.client.table("processos_judiciais").select("*").eq("politico_id", politico_uuid).execute()
        processos = processos_result.data if processos_result.data else []
        processos_ativos = [p for p in processos if p.get("status") == "ativo"]
    
    # Busca doações feitas
    doacoes_feitas = []
    if cpf:
        doacoes_feitas_result = db.client.table("doacoes_eleitorais").select("*").eq("cpf_doador", cpf).execute()
        doacoes_feitas = doacoes_feitas_result.data if doacoes_feitas_result.data else []
    
    # Busca doações recebidas
    doacoes_recebidas = []
    if cpf:
        doacoes_recebidas_result = db.client.table("doacoes_eleitorais").select("*").eq("cpf_candidato", cpf).execute()
        doacoes_recebidas = doacoes_recebidas_result.data if doacoes_recebidas_result.data else []
    
    # Busca candidaturas usando UUID
    candidaturas = []
    eleicoes_vencidas = []
    if politico_uuid:
        candidaturas_result = db.client.table("candidaturas").select("*").eq("politico_id", politico_uuid).execute()
        candidaturas = candidaturas_result.data if candidaturas_result.data else []
        eleicoes_vencidas = [c for c in candidaturas if c.get("situacao_totalizacao") and "eleito" in c.get("situacao_totalizacao", "").lower()]
    
    # Busca filiações usando UUID
    partidos = []
    if politico_uuid:
        filiacoes_result = db.client.table("filiacoes_partidarias").select("sigla_partido").eq("politico_id", politico_uuid).execute()
        filiacoes = filiacoes_result.data if filiacoes_result.data else []
        partidos = list(set(f.get("sigla_partido") for f in filiacoes if f.get("sigla_partido")))
    
    # Busca última consulta usando UUID
    ultima_consulta = None
    if politico_uuid:
        logs_result = db.client.table("consulta_processual_logs").select("iniciado_em").eq("politico_id", politico_uuid).order("iniciado_em", desc=True).limit(1).execute()
        ultima_consulta = logs_result.data[0]["iniciado_em"] if logs_result.data else None
    
    return {
        "politico_id": politico_id,
        "nome": politico.get("name"),
        "cpf": cpf[:3] + "***" + cpf[-2:] if cpf else None,
        "total_processos": len(processos),
        "processos_ativos": len(processos_ativos),
        "total_doacoes_feitas": len(doacoes_feitas),
        "valor_total_doado": round(sum(float(d.get("valor", 0) or 0) for d in doacoes_feitas), 2),
        "total_doacoes_recebidas": len(doacoes_recebidas),
        "valor_total_recebido": round(sum(float(d.get("valor", 0) or 0) for d in doacoes_recebidas), 2),
        "total_candidaturas": len(candidaturas),
        "eleicoes_vencidas": len(eleicoes_vencidas),
        "historico_partidos": partidos,
        "ultima_atualizacao": ultima_consulta
    }


@app.get("/consulta-processual/logs", response_model=List[dict])
async def get_logs_consulta_processual(
    politico_id: Optional[int] = Query(default=None),
    fonte: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200)
):
    """
    Retorna logs de consultas processuais realizadas.
    
    - **politico_id**: Filtrar por político
    - **fonte**: Filtrar por fonte (TSE, TJSP, TRF3, DOE_SP)
    - **limit**: Número máximo de logs
    """
    query = db.client.table("consulta_processual_logs").select("*")
    
    if politico_id:
        # Converte ID inteiro para UUID
        politico_uuid = db.get_politico_uuid(politico_id)
        if politico_uuid:
            query = query.eq("politico_id", politico_uuid)
        else:
            return []  # Político não encontrado
    if fonte:
        query = query.eq("fonte", fonte)
    
    result = query.order("iniciado_em", desc=True).limit(limit).execute()
    
    return result.data if result.data else []


@app.put("/politicos/{politico_id}/cpf")
async def atualizar_cpf_politico(
    politico_id: int,
    cpf: str = Query(..., min_length=11, max_length=14, description="CPF do político")
):
    """
    Atualiza o CPF de um político para permitir consultas processuais.
    
    - **politico_id**: ID do político
    - **cpf**: CPF (apenas números ou formatado)
    """
    # Normaliza CPF
    cpf_normalizado = "".join(filter(str.isdigit, cpf))
    
    if len(cpf_normalizado) != 11:
        raise HTTPException(status_code=400, detail="CPF inválido - deve conter 11 dígitos")
    
    try:
        db.client.table("politico").update({"cpf": cpf_normalizado}).eq("id", politico_id).execute()
        return {"status": "ok", "mensagem": "CPF atualizado com sucesso"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar CPF: {e}")


# ==================== PROXY DE IMAGENS ====================

@app.get("/proxy/image")
async def proxy_image(url: str = Query(..., description="URL da imagem a ser carregada")):
    """
    Proxy para carregar imagens de CDNs externos (Instagram, etc.)
    Contorna proteção de hotlinking retornando a imagem diretamente.
    """
    if not url:
        raise HTTPException(status_code=400, detail="URL é obrigatória")
    
    # Valida se é uma URL de imagem permitida
    allowed_domains = [
        "cdninstagram.com",
        "fbcdn.net",
        "instagram.com",
        "scontent",
    ]
    
    is_allowed = any(domain in url for domain in allowed_domains)
    if not is_allowed:
        raise HTTPException(status_code=403, detail="Domínio não permitido")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.instagram.com/",
                },
                follow_redirects=True
            )
            
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail="Falha ao carregar imagem")
            
            content_type = response.headers.get("content-type", "image/jpeg")
            
            return StreamingResponse(
                BytesIO(response.content),
                media_type=content_type,
                headers={
                    "Cache-Control": "public, max-age=86400",
                    "Access-Control-Allow-Origin": "*"
                }
            )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Timeout ao carregar imagem")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar imagem: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
