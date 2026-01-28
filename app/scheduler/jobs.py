"""
Agendador de jobs para coleta automática diária.
"""
import asyncio
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

from app.config import settings
from app.database import db
from app.collectors.news_aggregator import news_aggregator
from app.collectors.instagram import instagram_collector
from app.collectors.socials import socials_collector
from app.collectors.trending import (
    trending_collector, 
    general_trending_collector,
    twitter_trending_collector,
    google_trending_collector
)
from app.collectors.social_mentions import social_mentions_aggregator
from app.collectors.tse_dados_abertos import tse_collector
from app.collectors.tse_divulgacand import divulgacand_collector

logger = logging.getLogger(__name__)

# Scheduler global
scheduler = AsyncIOScheduler(timezone=settings.coleta_timezone)


def job_listener(event):
    """Listener para eventos do scheduler"""
    if event.exception:
        logger.error(f"Job {event.job_id} falhou: {event.exception}")
    else:
        logger.info(f"Job {event.job_id} executado com sucesso")


async def job_coleta_noticias():
    """
    Job de coleta de notícias.
    Executa às 06:00
    """
    log_id = db.log_coleta_inicio("noticias")
    
    try:
        logger.info("Iniciando coleta de notícias")
        stats = await news_aggregator.executar_coleta_completa()
        
        total = sum([
            stats.get("politicos", 0),
            stats.get("concorrentes", 0),
            stats.get("cidades", 0),
            stats.get("geral", 0)
        ])
        
        status = "sucesso" if stats.get("erros", 0) == 0 else "parcial"
        mensagem = f"Coletadas: {total} notícias. Erros: {stats.get('erros', 0)}"
        
        db.log_coleta_fim(log_id, status, mensagem, total)
        logger.info(f"Coleta de notícias finalizada: {mensagem}")
        
    except Exception as e:
        logger.error(f"Erro na coleta de notícias: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def job_coleta_instagram():
    """
    Job de coleta do Instagram.
    Executa às 06:45
    """
    log_id = db.log_coleta_inicio("instagram")
    
    try:
        logger.info("Iniciando coleta do Instagram")
        stats = await instagram_collector.executar_coleta_completa()
        
        total = stats.get("posts_coletados", 0)
        status = "sucesso" if stats.get("erros", 0) == 0 else "parcial"
        mensagem = f"Coletados: {total} posts de {stats.get('politicos_processados', 0)} políticos"
        
        db.log_coleta_fim(log_id, status, mensagem, total)
        logger.info(f"Coleta Instagram finalizada: {mensagem}")
        
    except Exception as e:
        logger.error(f"Erro na coleta do Instagram: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def job_coleta_trending():
    """
    Job de coleta de todos os trending topics (política, Twitter, Google).
    Executa às 08:00
    """
    log_id = db.log_coleta_inicio("trending")
    
    try:
        logger.info("Iniciando coleta de todos os trending topics")
        
        # Coleta trending políticos
        count_politica = await trending_collector.executar_coleta()
        logger.info(f"Trending políticos: {count_politica} topics")
        
        # Coleta trending do Twitter/X
        count_twitter = await twitter_trending_collector.executar_coleta()
        logger.info(f"Trending Twitter: {count_twitter} topics")
        
        # Coleta trending do Google
        count_google = await google_trending_collector.executar_coleta()
        logger.info(f"Trending Google: {count_google} topics")
        
        total = count_politica + count_twitter + count_google
        mensagem = f"Atualizados: {count_politica} política, {count_twitter} Twitter, {count_google} Google"
        
        db.log_coleta_fim(log_id, "sucesso", mensagem, total)
        logger.info(f"Coleta trending finalizada: {mensagem}")
        
    except Exception as e:
        logger.error(f"Erro na coleta de trending: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def job_coleta_trending_twitter():
    """
    Job de coleta apenas de trending topics do Twitter/X.
    """
    log_id = db.log_coleta_inicio("trending_twitter")
    
    try:
        logger.info("Iniciando coleta de trending topics do Twitter")
        count = await twitter_trending_collector.executar_coleta()
        
        db.log_coleta_fim(log_id, "sucesso", f"Atualizados {count} trending Twitter", count)
        logger.info(f"Coleta trending Twitter finalizada: {count} topics")
        
    except Exception as e:
        logger.error(f"Erro na coleta de trending Twitter: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def job_coleta_trending_google():
    """
    Job de coleta apenas de trending topics do Google Trends.
    """
    log_id = db.log_coleta_inicio("trending_google")
    
    try:
        logger.info("Iniciando coleta de trending topics do Google")
        count = await google_trending_collector.executar_coleta()
        
        db.log_coleta_fim(log_id, "sucesso", f"Atualizados {count} trending Google", count)
        logger.info(f"Coleta trending Google finalizada: {count} topics")
        
    except Exception as e:
        logger.error(f"Erro na coleta de trending Google: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def job_coleta_socials(dry_run: bool = False):
    """
    Job de preenchimento de redes sociais (Instagram/X) via fontes oficiais.
    """
    log_id = db.log_coleta_inicio("socials_dry_run" if dry_run else "socials")

    try:
        logger.info("Iniciando preenchimento de redes sociais")
        stats = await socials_collector.executar_preenchimento(dry_run=dry_run)

        total_aplicadas = int(stats.get("atualizacoes_aplicadas", 0))
        total_planejadas = int(stats.get("atualizacoes_planejadas", 0))
        erros = int(stats.get("erros", 0))

        if dry_run:
            status = "sucesso" if erros == 0 else "parcial"
            mensagem = f"Dry-run: planejadas {total_planejadas} atualizações. Erros: {erros}"
            db.log_coleta_fim(log_id, status, mensagem, total_planejadas)
        else:
            status = "sucesso" if erros == 0 else "parcial"
            mensagem = f"Aplicadas {total_aplicadas} atualizações. Erros: {erros}"
            db.log_coleta_fim(log_id, status, mensagem, total_aplicadas)

        logger.info(f"Preenchimento de redes sociais finalizado: {mensagem}")

    except Exception as e:
        logger.error(f"Erro no preenchimento de redes sociais: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def job_coleta_social_mentions():
    """
    Job de coleta de menções em redes sociais (BlueSky, Google Trends, etc).
    Executa às 07:00 (1 hora após notícias).
    """
    log_id = db.log_coleta_inicio("social_mentions")
    
    try:
        logger.info("Iniciando coleta de menções em redes sociais")
        stats = await social_mentions_aggregator.executar_coleta_completa()
        
        total = stats.get("mencoes_inseridas", 0)
        erros = stats.get("erros", 0)
        status = "sucesso" if erros == 0 else "parcial"
        mensagem = (
            f"Coletadas: {stats.get('mencoes_coletadas', 0)} menções, "
            f"Inseridas: {total}, "
            f"Políticos: {stats.get('politicos_processados', 0)}, "
            f"Erros: {erros}"
        )
        
        db.log_coleta_fim(log_id, status, mensagem, total)
        logger.info(f"Coleta de menções sociais finalizada: {mensagem}")
        
    except Exception as e:
        logger.error(f"Erro na coleta de menções sociais: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def job_limpeza():
    """
    Job de limpeza de dados antigos.
    Executa às 08:15
    """
    log_id = db.log_coleta_inicio("limpeza")
    
    try:
        logger.info("Iniciando limpeza de dados antigos")
        
        noticias_removidas = db.limpar_noticias_antigas(settings.dias_retencao_noticias)
        posts_removidos = db.limpar_instagram_antigos(settings.dias_retencao_instagram)
        mencoes_removidas = db.limpar_social_mentions_antigas(30)  # 30 dias para menções
        topicos_removidos = db.limpar_mention_topics_antigos(30)  # 30 dias para tópicos
        
        total = noticias_removidas + posts_removidos + mencoes_removidas + topicos_removidos
        mensagem = (
            f"Removidos: {noticias_removidas} notícias, "
            f"{posts_removidos} posts Instagram, "
            f"{mencoes_removidas} menções sociais, "
            f"{topicos_removidos} tópicos"
        )
        
        db.log_coleta_fim(log_id, "sucesso", mensagem, total)
        logger.info(f"Limpeza finalizada: {mensagem}")
        
    except Exception as e:
        logger.error(f"Erro na limpeza: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def job_coleta_processual_tse():
    """
    Job de coleta de dados processuais do TSE.
    Executa semanalmente (domingo às 03:00).
    
    Coleta para políticos que possuem CPF cadastrado:
    - Candidaturas históricas
    - Doações eleitorais
    - Filiações partidárias
    """
    log_id = db.log_coleta_inicio("processual_tse")
    
    try:
        logger.info("Iniciando coleta processual do TSE")
        
        # Busca políticos com CPF cadastrado
        result = db.supabase.table("politico").select("id, name, cpf, estado").not_.is_("cpf", "null").execute()
        politicos = result.data if result.data else []
        
        total_candidaturas = 0
        total_doacoes = 0
        total_filiacoes = 0
        erros = 0
        
        for politico in politicos:
            try:
                cpf = politico.get("cpf")
                politico_id = politico.get("id")
                
                if not cpf:
                    continue
                
                logger.info(f"Processando político {politico.get('name')} (ID: {politico_id})")
                
                # Coleta candidaturas
                candidaturas = tse_collector.buscar_candidaturas_por_cpf(cpf, politico_id)
                total_candidaturas += len(candidaturas)
                
                # Coleta doações feitas
                doacoes_feitas = tse_collector.buscar_doacoes_por_cpf(cpf, politico_id, tipo="doador")
                total_doacoes += len(doacoes_feitas)
                
                # Coleta doações recebidas
                doacoes_recebidas = tse_collector.buscar_doacoes_por_cpf(cpf, politico_id, tipo="candidato")
                total_doacoes += len(doacoes_recebidas)
                
                # Coleta filiações
                filiacoes = tse_collector.buscar_filiacoes_por_cpf(cpf, politico_id)
                total_filiacoes += len(filiacoes)
                
                # Tenta DivulgaCandContas também
                try:
                    divulgacand_collector.consulta_completa_candidato(
                        nome=politico.get("name"),
                        uf=politico.get("estado", "SP"),
                        politico_id=politico_id
                    )
                except Exception as e:
                    logger.warning(f"Erro DivulgaCandContas para {politico.get('name')}: {e}")
                
            except Exception as e:
                logger.error(f"Erro ao processar político {politico.get('name')}: {e}")
                erros += 1
        
        total = total_candidaturas + total_doacoes + total_filiacoes
        status = "sucesso" if erros == 0 else "parcial"
        mensagem = (
            f"Políticos processados: {len(politicos)}. "
            f"Candidaturas: {total_candidaturas}. "
            f"Doações: {total_doacoes}. "
            f"Filiações: {total_filiacoes}. "
            f"Erros: {erros}"
        )
        
        db.log_coleta_fim(log_id, status, mensagem, total)
        logger.info(f"Coleta processual TSE finalizada: {mensagem}")
        
    except Exception as e:
        logger.error(f"Erro na coleta processual TSE: {e}")
        db.log_coleta_fim(log_id, "erro", str(e), 0)


async def executar_coleta_manual(tipo: str = "completa", dry_run: bool = False) -> dict:
    """
    Executa coleta manual (pode ser chamada pela API).
    
    Args:
        tipo: Tipo de coleta (completa, noticias, instagram, trending, trending_twitter, trending_google, socials, social_mentions, processual_tse)
        dry_run: quando tipo=socials, não grava no banco
        
    Returns:
        Dict com resultado da execução
    """
    resultado = {"status": "iniciado", "tipo": tipo, "inicio": datetime.now().isoformat()}
    
    try:
        if tipo == "completa":
            await job_coleta_noticias()
            await job_coleta_instagram()
            await job_coleta_trending()
            await job_coleta_social_mentions()
        elif tipo == "noticias":
            await job_coleta_noticias()
        elif tipo == "instagram":
            await job_coleta_instagram()
        elif tipo == "trending":
            await job_coleta_trending()
        elif tipo == "trending_twitter":
            await job_coleta_trending_twitter()
        elif tipo == "trending_google":
            await job_coleta_trending_google()
        elif tipo == "socials":
            await job_coleta_socials(dry_run=dry_run)
        elif tipo == "social_mentions":
            await job_coleta_social_mentions()
        elif tipo == "processual_tse":
            await job_coleta_processual_tse()
        else:
            return {"status": "erro", "mensagem": f"Tipo inválido: {tipo}"}
        
        resultado["status"] = "concluido"
        resultado["fim"] = datetime.now().isoformat()
        
    except Exception as e:
        resultado["status"] = "erro"
        resultado["mensagem"] = str(e)
    
    return resultado


def start_scheduler():
    """
    Inicia o scheduler com os jobs configurados.
    """
    # Parse horário de coleta do config
    hora, minuto = settings.coleta_horario.split(":")
    hora = int(hora)
    minuto = int(minuto)
    
    # Adiciona listener para eventos
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    
    # Job de coleta de notícias - 06:00 (ou horário configurado)
    scheduler.add_job(
        job_coleta_noticias,
        CronTrigger(hour=hora, minute=minuto),
        id="coleta_noticias",
        name="Coleta de Notícias",
        replace_existing=True
    )
    
    # Job de coleta do Instagram - 45 minutos depois
    scheduler.add_job(
        job_coleta_instagram,
        CronTrigger(hour=hora, minute=minuto + 45 if minuto + 45 < 60 else (minuto + 45) % 60),
        id="coleta_instagram",
        name="Coleta Instagram",
        replace_existing=True
    )
    
    # Job de menções sociais - 1 hora depois das notícias
    hora_social = hora + 1 if hora + 1 < 24 else (hora + 1) % 24
    scheduler.add_job(
        job_coleta_social_mentions,
        CronTrigger(hour=hora_social, minute=minuto),
        id="coleta_social_mentions",
        name="Coleta Menções Sociais",
        replace_existing=True
    )
    
    # Job de trending - 2 horas depois
    hora_trending = hora + 2 if hora + 2 < 24 else (hora + 2) % 24
    scheduler.add_job(
        job_coleta_trending,
        CronTrigger(hour=hora_trending, minute=minuto),
        id="coleta_trending",
        name="Coleta Trending Topics",
        replace_existing=True
    )
    
    # Job de limpeza - 2h15 depois
    scheduler.add_job(
        job_limpeza,
        CronTrigger(hour=hora_trending, minute=minuto + 15 if minuto + 15 < 60 else (minuto + 15) % 60),
        id="limpeza_dados",
        name="Limpeza de Dados Antigos",
        replace_existing=True
    )
    
    # Job de coleta processual TSE - Semanal (domingo às 03:00)
    scheduler.add_job(
        job_coleta_processual_tse,
        CronTrigger(day_of_week="sun", hour=3, minute=0),
        id="coleta_processual_tse",
        name="Coleta Processual TSE (Semanal)",
        replace_existing=True
    )
    
    # Inicia o scheduler
    scheduler.start()
    
    logger.info("Scheduler iniciado com os seguintes jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name} ({job.id}): próxima execução {job.next_run_time}")


def shutdown_scheduler():
    """Para o scheduler"""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler encerrado")


def get_scheduled_jobs() -> list:
    """Retorna lista de jobs agendados"""
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None
        })
    return jobs
