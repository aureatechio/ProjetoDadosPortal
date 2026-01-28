"""
Script para coletar notícias de políticos com usar_diretoriaja = TRUE
que ainda não possuem notícias cadastradas.
"""
import asyncio
import logging
import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import db
from app.collectors.news_aggregator import news_aggregator

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_politicos_diretoriaja_sem_noticias():
    """
    Busca políticos com usar_diretoriaja = TRUE que não possuem notícias.
    """
    # Busca todos os políticos com usar_diretoriaja = TRUE
    politicos_diretoriaja = db.get_politicos_diretoriaja()
    
    logger.info(f"Total de políticos com usar_diretoriaja=TRUE: {len(politicos_diretoriaja)}")
    
    # Filtra apenas os que não têm notícias
    politicos_sem_noticias = []
    
    for politico in politicos_diretoriaja:
        uuid = politico.get("uuid")
        if not uuid:
            continue
        
        # Verifica se tem notícias
        response = db.client.table("noticias").select("id", count="exact").eq("politico_id", uuid).limit(1).execute()
        
        if response.count == 0:
            politicos_sem_noticias.append(politico)
    
    return politicos_sem_noticias


async def coletar_noticias_para_politicos(politicos):
    """
    Coleta notícias para uma lista de políticos.
    """
    stats = {
        "politicos_processados": 0,
        "noticias_coletadas": 0,
        "erros": 0
    }
    
    for politico in politicos:
        try:
            politico_id = politico["id"]
            uuid = politico["uuid"]
            nome = politico.get("name")
            cidade = politico.get("cidade")
            estado = politico.get("estado")
            funcao = politico.get("funcao")
            
            if not nome:
                continue
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processando: {nome}")
            logger.info(f"  - Cidade: {cidade or 'N/A'}")
            logger.info(f"  - Estado: {estado or 'N/A'}")
            logger.info(f"  - Função: {funcao or 'N/A'}")
            logger.info(f"  - UUID: {uuid}")
            
            # Coleta notícias do político
            noticias = await news_aggregator.coletar_noticias_politico(
                politico_id, nome, cidade
            )
            
            if noticias:
                # Serializa e atualiza politico_id para usar UUID
                noticias_para_insert = []
                for n in noticias:
                    n_serializada = news_aggregator._serialize_for_db(n)
                    # Substitui politico_id (int) pelo UUID
                    n_serializada["politico_id"] = uuid
                    noticias_para_insert.append(n_serializada)
                
                inserted = db.insert_noticias_batch(noticias_para_insert)
                stats["noticias_coletadas"] += inserted
                logger.info(f"  -> Inseridas {inserted} notícias para {nome}")
            else:
                logger.info(f"  -> Nenhuma notícia encontrada para {nome}")
            
            stats["politicos_processados"] += 1
            
            # Delay entre políticos para não sobrecarregar
            await asyncio.sleep(2)
            
        except Exception as e:
            logger.error(f"Erro ao processar político {politico.get('name')}: {e}")
            stats["erros"] += 1
    
    return stats


async def main():
    """
    Função principal.
    """
    logger.info("="*60)
    logger.info("COLETA DE NOTÍCIAS PARA POLÍTICOS DIRETORIAJA SEM NOTÍCIAS")
    logger.info("="*60)
    
    # Busca políticos sem notícias
    logger.info("\nBuscando políticos com usar_diretoriaja=TRUE sem notícias...")
    politicos = await get_politicos_diretoriaja_sem_noticias()
    
    if not politicos:
        logger.info("Todos os políticos com usar_diretoriaja=TRUE já possuem notícias!")
        return
    
    logger.info(f"\nEncontrados {len(politicos)} políticos sem notícias:")
    for p in politicos:
        logger.info(f"  - {p.get('name')} ({p.get('funcao', 'N/A')}) - {p.get('estado', 'N/A')}")
    
    # Coleta notícias
    logger.info("\nIniciando coleta de notícias...")
    stats = await coletar_noticias_para_politicos(politicos)
    
    # Resumo final
    logger.info("\n" + "="*60)
    logger.info("RESUMO DA COLETA")
    logger.info("="*60)
    logger.info(f"Políticos processados: {stats['politicos_processados']}")
    logger.info(f"Notícias coletadas: {stats['noticias_coletadas']}")
    logger.info(f"Erros: {stats['erros']}")
    logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
