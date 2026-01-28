"""
Script para coletar notícias dos estados e capitais dos políticos com usar_diretoriaja = TRUE.
- Agrupa os políticos por estado para evitar buscas duplicadas
- Busca as 3 notícias mais importantes a nível de ESTADO (governo, assembleia)
- Busca as 3 notícias mais importantes a nível de CIDADE/CAPITAL (prefeitura, câmara)
"""
import asyncio
import logging
import sys
from pathlib import Path
from typing import Dict, List, Set

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import db
from app.collectors.news_google import GoogleNewsCollector
from app.relevance.engine import RelevanceEngine
from datetime import datetime

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mapeamento de estados (sigla) para suas capitais
CAPITAIS_ESTADOS: Dict[str, str] = {
    "AC": "Rio Branco",
    "AL": "Maceió",
    "AP": "Macapá",
    "AM": "Manaus",
    "BA": "Salvador",
    "CE": "Fortaleza",
    "DF": "Brasília",
    "ES": "Vitória",
    "GO": "Goiânia",
    "MA": "São Luís",
    "MT": "Cuiabá",
    "MS": "Campo Grande",
    "MG": "Belo Horizonte",
    "PA": "Belém",
    "PB": "João Pessoa",
    "PR": "Curitiba",
    "PE": "Recife",
    "PI": "Teresina",
    "RJ": "Rio de Janeiro",
    "RN": "Natal",
    "RS": "Porto Alegre",
    "RO": "Porto Velho",
    "RR": "Boa Vista",
    "SC": "Florianópolis",
    "SP": "São Paulo",
    "SE": "Aracaju",
    "TO": "Palmas",
}

# Mapeamento de siglas para nomes completos dos estados
NOMES_ESTADOS: Dict[str, str] = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
    "GO": "Goiás", "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais", "PA": "Pará", "PB": "Paraíba", "PR": "Paraná",
    "PE": "Pernambuco", "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina",
    "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins"
}


def get_estados_unicos_diretoriaja() -> Set[str]:
    """
    Busca todos os políticos com usar_diretoriaja = TRUE e retorna
    os estados únicos (agrupados).
    
    Returns:
        Set de siglas de estados únicos
    """
    politicos = db.get_politicos_diretoriaja()
    
    logger.info(f"Total de políticos com usar_diretoriaja=TRUE: {len(politicos)}")
    
    estados = set()
    for politico in politicos:
        estado = politico.get("estado")
        if estado and estado.strip():
            # Normaliza para maiúsculo
            estados.add(estado.strip().upper())
    
    return estados


def _serialize_for_db(noticia: Dict) -> Dict:
    """Converte objetos datetime para string ISO para inserção no banco"""
    serialized = {}
    for key, value in noticia.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif value is not None:
            serialized[key] = value
    return serialized


async def coletar_noticias_estado(
    estado: str,
    google_collector: GoogleNewsCollector,
    relevance_engine: RelevanceEngine,
    limite: int = 3
) -> List[Dict]:
    """
    Coleta as notícias mais importantes a nível de ESTADO.
    (governo estadual, assembleia legislativa, política estadual)
    
    Args:
        estado: Sigla do estado
        google_collector: Instância do coletor do Google News
        relevance_engine: Engine de relevância
        limite: Número máximo de notícias a retornar (padrão: 3)
        
    Returns:
        Lista com as notícias mais importantes do estado
    """
    nome_estado = NOMES_ESTADOS.get(estado, estado)
    logger.info(f"Buscando notícias do ESTADO: {nome_estado} ({estado})...")
    
    # Queries para buscar notícias a nível estadual
    queries = [
        f"governo {nome_estado}",
        f"assembleia legislativa {estado}",
        f"política estadual {nome_estado}",
    ]
    
    todas_noticias = []
    
    for query in queries:
        try:
            noticias = await google_collector.buscar_noticias(query, extrair_conteudo=False)
            todas_noticias.extend(noticias)
            await asyncio.sleep(1)  # Rate limiting
        except Exception as e:
            logger.warning(f"Erro ao buscar '{query}': {e}")
    
    if not todas_noticias:
        logger.info(f"Nenhuma notícia encontrada para o estado {nome_estado}")
        return []
    
    # Remove duplicatas por URL
    urls_vistas = set()
    noticias_unicas = []
    for noticia in todas_noticias:
        url = noticia.get("url", "")
        if url and url not in urls_vistas:
            urls_vistas.add(url)
            noticias_unicas.append(noticia)
    
    # Processa relevância
    noticias_processadas = relevance_engine.processar_noticias(noticias_unicas)
    
    # Ordena por relevância e pega as top N
    noticias_ordenadas = sorted(
        noticias_processadas, 
        key=lambda x: x.get("relevancia_total", 0), 
        reverse=True
    )
    
    top_noticias = noticias_ordenadas[:limite]
    
    # Adiciona metadados - tipo='estado' SEM cidade (nível estadual)
    for noticia in top_noticias:
        noticia["tipo"] = "estado"
        noticia["estado"] = estado
        # Não define cidade - é notícia a nível de estado
    
    logger.info(f"Selecionadas {len(top_noticias)} notícias do estado {nome_estado}")
    
    return top_noticias


async def coletar_noticias_cidade(
    estado: str, 
    capital: str,
    google_collector: GoogleNewsCollector,
    relevance_engine: RelevanceEngine,
    limite: int = 3
) -> List[Dict]:
    """
    Coleta as notícias mais importantes a nível de CIDADE/CAPITAL.
    (prefeitura, câmara municipal, política municipal)
    
    Args:
        estado: Sigla do estado
        capital: Nome da capital
        google_collector: Instância do coletor do Google News
        relevance_engine: Engine de relevância
        limite: Número máximo de notícias a retornar (padrão: 3)
        
    Returns:
        Lista com as notícias mais importantes da cidade
    """
    logger.info(f"Buscando notícias da CIDADE: {capital} ({estado})...")
    
    # Queries para buscar notícias a nível municipal
    queries = [
        f"prefeitura {capital}",
        f"câmara municipal {capital}",
        f"política municipal {capital}",
    ]
    
    todas_noticias = []
    
    for query in queries:
        try:
            noticias = await google_collector.buscar_noticias(query, extrair_conteudo=False)
            todas_noticias.extend(noticias)
            await asyncio.sleep(1)  # Rate limiting
        except Exception as e:
            logger.warning(f"Erro ao buscar '{query}': {e}")
    
    if not todas_noticias:
        logger.info(f"Nenhuma notícia encontrada para {capital}")
        return []
    
    # Remove duplicatas por URL
    urls_vistas = set()
    noticias_unicas = []
    for noticia in todas_noticias:
        url = noticia.get("url", "")
        if url and url not in urls_vistas:
            urls_vistas.add(url)
            noticias_unicas.append(noticia)
    
    # Processa relevância
    noticias_processadas = relevance_engine.processar_noticias(noticias_unicas)
    
    # Ordena por relevância e pega as top N
    noticias_ordenadas = sorted(
        noticias_processadas, 
        key=lambda x: x.get("relevancia_total", 0), 
        reverse=True
    )
    
    top_noticias = noticias_ordenadas[:limite]
    
    # Adiciona metadados - tipo='cidade' COM cidade (nível municipal)
    for noticia in top_noticias:
        noticia["tipo"] = "cidade"
        noticia["estado"] = estado
        noticia["cidade"] = capital
    
    logger.info(f"Selecionadas {len(top_noticias)} notícias da cidade {capital}")
    
    return top_noticias


async def main():
    """
    Função principal que executa a coleta de notícias dos estados e capitais.
    """
    logger.info("=" * 60)
    logger.info("COLETA DE NOTÍCIAS - ESTADOS E CAPITAIS (DIRETORIAJA)")
    logger.info("=" * 60)
    
    # 1. Busca estados únicos dos políticos com usar_diretoriaja = TRUE
    estados_unicos = get_estados_unicos_diretoriaja()
    
    if not estados_unicos:
        logger.warning("Nenhum estado encontrado nos políticos com usar_diretoriaja=TRUE")
        return
    
    logger.info(f"\nEstados únicos encontrados: {len(estados_unicos)}")
    for estado in sorted(estados_unicos):
        capital = CAPITAIS_ESTADOS.get(estado, "Desconhecida")
        nome = NOMES_ESTADOS.get(estado, estado)
        logger.info(f"  - {estado} ({nome}): Capital = {capital}")
    
    # 2. Inicializa coletores
    google_collector = GoogleNewsCollector()
    relevance_engine = RelevanceEngine()
    
    # 3. Estatísticas
    stats = {
        "estados_processados": 0,
        "noticias_estado_coletadas": 0,
        "noticias_estado_inseridas": 0,
        "noticias_cidade_coletadas": 0,
        "noticias_cidade_inseridas": 0,
        "erros": 0
    }
    
    logger.info("\n" + "=" * 60)
    logger.info("INICIANDO COLETA DE NOTÍCIAS")
    logger.info("=" * 60)
    
    for estado in sorted(estados_unicos):
        capital = CAPITAIS_ESTADOS.get(estado)
        nome_estado = NOMES_ESTADOS.get(estado, estado)
        
        if not capital:
            logger.warning(f"Capital não encontrada para o estado: {estado}")
            stats["erros"] += 1
            continue
        
        try:
            logger.info(f"\n--- Processando {nome_estado} ({estado}) ---")
            
            # 3a. Coleta notícias a nível de ESTADO
            noticias_estado = await coletar_noticias_estado(
                estado=estado,
                google_collector=google_collector,
                relevance_engine=relevance_engine,
                limite=3
            )
            
            stats["noticias_estado_coletadas"] += len(noticias_estado)
            
            if noticias_estado:
                noticias_serializadas = [_serialize_for_db(n) for n in noticias_estado]
                inserted = db.insert_noticias_batch(noticias_serializadas)
                stats["noticias_estado_inseridas"] += inserted
                logger.info(f"  [ESTADO] Inseridas {inserted} notícias")
            
            await asyncio.sleep(1)
            
            # 3b. Coleta notícias a nível de CIDADE/CAPITAL
            noticias_cidade = await coletar_noticias_cidade(
                estado=estado,
                capital=capital,
                google_collector=google_collector,
                relevance_engine=relevance_engine,
                limite=3
            )
            
            stats["noticias_cidade_coletadas"] += len(noticias_cidade)
            
            if noticias_cidade:
                noticias_serializadas = [_serialize_for_db(n) for n in noticias_cidade]
                inserted = db.insert_noticias_batch(noticias_serializadas)
                stats["noticias_cidade_inseridas"] += inserted
                logger.info(f"  [CIDADE] Inseridas {inserted} notícias de {capital}")
            
            stats["estados_processados"] += 1
            
            # Delay entre estados
            await asyncio.sleep(2)
            
        except Exception as e:
            logger.error(f"Erro ao processar {nome_estado} ({estado}): {e}")
            stats["erros"] += 1
    
    # 4. Resumo final
    logger.info("\n" + "=" * 60)
    logger.info("RESUMO DA COLETA")
    logger.info("=" * 60)
    logger.info(f"Estados processados: {stats['estados_processados']}/{len(estados_unicos)}")
    logger.info(f"")
    logger.info(f"NÍVEL ESTADO:")
    logger.info(f"  - Notícias coletadas: {stats['noticias_estado_coletadas']}")
    logger.info(f"  - Notícias inseridas: {stats['noticias_estado_inseridas']}")
    logger.info(f"")
    logger.info(f"NÍVEL CIDADE:")
    logger.info(f"  - Notícias coletadas: {stats['noticias_cidade_coletadas']}")
    logger.info(f"  - Notícias inseridas: {stats['noticias_cidade_inseridas']}")
    logger.info(f"")
    logger.info(f"TOTAL: {stats['noticias_estado_inseridas'] + stats['noticias_cidade_inseridas']} notícias inseridas")
    logger.info(f"Erros: {stats['erros']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
