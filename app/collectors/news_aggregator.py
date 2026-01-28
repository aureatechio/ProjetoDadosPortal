"""
Agregador de notícias de múltiplas fontes.
Combina Google News e NewsAPI, remove duplicatas e aplica relevância.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from urllib.parse import urlparse

from app.collectors.news_google import GoogleNewsCollector
from app.collectors.news_api import NewsAPICollector
from app.relevance.engine import RelevanceEngine
from app.database import db

logger = logging.getLogger(__name__)


class NewsAggregator:
    """
    Agrega notícias de múltiplas fontes, remove duplicatas e calcula relevância.
    """
    
    # Mapeamento de cargo para escopo de notícias
    ESCOPO_POR_FUNCAO = {
        'Presidente': {'brasil': True, 'estado': False, 'cidade': False},
        'Senador': {'brasil': True, 'estado': False, 'cidade': False},
        'Deputado Federal': {'brasil': True, 'estado': False, 'cidade': False},
        'Governador': {'brasil': True, 'estado': True, 'cidade': False},
        'Vice Governador': {'brasil': True, 'estado': True, 'cidade': False},
        'Deputado Estadual': {'brasil': False, 'estado': True, 'cidade': False},
        'Prefeito': {'brasil': False, 'estado': True, 'cidade': True},
        'Vereador': {'brasil': False, 'estado': False, 'cidade': True},
    }
    
    def __init__(self):
        self.google_collector = GoogleNewsCollector()
        self.newsapi_collector = NewsAPICollector()
        self.relevance_engine = RelevanceEngine()
    
    def _get_escopo_noticia(self, funcao: str) -> dict:
        """
        Retorna o escopo de notícias baseado no cargo do político.
        
        Args:
            funcao: Cargo do político
            
        Returns:
            Dict com flags brasil, estado, cidade
        """
        return self.ESCOPO_POR_FUNCAO.get(
            funcao, 
            {'brasil': False, 'estado': False, 'cidade': True}
        )
    
    def _serialize_for_db(self, noticia: Dict[str, Any]) -> Dict[str, Any]:
        """Converte objetos datetime para string ISO para inserção no banco"""
        serialized = {}
        for key, value in noticia.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            elif value is not None:
                serialized[key] = value
        return serialized
    
    def _normalize_url(self, url: str) -> str:
        """Normaliza URL para comparação de duplicatas"""
        if not url:
            return ""
        
        try:
            parsed = urlparse(url)
            # Remove parâmetros de tracking comuns
            clean_path = parsed.path.rstrip("/")
            return f"{parsed.netloc}{clean_path}".lower()
        except Exception:
            return url.lower()
    
    def _remove_duplicates(self, noticias: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove notícias duplicadas baseado na URL normalizada.
        Mantém a versão com mais conteúdo.
        """
        url_map = {}
        
        for noticia in noticias:
            url_norm = self._normalize_url(noticia.get("url", ""))
            
            if url_norm not in url_map:
                url_map[url_norm] = noticia
            else:
                # Mantém a versão com mais conteúdo
                existing = url_map[url_norm]
                existing_content = len(existing.get("conteudo_completo") or "")
                new_content = len(noticia.get("conteudo_completo") or "")
                
                if new_content > existing_content:
                    url_map[url_norm] = noticia
        
        return list(url_map.values())
    
    async def coletar_noticias_politico(
        self,
        politico_id: int,
        nome_politico: str,
        cidade: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Coleta notícias de um político de todas as fontes.
        
        Args:
            politico_id: ID do político no banco
            nome_politico: Nome do político
            cidade: Cidade do político
            
        Returns:
            Lista de notícias processadas com relevância
        """
        logger.info(f"Coletando notícias para: {nome_politico}")
        
        # Coleta em paralelo de ambas as fontes
        tasks = [
            self.google_collector.buscar_noticias_politico(nome_politico, cidade),
        ]
        
        if self.newsapi_collector.is_available:
            tasks.append(self.newsapi_collector.buscar_noticias_politico(nome_politico, cidade))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combina resultados
        todas_noticias = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Erro na coleta: {result}")
            elif result:
                todas_noticias.extend(result)
        
        # Remove duplicatas
        noticias_unicas = self._remove_duplicates(todas_noticias)
        
        # Calcula relevância
        noticias_processadas = self.relevance_engine.processar_noticias(
            noticias_unicas,
            nome_politico
        )
        
        # FILTRO DE QUALIDADE: Remove notícias sem menção ao político
        # Mantém apenas notícias que mencionam o político no título OU conteúdo
        noticias_relevantes = []
        for noticia in noticias_processadas:
            mencao_titulo = noticia.get("mencao_titulo", False)
            mencao_conteudo = noticia.get("mencao_conteudo", 0)
            score_mencao = noticia.get("score_mencao", 0)
            
            # Aceita se: mencionado no título OU pelo menos 1 menção no conteúdo OU score > 20
            if mencao_titulo or mencao_conteudo > 0 or score_mencao > 20:
                noticias_relevantes.append(noticia)
        
        # Log da filtragem
        descartadas = len(noticias_processadas) - len(noticias_relevantes)
        if descartadas > 0:
            logger.info(f"Filtradas {descartadas} notícias sem menção a {nome_politico}")
        
        noticias_processadas = noticias_relevantes
        
        # Adiciona politico_id e tipo
        for noticia in noticias_processadas:
            noticia["politico_id"] = politico_id
            noticia["tipo"] = "politico"
        
        logger.info(f"Coletadas {len(noticias_processadas)} notícias para {nome_politico}")
        
        return noticias_processadas
    
    async def coletar_noticias_concorrentes(
        self,
        politico_id: int,
        concorrentes: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Coleta notícias dos concorrentes de um político.
        
        Args:
            politico_id: ID do político principal
            concorrentes: Lista de concorrentes
            
        Returns:
            Lista de notícias dos concorrentes
        """
        todas_noticias = []
        
        for concorrente in concorrentes:
            nome = concorrente.get("name")
            if not nome:
                continue
            
            noticias = await self.coletar_noticias_politico(
                concorrente.get("id"),
                nome,
                concorrente.get("cidade")
            )
            
            # Marca como notícia de concorrente
            for noticia in noticias:
                noticia["tipo"] = "concorrente"
            
            todas_noticias.extend(noticias)
            
            await asyncio.sleep(1)  # Pequeno delay entre concorrentes
        
        return todas_noticias
    
    async def coletar_noticias_cidade(
        self,
        cidade: str,
        estado: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Coleta notícias de uma cidade.
        
        Args:
            cidade: Nome da cidade
            estado: Sigla do estado
            
        Returns:
            Lista de notícias da cidade
        """
        logger.info(f"Coletando notícias da cidade: {cidade}")
        
        tasks = [
            self.google_collector.buscar_noticias_cidade(cidade, estado),
        ]
        
        if self.newsapi_collector.is_available:
            tasks.append(self.newsapi_collector.buscar_noticias_cidade(cidade, estado))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        todas_noticias = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Erro na coleta cidade: {result}")
            elif result:
                todas_noticias.extend(result)
        
        noticias_unicas = self._remove_duplicates(todas_noticias)
        
        # Processa relevância (sem nome de político específico)
        noticias_processadas = self.relevance_engine.processar_noticias(noticias_unicas)
        
        for noticia in noticias_processadas:
            noticia["tipo"] = "cidade"
            noticia["cidade"] = cidade
        
        logger.info(f"Coletadas {len(noticias_processadas)} notícias de {cidade}")
        
        return noticias_processadas
    
    async def coletar_noticias_politicas_gerais(self) -> List[Dict[str, Any]]:
        """
        Coleta notícias políticas gerais do Brasil.
        
        Returns:
            Lista de notícias políticas
        """
        logger.info("Coletando notícias políticas gerais")
        
        tasks = [
            self.google_collector.buscar_noticias_politicas_gerais(),
        ]
        
        if self.newsapi_collector.is_available:
            tasks.append(self.newsapi_collector.buscar_noticias_politicas_brasil())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        todas_noticias = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Erro na coleta geral: {result}")
            elif result:
                todas_noticias.extend(result)
        
        noticias_unicas = self._remove_duplicates(todas_noticias)
        noticias_processadas = self.relevance_engine.processar_noticias(noticias_unicas)
        
        for noticia in noticias_processadas:
            noticia["tipo"] = "geral"
        
        logger.info(f"Coletadas {len(noticias_processadas)} notícias políticas gerais")
        
        return noticias_processadas
    
    async def coletar_noticias_estado(
        self,
        estado: str
    ) -> List[Dict[str, Any]]:
        """
        Coleta notícias de um estado.
        
        Args:
            estado: Sigla do estado (ex: SP, RJ, MG)
            
        Returns:
            Lista de notícias do estado
        """
        logger.info(f"Coletando notícias do estado: {estado}")
        
        tasks = [
            self.google_collector.buscar_noticias_estado(estado),
        ]
        
        if self.newsapi_collector.is_available:
            tasks.append(self.newsapi_collector.buscar_noticias_estado(estado))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        todas_noticias = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Erro na coleta estado: {result}")
            elif result:
                todas_noticias.extend(result)
        
        noticias_unicas = self._remove_duplicates(todas_noticias)
        noticias_processadas = self.relevance_engine.processar_noticias(noticias_unicas)
        
        for noticia in noticias_processadas:
            noticia["tipo"] = "estado"
            noticia["estado"] = estado
        
        logger.info(f"Coletadas {len(noticias_processadas)} notícias de {estado}")
        
        return noticias_processadas
    
    async def executar_coleta_completa(self) -> Dict[str, int]:
        """
        Executa coleta completa para todos os políticos ativos.
        Aplica escopo regional baseado no cargo do político.
        
        Returns:
            Dict com contagem de registros coletados por tipo
        """
        stats = {
            "politicos": 0,
            "concorrentes": 0,
            "cidades": 0,
            "estados": 0,
            "geral": 0,
            "erros": 0
        }
        
        try:
            # Obtém todos os políticos ativos
            politicos = db.get_politicos_ativos()
            logger.info(f"Iniciando coleta para {len(politicos)} políticos")
            
            cidades_processadas = set()
            estados_processados = set()
            coletar_brasil = False
            
            for politico in politicos:
                try:
                    politico_id = politico["id"]
                    nome = politico.get("name")
                    cidade = politico.get("cidade")
                    estado = politico.get("estado")
                    funcao = politico.get("funcao")
                    
                    if not nome:
                        continue
                    
                    # Determina escopo baseado no cargo
                    escopo = self._get_escopo_noticia(funcao)
                    
                    # Coleta notícias do político
                    noticias = await self.coletar_noticias_politico(
                        politico_id, nome, cidade
                    )
                    
                    if noticias:
                        noticias_serializadas = [self._serialize_for_db(n) for n in noticias]
                        inserted = db.insert_noticias_batch(noticias_serializadas)
                        stats["politicos"] += inserted
                    
                    # Coleta notícias dos concorrentes
                    concorrentes = db.get_concorrentes(politico_id)
                    if concorrentes:
                        noticias_conc = await self.coletar_noticias_concorrentes(
                            politico_id, concorrentes
                        )
                        if noticias_conc:
                            noticias_conc_serializadas = [self._serialize_for_db(n) for n in noticias_conc]
                            inserted = db.insert_noticias_batch(noticias_conc_serializadas)
                            stats["concorrentes"] += inserted
                    
                    # Coleta notícias do estado (baseado no escopo)
                    if escopo.get("estado") and estado and estado not in estados_processados:
                        noticias_estado = await self.coletar_noticias_estado(estado)
                        if noticias_estado:
                            noticias_estado_serializadas = [self._serialize_for_db(n) for n in noticias_estado]
                            inserted = db.insert_noticias_batch(noticias_estado_serializadas)
                            stats["estados"] += inserted
                        estados_processados.add(estado)
                    
                    # Coleta notícias da cidade (baseado no escopo)
                    if escopo.get("cidade") and cidade and cidade not in cidades_processadas:
                        noticias_cidade = await self.coletar_noticias_cidade(cidade, estado)
                        if noticias_cidade:
                            noticias_cidade_serializadas = [self._serialize_for_db(n) for n in noticias_cidade]
                            inserted = db.insert_noticias_batch(noticias_cidade_serializadas)
                            stats["cidades"] += inserted
                        cidades_processadas.add(cidade)
                    
                    # Marca se precisa coletar notícias do Brasil
                    if escopo.get("brasil"):
                        coletar_brasil = True
                    
                    await asyncio.sleep(2)  # Delay entre políticos
                    
                except Exception as e:
                    logger.error(f"Erro ao coletar para político {politico.get('name')}: {e}")
                    stats["erros"] += 1
            
            # Coleta notícias políticas gerais (Brasil) - se algum político precisar
            if coletar_brasil:
                noticias_gerais = await self.coletar_noticias_politicas_gerais()
                if noticias_gerais:
                    noticias_gerais_serializadas = [self._serialize_for_db(n) for n in noticias_gerais]
                    inserted = db.insert_noticias_batch(noticias_gerais_serializadas)
                    stats["geral"] += inserted
            
        except Exception as e:
            logger.error(f"Erro na coleta completa: {e}")
            stats["erros"] += 1
        
        logger.info(f"Coleta completa finalizada: {stats}")
        return stats


# Instância global do agregador
news_aggregator = NewsAggregator()
