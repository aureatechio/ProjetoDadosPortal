"""
Agregador de menções sociais.
Combina dados de BlueSky, Google Trends e Google Search,
classifica por assunto usando IA e salva no banco.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

from app.collectors.bluesky import bluesky_collector
from app.collectors.google_trends import google_trends_collector
from app.ai.topic_analyzer import topic_analyzer
from app.database import db

logger = logging.getLogger(__name__)


class SocialMentionsAggregator:
    """
    Agrega menções de múltiplas fontes sociais.
    """
    
    def __init__(self):
        self.bluesky = bluesky_collector
        self.trends = google_trends_collector
        self.analyzer = topic_analyzer
    
    def _serialize_for_db(self, mencao: Dict[str, Any]) -> Dict[str, Any]:
        """Converte objetos datetime para string ISO para inserção no banco"""
        serialized = {}
        for key, value in mencao.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            elif value is not None:
                serialized[key] = value
        return serialized
    
    async def coletar_mencoes_politico(
        self,
        politico_id: int,
        nome_politico: str,
        classificar: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Coleta menções de um político de todas as fontes.
        
        Args:
            politico_id: ID do político no banco
            nome_politico: Nome do político
            classificar: Se deve classificar por assunto
            
        Returns:
            Lista de menções processadas
        """
        logger.info(f"Coletando menções sociais para: {nome_politico}")
        
        todas_mencoes = []
        
        # 1. Coleta do BlueSky
        try:
            mencoes_bluesky = await self.bluesky.buscar_mencoes(nome_politico, limit=50)
            for mencao in mencoes_bluesky:
                mencao["politico_id"] = politico_id
            todas_mencoes.extend(mencoes_bluesky)
            logger.info(f"BlueSky: {len(mencoes_bluesky)} menções para {nome_politico}")
        except Exception as e:
            logger.error(f"Erro ao coletar BlueSky para {nome_politico}: {e}")
        
        # 2. Coleta do Google Trends
        try:
            dados_trends = await self.trends.buscar_interesse(nome_politico)
            if dados_trends:
                mencao_trends = self.trends.converter_para_mencao(dados_trends, politico_id)
                if mencao_trends:
                    todas_mencoes.append(mencao_trends)
                    logger.info(f"Google Trends: dados coletados para {nome_politico}")
        except Exception as e:
            logger.error(f"Erro ao coletar Google Trends para {nome_politico}: {e}")
        
        # 3. Remove duplicatas
        mencoes_unicas = self._remove_duplicates(todas_mencoes)
        
        # 4. Classifica por assunto usando IA
        if classificar and mencoes_unicas:
            try:
                mencoes_unicas = await self.analyzer.classificar_batch(
                    mencoes_unicas,
                    nome_politico
                )
            except Exception as e:
                logger.error(f"Erro ao classificar menções para {nome_politico}: {e}")
        
        logger.info(f"Total: {len(mencoes_unicas)} menções únicas para {nome_politico}")
        
        return mencoes_unicas
    
    def _remove_duplicates(self, mencoes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove menções duplicadas baseado em plataforma + mention_id.
        """
        seen = set()
        unicas = []
        
        for mencao in mencoes:
            key = (mencao.get("plataforma"), mencao.get("mention_id"))
            if key not in seen:
                seen.add(key)
                unicas.append(mencao)
        
        return unicas
    
    async def executar_coleta_completa(self) -> Dict[str, int]:
        """
        Executa coleta completa para todos os políticos ativos.
        
        Returns:
            Dict com estatísticas da coleta
        """
        stats = {
            "politicos_processados": 0,
            "mencoes_coletadas": 0,
            "mencoes_inseridas": 0,
            "topicos_agregados": 0,
            "erros": 0
        }
        
        try:
            # Obtém políticos com usar_diretoriaja = True
            politicos = db.get_politicos_diretoriaja()
            logger.info(f"Iniciando coleta de menções para {len(politicos)} políticos (usar_diretoriaja=True)")
            
            for politico in politicos:
                try:
                    politico_id = politico["id"]
                    nome = politico.get("name")
                    
                    if not nome:
                        continue
                    
                    # Coleta menções
                    mencoes = await self.coletar_mencoes_politico(
                        politico_id,
                        nome,
                        classificar=True
                    )
                    
                    stats["politicos_processados"] += 1
                    stats["mencoes_coletadas"] += len(mencoes)
                    
                    # Salva no banco
                    if mencoes:
                        mencoes_serializadas = [self._serialize_for_db(m) for m in mencoes]
                        inserted = db.insert_social_mentions_batch(mencoes_serializadas)
                        stats["mencoes_inseridas"] += inserted
                    
                    # Agrega tópicos
                    try:
                        agregados = await self._agregar_topicos_politico(politico_id)
                        stats["topicos_agregados"] += agregados
                    except Exception as e:
                        logger.error(f"Erro ao agregar tópicos para {nome}: {e}")
                    
                    # Delay entre políticos
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Erro ao coletar menções para {politico.get('name')}: {e}")
                    stats["erros"] += 1
            
        except Exception as e:
            logger.error(f"Erro na coleta completa de menções: {e}")
            stats["erros"] += 1
        
        logger.info(f"Coleta de menções finalizada: {stats}")
        return stats
    
    async def _agregar_topicos_politico(
        self,
        politico_id: int,
        periodo_dias: int = 7
    ) -> int:
        """
        Agrega menções por assunto para um político.
        
        Args:
            politico_id: ID do político
            periodo_dias: Período de agregação em dias
            
        Returns:
            Número de tópicos agregados
        """
        # Calcula período
        agora = datetime.now(timezone.utc)
        inicio = agora - timedelta(days=periodo_dias)
        
        # Busca menções do período
        mencoes = db.get_social_mentions_by_periodo(
            politico_id,
            inicio,
            agora
        )
        
        if not mencoes:
            return 0
        
        # Agrega por assunto
        agregados = {}
        for mencao in mencoes:
            assunto = mencao.get("assunto") or "Outro"
            
            if assunto not in agregados:
                agregados[assunto] = {
                    "politico_id": politico_id,
                    "assunto": assunto,
                    "total_mencoes": 0,
                    "mencoes_positivas": 0,
                    "mencoes_negativas": 0,
                    "mencoes_neutras": 0,
                    "engagement_total": 0,
                    "ultima_mencao": None,
                    "periodo_inicio": inicio.isoformat(),
                    "periodo_fim": agora.isoformat()
                }
            
            agg = agregados[assunto]
            agg["total_mencoes"] += 1
            agg["engagement_total"] += float(mencao.get("engagement_score", 0) or 0)
            
            sentimento = mencao.get("sentimento")
            if sentimento == "positivo":
                agg["mencoes_positivas"] += 1
            elif sentimento == "negativo":
                agg["mencoes_negativas"] += 1
            else:
                agg["mencoes_neutras"] += 1
            
            posted_at = mencao.get("posted_at")
            if posted_at:
                if agg["ultima_mencao"] is None or posted_at > agg["ultima_mencao"]:
                    agg["ultima_mencao"] = posted_at
        
        # Salva agregações
        count = 0
        for topic_data in agregados.values():
            if topic_data.get("ultima_mencao"):
                topic_data["ultima_mencao"] = (
                    topic_data["ultima_mencao"].isoformat()
                    if isinstance(topic_data["ultima_mencao"], datetime)
                    else topic_data["ultima_mencao"]
                )
            result = db.upsert_mention_topic(topic_data)
            if result:
                count += 1
        
        return count
    
    async def get_assuntos_politico(
        self,
        politico_id: int,
        limite: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Retorna os principais assuntos discutidos sobre um político.
        
        Args:
            politico_id: ID do político
            limite: Número máximo de assuntos
            
        Returns:
            Lista de assuntos com estatísticas
        """
        return db.get_top_assuntos_politico(politico_id, limite)


# Instância global
social_mentions_aggregator = SocialMentionsAggregator()
