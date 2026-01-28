"""
Coletor de dados do Google Trends.
Usa pytrends para obter interesse de busca sobre políticos.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Tenta importar pytrends
try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError:
    PYTRENDS_AVAILABLE = False
    logger.warning("pytrends não instalado. Execute: pip install pytrends")


class GoogleTrendsCollector:
    """
    Coleta dados de interesse de busca do Google Trends.
    """
    
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=2)
        self._pytrend = None
    
    @property
    def is_available(self) -> bool:
        """Verifica se o coletor está disponível"""
        return PYTRENDS_AVAILABLE
    
    def _get_pytrend(self) -> Optional['TrendReq']:
        """Retorna instância do pytrends (lazy loading)"""
        if not PYTRENDS_AVAILABLE:
            return None
        if self._pytrend is None:
            # Versão simplificada sem parâmetros que podem causar incompatibilidade
            self._pytrend = TrendReq(
                hl='pt-BR',
                tz=-180  # Fuso horário de Brasília
            )
        return self._pytrend
    
    def _buscar_interesse_sync(self, nome_politico: str) -> Dict[str, Any]:
        """
        Busca dados de interesse (síncrono, para executar em thread).
        
        Args:
            nome_politico: Nome do político
            
        Returns:
            Dict com dados de tendência
        """
        pytrend = self._get_pytrend()
        if not pytrend:
            return {}
        
        try:
            # Configura busca para últimos 7 dias no Brasil
            pytrend.build_payload(
                [nome_politico],
                cat=0,  # Todas as categorias
                timeframe='now 7-d',  # Últimos 7 dias
                geo='BR'  # Brasil
            )
            
            # Obtém interesse ao longo do tempo
            interest_over_time = pytrend.interest_over_time()
            
            # Obtém termos relacionados
            related_queries = pytrend.related_queries()
            
            # Processa dados
            resultado = {
                "nome": nome_politico,
                "periodo": "7 dias",
                "interesse_medio": 0,
                "interesse_maximo": 0,
                "interesse_atual": 0,
                "tendencia": "estável",
                "termos_relacionados": [],
                "dados_diarios": []
            }
            
            if not interest_over_time.empty and nome_politico in interest_over_time.columns:
                serie = interest_over_time[nome_politico]
                resultado["interesse_medio"] = round(serie.mean(), 2)
                resultado["interesse_maximo"] = int(serie.max())
                resultado["interesse_atual"] = int(serie.iloc[-1]) if len(serie) > 0 else 0
                
                # Calcula tendência
                if len(serie) >= 2:
                    primeira_metade = serie.iloc[:len(serie)//2].mean()
                    segunda_metade = serie.iloc[len(serie)//2:].mean()
                    if segunda_metade > primeira_metade * 1.2:
                        resultado["tendencia"] = "subindo"
                    elif segunda_metade < primeira_metade * 0.8:
                        resultado["tendencia"] = "descendo"
                    else:
                        resultado["tendencia"] = "estável"
                
                # Dados diários
                for timestamp, valor in serie.items():
                    resultado["dados_diarios"].append({
                        "data": timestamp.isoformat(),
                        "interesse": int(valor)
                    })
            
            # Termos relacionados
            if nome_politico in related_queries:
                queries = related_queries[nome_politico]
                if "top" in queries and queries["top"] is not None:
                    top_queries = queries["top"].head(10)
                    for _, row in top_queries.iterrows():
                        resultado["termos_relacionados"].append({
                            "termo": row.get("query", ""),
                            "valor": int(row.get("value", 0))
                        })
            
            return resultado
            
        except Exception as e:
            logger.error(f"Erro ao buscar Google Trends para {nome_politico}: {e}")
            return {}
    
    async def buscar_interesse(self, nome_politico: str) -> Dict[str, Any]:
        """
        Busca dados de interesse de um político no Google Trends.
        
        Args:
            nome_politico: Nome do político
            
        Returns:
            Dict com dados de tendência
        """
        if not self.is_available:
            logger.warning("Google Trends não disponível (pytrends não instalado)")
            return {}
        
        try:
            loop = asyncio.get_event_loop()
            resultado = await loop.run_in_executor(
                self.executor,
                self._buscar_interesse_sync,
                nome_politico
            )
            
            if resultado:
                logger.info(
                    f"Google Trends: {nome_politico} - "
                    f"interesse médio {resultado.get('interesse_medio', 0)}, "
                    f"tendência {resultado.get('tendencia', 'N/A')}"
                )
            
            return resultado
            
        except Exception as e:
            logger.error(f"Erro ao buscar interesse no Google Trends: {e}")
            return {}
    
    async def buscar_interesse_multiplos(
        self,
        nomes: List[str],
        delay: float = 2.0
    ) -> Dict[str, Dict[str, Any]]:
        """
        Busca interesse para múltiplos políticos.
        
        Args:
            nomes: Lista de nomes de políticos
            delay: Delay entre requisições (segundos)
            
        Returns:
            Dict com nome -> dados de interesse
        """
        resultados = {}
        
        for nome in nomes:
            resultado = await self.buscar_interesse(nome)
            if resultado:
                resultados[nome] = resultado
            await asyncio.sleep(delay)
        
        return resultados
    
    def converter_para_mencao(
        self,
        dados_trends: Dict[str, Any],
        politico_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Converte dados do Google Trends para formato de menção.
        
        Args:
            dados_trends: Dados retornados do buscar_interesse
            politico_id: ID do político
            
        Returns:
            Dict no formato de social_mention ou None
        """
        if not dados_trends:
            return None
        
        nome = dados_trends.get("nome", "")
        interesse_medio = dados_trends.get("interesse_medio", 0)
        tendencia = dados_trends.get("tendencia", "estável")
        termos = dados_trends.get("termos_relacionados", [])
        
        # Monta conteúdo descritivo
        termos_texto = ", ".join([t["termo"] for t in termos[:5]]) if termos else "N/A"
        conteudo = (
            f"Interesse de busca no Google para '{nome}': "
            f"média de {interesse_medio}% nos últimos 7 dias, "
            f"tendência {tendencia}. "
            f"Termos relacionados: {termos_texto}"
        )
        
        # Cria ID único baseado na data
        hoje = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        mention_id = f"trends_{politico_id}_{hoje}"
        
        return {
            "politico_id": politico_id,
            "plataforma": "google_trends",
            "mention_id": mention_id,
            "autor": "Google Trends",
            "autor_username": "google_trends",
            "conteudo": conteudo,
            "url": f"https://trends.google.com.br/trends/explore?geo=BR&q={nome.replace(' ', '%20')}",
            "likes": 0,
            "reposts": 0,
            "replies": 0,
            "engagement_score": interesse_medio,  # Usa interesse como score
            "posted_at": datetime.now(timezone.utc),
            "metadata": {
                "interesse_medio": interesse_medio,
                "interesse_maximo": dados_trends.get("interesse_maximo", 0),
                "interesse_atual": dados_trends.get("interesse_atual", 0),
                "tendencia": tendencia,
                "termos_relacionados": termos,
                "periodo": dados_trends.get("periodo", "7 dias")
            }
        }


# Instância global
google_trends_collector = GoogleTrendsCollector()
