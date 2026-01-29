"""
Coletor de notícias usando NewsAPI.org
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from newsapi import NewsApiClient
from newsapi.newsapi_exception import NewsAPIException

from app.config import settings
from app.utils.storage import upload_image_from_url_async

logger = logging.getLogger(__name__)


class NewsAPICollector:
    """
    Coleta notícias usando a API do NewsAPI.org
    
    Nota: O plano gratuito tem limite de 100 requisições/dia.
    """
    
    def __init__(self):
        self.api_key = settings.newsapi_key
        self.client = None
        
        if self.api_key:
            self.client = NewsApiClient(api_key=self.api_key)
        else:
            logger.warning("NewsAPI key não configurada. Coletor NewsAPI desabilitado.")
    
    @property
    def is_available(self) -> bool:
        """Verifica se o coletor está disponível"""
        return self.client is not None
    
    async def buscar_noticias(
        self,
        query: str,
        language: str = "pt",
        sort_by: str = "relevancy",
        page_size: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Busca notícias no NewsAPI.
        
        Args:
            query: Termo de busca
            language: Idioma das notícias
            sort_by: Ordenação (relevancy, popularity, publishedAt)
            page_size: Quantidade de resultados
            
        Returns:
            Lista de notícias
        """
        if not self.is_available:
            return []
        
        noticias = []
        
        try:
            # NewsAPI é síncrono, executa em thread
            loop = asyncio.get_event_loop()
            
            # Busca dos últimos 7 dias (limite do plano gratuito)
            from_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
            
            response = await loop.run_in_executor(
                None,
                lambda: self.client.get_everything(
                    q=query,
                    language=language,
                    sort_by=sort_by,
                    page_size=min(page_size, settings.max_noticias_por_politico),
                    from_param=from_date
                )
            )
            
            if response.get("status") != "ok":
                logger.warning(f"NewsAPI retornou status: {response.get('status')}")
                return []
            
            articles = response.get("articles", [])
            
            for article in articles:
                # Upload da imagem para o Supabase Storage
                imagem_original = article.get("urlToImage")
                imagem_url = imagem_original
                if imagem_original:
                    try:
                        imagem_url = await upload_image_from_url_async(
                            image_url=imagem_original,
                            folder="noticias",
                            fallback_to_original=True
                        )
                    except Exception as e:
                        logger.warning(f"Erro ao fazer upload de imagem de notícia: {e}")
                
                noticia = {
                    "titulo": article.get("title"),
                    "descricao": article.get("description"),
                    "conteudo_completo": article.get("content"),
                    "url": article.get("url"),
                    "fonte_nome": article.get("source", {}).get("name"),
                    "imagem_url": imagem_url,
                    "publicado_em": self._parse_date(article.get("publishedAt")),
                }
                noticias.append(noticia)
                
        except NewsAPIException as e:
            logger.error(f"Erro NewsAPI: {e}")
        except Exception as e:
            logger.error(f"Erro ao buscar notícias NewsAPI para '{query}': {e}")
        
        return noticias
    
    async def buscar_noticias_politico(
        self,
        nome_politico: str,
        cidade: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca notícias específicas de um político.
        
        Args:
            nome_politico: Nome do político
            cidade: Cidade do político (opcional)
            
        Returns:
            Lista de notícias
        """
        query = nome_politico
        if cidade:
            query = f"{nome_politico} {cidade}"
        
        noticias = await self.buscar_noticias(query)
        
        for noticia in noticias:
            noticia["tipo"] = "politico"
        
        return noticias
    
    async def buscar_noticias_cidade(
        self,
        cidade: str,
        estado: str = None
    ) -> List[Dict[str, Any]]:
        """
        Busca notícias de uma cidade.
        
        Args:
            cidade: Nome da cidade
            estado: Sigla do estado
            
        Returns:
            Lista de notícias
        """
        query = cidade
        if estado:
            query = f"{cidade} {estado}"
        
        noticias = await self.buscar_noticias(query)
        
        for noticia in noticias:
            noticia["tipo"] = "cidade"
            noticia["cidade"] = cidade
        
        return noticias
    
    async def buscar_noticias_politicas_brasil(self) -> List[Dict[str, Any]]:
        """
        Busca notícias políticas do Brasil.
        
        Returns:
            Lista de notícias
        """
        noticias = await self.buscar_noticias(
            query="política Brasil OR Congresso OR Câmara OR Senado",
            sort_by="publishedAt",
            page_size=30
        )
        
        for noticia in noticias:
            noticia["tipo"] = "geral"
        
        return noticias
    
    async def buscar_noticias_estado(self, estado: str) -> List[Dict[str, Any]]:
        """
        Busca notícias de um estado específico.
        
        Args:
            estado: Sigla do estado (ex: SP, RJ, MG)
            
        Returns:
            Lista de notícias do estado
        """
        # Mapeamento de siglas para nomes completos
        estados_nomes = {
            "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
            "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
            "GO": "Goiás", "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul",
            "MG": "Minas Gerais", "PA": "Pará", "PB": "Paraíba", "PR": "Paraná",
            "PE": "Pernambuco", "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
            "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina",
            "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins"
        }
        
        nome_estado = estados_nomes.get(estado.upper(), estado)
        
        noticias = await self.buscar_noticias(
            query=f"política {nome_estado} OR governo {nome_estado}",
            sort_by="publishedAt",
            page_size=20
        )
        
        for noticia in noticias:
            noticia["tipo"] = "estado"
            noticia["estado"] = estado
        
        return noticias
    
    async def buscar_top_headlines_brasil(self, categoria: str = "general") -> List[Dict[str, Any]]:
        """
        Busca as principais manchetes do Brasil.
        
        Args:
            categoria: Categoria (business, entertainment, general, health, science, sports, technology)
            
        Returns:
            Lista de notícias
        """
        if not self.is_available:
            return []
        
        noticias = []
        
        try:
            loop = asyncio.get_event_loop()
            
            response = await loop.run_in_executor(
                None,
                lambda: self.client.get_top_headlines(
                    country="br",
                    category=categoria,
                    page_size=20
                )
            )
            
            if response.get("status") != "ok":
                return []
            
            for article in response.get("articles", []):
                # Upload da imagem para o Supabase Storage
                imagem_original = article.get("urlToImage")
                imagem_url = imagem_original
                if imagem_original:
                    try:
                        imagem_url = await upload_image_from_url_async(
                            image_url=imagem_original,
                            folder="noticias",
                            fallback_to_original=True
                        )
                    except Exception as e:
                        logger.warning(f"Erro ao fazer upload de imagem de headline: {e}")
                
                noticia = {
                    "titulo": article.get("title"),
                    "descricao": article.get("description"),
                    "conteudo_completo": article.get("content"),
                    "url": article.get("url"),
                    "fonte_nome": article.get("source", {}).get("name"),
                    "imagem_url": imagem_url,
                    "publicado_em": self._parse_date(article.get("publishedAt")),
                    "tipo": "geral"
                }
                noticias.append(noticia)
                
        except Exception as e:
            logger.error(f"Erro ao buscar top headlines: {e}")
        
        return noticias
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parseia string de data ISO para datetime"""
        if not date_str:
            return None
        
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            return None
