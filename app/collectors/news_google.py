"""
Coletor de notícias do Google News.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from gnews import GNews
from newspaper import Article, ArticleException
import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class GoogleNewsCollector:
    """
    Coleta notícias do Google News.
    Usa a biblioteca gnews para busca e newspaper3k para extração de conteúdo.
    """
    
    def __init__(self, language: str = "pt", country: str = "BR"):
        self.gnews = GNews(
            language=language,
            country=country,
            max_results=settings.max_noticias_por_politico
        )
        self.delay = settings.delay_entre_requisicoes
    
    async def _extract_article_content(self, url: str) -> Dict[str, Any]:
        """
        Extrai o conteúdo completo de um artigo usando newspaper3k.
        
        Args:
            url: URL do artigo
            
        Returns:
            Dict com titulo, descricao, conteudo_completo, imagem_url
        """
        try:
            article = Article(url)
            article.download()
            article.parse()
            
            return {
                "titulo": article.title,
                "descricao": article.meta_description or article.text[:300] if article.text else None,
                "conteudo_completo": article.text,
                "imagem_url": article.top_image,
                "publicado_em": article.publish_date
            }
        except ArticleException as e:
            logger.warning(f"Erro ao extrair artigo {url}: {e}")
            return {}
        except Exception as e:
            logger.warning(f"Erro inesperado ao extrair artigo {url}: {e}")
            return {}
    
    async def buscar_noticias(
        self, 
        query: str,
        extrair_conteudo: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Busca notícias no Google News.
        
        Args:
            query: Termo de busca (nome do político, cidade, etc)
            extrair_conteudo: Se deve extrair o conteúdo completo dos artigos
            
        Returns:
            Lista de notícias com dados extraídos
        """
        noticias = []
        
        try:
            # Busca no Google News (síncrono, executado em thread)
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None, 
                self.gnews.get_news, 
                query
            )
            
            if not results:
                logger.info(f"Nenhuma notícia encontrada para: {query}")
                return []
            
            for item in results:
                noticia = {
                    "titulo": item.get("title"),
                    "descricao": item.get("description"),
                    "url": item.get("url"),
                    "fonte_nome": item.get("publisher", {}).get("title"),
                    "publicado_em": self._parse_date(item.get("published date")),
                }
                
                # Extrai conteúdo completo se solicitado
                if extrair_conteudo and noticia["url"]:
                    await asyncio.sleep(self.delay)  # Rate limiting
                    conteudo = await self._extract_article_content(noticia["url"])
                    
                    # Mescla dados extraídos (preferência para dados do artigo)
                    if conteudo.get("conteudo_completo"):
                        noticia["conteudo_completo"] = conteudo["conteudo_completo"]
                    if conteudo.get("imagem_url"):
                        noticia["imagem_url"] = conteudo["imagem_url"]
                    if conteudo.get("publicado_em") and not noticia.get("publicado_em"):
                        noticia["publicado_em"] = conteudo["publicado_em"]
                
                noticias.append(noticia)
                
        except Exception as e:
            logger.error(f"Erro ao buscar notícias para '{query}': {e}")
        
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
            cidade: Cidade do político (opcional, para refinar busca)
            
        Returns:
            Lista de notícias
        """
        # Query principal: nome do político
        query = nome_politico
        
        # Adiciona cidade para refinar se disponível
        if cidade:
            query = f"{nome_politico} {cidade}"
        
        noticias = await self.buscar_noticias(query)
        
        # Marca como notícia do tipo 'politico'
        for noticia in noticias:
            noticia["tipo"] = "politico"
        
        return noticias
    
    async def buscar_noticias_cidade(self, cidade: str, estado: str = None) -> List[Dict[str, Any]]:
        """
        Busca notícias de uma cidade.
        
        Args:
            cidade: Nome da cidade
            estado: Sigla do estado (opcional)
            
        Returns:
            Lista de notícias
        """
        query = cidade
        if estado:
            query = f"{cidade} {estado}"
        
        noticias = await self.buscar_noticias(query)
        
        # Marca como notícia do tipo 'cidade'
        for noticia in noticias:
            noticia["tipo"] = "cidade"
            noticia["cidade"] = cidade
        
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
        
        queries = [
            f"política {nome_estado}",
            f"governo {nome_estado}",
            f"assembleia legislativa {estado}"
        ]
        
        todas_noticias = []
        
        for query in queries:
            noticias = await self.buscar_noticias(query, extrair_conteudo=False)
            for noticia in noticias:
                noticia["tipo"] = "estado"
                noticia["estado"] = estado
            todas_noticias.extend(noticias)
            await asyncio.sleep(self.delay)
        
        # Remove duplicatas por URL
        urls_vistas = set()
        noticias_unicas = []
        for noticia in todas_noticias:
            if noticia["url"] not in urls_vistas:
                urls_vistas.add(noticia["url"])
                noticias_unicas.append(noticia)
        
        return noticias_unicas
    
    async def buscar_noticias_politicas_gerais(self) -> List[Dict[str, Any]]:
        """
        Busca notícias políticas gerais do Brasil.
        
        Returns:
            Lista de notícias
        """
        queries = [
            "política Brasil",
            "Congresso Nacional",
            "Câmara dos Deputados",
            "Senado Federal"
        ]
        
        todas_noticias = []
        
        for query in queries:
            noticias = await self.buscar_noticias(query, extrair_conteudo=False)
            for noticia in noticias:
                noticia["tipo"] = "geral"
            todas_noticias.extend(noticias)
            await asyncio.sleep(self.delay)
        
        # Remove duplicatas por URL
        urls_vistas = set()
        noticias_unicas = []
        for noticia in todas_noticias:
            if noticia["url"] not in urls_vistas:
                urls_vistas.add(noticia["url"])
                noticias_unicas.append(noticia)
        
        return noticias_unicas
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Tenta parsear string de data em datetime"""
        if not date_str:
            return None
        
        try:
            # Google News geralmente retorna formato como "Wed, 15 Jan 2025 10:30:00 GMT"
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(date_str)
        except Exception:
            try:
                # Tenta formato ISO
                return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except Exception:
                return None
