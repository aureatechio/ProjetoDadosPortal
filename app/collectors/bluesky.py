"""
Coletor de menções do BlueSky.
Usa a API pública do BlueSky para buscar posts que mencionam políticos.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class BlueSkyCollector:
    """
    Coleta menções de políticos no BlueSky usando a API pública.
    """
    
    SEARCH_ENDPOINT = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
    
    def __init__(self):
        self.delay = settings.delay_entre_requisicoes
        self.max_results = 100  # Limite da API
    
    async def buscar_mencoes(
        self,
        nome_politico: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Busca posts no BlueSky que mencionam o político.
        
        Args:
            nome_politico: Nome do político para buscar
            limit: Número máximo de resultados
            
        Returns:
            Lista de menções formatadas
        """
        mencoes = []
        
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                params = {
                    "q": nome_politico,
                    "limit": min(limit, self.max_results)
                }
                
                response = await client.get(
                    self.SEARCH_ENDPOINT,
                    params=params,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                    }
                )
                
                if response.status_code != 200:
                    logger.warning(f"BlueSky API retornou status {response.status_code}")
                    return []
                
                data = response.json()
                posts = data.get("posts", [])
                
                for post in posts:
                    mencao = self._parse_post(post, nome_politico)
                    if mencao:
                        mencoes.append(mencao)
                
                logger.info(f"BlueSky: {len(mencoes)} menções encontradas para {nome_politico}")
                
        except httpx.TimeoutException:
            logger.warning(f"Timeout ao buscar menções no BlueSky para {nome_politico}")
        except Exception as e:
            logger.error(f"Erro ao buscar menções no BlueSky: {e}")
        
        return mencoes
    
    def _parse_post(self, post: Dict[str, Any], nome_politico: str) -> Optional[Dict[str, Any]]:
        """
        Converte um post do BlueSky para o formato interno.
        
        Args:
            post: Dados do post da API
            nome_politico: Nome do político (para validação)
            
        Returns:
            Dict formatado ou None se inválido
        """
        try:
            # Extrai dados do autor
            author = post.get("author", {})
            author_handle = author.get("handle", "")
            author_name = author.get("displayName", author_handle)
            
            # Extrai conteúdo do post
            record = post.get("record", {})
            text = record.get("text", "")
            
            if not text:
                return None
            
            # Verifica se realmente menciona o político
            nome_lower = nome_politico.lower()
            if nome_lower not in text.lower():
                # Pode ser um match parcial da busca, verifica partes do nome
                partes_nome = nome_politico.split()
                if len(partes_nome) >= 2:
                    # Verifica se pelo menos primeiro e último nome estão presentes
                    if not (partes_nome[0].lower() in text.lower() or 
                            partes_nome[-1].lower() in text.lower()):
                        return None
            
            # Extrai URI e constrói URL
            uri = post.get("uri", "")
            post_id = uri.split("/")[-1] if uri else ""
            post_url = f"https://bsky.app/profile/{author_handle}/post/{post_id}" if post_id else None
            
            # Extrai métricas de engajamento
            like_count = post.get("likeCount", 0)
            reply_count = post.get("replyCount", 0)
            repost_count = post.get("repostCount", 0)
            
            # Calcula score de engajamento
            engagement_score = (like_count * 1) + (reply_count * 2) + (repost_count * 3)
            
            # Parse da data
            created_at = record.get("createdAt")
            posted_at = None
            if created_at:
                try:
                    posted_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                except Exception:
                    posted_at = datetime.now(timezone.utc)
            
            return {
                "plataforma": "bluesky",
                "mention_id": post_id or uri,
                "autor": author_name,
                "autor_username": author_handle,
                "conteudo": text,
                "url": post_url,
                "likes": like_count,
                "reposts": repost_count,
                "replies": reply_count,
                "engagement_score": engagement_score,
                "posted_at": posted_at,
                "metadata": {
                    "uri": uri,
                    "author_avatar": author.get("avatar"),
                    "author_followers": author.get("followersCount"),
                    "indexed_at": post.get("indexedAt")
                }
            }
            
        except Exception as e:
            logger.warning(f"Erro ao parsear post do BlueSky: {e}")
            return None
    
    async def buscar_mencoes_multiplos(
        self,
        nomes: List[str],
        limit_por_nome: int = 30
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Busca menções para múltiplos políticos.
        
        Args:
            nomes: Lista de nomes de políticos
            limit_por_nome: Limite de resultados por político
            
        Returns:
            Dict com nome -> lista de menções
        """
        resultados = {}
        
        for nome in nomes:
            mencoes = await self.buscar_mencoes(nome, limit_por_nome)
            resultados[nome] = mencoes
            await asyncio.sleep(self.delay)
        
        return resultados


# Instância global
bluesky_collector = BlueSkyCollector()
