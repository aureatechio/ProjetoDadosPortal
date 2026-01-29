"""
Coletor de dados do Instagram usando Instaloader.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import instaloader
from instaloader import Profile, Post
from itertools import islice

from app.config import settings
from app.database import db
from app.utils.storage import upload_image_from_url_async

logger = logging.getLogger(__name__)


class InstagramCollector:
    """
    Coleta posts do Instagram usando Instaloader.
    
    Nota: Pode ter limitações de rate limit. 
    Recomenda-se usar sessão logada para maior estabilidade.
    """
    
    def __init__(self):
        self.loader = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False
        )
        self.delay = settings.delay_instagram
        self._logged_in = False
        
        # Tenta fazer login se credenciais disponíveis
        self._try_login()
    
    def _try_login(self):
        """Tenta fazer login no Instagram se credenciais disponíveis"""
        username = settings.instagram_username
        password = settings.instagram_password
        
        if username and password:
            try:
                self.loader.login(username, password)
                self._logged_in = True
                logger.info("Login no Instagram realizado com sucesso")
            except Exception as e:
                logger.warning(f"Falha no login do Instagram: {e}")
                self._logged_in = False
    
    @property
    def is_logged_in(self) -> bool:
        return self._logged_in
    
    def _calculate_engagement_score(self, likes: int, comments: int) -> float:
        """
        Calcula score de engajamento de um post.
        
        Args:
            likes: Número de curtidas
            comments: Número de comentários
            
        Returns:
            Score de engajamento (comentários valem mais)
        """
        # Comentários indicam maior engajamento que likes
        return likes + (comments * 5)
    
    def _format_for_social_media_table(self, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Formata posts do Instagram para a tabela unificada social_media_posts.
        
        Args:
            posts: Lista de posts no formato do Instagram
            
        Returns:
            Lista de posts formatados para social_media_posts
        """
        formatted = []
        for post in posts:
            formatted.append({
                "politico_id": post.get("politico_id"),
                "plataforma": "instagram",
                "post_id": post.get("post_shortcode"),
                "post_url": post.get("post_url"),
                "conteudo": post.get("caption"),
                "likes": post.get("likes", 0),
                "comments": post.get("comments", 0),
                "shares": 0,  # Instagram não tem compartilhamentos públicos
                "views": 0,
                "engagement_score": post.get("engagement_score", 0),
                "media_type": post.get("media_type", "image"),
                "media_url": post.get("thumbnail_url"),
                "posted_at": post.get("posted_at").isoformat() if post.get("posted_at") else None,
                "metadata": {
                    "source": "instaloader"
                }
            })
        return formatted
    
    async def _get_profile_posts(
        self, 
        username: str, 
        max_posts: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Obtém posts de um perfil do Instagram.
        
        Args:
            username: Nome de usuário do Instagram
            max_posts: Número máximo de posts a coletar
            
        Returns:
            Lista de posts com dados de engajamento
        """
        posts = []
        
        try:
            # Executa em thread separada (instaloader é síncrono)
            loop = asyncio.get_event_loop()
            
            def fetch_posts():
                try:
                    profile = Profile.from_username(self.loader.context, username)
                    profile_posts = []
                    
                    for post in islice(profile.get_posts(), max_posts):
                        post_data = {
                            "post_shortcode": post.shortcode,
                            "post_url": f"https://www.instagram.com/p/{post.shortcode}/",
                            "caption": post.caption[:1000] if post.caption else None,
                            "likes": post.likes,
                            "comments": post.comments,
                            "engagement_score": self._calculate_engagement_score(post.likes, post.comments),
                            "media_type": "video" if post.is_video else "image",
                            "thumbnail_url": post.url,
                            "posted_at": post.date_utc.replace(tzinfo=timezone.utc) if post.date_utc else None
                        }
                        profile_posts.append(post_data)
                    
                    return profile_posts
                except instaloader.exceptions.ProfileNotExistsException:
                    logger.warning(f"Perfil não existe: {username}")
                    return []
                except instaloader.exceptions.PrivateProfileNotFollowedException:
                    logger.warning(f"Perfil privado: {username}")
                    return []
                except Exception as e:
                    logger.error(f"Erro ao coletar posts de {username}: {e}")
                    return []
            
            posts = await loop.run_in_executor(None, fetch_posts)
            
        except Exception as e:
            logger.error(f"Erro ao obter posts de {username}: {e}")
        
        return posts
    
    async def coletar_posts_politico(
        self,
        politico_id: int,
        instagram_username: str,
        top_n: int = None
    ) -> List[Dict[str, Any]]:
        """
        Coleta os posts mais engajados de um político.
        
        Args:
            politico_id: ID do político no banco
            instagram_username: Username do Instagram
            top_n: Número de top posts a retornar (default: config)
            
        Returns:
            Lista de posts ordenados por engajamento
        """
        if not instagram_username:
            return []
        
        # Remove @ se presente
        username = instagram_username.lstrip("@")
        
        logger.info(f"Coletando Instagram de: {username}")
        
        top_n = top_n or settings.max_posts_instagram
        
        # Coleta mais posts do que o necessário para ter margem de ordenação
        posts = await self._get_profile_posts(username, max_posts=top_n * 3)
        
        if not posts:
            return []
        
        # Ordena por engajamento e pega os top N
        posts_sorted = sorted(
            posts, 
            key=lambda p: p.get("engagement_score", 0), 
            reverse=True
        )[:top_n]
        
        # Upload das imagens para o Supabase Storage
        for post in posts_sorted:
            post["politico_id"] = politico_id
            
            # Upload do thumbnail para o storage
            thumbnail_original = post.get("thumbnail_url")
            if thumbnail_original:
                try:
                    shortcode = post.get("post_shortcode", "")
                    storage_url = await upload_image_from_url_async(
                        image_url=thumbnail_original,
                        folder="instagram",
                        filename=f"post_{shortcode}" if shortcode else None,
                        fallback_to_original=True
                    )
                    post["thumbnail_url"] = storage_url
                except Exception as e:
                    logger.warning(f"Erro ao fazer upload de thumbnail do Instagram: {e}")
        
        logger.info(f"Coletados {len(posts_sorted)} posts de {username}")
        
        return posts_sorted
    
    async def executar_coleta_completa(self) -> Dict[str, int]:
        """
        Executa coleta de Instagram para todos os políticos com username configurado.
        
        Returns:
            Dict com estatísticas da coleta
        """
        stats = {
            "politicos_processados": 0,
            "posts_coletados": 0,
            "erros": 0
        }
        
        try:
            # Obtém políticos com usar_diretoriaja = True
            politicos = db.get_politicos_diretoriaja()
            logger.info(f"Coletando Instagram para {len(politicos)} políticos (usar_diretoriaja=True)")
            
            for politico in politicos:
                instagram_username = politico.get("instagram_username")
                
                if not instagram_username:
                    continue
                
                try:
                    posts = await self.coletar_posts_politico(
                        politico["id"],
                        instagram_username
                    )
                    
                    if posts:
                        # Formata para tabela unificada social_media_posts
                        posts_formatados = self._format_for_social_media_table(posts)
                        inserted = db.insert_social_media_posts_batch(posts_formatados)
                        stats["posts_coletados"] += inserted
                    
                    stats["politicos_processados"] += 1
                    
                    # Delay entre perfis para evitar rate limit
                    await asyncio.sleep(self.delay)
                    
                except Exception as e:
                    logger.error(f"Erro ao coletar Instagram de {instagram_username}: {e}")
                    stats["erros"] += 1
            
        except Exception as e:
            logger.error(f"Erro na coleta completa de Instagram: {e}")
            stats["erros"] += 1
        
        logger.info(f"Coleta Instagram finalizada: {stats}")
        return stats


# Instância global do coletor
instagram_collector = InstagramCollector()
