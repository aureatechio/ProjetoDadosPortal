"""
Cliente Supabase para operações no banco de dados.
"""
from supabase import create_client, Client
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class Database:
    """Classe para gerenciar conexão e operações com Supabase"""
    
    def __init__(self):
        self.client: Client = create_client(
            settings.supabase_url,
            settings.supabase_key
        )
    
    # ==================== POLÍTICOS ====================
    
    def get_politicos_ativos(self) -> List[Dict[str, Any]]:
        """Retorna todos os políticos ativos com dados de redes sociais"""
        response = self.client.table("politico").select("*").eq("active", True).execute()
        return response.data
    
    def get_politico_by_id(self, politico_id: int) -> Optional[Dict[str, Any]]:
        """Retorna um político pelo ID"""
        response = self.client.table("politico").select("*").eq("id", politico_id).single().execute()
        return response.data

    def update_politico_socials(
        self,
        politico_id: int,
        instagram_username: Optional[str] = None,
        twitter_username: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Atualiza campos de redes sociais de um político.

        Observação: atualiza somente os campos passados (não-None).
        """
        payload: Dict[str, Any] = {}
        if instagram_username is not None:
            payload["instagram_username"] = instagram_username
        if twitter_username is not None:
            payload["twitter_username"] = twitter_username

        if not payload:
            return None

        try:
            response = (
                self.client.table("politico")
                .update(payload)
                .eq("id", politico_id)
                .execute()
            )
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"Erro ao atualizar redes sociais do politico {politico_id}: {e}")
            return None
    
    def get_concorrentes(self, politico_id: int) -> List[Dict[str, Any]]:
        """Retorna os concorrentes de um político"""
        response = self.client.table("politico_concorrentes")\
            .select("concorrente_id, politico!politico_concorrentes_concorrente_id_fkey(*)")\
            .eq("politico_id", politico_id)\
            .execute()
        return [item["politico"] for item in response.data if item.get("politico")]
    
    # ==================== NOTÍCIAS ====================
    
    def insert_noticia(self, noticia: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Insere uma notícia no banco (ignora se URL já existe)"""
        try:
            response = self.client.table("noticias").upsert(
                noticia,
                on_conflict="url"
            ).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"Erro ao inserir notícia: {e}")
            return None
    
    def insert_noticias_batch(self, noticias: List[Dict[str, Any]]) -> int:
        """Insere múltiplas notícias de uma vez"""
        if not noticias:
            return 0
        try:
            response = self.client.table("noticias").upsert(
                noticias,
                on_conflict="url"
            ).execute()
            return len(response.data) if response.data else 0
        except Exception as e:
            logger.error(f"Erro ao inserir notícias em batch: {e}")
            return 0

    def get_noticia_by_id(self, noticia_id: str) -> Optional[Dict[str, Any]]:
        """Retorna uma notícia pelo ID (UUID)"""
        try:
            response = self.client.table("noticias").select("*").eq("id", noticia_id).single().execute()
            return response.data
        except Exception as e:
            logger.error(f"Erro ao buscar notícia por id {noticia_id}: {e}")
            return None
    
    def get_noticias_politico(
        self, 
        politico_id: int, 
        limit: int = 20,
        min_score: float = 0
    ) -> List[Dict[str, Any]]:
        """Retorna notícias de um político ordenadas por relevância"""
        response = self.client.table("noticias")\
            .select("*")\
            .eq("politico_id", politico_id)\
            .gte("relevancia_total", min_score)\
            .order("relevancia_total", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def get_noticias_cidade(
        self, 
        cidade: str, 
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Retorna notícias de uma cidade ordenadas por relevância"""
        response = self.client.table("noticias")\
            .select("*")\
            .eq("cidade", cidade)\
            .order("relevancia_total", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def get_noticias_gerais(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Retorna notícias políticas gerais ordenadas por relevância"""
        response = self.client.table("noticias")\
            .select("*")\
            .eq("tipo", "geral")\
            .order("relevancia_total", desc=True)\
            .limit(limit)\
            .execute()
        return response.data

    def get_noticias_estado(
        self,
        estado: str,
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """Retorna notícias de um estado ordenadas por relevância"""
        response = self.client.table("noticias")\
            .select("*")\
            .eq("tipo", "estado")\
            .eq("estado", estado)\
            .order("relevancia_total", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def limpar_noticias_antigas(self, dias: int = 7) -> int:
        """Remove notícias mais antigas que X dias"""
        data_limite = datetime.utcnow() - timedelta(days=dias)
        response = self.client.table("noticias")\
            .delete()\
            .lt("coletado_em", data_limite.isoformat())\
            .execute()
        return len(response.data) if response.data else 0
    
    # ==================== INSTAGRAM ====================
    
    def insert_instagram_post(self, post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Insere um post do Instagram"""
        try:
            response = self.client.table("instagram_posts").upsert(
                post,
                on_conflict="post_shortcode"
            ).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"Erro ao inserir post Instagram: {e}")
            return None
    
    def insert_instagram_posts_batch(self, posts: List[Dict[str, Any]]) -> int:
        """Insere múltiplos posts do Instagram"""
        if not posts:
            return 0
        try:
            response = self.client.table("instagram_posts").upsert(
                posts,
                on_conflict="post_shortcode"
            ).execute()
            return len(response.data) if response.data else 0
        except Exception as e:
            logger.error(f"Erro ao inserir posts Instagram em batch: {e}")
            return 0
    
    def get_instagram_posts(
        self, 
        politico_id: int, 
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Retorna posts do Instagram ordenados por engajamento"""
        response = self.client.table("instagram_posts")\
            .select("*")\
            .eq("politico_id", politico_id)\
            .order("engagement_score", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def limpar_instagram_antigos(self, dias: int = 30) -> int:
        """Remove posts do Instagram mais antigos que X dias"""
        data_limite = datetime.utcnow() - timedelta(days=dias)
        response = self.client.table("instagram_posts")\
            .delete()\
            .lt("collected_at", data_limite.isoformat())\
            .execute()
        return len(response.data) if response.data else 0
    
    # ==================== REDES SOCIAIS (UNIFICADO) ====================
    
    def insert_social_media_post(self, post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Insere um post de rede social na tabela unificada"""
        try:
            response = self.client.table("social_media_posts").upsert(
                post,
                on_conflict="plataforma,post_id"
            ).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"Erro ao inserir post social: {e}")
            return None
    
    def insert_social_media_posts_batch(self, posts: List[Dict[str, Any]]) -> int:
        """Insere múltiplos posts de redes sociais"""
        if not posts:
            return 0
        try:
            response = self.client.table("social_media_posts").upsert(
                posts,
                on_conflict="plataforma,post_id"
            ).execute()
            return len(response.data) if response.data else 0
        except Exception as e:
            logger.error(f"Erro ao inserir posts sociais em batch: {e}")
            return 0
    
    def get_social_media_posts(
        self, 
        politico_id: int, 
        plataforma: str = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Retorna posts de redes sociais ordenados por engajamento"""
        query = self.client.table("social_media_posts")\
            .select("*")\
            .eq("politico_id", politico_id)
        
        if plataforma:
            query = query.eq("plataforma", plataforma)
        
        response = query\
            .order("engagement_score", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def limpar_social_posts_antigos(self, dias: int = 30) -> int:
        """Remove posts de redes sociais mais antigos que X dias"""
        data_limite = datetime.utcnow() - timedelta(days=dias)
        response = self.client.table("social_media_posts")\
            .delete()\
            .lt("collected_at", data_limite.isoformat())\
            .execute()
        return len(response.data) if response.data else 0
    
    # ==================== FONTES ====================
    
    def get_fontes_ativas(self) -> List[Dict[str, Any]]:
        """Retorna todas as fontes de notícias ativas"""
        response = self.client.table("fontes_noticias")\
            .select("*")\
            .eq("ativo", True)\
            .execute()
        return response.data
    
    def get_fonte_by_dominio(self, dominio: str) -> Optional[Dict[str, Any]]:
        """Busca fonte pelo domínio"""
        response = self.client.table("fontes_noticias")\
            .select("*")\
            .eq("dominio", dominio)\
            .single()\
            .execute()
        return response.data
    
    def update_fonte_peso(self, fonte_id: str, novo_peso: float) -> bool:
        """Atualiza o peso de confiabilidade de uma fonte"""
        try:
            self.client.table("fontes_noticias")\
                .update({"peso_confiabilidade": novo_peso})\
                .eq("id", fonte_id)\
                .execute()
            return True
        except Exception as e:
            logger.error(f"Erro ao atualizar peso da fonte: {e}")
            return False
    
    # ==================== TRENDING ====================
    
    def update_trending_topics(self, topics: List[Dict[str, Any]], category: str = "politica") -> int:
        """
        Atualiza os trending topics de uma categoria específica (substitui todos da categoria).
        
        Args:
            topics: Lista de trending topics
            category: Categoria dos topics ('politica' ou 'geral')
        """
        try:
            # Limpa os topics existentes da categoria
            self.client.table("portal_trending_topics")\
                .delete()\
                .eq("category", category)\
                .execute()
            
            # Adiciona category a cada topic e insere os novos
            if topics:
                for topic in topics:
                    topic["category"] = category
                response = self.client.table("portal_trending_topics").insert(topics).execute()
                return len(response.data) if response.data else 0
            return 0
        except Exception as e:
            logger.error(f"Erro ao atualizar trending topics ({category}): {e}")
            return 0
    
    def get_trending_topics(self, category: str = None) -> List[Dict[str, Any]]:
        """
        Retorna os trending topics ordenados por rank.
        
        Args:
            category: Filtro por categoria ('politica', 'geral', ou None para todos)
        """
        query = self.client.table("portal_trending_topics").select("*")
        
        if category:
            query = query.eq("category", category)
        
        response = query.order("category").order("rank", desc=False).execute()
        return response.data
    
    # ==================== LOGS ====================
    
    def log_coleta_inicio(self, tipo_coleta: str) -> str:
        """Registra início de uma coleta e retorna o ID do log"""
        response = self.client.table("coleta_logs").insert({
            "tipo_coleta": tipo_coleta,
            "status": "iniciado",
            "iniciado_em": datetime.utcnow().isoformat()
        }).execute()
        return response.data[0]["id"] if response.data else None
    
    def log_coleta_fim(
        self, 
        log_id: str, 
        status: str, 
        mensagem: str = None,
        registros: int = 0
    ):
        """Finaliza o log de uma coleta"""
        self.client.table("coleta_logs").update({
            "status": status,
            "mensagem": mensagem,
            "registros_coletados": registros,
            "finalizado_em": datetime.utcnow().isoformat()
        }).eq("id", log_id).execute()
    
    def get_logs_coleta(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retorna os logs de coleta mais recentes"""
        response = self.client.table("coleta_logs")\
            .select("*")\
            .order("iniciado_em", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    # ==================== SOCIAL MENTIONS ====================
    
    def insert_social_mention(self, mention: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Insere uma menção social no banco (ignora se já existe)"""
        try:
            response = self.client.table("social_mentions").upsert(
                mention,
                on_conflict="plataforma,mention_id"
            ).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"Erro ao inserir menção social: {e}")
            return None
    
    def insert_social_mentions_batch(self, mentions: List[Dict[str, Any]]) -> int:
        """Insere múltiplas menções sociais de uma vez"""
        if not mentions:
            return 0
        try:
            response = self.client.table("social_mentions").upsert(
                mentions,
                on_conflict="plataforma,mention_id"
            ).execute()
            return len(response.data) if response.data else 0
        except Exception as e:
            logger.error(f"Erro ao inserir menções sociais em batch: {e}")
            return 0
    
    def get_social_mentions_politico(
        self, 
        politico_id: int, 
        plataforma: str = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Retorna menções sociais de um político ordenadas por engajamento"""
        query = self.client.table("social_mentions")\
            .select("*")\
            .eq("politico_id", politico_id)
        
        if plataforma:
            query = query.eq("plataforma", plataforma)
        
        response = query\
            .order("engagement_score", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def get_social_mentions_by_assunto(
        self,
        politico_id: int,
        assunto: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Retorna menções sociais de um político filtradas por assunto"""
        response = self.client.table("social_mentions")\
            .select("*")\
            .eq("politico_id", politico_id)\
            .eq("assunto", assunto)\
            .order("posted_at", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def get_social_mentions_by_periodo(
        self,
        politico_id: int,
        inicio: datetime,
        fim: datetime
    ) -> List[Dict[str, Any]]:
        """Retorna menções sociais de um político em um período específico"""
        response = self.client.table("social_mentions")\
            .select("*")\
            .eq("politico_id", politico_id)\
            .gte("collected_at", inicio.isoformat())\
            .lte("collected_at", fim.isoformat())\
            .execute()
        return response.data
    
    def limpar_social_mentions_antigas(self, dias: int = 30) -> int:
        """Remove menções sociais mais antigas que X dias"""
        data_limite = datetime.utcnow() - timedelta(days=dias)
        response = self.client.table("social_mentions")\
            .delete()\
            .lt("collected_at", data_limite.isoformat())\
            .execute()
        return len(response.data) if response.data else 0
    
    # ==================== MENTION TOPICS (AGREGAÇÃO) ====================
    
    def upsert_mention_topic(self, topic: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Insere ou atualiza um tópico agregado de menções"""
        try:
            # Adiciona timestamp de atualização
            topic["atualizado_em"] = datetime.utcnow().isoformat()
            
            response = self.client.table("mention_topics").upsert(
                topic,
                on_conflict="politico_id,assunto,periodo_inicio"
            ).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"Erro ao inserir/atualizar tópico de menção: {e}")
            return None
    
    def get_mention_topics_politico(
        self, 
        politico_id: int
    ) -> List[Dict[str, Any]]:
        """Retorna todos os tópicos de menções de um político"""
        response = self.client.table("mention_topics")\
            .select("*")\
            .eq("politico_id", politico_id)\
            .order("total_mencoes", desc=True)\
            .execute()
        return response.data
    
    def get_top_assuntos_politico(
        self,
        politico_id: int,
        limite: int = 10
    ) -> List[Dict[str, Any]]:
        """Retorna os principais assuntos discutidos sobre um político"""
        response = self.client.table("mention_topics")\
            .select("*")\
            .eq("politico_id", politico_id)\
            .order("total_mencoes", desc=True)\
            .limit(limite)\
            .execute()
        return response.data
    
    def limpar_mention_topics_antigos(self, dias: int = 30) -> int:
        """Remove agregações de tópicos mais antigas que X dias"""
        data_limite = datetime.utcnow() - timedelta(days=dias)
        response = self.client.table("mention_topics")\
            .delete()\
            .lt("periodo_fim", data_limite.isoformat())\
            .execute()
        return len(response.data) if response.data else 0


# Instância global do banco
db = Database()


def get_supabase() -> Client:
    """Retorna o cliente Supabase para uso em outros módulos."""
    return db.client
