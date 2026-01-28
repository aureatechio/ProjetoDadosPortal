"""
Cliente Supabase para operações no banco de dados.
"""
from supabase import create_client, Client
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone
import logging
from postgrest.types import CountMethod

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
        """Retorna todos os políticos ativos."""
        # PostgREST costuma impor um limite padrão (~1000 linhas). Paginar garante
        # que a API retorne todos os políticos.
        page_size = 1000
        offset = 0
        rows: List[Dict[str, Any]] = []

        while True:
            query = (
                self.client.table("politico")
                .select("*")
                .eq("active", True)
            )
            response = (
                query.order("id", desc=False)
                .limit(page_size)
                .offset(offset)
                .execute()
            )
            batch = response.data or []
            rows.extend(batch)

            if len(batch) < page_size:
                break
            offset += page_size

        return rows

    def get_politicos_diretoriaja(self) -> List[Dict[str, Any]]:
        """Retorna políticos com usar_diretoriaja = true (sem filtrar por active)."""
        page_size = 1000
        offset = 0
        rows: List[Dict[str, Any]] = []

        while True:
            response = (
                self.client.table("politico")
                .select("*")
                .eq("usar_diretoriaja", True)
                .order("id", desc=False)
                .limit(page_size)
                .offset(offset)
                .execute()
            )
            batch = response.data or []
            rows.extend(batch)

            if len(batch) < page_size:
                break
            offset += page_size

        return rows
    
    def get_politico_by_id(self, politico_id: int) -> Optional[Dict[str, Any]]:
        """Retorna um político pelo ID"""
        response = self.client.table("politico").select("*").eq("id", politico_id).single().execute()
        return response.data
    
    def get_politico_uuid(self, politico_id: int) -> Optional[str]:
        """Retorna o UUID de um político dado o ID inteiro"""
        politico = self.get_politico_by_id(politico_id)
        return politico.get("uuid") if politico else None

    # ==================== HELPERS ====================

    def _safe_count(self, response: Any) -> int:
        """
        Extrai o campo `count` de uma resposta do PostgREST/supabase-py.
        """
        try:
            count = getattr(response, "count", None)
            return int(count or 0)
        except Exception:
            return 0

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

    def get_concorrentes_twitter_insights(
        self,
        politico_id: int,
        *,
        days_back: int = 7,
        limit_top_mentions: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Retorna insights de Twitter/X para os concorrentes de um político:
        - followers_count (snapshot mais recente em concorrente_twitter_insights, se existir)
        - top 3 menções mais engajadas (preferindo snapshot; fallback em social_mentions)

        Observações:
        - social_mentions usa politico_id como UUID (string) no Supabase.
        - o snapshot diário é mantido por scripts em scripts/collect_concorrentes_twitter_insights.py
        """
        concorrentes = self.get_concorrentes(politico_id)
        if not concorrentes:
            return []

        cutoff = (datetime.now(timezone.utc) - timedelta(days=int(days_back))).isoformat()
        out: List[Dict[str, Any]] = []

        for c in concorrentes:
            cuuid = (c.get("uuid") or "").strip()
            if not cuuid:
                continue

            # 1) Tenta usar snapshot (se existir)
            snapshot = None
            try:
                snap_resp = (
                    self.client.table("concorrente_twitter_insights")
                    .select("followers_count,top_mentions,computed_at,computed_date,mentions_window_days,twitter_username")
                    .eq("concorrente_politico_id", cuuid)
                    .eq("mentions_window_days", int(days_back))
                    .order("computed_at", desc=True)
                    .limit(1)
                    .execute()
                )
                rows = snap_resp.data or []
                snapshot = rows[0] if rows else None
            except Exception:
                snapshot = None

            followers_count = snapshot.get("followers_count") if isinstance(snapshot, dict) else None
            top_mentions = snapshot.get("top_mentions") if isinstance(snapshot, dict) else None
            snapshot_at = snapshot.get("computed_at") if isinstance(snapshot, dict) else None

            # 2) Fallback: calcula top mentions direto de social_mentions
            if not isinstance(top_mentions, list) or len(top_mentions) == 0:
                try:
                    mentions_resp = (
                        self.client.table("social_mentions")
                        .select("*")
                        .eq("politico_id", cuuid)
                        .eq("plataforma", "twitter")
                        .gte("collected_at", cutoff)
                        .order("engagement_score", desc=True)
                        .limit(int(limit_top_mentions))
                        .execute()
                    )
                    top_mentions = mentions_resp.data or []
                except Exception:
                    top_mentions = []

            out.append(
                {
                    "politico": c,
                    "followers_count": followers_count,
                    "top_mentions": top_mentions[: int(limit_top_mentions)] if isinstance(top_mentions, list) else [],
                    "snapshot_at": snapshot_at,
                    "days_back": int(days_back),
                }
            )

        return out
    
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
        min_score: float = 0,
        diversificar_fontes: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Retorna notícias de um político ordenadas por relevância.
        
        Args:
            politico_id: ID do político
            limit: Número máximo de notícias
            min_score: Score mínimo de relevância
            diversificar_fontes: Se True, diversifica as notícias por fonte/canal
        """
        # Converte ID inteiro para UUID
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return []
        
        # Busca mais notícias para poder diversificar
        fetch_limit = limit * 5 if diversificar_fontes else limit
        
        response = self.client.table("noticias")\
            .select("*")\
            .eq("politico_id", politico_uuid)\
            .gte("relevancia_total", min_score)\
            .order("relevancia_total", desc=True)\
            .limit(fetch_limit)\
            .execute()
        
        noticias = response.data or []
        
        if not diversificar_fontes or len(noticias) <= limit:
            return noticias[:limit]
        
        # Diversifica as notícias por fonte
        return self._diversificar_noticias_por_fonte(noticias, limit)

    def count_noticias_politico(self, politico_id: int, min_score: float = 0) -> int:
        """
        Retorna a contagem total de notícias de um político (sem carregar os registros).
        """
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return 0
        try:
            response = (
                self.client.table("noticias")
                .select("id", count=CountMethod.exact)
                .eq("politico_id", politico_uuid)
                .gte("relevancia_total", min_score)
                .execute()
            )
            return self._safe_count(response)
        except Exception as e:
            logger.error(f"Erro ao contar noticias do politico {politico_id}: {e}")
            return 0
    
    def _diversificar_noticias_por_fonte(
        self, 
        noticias: List[Dict[str, Any]], 
        limit: int
    ) -> List[Dict[str, Any]]:
        """
        Diversifica uma lista de notícias para incluir diferentes fontes/canais.
        
        Utiliza um algoritmo round-robin que alterna entre fontes diferentes,
        mantendo a ordenação por relevância dentro de cada fonte.
        """
        if not noticias:
            return []
        
        # Agrupa notícias por fonte
        por_fonte: Dict[str, List[Dict[str, Any]]] = {}
        for noticia in noticias:
            fonte = noticia.get("fonte_nome") or noticia.get("fonte_id") or "desconhecida"
            if fonte not in por_fonte:
                por_fonte[fonte] = []
            por_fonte[fonte].append(noticia)
        
        # Se só tem uma fonte, retorna ordenado por relevância
        if len(por_fonte) <= 1:
            return noticias[:limit]
        
        # Ordena fontes pelo melhor score de cada uma (para priorizar fontes relevantes)
        fontes_ordenadas = sorted(
            por_fonte.keys(),
            key=lambda f: max((n.get("relevancia_total") or 0) for n in por_fonte[f]),
            reverse=True
        )
        
        # Alterna entre fontes usando round-robin
        resultado: List[Dict[str, Any]] = []
        urls_vistas = set()  # Evita duplicatas
        indices = {fonte: 0 for fonte in fontes_ordenadas}
        
        while len(resultado) < limit:
            adicionou_alguma = False
            
            for fonte in fontes_ordenadas:
                if len(resultado) >= limit:
                    break
                
                lista_fonte = por_fonte[fonte]
                idx = indices[fonte]
                
                # Encontra a próxima notícia válida desta fonte
                while idx < len(lista_fonte):
                    noticia = lista_fonte[idx]
                    url = noticia.get("url")
                    
                    if url not in urls_vistas:
                        urls_vistas.add(url)
                        resultado.append(noticia)
                        indices[fonte] = idx + 1
                        adicionou_alguma = True
                        break
                    
                    idx += 1
                    indices[fonte] = idx
            
            # Se não conseguiu adicionar nenhuma notícia, para o loop
            if not adicionou_alguma:
                break
        
        return resultado
    
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
    
    def get_noticias_capital(
        self,
        estado: str,
        limit: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Retorna notícias da capital/cidade de um estado ordenadas por relevância.
        (tipo='cidade' com cidade preenchida)
        
        Args:
            estado: Sigla do estado (ex: SP, RJ, MG)
            limit: Número máximo de notícias (padrão: 3)
            
        Returns:
            Lista de notícias da capital/cidade
        """
        response = self.client.table("noticias")\
            .select("*")\
            .eq("tipo", "cidade")\
            .eq("estado", estado)\
            .not_.is_("cidade", "null")\
            .order("relevancia_total", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def get_noticias_nivel_estado(
        self,
        estado: str,
        limit: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Retorna notícias a nível de estado ordenadas por relevância.
        (tipo='estado' sem cidade - governo, assembleia, política estadual)
        
        Args:
            estado: Sigla do estado (ex: SP, RJ, MG)
            limit: Número máximo de notícias (padrão: 3)
            
        Returns:
            Lista de notícias do estado
        """
        response = self.client.table("noticias")\
            .select("*")\
            .eq("tipo", "estado")\
            .eq("estado", estado)\
            .is_("cidade", "null")\
            .order("relevancia_total", desc=True)\
            .limit(limit)\
            .execute()
        return response.data
    
    def get_noticias_todas_capitais(
        self,
        limit_por_capital: int = 3
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Retorna notícias de todas as capitais/cidades agrupadas por estado.
        (tipo='cidade')
        
        Args:
            limit_por_capital: Número máximo de notícias por capital (padrão: 3)
            
        Returns:
            Dict com estado como chave e lista de notícias como valor
        """
        # Busca todas as notícias de cidades
        response = self.client.table("noticias")\
            .select("*")\
            .eq("tipo", "cidade")\
            .not_.is_("cidade", "null")\
            .order("relevancia_total", desc=True)\
            .execute()
        
        # Agrupa por estado e limita
        noticias_por_estado: Dict[str, List[Dict[str, Any]]] = {}
        
        for noticia in response.data:
            estado = noticia.get("estado")
            if estado:
                if estado not in noticias_por_estado:
                    noticias_por_estado[estado] = []
                if len(noticias_por_estado[estado]) < limit_por_capital:
                    noticias_por_estado[estado].append(noticia)
        
        return noticias_por_estado
    
    def get_noticias_todos_estados(
        self,
        limit_por_estado: int = 3
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Retorna notícias a nível de estado agrupadas por estado.
        (tipo='estado' sem cidade)
        
        Args:
            limit_por_estado: Número máximo de notícias por estado (padrão: 3)
            
        Returns:
            Dict com estado como chave e lista de notícias como valor
        """
        # Busca todas as notícias de estados
        response = self.client.table("noticias")\
            .select("*")\
            .eq("tipo", "estado")\
            .is_("cidade", "null")\
            .order("relevancia_total", desc=True)\
            .execute()
        
        # Agrupa por estado e limita
        noticias_por_estado: Dict[str, List[Dict[str, Any]]] = {}
        
        for noticia in response.data:
            estado = noticia.get("estado")
            if estado:
                if estado not in noticias_por_estado:
                    noticias_por_estado[estado] = []
                if len(noticias_por_estado[estado]) < limit_por_estado:
                    noticias_por_estado[estado].append(noticia)
        
        return noticias_por_estado
    
    def get_estados_com_noticias(self) -> List[str]:
        """
        Retorna lista de estados que possuem notícias coletadas.
        
        Returns:
            Lista de siglas de estados
        """
        response = self.client.table("noticias")\
            .select("estado")\
            .in_("tipo", ["estado", "cidade"])\
            .not_.is_("estado", "null")\
            .execute()
        
        # Extrai estados únicos
        estados = set()
        for row in response.data:
            if row.get("estado"):
                estados.add(row["estado"])
        
        return sorted(list(estados))
    
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
        # Converte ID inteiro para UUID
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return []
        
        response = self.client.table("instagram_posts")\
            .select("*")\
            .eq("politico_id", politico_uuid)\
            .order("engagement_score", desc=True)\
            .limit(limit)\
            .execute()
        return response.data

    def count_instagram_posts(self, politico_id: int) -> int:
        """
        Retorna a contagem total de posts de Instagram para um político.

        Prioriza a tabela unificada `social_media_posts` (plataforma='instagram').
        Se estiver vazia, faz fallback para a tabela legada `instagram_posts`.
        """
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return 0

        # 1) Tabela unificada
        try:
            unified = (
                self.client.table("social_media_posts")
                .select("id", count=CountMethod.exact)
                .eq("politico_id", politico_uuid)
                .eq("plataforma", "instagram")
                .execute()
            )
            unified_count = self._safe_count(unified)
            if unified_count > 0:
                return unified_count
        except Exception as e:
            logger.error(f"Erro ao contar posts (unificado) do politico {politico_id}: {e}")

        # 2) Tabela legada
        try:
            legacy = (
                self.client.table("instagram_posts")
                .select("id", count=CountMethod.exact)
                .eq("politico_id", politico_uuid)
                .execute()
            )
            return self._safe_count(legacy)
        except Exception as e:
            logger.error(f"Erro ao contar posts (legado) do politico {politico_id}: {e}")
            return 0

    # ==================== SOCIAL MENTIONS ====================

    def count_social_mentions_politico(self, politico_id: int, plataforma: Optional[str] = None) -> int:
        """
        Retorna a contagem total de menções sociais de um político.
        """
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return 0

        try:
            query = (
                self.client.table("social_mentions")
                .select("id", count=CountMethod.exact)
                .eq("politico_id", politico_uuid)
            )
            if plataforma:
                query = query.eq("plataforma", plataforma)
            response = query.execute()
            return self._safe_count(response)
        except Exception as e:
            logger.error(f"Erro ao contar social_mentions do politico {politico_id}: {e}")
            return 0
    
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
                on_conflict="politico_id,plataforma,post_id"
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
                on_conflict="politico_id,plataforma,post_id"
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
        # Converte ID inteiro para UUID
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return []
        
        query = self.client.table("social_media_posts")\
            .select("*")\
            .eq("politico_id", politico_uuid)
        
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
        # Converte ID inteiro para UUID
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return []
        
        query = self.client.table("social_mentions")\
            .select("*")\
            .eq("politico_id", politico_uuid)
        
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
        # Converte ID inteiro para UUID
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return []
        
        response = self.client.table("social_mentions")\
            .select("*")\
            .eq("politico_id", politico_uuid)\
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
        # Converte ID inteiro para UUID
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return []
        
        response = self.client.table("social_mentions")\
            .select("*")\
            .eq("politico_id", politico_uuid)\
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
        # Converte ID inteiro para UUID
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return []
        
        response = self.client.table("mention_topics")\
            .select("*")\
            .eq("politico_id", politico_uuid)\
            .order("total_mencoes", desc=True)\
            .execute()
        return response.data
    
    def get_top_assuntos_politico(
        self,
        politico_id: int,
        limite: int = 10
    ) -> List[Dict[str, Any]]:
        """Retorna os principais assuntos discutidos sobre um político"""
        # Converte ID inteiro para UUID
        politico_uuid = self.get_politico_uuid(politico_id)
        if not politico_uuid:
            return []
        
        response = self.client.table("mention_topics")\
            .select("*")\
            .eq("politico_id", politico_uuid)\
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
