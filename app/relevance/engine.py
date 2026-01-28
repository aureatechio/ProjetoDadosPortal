"""
Motor de cálculo de relevância de notícias.
"""
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse
import logging

from app.relevance.weights import RelevanceWeights, DEFAULT_WEIGHTS
from app.relevance.analyzer import ContentAnalyzer
from app.database import db

logger = logging.getLogger(__name__)


class RelevanceEngine:
    """
    Motor principal para cálculo de relevância de notícias.
    
    Utiliza 4 fatores com pesos configuráveis:
    1. Recência: Quão recente é a notícia
    2. Menção: Se o político é mencionado diretamente
    3. Fonte: Confiabilidade da fonte
    4. Engajamento: Viralização/compartilhamentos
    """
    
    def __init__(
        self, 
        weights: RelevanceWeights = None,
        fontes_cache: Dict[str, Dict[str, Any]] = None
    ):
        self.weights = weights or DEFAULT_WEIGHTS
        self.analyzer = ContentAnalyzer()
        self._fontes_cache = fontes_cache or {}
        self._load_fontes_cache()
    
    def _load_fontes_cache(self):
        """Carrega cache de fontes do banco de dados"""
        try:
            fontes = db.get_fontes_ativas()
            for fonte in fontes:
                self._fontes_cache[fonte["dominio"]] = fonte
        except Exception as e:
            logger.warning(f"Não foi possível carregar cache de fontes: {e}")
    
    def _extract_domain(self, url: str) -> str:
        """Extrai o domínio de uma URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove www. se presente
            if domain.startswith("www."):
                domain = domain[4:]
            return domain
        except Exception:
            return "outros"
    
    def _get_fonte_peso(self, url: str) -> float:
        """
        Obtém o peso de confiabilidade da fonte baseado na URL.
        
        Args:
            url: URL da notícia
            
        Returns:
            Peso da fonte (0.0 a 2.0)
        """
        domain = self._extract_domain(url)
        
        # Busca no cache
        if domain in self._fontes_cache:
            return self._fontes_cache[domain].get("peso_confiabilidade", 1.0)
        
        # Verifica se é subdomínio de alguma fonte conhecida
        for cached_domain, fonte in self._fontes_cache.items():
            if domain.endswith(cached_domain) or cached_domain.endswith(domain):
                return fonte.get("peso_confiabilidade", 1.0)
        
        # Fonte desconhecida = peso padrão
        return 1.0
    
    def _get_fonte_info(self, url: str) -> Dict[str, Any]:
        """Obtém informações completas da fonte"""
        domain = self._extract_domain(url)
        
        if domain in self._fontes_cache:
            return self._fontes_cache[domain]
        
        for cached_domain, fonte in self._fontes_cache.items():
            if domain.endswith(cached_domain) or cached_domain.endswith(domain):
                return fonte
        
        return {"id": None, "nome": domain, "peso_confiabilidade": 1.0}
    
    def calcular_score_recencia(
        self, 
        publicado_em: datetime,
        agora: datetime = None
    ) -> float:
        """
        Calcula score de recência (0-100).
        
        Decai 2 pontos por hora desde a publicação.
        Notícias com mais de 50 horas recebem score 0.
        
        Args:
            publicado_em: Data de publicação da notícia
            agora: Data atual (opcional, para testes)
            
        Returns:
            Score de recência (0-100)
        """
        if not publicado_em:
            return 50  # Score médio se não houver data
        
        agora = agora or datetime.now(timezone.utc)
        
        # Garante que ambas as datas têm timezone
        if publicado_em.tzinfo is None:
            publicado_em = publicado_em.replace(tzinfo=timezone.utc)
        
        # Calcula diferença em horas
        delta = agora - publicado_em
        horas = delta.total_seconds() / 3600
        
        # Decai 2 pontos por hora, mínimo 0
        score = max(0, 100 - (horas * 2))
        
        return round(score, 2)
    
    def calcular_score_mencao(
        self,
        titulo: str,
        conteudo: str,
        nome_politico: str
    ) -> tuple:
        """
        Calcula score de menção direta (0-100).
        
        - Menção no título: +50 pontos
        - Cada menção no conteúdo: +10 pontos (max 50)
        
        Args:
            titulo: Título da notícia
            conteudo: Conteúdo da notícia
            nome_politico: Nome do político
            
        Returns:
            Tuple (score, mencao_titulo, mencoes_conteudo)
        """
        if not nome_politico:
            return 0, False, 0
        
        mencao_titulo, mencoes_conteudo, _ = self.analyzer.analyze_mentions(
            titulo, conteudo, nome_politico
        )
        
        # Calcula score
        score = 0
        if mencao_titulo:
            score += 50  # Menção no título é muito importante
        
        # Cada menção no conteúdo vale até 10 pontos (max 50)
        score += min(50, mencoes_conteudo * 10)
        
        return round(score, 2), mencao_titulo, mencoes_conteudo
    
    def calcular_score_fonte(self, url: str) -> float:
        """
        Calcula score da fonte (0-100).
        
        Baseado no peso de confiabilidade da fonte (0-2).
        Peso 2.0 = 100 pontos
        Peso 1.0 = 50 pontos
        
        Args:
            url: URL da notícia
            
        Returns:
            Score da fonte (0-100)
        """
        peso = self._get_fonte_peso(url)
        score = peso * 50  # Converte peso (0-2) para score (0-100)
        return round(min(100, score), 2)
    
    def calcular_score_engajamento(
        self,
        compartilhamentos: int = 0,
        comentarios: int = 0,
        likes: int = 0
    ) -> float:
        """
        Calcula score de engajamento (0-100).
        
        Args:
            compartilhamentos: Número de compartilhamentos
            comentarios: Número de comentários
            likes: Número de likes/reações
            
        Returns:
            Score de engajamento (0-100)
        """
        # Ponderação: compartilhamentos valem mais
        engajamento_total = (
            compartilhamentos * 3 +
            comentarios * 2 +
            likes
        )
        
        # Normaliza: 1000 interações = 100 pontos
        score = min(100, engajamento_total / 10)
        
        return round(score, 2)
    
    def calcular_relevancia(
        self,
        noticia: Dict[str, Any],
        nome_politico: str = None,
        engajamento: Dict[str, int] = None
    ) -> Dict[str, Any]:
        """
        Calcula a relevância total de uma notícia.
        
        Args:
            noticia: Dados da notícia (titulo, conteudo, url, publicado_em)
            nome_politico: Nome do político relacionado
            engajamento: Dict com compartilhamentos, comentarios, likes
            
        Returns:
            Dict com todos os scores calculados
        """
        titulo = noticia.get("titulo", "") or noticia.get("title", "")
        conteudo = noticia.get("conteudo_completo") or noticia.get("conteudo", "") or noticia.get("description", "")
        url = noticia.get("url", "")
        publicado_em = noticia.get("publicado_em") or noticia.get("published_at")
        
        # Converte string para datetime se necessário
        if isinstance(publicado_em, str):
            try:
                publicado_em = datetime.fromisoformat(publicado_em.replace("Z", "+00:00"))
            except Exception:
                publicado_em = None
        
        engajamento = engajamento or {}
        
        # Calcula cada score
        score_recencia = self.calcular_score_recencia(publicado_em)
        score_mencao, mencao_titulo, mencoes_conteudo = self.calcular_score_mencao(
            titulo, conteudo, nome_politico
        )
        score_fonte = self.calcular_score_fonte(url)
        score_engajamento = self.calcular_score_engajamento(
            engajamento.get("compartilhamentos", 0),
            engajamento.get("comentarios", 0),
            engajamento.get("likes", 0)
        )
        
        # Calcula relevância total ponderada
        relevancia_total = (
            score_recencia * self.weights.recencia +
            score_mencao * self.weights.mencao +
            score_fonte * self.weights.fonte +
            score_engajamento * self.weights.engajamento
        )
        
        # Obtém info da fonte
        fonte_info = self._get_fonte_info(url)
        
        return {
            "score_recencia": score_recencia,
            "score_mencao": score_mencao,
            "score_fonte": score_fonte,
            "score_engajamento": score_engajamento,
            "relevancia_total": round(relevancia_total, 2),
            "mencao_titulo": mencao_titulo,
            "mencao_conteudo": mencoes_conteudo,
            "fonte_id": fonte_info.get("id"),
            "fonte_nome": fonte_info.get("nome")
        }
    
    def processar_noticias(
        self,
        noticias: List[Dict[str, Any]],
        nome_politico: str = None
    ) -> List[Dict[str, Any]]:
        """
        Processa uma lista de notícias, calculando relevância e ordenando.
        
        Args:
            noticias: Lista de notícias brutas
            nome_politico: Nome do político para análise de menções
            
        Returns:
            Lista de notícias com scores, ordenada por relevância
        """
        processed = []
        
        for noticia in noticias:
            scores = self.calcular_relevancia(noticia, nome_politico)
            
            # Mescla notícia original com scores
            noticia_processada = {
                **noticia,
                **scores
            }
            processed.append(noticia_processada)
        
        # Ordena por relevância total (decrescente)
        processed.sort(key=lambda x: x.get("relevancia_total", 0), reverse=True)
        
        return processed


# Instância global do motor de relevância
relevance_engine = RelevanceEngine()
