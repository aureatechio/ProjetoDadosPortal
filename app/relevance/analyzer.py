"""
Analisador de conteúdo para detecção de menções.
"""
import re
from typing import Tuple, List
from fuzzywuzzy import fuzz
import unicodedata


class ContentAnalyzer:
    """Analisa conteúdo de notícias para detectar menções a políticos"""
    
    def __init__(self, similarity_threshold: int = 85):
        """
        Args:
            similarity_threshold: Limiar mínimo de similaridade para match (0-100)
        """
        self.similarity_threshold = similarity_threshold
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normaliza texto removendo acentos e convertendo para minúsculo.
        """
        if not text:
            return ""
        # Remove acentos
        text = unicodedata.normalize('NFKD', text)
        text = text.encode('ASCII', 'ignore').decode('ASCII')
        # Converte para minúsculo
        return text.lower().strip()
    
    @staticmethod
    def extract_name_variations(full_name: str) -> List[str]:
        """
        Extrai variações do nome para busca.
        Ex: "João da Silva Neto" -> ["João da Silva Neto", "João Silva", "Silva Neto", "João", "Silva"]
        """
        if not full_name:
            return []
        
        normalized = ContentAnalyzer.normalize_text(full_name)
        parts = normalized.split()
        
        variations = [normalized]  # Nome completo
        
        # Conectivos que não devem ser considerados como nome
        conectivos = {'da', 'de', 'do', 'das', 'dos', 'e'}
        
        # Filtra partes significativas (remove conectivos)
        significant_parts = [p for p in parts if p not in conectivos and len(p) > 2]
        
        if len(significant_parts) >= 2:
            # Primeiro + último nome significativo
            variations.append(f"{significant_parts[0]} {significant_parts[-1]}")
            # Último nome sozinho (sobrenome)
            variations.append(significant_parts[-1])
        
        if significant_parts:
            # Primeiro nome sozinho
            variations.append(significant_parts[0])
        
        return list(set(variations))
    
    def analyze_mentions(
        self, 
        titulo: str, 
        conteudo: str, 
        nome_politico: str
    ) -> Tuple[bool, int, float]:
        """
        Analisa menções a um político no título e conteúdo.
        
        Args:
            titulo: Título da notícia
            conteudo: Conteúdo/texto da notícia
            nome_politico: Nome do político a buscar
            
        Returns:
            Tuple contendo:
                - mencao_titulo: Se há menção no título
                - mencoes_conteudo: Quantidade de menções no conteúdo
                - score_similaridade: Score de similaridade máximo encontrado
        """
        titulo_norm = self.normalize_text(titulo)
        conteudo_norm = self.normalize_text(conteudo or "")
        
        # Obtém variações do nome
        nome_variations = self.extract_name_variations(nome_politico)
        
        mencao_titulo = False
        mencoes_conteudo = 0
        max_similarity = 0
        
        for variation in nome_variations:
            # Verifica menção no título
            if variation in titulo_norm:
                mencao_titulo = True
                max_similarity = 100
            else:
                # Usa fuzzy matching para nomes com pequenas variações
                similarity = fuzz.partial_ratio(variation, titulo_norm)
                if similarity >= self.similarity_threshold:
                    mencao_titulo = True
                    max_similarity = max(max_similarity, similarity)
            
            # Conta menções no conteúdo
            if conteudo_norm:
                # Busca exata
                exact_count = conteudo_norm.count(variation)
                mencoes_conteudo += exact_count
                
                if exact_count > 0:
                    max_similarity = max(max_similarity, 100)
        
        return mencao_titulo, mencoes_conteudo, max_similarity
    
    def extract_city_from_content(self, conteudo: str, cidades_conhecidas: List[str]) -> str:
        """
        Tenta extrair a cidade mencionada no conteúdo.
        
        Args:
            conteudo: Texto do conteúdo
            cidades_conhecidas: Lista de cidades para buscar
            
        Returns:
            Nome da cidade encontrada ou None
        """
        if not conteudo or not cidades_conhecidas:
            return None
        
        conteudo_norm = self.normalize_text(conteudo)
        
        for cidade in cidades_conhecidas:
            cidade_norm = self.normalize_text(cidade)
            if cidade_norm in conteudo_norm:
                return cidade
        
        return None
    
    def is_political_news(self, titulo: str, conteudo: str) -> bool:
        """
        Verifica se uma notícia é de cunho político.
        
        Args:
            titulo: Título da notícia
            conteudo: Conteúdo da notícia
            
        Returns:
            True se for notícia política
        """
        texto_completo = self.normalize_text(f"{titulo} {conteudo or ''}")
        
        # Palavras-chave políticas
        keywords = [
            'deputado', 'senador', 'vereador', 'prefeito', 'governador',
            'presidente', 'ministro', 'secretario', 'camara', 'senado',
            'congresso', 'assembleia', 'legislativo', 'executivo',
            'eleicao', 'voto', 'urna', 'candidato', 'partido',
            'pt', 'psdb', 'mdb', 'pl', 'psd', 'pp', 'pdt', 'psol',
            'stf', 'tse', 'trf', 'mpf', 'pf',
            'projeto de lei', 'pec', 'cpi', 'reforma',
            'corrupcao', 'lava jato', 'mensalao',
            'brasilia', 'planalto', 'esplanada'
        ]
        
        return any(kw in texto_completo for kw in keywords)
