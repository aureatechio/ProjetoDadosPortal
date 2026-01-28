"""
Pesos configuráveis para o sistema de relevância.
"""
from dataclasses import dataclass


@dataclass
class RelevanceWeights:
    """
    Pesos para cálculo de relevância.
    Os pesos devem somar 1.0 (100%)
    """
    # Peso da recência (quão recente é a notícia)
    recencia: float = 0.25
    
    # Peso da menção direta (nome do político no título/conteúdo)
    # MAIOR PESO - notícias que mencionam diretamente são mais relevantes
    mencao: float = 0.35
    
    # Peso da fonte (confiabilidade do portal de notícias)
    fonte: float = 0.25
    
    # Peso do engajamento (compartilhamentos, viralização)
    engajamento: float = 0.15
    
    def __post_init__(self):
        """Valida que os pesos somam 1.0"""
        total = self.recencia + self.mencao + self.fonte + self.engajamento
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Os pesos devem somar 1.0, mas somam {total}")
    
    def to_dict(self) -> dict:
        return {
            "recencia": self.recencia,
            "mencao": self.mencao,
            "fonte": self.fonte,
            "engajamento": self.engajamento
        }


# Configuração padrão de pesos
DEFAULT_WEIGHTS = RelevanceWeights()

# Configuração alternativa com mais peso em recência
BREAKING_NEWS_WEIGHTS = RelevanceWeights(
    recencia=0.40,
    mencao=0.30,
    fonte=0.20,
    engajamento=0.10
)

# Configuração com mais peso em fonte confiável
VERIFIED_NEWS_WEIGHTS = RelevanceWeights(
    recencia=0.20,
    mencao=0.30,
    fonte=0.40,
    engajamento=0.10
)
