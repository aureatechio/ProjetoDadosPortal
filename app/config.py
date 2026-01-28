"""
Configurações do sistema de coleta de dados políticos.
Carrega variáveis de ambiente do arquivo .env
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Configurações da aplicação"""
    
    # Supabase
    supabase_url: str
    supabase_key: str
    
    # NewsAPI
    newsapi_key: Optional[str] = None

    # OpenAI (para resumo técnico de notícias)
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-4o-mini"
    
    # Instagram (opcional)
    instagram_username: Optional[str] = None
    instagram_password: Optional[str] = None
    
    # Configurações de Coleta
    coleta_horario: str = "06:00"
    coleta_timezone: str = "America/Sao_Paulo"
    
    # Limites
    max_noticias_por_politico: int = 20
    max_posts_instagram: int = 10
    dias_retencao_noticias: int = 7
    dias_retencao_instagram: int = 30
    
    # Delays para evitar rate limiting (em segundos)
    delay_entre_requisicoes: float = 2.0
    delay_instagram: float = 5.0
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Instância global de configurações
settings = Settings()
