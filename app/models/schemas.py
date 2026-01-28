"""
Schemas Pydantic para validação de dados.
"""
from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, List
from datetime import datetime
from enum import Enum


class TipoNoticia(str, Enum):
    POLITICO = "politico"
    CONCORRENTE = "concorrente"
    CIDADE = "cidade"
    GERAL = "geral"


class TipoFonte(str, Enum):
    NACIONAL = "nacional"
    REGIONAL = "regional"
    LOCAL = "local"
    SOCIAL = "social"


class StatusColeta(str, Enum):
    INICIADO = "iniciado"
    SUCESSO = "sucesso"
    ERRO = "erro"
    PARCIAL = "parcial"


# ==================== POLÍTICOS ====================

class Politico(BaseModel):
    """Schema de político"""
    id: int
    name: str
    image: Optional[str] = None
    description: Optional[str] = None
    active: bool = True
    instagram_username: Optional[str] = None
    twitter_username: Optional[str] = None
    cidade: Optional[str] = None
    estado: Optional[str] = None
    funcao: Optional[str] = None
    eleito: Optional[bool] = None
    cpf: Optional[str] = None
    
    class Config:
        from_attributes = True


class PoliticoConcorrente(BaseModel):
    """Schema de relacionamento político-concorrente"""
    id: str
    politico_id: int
    concorrente_id: int
    created_at: datetime


# ==================== NOTÍCIAS ====================

class RelevanciaScore(BaseModel):
    """Scores de relevância de uma notícia"""
    score_recencia: float = Field(default=0, ge=0, le=100)
    score_mencao: float = Field(default=0, ge=0, le=100)
    score_fonte: float = Field(default=0, ge=0, le=100)
    score_engajamento: float = Field(default=0, ge=0, le=100)
    relevancia_total: float = Field(default=0, ge=0, le=100)
    mencao_titulo: bool = False
    mencao_conteudo: int = 0


class NoticiaCreate(BaseModel):
    """Schema para criação de notícia"""
    politico_id: Optional[int] = None
    tipo: TipoNoticia
    titulo: str
    descricao: Optional[str] = None
    conteudo_completo: Optional[str] = None
    url: str
    fonte_id: Optional[str] = None
    fonte_nome: Optional[str] = None
    imagem_url: Optional[str] = None
    publicado_em: Optional[datetime] = None
    cidade: Optional[str] = None
    # Scores de relevância
    score_recencia: float = 0
    score_mencao: float = 0
    score_fonte: float = 0
    score_engajamento: float = 0
    relevancia_total: float = 0
    mencao_titulo: bool = False
    mencao_conteudo: int = 0


class Noticia(NoticiaCreate):
    """Schema completo de notícia"""
    id: str
    coletado_em: datetime
    
    class Config:
        from_attributes = True


class NoticiaResponse(BaseModel):
    """Resposta da API para notícias"""
    noticias: List[Noticia]
    total: int


# ==================== INSTAGRAM ====================

class InstagramPostCreate(BaseModel):
    """Schema para criação de post do Instagram"""
    politico_id: int
    post_shortcode: str
    post_url: Optional[str] = None
    caption: Optional[str] = None
    likes: int = 0
    comments: int = 0
    engagement_score: float = 0
    media_type: Optional[str] = None
    thumbnail_url: Optional[str] = None
    posted_at: Optional[datetime] = None


class InstagramPost(InstagramPostCreate):
    """Schema completo de post do Instagram"""
    id: str
    collected_at: datetime
    
    class Config:
        from_attributes = True


class InstagramStats(BaseModel):
    """Estatísticas de Instagram de um político"""
    politico_id: int
    total_posts: int
    total_likes: int
    total_comments: int
    media_engagement: float
    top_post: Optional[InstagramPost] = None


# ==================== FONTES ====================

class FonteNoticia(BaseModel):
    """Schema de fonte de notícias"""
    id: str
    nome: str
    dominio: str
    tipo: TipoFonte
    peso_confiabilidade: float = Field(ge=0, le=2.0)
    ativo: bool = True
    created_at: datetime
    
    class Config:
        from_attributes = True


class FonteUpdate(BaseModel):
    """Schema para atualização de fonte"""
    peso_confiabilidade: float = Field(ge=0, le=2.0)


# ==================== TRENDING ====================

class TrendingTopic(BaseModel):
    """Schema de trending topic"""
    id: Optional[str] = None
    rank: int = Field(ge=1)
    title: str
    subtitle: Optional[str] = None
    created_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


# ==================== LOGS ====================

class ColetaLog(BaseModel):
    """Schema de log de coleta"""
    id: str
    tipo_coleta: str
    status: StatusColeta
    mensagem: Optional[str] = None
    registros_coletados: int = 0
    iniciado_em: datetime
    finalizado_em: Optional[datetime] = None
    
    class Config:
        from_attributes = True


# ==================== SOCIAL MENTIONS ====================

class PlataformaSocial(str, Enum):
    BLUESKY = "bluesky"
    TWITTER = "twitter"
    GOOGLE_TRENDS = "google_trends"
    GOOGLE_SEARCH = "google_search"


class Sentimento(str, Enum):
    POSITIVO = "positivo"
    NEUTRO = "neutro"
    NEGATIVO = "negativo"


class AssuntoCategoria(str, Enum):
    SAUDE = "Saúde"
    EDUCACAO = "Educação"
    SEGURANCA = "Segurança"
    ECONOMIA = "Economia"
    INFRAESTRUTURA = "Infraestrutura"
    MEIO_AMBIENTE = "Meio Ambiente"
    CORRUPCAO = "Corrupção"
    POLITICA = "Política"
    SOCIAL = "Social"
    CULTURA = "Cultura"
    TECNOLOGIA = "Tecnologia"
    AGRONEGOCIO = "Agronegócio"
    OUTRO = "Outro"


class SocialMentionCreate(BaseModel):
    """Schema para criação de menção social"""
    politico_id: int
    plataforma: PlataformaSocial
    mention_id: str
    autor: Optional[str] = None
    autor_username: Optional[str] = None
    conteudo: Optional[str] = None
    url: Optional[str] = None
    assunto: Optional[str] = None
    assunto_detalhe: Optional[str] = None
    sentimento: Optional[Sentimento] = None
    likes: int = 0
    reposts: int = 0
    replies: int = 0
    engagement_score: float = 0
    posted_at: Optional[datetime] = None
    metadata: Optional[dict] = None


class SocialMention(SocialMentionCreate):
    """Schema completo de menção social"""
    id: str
    collected_at: datetime
    
    class Config:
        from_attributes = True


class MentionTopicCreate(BaseModel):
    """Schema para criação de agregação de menções por assunto"""
    politico_id: int
    assunto: str
    total_mencoes: int = 0
    mencoes_positivas: int = 0
    mencoes_negativas: int = 0
    mencoes_neutras: int = 0
    engagement_total: float = 0
    ultima_mencao: Optional[datetime] = None
    periodo_inicio: datetime
    periodo_fim: datetime


class MentionTopic(MentionTopicCreate):
    """Schema completo de tópico de menção"""
    id: str
    atualizado_em: datetime
    
    class Config:
        from_attributes = True


class AssuntoStats(BaseModel):
    """Estatísticas de um assunto para um político"""
    assunto: str
    total_mencoes: int
    mencoes_positivas: int
    mencoes_negativas: int
    mencoes_neutras: int
    sentimento_predominante: str
    engagement_total: float
    exemplo: Optional[str] = None


class PoliticoAssuntosResponse(BaseModel):
    """Resposta com assuntos discutidos sobre um político"""
    politico_id: int
    nome: str
    periodo: str
    assuntos: List[AssuntoStats]


# ==================== API RESPONSES ====================

class HealthResponse(BaseModel):
    """Resposta do health check"""
    status: str
    version: str
    timestamp: datetime


class ColetaExecutarResponse(BaseModel):
    """Resposta da execução manual de coleta"""
    status: str
    mensagem: str
    log_id: Optional[str] = None


# ==================== PROCESSOS JUDICIAIS ====================

class TipoProcesso(str, Enum):
    CRIMINAL = "criminal"
    CIVEL = "civel"
    ELEITORAL = "eleitoral"
    TRABALHISTA = "trabalhista"
    FEDERAL = "federal"
    ADMINISTRATIVO = "administrativo"


class StatusProcesso(str, Enum):
    ATIVO = "ativo"
    ARQUIVADO = "arquivado"
    BAIXADO = "baixado"
    SUSPENSO = "suspenso"


class PoloProcesso(str, Enum):
    AUTOR = "autor"
    REU = "reu"
    TERCEIRO = "terceiro"
    INTERESSADO = "interessado"


class TribunalEnum(str, Enum):
    TJSP = "TJSP"
    TRF3 = "TRF3"
    TSE = "TSE"
    TRT = "TRT"
    STF = "STF"
    STJ = "STJ"


class ProcessoJudicialCreate(BaseModel):
    """Schema para criação de processo judicial"""
    politico_id: Optional[int] = None
    cpf: Optional[str] = None
    numero_processo: str
    tribunal: str
    tipo: Optional[str] = None
    classe: Optional[str] = None
    assunto: Optional[str] = None
    vara: Optional[str] = None
    comarca: Optional[str] = None
    data_distribuicao: Optional[datetime] = None
    status: str = "ativo"
    polo: Optional[str] = None
    url_consulta: Optional[str] = None
    ultima_movimentacao: Optional[str] = None
    data_ultima_movimentacao: Optional[datetime] = None
    dados_raw: Optional[dict] = None


class ProcessoJudicial(ProcessoJudicialCreate):
    """Schema completo de processo judicial"""
    id: str
    coletado_em: datetime
    atualizado_em: datetime
    
    class Config:
        from_attributes = True


class ProcessoJudicialResponse(BaseModel):
    """Resposta da API para processos judiciais"""
    processos: List[ProcessoJudicial]
    total: int
    por_tribunal: dict = {}
    por_tipo: dict = {}


# ==================== DOAÇÕES ELEITORAIS ====================

class TipoDoacao(str, Enum):
    PESSOA_FISICA = "pessoa_fisica"
    PESSOA_JURIDICA = "pessoa_juridica"
    FUNDO_PARTIDARIO = "fundo_partidario"
    FUNDO_ELEITORAL = "fundo_eleitoral"
    RECURSOS_PROPRIOS = "recursos_proprios"


class DoacaoEleitoralCreate(BaseModel):
    """Schema para criação de doação eleitoral"""
    politico_id: Optional[int] = None
    cpf_doador: Optional[str] = None
    nome_doador: Optional[str] = None
    cpf_candidato: Optional[str] = None
    nome_candidato: Optional[str] = None
    valor: float
    tipo_doacao: Optional[str] = None
    tipo_receita: Optional[str] = None
    eleicao: str
    turno: int = 1
    partido: Optional[str] = None
    cargo: Optional[str] = None
    uf: Optional[str] = None
    municipio: Optional[str] = None
    fonte: str = "TSE"
    sequencial_candidato: Optional[str] = None
    numero_documento: Optional[str] = None


class DoacaoEleitoral(DoacaoEleitoralCreate):
    """Schema completo de doação eleitoral"""
    id: str
    coletado_em: datetime
    
    class Config:
        from_attributes = True


class DoacaoEleitoralResponse(BaseModel):
    """Resposta da API para doações eleitorais"""
    doacoes: List[DoacaoEleitoral]
    total: int
    valor_total: float
    por_eleicao: dict = {}


# ==================== FILIAÇÕES PARTIDÁRIAS ====================

class SituacaoFiliacao(str, Enum):
    REGULAR = "regular"
    CANCELADO = "cancelado"
    DESFILIADO = "desfiliado"
    SUB_JUDICE = "sub_judice"


class FiliacaoPartidariaCriada(BaseModel):
    """Schema para criação de filiação partidária"""
    politico_id: Optional[int] = None
    cpf: Optional[str] = None
    nome: Optional[str] = None
    titulo_eleitoral: Optional[str] = None
    partido: str
    sigla_partido: Optional[str] = None
    data_filiacao: Optional[datetime] = None
    data_desfiliacao: Optional[datetime] = None
    data_cancelamento: Optional[datetime] = None
    data_regularizacao: Optional[datetime] = None
    situacao: Optional[str] = None
    motivo_cancelamento: Optional[str] = None
    uf: Optional[str] = None
    municipio: Optional[str] = None
    zona_eleitoral: Optional[str] = None
    secao_eleitoral: Optional[str] = None
    fonte: str = "TSE"


class FiliacaoPartidaria(FiliacaoPartidariaCriada):
    """Schema completo de filiação partidária"""
    id: str
    coletado_em: datetime
    
    class Config:
        from_attributes = True


class FiliacaoPartidariaeResponse(BaseModel):
    """Resposta da API para filiações partidárias"""
    filiacoes: List[FiliacaoPartidaria]
    total: int
    historico_partidos: List[str] = []


# ==================== CANDIDATURAS ====================

class SituacaoCandidatura(str, Enum):
    APTO = "apto"
    INAPTO = "inapto"
    INDEFERIDO = "indeferido"
    RENUNCIADO = "renunciado"


class SituacaoTotalizacao(str, Enum):
    ELEITO = "eleito"
    NAO_ELEITO = "nao_eleito"
    SEGUNDO_TURNO = "segundo_turno"
    SUPLENTE = "suplente"


class CandidaturaCreate(BaseModel):
    """Schema para criação de candidatura"""
    politico_id: Optional[int] = None
    cpf: Optional[str] = None
    nome: Optional[str] = None
    nome_urna: Optional[str] = None
    numero_candidato: Optional[str] = None
    sequencial_candidato: Optional[str] = None
    eleicao: str
    turno: int = 1
    cargo: Optional[str] = None
    uf: Optional[str] = None
    municipio: Optional[str] = None
    partido: Optional[str] = None
    sigla_partido: Optional[str] = None
    coligacao: Optional[str] = None
    situacao_candidatura: Optional[str] = None
    situacao_totalizacao: Optional[str] = None
    total_votos: Optional[int] = None
    percentual_votos: Optional[float] = None
    ocupacao: Optional[str] = None
    grau_instrucao: Optional[str] = None
    estado_civil: Optional[str] = None
    nacionalidade: Optional[str] = None
    data_nascimento: Optional[datetime] = None
    genero: Optional[str] = None
    cor_raca: Optional[str] = None
    email: Optional[str] = None
    bens_declarados: Optional[float] = None
    url_foto: Optional[str] = None
    dados_raw: Optional[dict] = None
    fonte: str = "TSE"


class Candidatura(CandidaturaCreate):
    """Schema completo de candidatura"""
    id: str
    coletado_em: datetime
    
    class Config:
        from_attributes = True


class CandidaturaResponse(BaseModel):
    """Resposta da API para candidaturas"""
    candidaturas: List[Candidatura]
    total: int
    por_eleicao: dict = {}


# ==================== CONSULTA PROCESSUAL ====================

class FonteConsulta(str, Enum):
    TSE = "TSE"
    TSE_DIVULGACAND = "TSE_DIVULGACAND"
    TJSP = "TJSP"
    TRF3 = "TRF3"
    DOE_SP = "DOE_SP"


class ConsultaProcessualLog(BaseModel):
    """Schema de log de consulta processual"""
    id: str
    politico_id: Optional[int] = None
    cpf: Optional[str] = None
    fonte: str
    tipo_consulta: Optional[str] = None
    status: str
    registros_encontrados: int = 0
    mensagem: Optional[str] = None
    iniciado_em: datetime
    finalizado_em: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class ConsultaProcessualRequest(BaseModel):
    """Request para iniciar consulta processual"""
    politico_id: Optional[int] = None
    cpf: Optional[str] = None
    fontes: List[FonteConsulta] = [FonteConsulta.TSE]


class ConsultaProcessualResponse(BaseModel):
    """Resposta consolidada de consulta processual"""
    politico_id: Optional[int] = None
    cpf: Optional[str] = None
    processos: List[ProcessoJudicial] = []
    doacoes: List[DoacaoEleitoral] = []
    filiacoes: List[FiliacaoPartidaria] = []
    candidaturas: List[Candidatura] = []
    logs: List[ConsultaProcessualLog] = []
    resumo: dict = {}


# ==================== RESUMO DO POLÍTICO ====================

class PoliticoResumoProcessual(BaseModel):
    """Resumo processual de um político"""
    politico_id: int
    nome: str
    cpf: Optional[str] = None
    total_processos: int = 0
    processos_ativos: int = 0
    total_doacoes_feitas: int = 0
    valor_total_doacoes: float = 0
    total_doacoes_recebidas: int = 0
    valor_total_recebido: float = 0
    total_candidaturas: int = 0
    eleicoes_vencidas: int = 0
    historico_partidos: List[str] = []
    ultima_atualizacao: Optional[datetime] = None
