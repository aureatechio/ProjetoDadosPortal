export type Politico = {
  id: number
  name: string
  image?: string | null
  description?: string | null
  active?: boolean
  usar_diretoriaja?: boolean | null
  instagram_username?: string | null
  twitter_username?: string | null
  cidade?: string | null
  estado?: string | null
  funcao?: string | null
}

export type Noticia = {
  id: string
  politico_id?: number | null
  tipo: 'politico' | 'concorrente' | 'cidade' | 'geral' | string
  titulo: string
  descricao?: string | null
  conteudo_completo?: string | null
  url: string
  fonte_id?: string | null
  fonte_nome?: string | null
  imagem_url?: string | null
  publicado_em?: string | null
  coletado_em?: string | null
  cidade?: string | null
  estado?: string | null
  score_recencia?: number
  score_mencao?: number
  score_fonte?: number
  score_engajamento?: number
  relevancia_total?: number
  mencao_titulo?: boolean
  mencao_conteudo?: number
}

export type InstagramPost = {
  id: string
  politico_id: number
  post_shortcode: string
  post_url?: string | null
  caption?: string | null
  likes?: number
  comments?: number
  engagement_score?: number
  media_type?: string | null
  thumbnail_url?: string | null
  posted_at?: string | null
  collected_at?: string | null
}

export type TrendingTopic = {
  id?: string | null
  rank: number
  title: string
  subtitle?: string | null
  category: 'politica' | 'twitter' | 'google'
  created_at?: string | null
}

export type ColetaLog = {
  id: string
  tipo_coleta: string
  status: string
  mensagem?: string | null
  registros_coletados?: number | null
  iniciado_em?: string | null
  finalizado_em?: string | null
}

export type ScheduledJob = {
  id: string
  name: string
  next_run: string | null
}

export type SocialMention = {
  id: string
  politico_id: number
  plataforma: string
  mention_id: string
  autor?: string | null
  autor_username?: string | null
  conteudo?: string | null
  url?: string | null
  assunto?: string | null
  assunto_detalhe?: string | null
  sentimento?: 'positivo' | 'neutro' | 'negativo' | null
  likes?: number | null
  reposts?: number | null
  replies?: number | null
  engagement_score?: number | null
  posted_at?: string | null
  collected_at?: string | null
}

export type MentionTopic = {
  id: string
  politico_id: number
  assunto: string
  total_mencoes: number
  mencoes_positivas: number
  mencoes_negativas: number
  mencoes_neutras: number
  engagement_total: number
  ultima_mencao?: string | null
  atualizado_em?: string | null
  periodo_inicio?: string | null
  periodo_fim?: string | null
}

export type AssuntoStats = {
  assunto: string
  total_mencoes: number
  mencoes_positivas: number
  mencoes_negativas: number
  mencoes_neutras: number
  sentimento_predominante: string
  engagement_total: number
  exemplo?: string | null
}

export type PoliticoAssuntosResponse = {
  politico_id: number
  nome: string
  periodo: string
  assuntos: AssuntoStats[]
}

export type PoliticoResumo = {
  politico: Politico
  top_noticias: Noticia[]
  top_instagram: InstagramPost[]
  concorrentes: Politico[]
  noticias_cidade: Noticia[]
  total_noticias: number
  total_posts_instagram: number
}

// ==================== CONSULTA PROCESSUAL ====================

export type ProcessoJudicial = {
  id: string
  politico_id?: number | null
  cpf?: string | null
  numero_processo: string
  tribunal: string
  tipo?: string | null
  classe?: string | null
  assunto?: string | null
  vara?: string | null
  comarca?: string | null
  data_distribuicao?: string | null
  status?: string | null
  polo?: string | null
  url_consulta?: string | null
  ultima_movimentacao?: string | null
  data_ultima_movimentacao?: string | null
  coletado_em?: string | null
  atualizado_em?: string | null
}

export type ProcessosResponse = {
  processos: ProcessoJudicial[]
  total: number
  por_tribunal: Record<string, number>
  por_tipo: Record<string, number>
}

export type DoacaoEleitoral = {
  id: string
  politico_id?: number | null
  cpf_doador?: string | null
  nome_doador?: string | null
  cpf_candidato?: string | null
  nome_candidato?: string | null
  valor: number
  tipo_doacao?: string | null
  tipo_receita?: string | null
  eleicao: string
  turno?: number | null
  partido?: string | null
  cargo?: string | null
  uf?: string | null
  municipio?: string | null
  fonte?: string | null
  coletado_em?: string | null
}

export type DoacoesResponse = {
  doacoes: DoacaoEleitoral[]
  total: number
  valor_total: number
  por_eleicao: Record<string, number>
}

export type FiliacaoPartidaria = {
  id: string
  politico_id?: number | null
  cpf?: string | null
  nome?: string | null
  titulo_eleitoral?: string | null
  partido: string
  sigla_partido?: string | null
  data_filiacao?: string | null
  data_desfiliacao?: string | null
  data_cancelamento?: string | null
  situacao?: string | null
  motivo_cancelamento?: string | null
  uf?: string | null
  municipio?: string | null
  zona_eleitoral?: string | null
  secao_eleitoral?: string | null
  coletado_em?: string | null
}

export type FiliacoesResponse = {
  filiacoes: FiliacaoPartidaria[]
  total: number
  historico_partidos: string[]
}

export type Candidatura = {
  id: string
  politico_id?: number | null
  cpf?: string | null
  nome?: string | null
  nome_urna?: string | null
  numero_candidato?: string | null
  sequencial_candidato?: string | null
  eleicao: string
  turno?: number | null
  cargo?: string | null
  uf?: string | null
  municipio?: string | null
  partido?: string | null
  sigla_partido?: string | null
  coligacao?: string | null
  situacao_candidatura?: string | null
  situacao_totalizacao?: string | null
  total_votos?: number | null
  percentual_votos?: number | null
  ocupacao?: string | null
  grau_instrucao?: string | null
  genero?: string | null
  cor_raca?: string | null
  email?: string | null
  bens_declarados?: number | null
  url_foto?: string | null
  coletado_em?: string | null
}

export type CandidaturasResponse = {
  candidaturas: Candidatura[]
  total: number
  por_eleicao: Record<string, number>
}

export type ResumoProcessual = {
  politico_id: number
  nome: string
  cpf?: string | null
  total_processos: number
  processos_ativos: number
  total_doacoes_feitas: number
  valor_total_doado: number
  total_doacoes_recebidas: number
  valor_total_recebido: number
  total_candidaturas: number
  eleicoes_vencidas: number
  historico_partidos: string[]
  ultima_atualizacao?: string | null
}

export type ConsultaProcessualResponse = {
  politico_id: number
  nome: string
  cpf?: string | null
  fontes_consultadas: string[]
  urls_pendentes: Array<{
    fonte: string
    url?: string
    urls?: string[]
    instrucoes: string[]
  }>
  dados_coletados: Record<string, any>
}

export type ConsultaLog = {
  id: string
  politico_id?: number | null
  cpf?: string | null
  fonte: string
  tipo_consulta?: string | null
  status: string
  registros_encontrados: number
  mensagem?: string | null
  iniciado_em?: string | null
  finalizado_em?: string | null
}

