import { apiFetch } from './http'
import type {
  ColetaLog,
  Politico,
  PoliticoResumo,
  InstagramPost,
  Noticia,
  TrendingTopic,
  ScheduledJob,
  SocialMention,
  MentionTopic,
  PoliticoAssuntosResponse,
  ProcessosResponse,
  DoacoesResponse,
  FiliacoesResponse,
  CandidaturasResponse,
  ResumoProcessual,
  ConsultaProcessualResponse,
  ConsultaLog,
} from '../types'

export const portalApi = {
  health: () => apiFetch<{ status: string; version: string; timestamp: string }>('/health'),

  getPoliticos: () => apiFetch<Politico[]>('/politicos'),
  getPolitico: (id: number) => apiFetch<Politico>(`/politicos/${id}`),
  getResumo: (id: number) => apiFetch<PoliticoResumo>(`/politicos/${id}/resumo`),

  getNoticiasPolitico: (id: number, limit = 20, minScore = 0) =>
    apiFetch<Noticia[]>(`/politicos/${id}/noticias?limit=${limit}&min_score=${minScore}`),
  getInstagramPolitico: (id: number, limit = 10) =>
    apiFetch<InstagramPost[]>(`/politicos/${id}/instagram?limit=${limit}`),

  getTrending: (category?: 'politica' | 'twitter' | 'google') => 
    apiFetch<TrendingTopic[]>(`/trending${category ? `?category=${category}` : ''}`),
  getNoticiasPolitica: () => apiFetch<Noticia[]>('/noticias/politica'),
  getNoticiasEstado: (estado: string, limit = 30) => apiFetch<Noticia[]>(`/noticias/estado/${encodeURIComponent(estado)}?limit=${limit}`),
  getNoticiaAnalise: (noticiaId: string) =>
    apiFetch<{
      noticia_id: string
      pontos: any
      resumo_tecnico: string | null
      porque_pontuou: string[]
      hipoteses: string[]
      alertas: string[]
    }>(`/noticias/${noticiaId}/analise`),

  getSocialMentionsPolitico: (id: number, plataforma?: string | null, limit = 50) =>
    apiFetch<SocialMention[]>(
      `/politicos/${id}/social_mentions?limit=${limit}${plataforma ? `&plataforma=${encodeURIComponent(plataforma)}` : ''}`,
    ),
  getMentionTopicsPolitico: (id: number) => apiFetch<MentionTopic[]>(`/politicos/${id}/mention_topics`),
  getAssuntosPolitico: (id: number, limite = 10) =>
    apiFetch<PoliticoAssuntosResponse>(`/politicos/${id}/assuntos?limite=${limite}`),

  getColetaLogs: (limit = 50) => apiFetch<ColetaLog[]>(`/coleta/logs?limit=${limit}`),
  getColetaJobs: () => apiFetch<ScheduledJob[]>('/coleta/jobs'),
  executarColeta: (
    tipo: 'completa' | 'noticias' | 'instagram' | 'trending' | 'trending_twitter' | 'trending_google' | 'socials' | 'social_mentions' | 'processual_tse' = 'completa',
    dryRun = false,
  ) =>
    apiFetch<{ status: string; mensagem: string; log_id?: string | null }>(
      `/coleta/executar?tipo=${tipo}${dryRun ? '&dry_run=true' : ''}`,
      {
      method: 'POST',
      },
    ),

  // ==================== CONSULTA PROCESSUAL ====================
  
  getProcessosPolitico: (id: number, tribunal?: string, tipo?: string, status?: string, limit = 50) => {
    const params = new URLSearchParams()
    if (tribunal) params.append('tribunal', tribunal)
    if (tipo) params.append('tipo', tipo)
    if (status) params.append('status', status)
    params.append('limit', String(limit))
    return apiFetch<ProcessosResponse>(`/politicos/${id}/processos?${params.toString()}`)
  },

  getDoacoesPolitico: (id: number, tipo: 'feitas' | 'recebidas' | 'todas' = 'todas', eleicao?: string, limit = 100) => {
    const params = new URLSearchParams()
    params.append('tipo', tipo)
    if (eleicao) params.append('eleicao', eleicao)
    params.append('limit', String(limit))
    return apiFetch<DoacoesResponse>(`/politicos/${id}/doacoes?${params.toString()}`)
  },

  getFiliacoesPolitico: (id: number) => 
    apiFetch<FiliacoesResponse>(`/politicos/${id}/filiacoes`),

  getCandidaturasPolitico: (id: number, eleicao?: string) => {
    const params = eleicao ? `?eleicao=${eleicao}` : ''
    return apiFetch<CandidaturasResponse>(`/politicos/${id}/candidaturas${params}`)
  },

  getResumoProcessual: (id: number) => 
    apiFetch<ResumoProcessual>(`/politicos/${id}/resumo-processual`),

  executarConsultaProcessual: (id: number, fontes: string[] = ['TSE']) =>
    apiFetch<ConsultaProcessualResponse>(
      `/politicos/${id}/consulta-processual?fontes=${fontes.join(',')}`,
      { method: 'POST' }
    ),

  getConsultaProcessualLogs: (politicoId?: number, fonte?: string, limit = 50) => {
    const params = new URLSearchParams()
    if (politicoId) params.append('politico_id', String(politicoId))
    if (fonte) params.append('fonte', fonte)
    params.append('limit', String(limit))
    return apiFetch<ConsultaLog[]>(`/consulta-processual/logs?${params.toString()}`)
  },

  atualizarCpfPolitico: (id: number, cpf: string) =>
    apiFetch<{ status: string; mensagem: string }>(
      `/politicos/${id}/cpf?cpf=${encodeURIComponent(cpf)}`,
      { method: 'PUT' }
    ),
}

