/**
 * API que conecta diretamente ao Supabase.
 * Substitui as chamadas ao backend Python para leitura de dados.
 */
import { supabase, getPoliticoUuid } from '../lib/supabase'
import type {
  Politico,
  PoliticoResumo,
  Noticia,
  InstagramPost,
  TrendingTopic,
  SocialMention,
  MentionTopic,
  PoliticoAssuntosResponse,
  AssuntoStats,
  ProcessosResponse,
  DoacoesResponse,
  FiliacoesResponse,
  CandidaturasResponse,
  ResumoProcessual,
  ColetaLog,
  ConsultaLog,
  ConcorrenteResumo,
} from '../types'

// ==================== HELPERS ====================

/**
 * Diversifica notícias por fonte (algoritmo round-robin)
 */
function diversificarNoticiasPorFonte(noticias: Noticia[], limit: number): Noticia[] {
  if (!noticias.length || noticias.length <= limit) return noticias.slice(0, limit)

  // Agrupa por fonte
  const porFonte = new Map<string, Noticia[]>()
  for (const n of noticias) {
    const fonte = n.fonte_nome || n.fonte_id || 'desconhecida'
    if (!porFonte.has(fonte)) porFonte.set(fonte, [])
    porFonte.get(fonte)!.push(n)
  }

  if (porFonte.size <= 1) return noticias.slice(0, limit)

  // Ordena fontes pelo melhor score
  const fontesOrdenadas = [...porFonte.keys()].sort((a, b) => {
    const maxA = Math.max(...porFonte.get(a)!.map(n => n.relevancia_total || 0))
    const maxB = Math.max(...porFonte.get(b)!.map(n => n.relevancia_total || 0))
    return maxB - maxA
  })

  // Round-robin entre fontes
  const resultado: Noticia[] = []
  const urlsVistas = new Set<string>()
  const indices = new Map(fontesOrdenadas.map(f => [f, 0]))

  while (resultado.length < limit) {
    let adicionou = false
    for (const fonte of fontesOrdenadas) {
      if (resultado.length >= limit) break
      const lista = porFonte.get(fonte)!
      let idx = indices.get(fonte)!
      while (idx < lista.length) {
        const n = lista[idx]
        if (!urlsVistas.has(n.url)) {
          urlsVistas.add(n.url)
          resultado.push(n)
          indices.set(fonte, idx + 1)
          adicionou = true
          break
        }
        idx++
        indices.set(fonte, idx)
      }
    }
    if (!adicionou) break
  }

  return resultado
}

// ==================== POLÍTICOS ====================

async function getPoliticos(): Promise<Politico[]> {
  const { data, error } = await supabase
    .from('politico')
    .select('*')
    .eq('usar_diretoriaja', true)
    .order('id')

  if (error) throw new Error(`Erro ao buscar políticos: ${error.message}`)
  return data || []
}

async function getPolitico(id: number): Promise<Politico> {
  const { data, error } = await supabase
    .from('politico')
    .select('*')
    .eq('id', id)
    .single()

  if (error) throw new Error(`Político não encontrado: ${error.message}`)
  return data
}

async function getConcorrentes(politicoId: number): Promise<Politico[]> {
  const { data, error } = await supabase
    .from('politico_concorrentes')
    .select('concorrente_id, politico!politico_concorrentes_concorrente_id_fkey(*)')
    .eq('politico_id', politicoId)

  if (error) throw new Error(`Erro ao buscar concorrentes: ${error.message}`)
  return (data || []).map((item: any) => item.politico).filter(Boolean)
}

// ==================== NOTÍCIAS ====================

async function getNoticiasPolitico(
  id: number,
  limit = 20,
  minScore = 0,
  diversificar = true
): Promise<Noticia[]> {
  const uuid = await getPoliticoUuid(id)
  if (!uuid) return []

  const fetchLimit = diversificar ? limit * 5 : limit

  const { data, error } = await supabase
    .from('noticias')
    .select('*')
    .eq('politico_id', uuid)
    .gte('relevancia_total', minScore)
    .order('relevancia_total', { ascending: false })
    .limit(fetchLimit)

  if (error) throw new Error(`Erro ao buscar notícias: ${error.message}`)

  const noticias = data || []
  return diversificar ? diversificarNoticiasPorFonte(noticias, limit) : noticias.slice(0, limit)
}

async function getNoticiasPolitica(): Promise<Noticia[]> {
  const { data, error } = await supabase
    .from('noticias')
    .select('*')
    .eq('tipo', 'geral')
    .order('relevancia_total', { ascending: false })
    .limit(30)

  if (error) throw new Error(`Erro ao buscar notícias políticas: ${error.message}`)
  return data || []
}

async function getNoticiasEstado(estado: string, limit = 30): Promise<Noticia[]> {
  const { data, error } = await supabase
    .from('noticias')
    .select('*')
    .eq('tipo', 'estado')
    .eq('estado', estado.toUpperCase())
    .order('relevancia_total', { ascending: false })
    .limit(limit)

  if (error) throw new Error(`Erro ao buscar notícias do estado: ${error.message}`)
  return data || []
}

async function getNoticiasCapital(estado: string, limit = 3): Promise<Noticia[]> {
  const { data, error } = await supabase
    .from('noticias')
    .select('*')
    .eq('tipo', 'cidade')
    .eq('estado', estado.toUpperCase())
    .not('cidade', 'is', null)
    .order('relevancia_total', { ascending: false })
    .limit(limit)

  if (error) throw new Error(`Erro ao buscar notícias da capital: ${error.message}`)
  return data || []
}

async function getNoticiasTodasCapitais(limitPorCapital = 3): Promise<{
  noticias_por_estado: Record<string, Noticia[]>
  estados_com_noticias: string[]
  total_estados: number
}> {
  const { data, error } = await supabase
    .from('noticias')
    .select('*')
    .eq('tipo', 'cidade')
    .not('cidade', 'is', null)
    .order('relevancia_total', { ascending: false })

  if (error) throw new Error(`Erro ao buscar notícias das capitais: ${error.message}`)

  const noticiasPorEstado: Record<string, Noticia[]> = {}
  for (const noticia of data || []) {
    const estado = noticia.estado
    if (estado) {
      if (!noticiasPorEstado[estado]) noticiasPorEstado[estado] = []
      if (noticiasPorEstado[estado].length < limitPorCapital) {
        noticiasPorEstado[estado].push(noticia)
      }
    }
  }

  return {
    noticias_por_estado: noticiasPorEstado,
    estados_com_noticias: Object.keys(noticiasPorEstado),
    total_estados: Object.keys(noticiasPorEstado).length,
  }
}

// ==================== INSTAGRAM ====================

async function getInstagramPolitico(id: number, limit = 10): Promise<InstagramPost[]> {
  const uuid = await getPoliticoUuid(id)
  if (!uuid) return []

  // Tenta tabela unificada primeiro
  let { data, error } = await supabase
    .from('social_media_posts')
    .select('*')
    .eq('politico_id', uuid)
    .eq('plataforma', 'instagram')
    .order('engagement_score', { ascending: false })
    .limit(limit)

  if (error || !data?.length) {
    // Fallback para tabela legada
    const legacy = await supabase
      .from('instagram_posts')
      .select('*')
      .eq('politico_id', uuid)
      .order('engagement_score', { ascending: false })
      .limit(limit)

    if (legacy.error) throw new Error(`Erro ao buscar posts: ${legacy.error.message}`)
    return legacy.data || []
  }

  return data || []
}

// ==================== TRENDING ====================

async function getTrending(category?: 'politica' | 'twitter' | 'google'): Promise<TrendingTopic[]> {
  let query = supabase
    .from('portal_trending_topics')
    .select('*')

  if (category) {
    query = query.eq('category', category)
  }

  const { data, error } = await query.order('category').order('rank')

  if (error) throw new Error(`Erro ao buscar trending: ${error.message}`)
  return data || []
}

// ==================== MENÇÕES SOCIAIS ====================

async function getSocialMentionsPolitico(
  id: number,
  plataforma?: string | null,
  limit = 50
): Promise<SocialMention[]> {
  const uuid = await getPoliticoUuid(id)
  if (!uuid) return []

  let query = supabase
    .from('social_mentions')
    .select('*')
    .eq('politico_id', uuid)

  if (plataforma) {
    query = query.eq('plataforma', plataforma)
  }

  const { data, error } = await query
    .order('engagement_score', { ascending: false })
    .limit(limit)

  if (error) throw new Error(`Erro ao buscar menções: ${error.message}`)
  return data || []
}

async function getMentionTopicsPolitico(id: number): Promise<MentionTopic[]> {
  const uuid = await getPoliticoUuid(id)
  if (!uuid) return []

  const { data, error } = await supabase
    .from('mention_topics')
    .select('*')
    .eq('politico_id', uuid)
    .order('total_mencoes', { ascending: false })

  if (error) throw new Error(`Erro ao buscar tópicos: ${error.message}`)
  return data || []
}

async function getAssuntosPolitico(id: number, limite = 10): Promise<PoliticoAssuntosResponse> {
  const politico = await getPolitico(id)
  const uuid = await getPoliticoUuid(id)
  if (!uuid) {
    return { politico_id: id, nome: politico.name, periodo: 'últimos registros', assuntos: [] }
  }

  const { data, error } = await supabase
    .from('mention_topics')
    .select('*')
    .eq('politico_id', uuid)
    .order('total_mencoes', { ascending: false })
    .limit(limite)

  if (error) throw new Error(`Erro ao buscar assuntos: ${error.message}`)

  const assuntos: AssuntoStats[] = (data || []).map((t: any) => {
    const total = t.total_mencoes || 0
    const pos = t.mencoes_positivas || 0
    const neg = t.mencoes_negativas || 0
    const neu = t.mencoes_neutras || 0
    const eng = t.engagement_total || 0

    let sentimento = 'neutro'
    if (pos >= neg && pos >= neu) sentimento = 'positivo'
    else if (neg >= pos && neg >= neu) sentimento = 'negativo'

    return {
      assunto: t.assunto,
      total_mencoes: total,
      mencoes_positivas: pos,
      mencoes_negativas: neg,
      mencoes_neutras: neu,
      sentimento_predominante: sentimento,
      engagement_total: eng,
      exemplo: null,
    }
  })

  return {
    politico_id: id,
    nome: politico.name,
    periodo: 'últimos registros',
    assuntos,
  }
}

// ==================== RESUMO ====================

async function getResumo(id: number): Promise<PoliticoResumo> {
  const politico = await getPolitico(id)
  const uuid = await getPoliticoUuid(id)

  // Busca dados em paralelo
  const [noticias, instagram, concorrentes, noticiasEstado, noticiasCapital, noticiasCidade] =
    await Promise.all([
      getNoticiasPolitico(id, 5, 30, true),
      getInstagramPolitico(id, 5),
      getConcorrentes(id),
      politico.estado ? getNoticiasNivelEstado(politico.estado, 3) : Promise.resolve([]),
      politico.estado ? getNoticiasCapital(politico.estado, 3) : Promise.resolve([]),
      politico.cidade ? getNoticiasCidade(politico.cidade, 5) : Promise.resolve([]),
    ])

  // Contagens
  let totalNoticias = 0
  let totalInstagram = 0
  let totalMencoes = 0

  if (uuid) {
    const [countNoticias, countInstagram, countMencoes] = await Promise.all([
      supabase.from('noticias').select('id', { count: 'exact', head: true }).eq('politico_id', uuid),
      supabase.from('social_media_posts').select('id', { count: 'exact', head: true }).eq('politico_id', uuid).eq('plataforma', 'instagram'),
      supabase.from('social_mentions').select('id', { count: 'exact', head: true }).eq('politico_id', uuid),
    ])
    totalNoticias = countNoticias.count || 0
    totalInstagram = countInstagram.count || 0
    totalMencoes = countMencoes.count || 0
  }

  return {
    politico,
    top_noticias: noticias,
    top_instagram: instagram,
    concorrentes,
    noticias_cidade: noticiasCidade,
    noticias_estado: noticiasEstado,
    noticias_capital: noticiasCapital,
    total_noticias: totalNoticias,
    total_posts_instagram: totalInstagram,
    total_mencoes: totalMencoes,
  }
}

async function getNoticiasCidade(cidade: string, limit = 20): Promise<Noticia[]> {
  const { data, error } = await supabase
    .from('noticias')
    .select('*')
    .eq('cidade', cidade)
    .order('relevancia_total', { ascending: false })
    .limit(limit)

  if (error) throw new Error(`Erro ao buscar notícias da cidade: ${error.message}`)
  return data || []
}

async function getNoticiasNivelEstado(estado: string, limit = 3): Promise<Noticia[]> {
  const { data, error } = await supabase
    .from('noticias')
    .select('*')
    .eq('tipo', 'estado')
    .eq('estado', estado.toUpperCase())
    .is('cidade', null)
    .order('relevancia_total', { ascending: false })
    .limit(limit)

  if (error) throw new Error(`Erro ao buscar notícias do estado: ${error.message}`)
  return data || []
}

async function getResumoConcorrentes(politicoId: number, limitNoticias = 5): Promise<ConcorrenteResumo[]> {
  const concorrentes = await getConcorrentes(politicoId)
  if (!concorrentes.length) return []

  const resultado: ConcorrenteResumo[] = []

  for (const c of concorrentes) {
    const uuid = await getPoliticoUuid(c.id)
    if (!uuid) continue

    const [noticias, instagram] = await Promise.all([
      getNoticiasPolitico(c.id, limitNoticias, 0, true),
      getInstagramPolitico(c.id, 3),
    ])

    const [countNoticias, countInstagram] = await Promise.all([
      supabase.from('noticias').select('id', { count: 'exact', head: true }).eq('politico_id', uuid),
      supabase.from('social_media_posts').select('id', { count: 'exact', head: true }).eq('politico_id', uuid).eq('plataforma', 'instagram'),
    ])

    resultado.push({
      politico: c,
      noticias,
      total_noticias: countNoticias.count || 0,
      instagram,
      total_instagram: countInstagram.count || 0,
    })
  }

  return resultado
}

// ==================== CONSULTA PROCESSUAL ====================

async function getProcessosPolitico(
  id: number,
  tribunal?: string,
  tipo?: string,
  status?: string,
  limit = 50
): Promise<ProcessosResponse> {
  const uuid = await getPoliticoUuid(id)
  if (!uuid) return { processos: [], total: 0, por_tribunal: {}, por_tipo: {} }

  let query = supabase
    .from('processos_judiciais')
    .select('*')
    .eq('politico_id', uuid)

  if (tribunal) query = query.eq('tribunal', tribunal.toUpperCase())
  if (tipo) query = query.eq('tipo', tipo)
  if (status) query = query.eq('status', status)

  const { data, error } = await query
    .order('coletado_em', { ascending: false })
    .limit(limit)

  if (error) throw new Error(`Erro ao buscar processos: ${error.message}`)

  const processos = data || []
  const porTribunal: Record<string, number> = {}
  const porTipo: Record<string, number> = {}

  for (const p of processos) {
    const t = p.tribunal || 'outros'
    const tp = p.tipo || 'outros'
    porTribunal[t] = (porTribunal[t] || 0) + 1
    porTipo[tp] = (porTipo[tp] || 0) + 1
  }

  return { processos, total: processos.length, por_tribunal: porTribunal, por_tipo: porTipo }
}

async function getDoacoesPolitico(
  id: number,
  tipo: 'feitas' | 'recebidas' | 'todas' = 'todas',
  eleicao?: string,
  limit = 100
): Promise<DoacoesResponse> {
  const politico = await getPolitico(id)
  const cpf = politico.cpf

  if (!cpf) return { doacoes: [], total: 0, valor_total: 0, por_eleicao: {} }

  const doacoes: any[] = []

  if (tipo === 'feitas' || tipo === 'todas') {
    let query = supabase.from('doacoes_eleitorais').select('*').eq('cpf_doador', cpf)
    if (eleicao) query = query.eq('eleicao', eleicao)
    const { data } = await query.limit(limit)
    doacoes.push(...(data || []))
  }

  if (tipo === 'recebidas' || tipo === 'todas') {
    let query = supabase.from('doacoes_eleitorais').select('*').eq('cpf_candidato', cpf)
    if (eleicao) query = query.eq('eleicao', eleicao)
    const { data } = await query.limit(limit)
    doacoes.push(...(data || []))
  }

  const porEleicao: Record<string, number> = {}
  let valorTotal = 0

  for (const d of doacoes) {
    const e = d.eleicao || 'outros'
    porEleicao[e] = (porEleicao[e] || 0) + 1
    valorTotal += parseFloat(d.valor || 0)
  }

  return {
    doacoes,
    total: doacoes.length,
    valor_total: Math.round(valorTotal * 100) / 100,
    por_eleicao: porEleicao,
  }
}

async function getFiliacoesPolitico(id: number): Promise<FiliacoesResponse> {
  const uuid = await getPoliticoUuid(id)
  if (!uuid) return { filiacoes: [], total: 0, historico_partidos: [] }

  const { data, error } = await supabase
    .from('filiacoes_partidarias')
    .select('*')
    .eq('politico_id', uuid)
    .order('data_filiacao', { ascending: false })

  if (error) throw new Error(`Erro ao buscar filiações: ${error.message}`)

  const filiacoes = data || []
  const partidos = [...new Set(filiacoes.map(f => f.sigla_partido || f.partido).filter(Boolean))]

  return { filiacoes, total: filiacoes.length, historico_partidos: partidos }
}

async function getCandidaturasPolitico(id: number, eleicao?: string): Promise<CandidaturasResponse> {
  const uuid = await getPoliticoUuid(id)
  if (!uuid) return { candidaturas: [], total: 0, por_eleicao: {} }

  let query = supabase.from('candidaturas').select('*').eq('politico_id', uuid)
  if (eleicao) query = query.eq('eleicao', eleicao)

  const { data, error } = await query.order('eleicao', { ascending: false })

  if (error) throw new Error(`Erro ao buscar candidaturas: ${error.message}`)

  const candidaturas = data || []
  const porEleicao: Record<string, number> = {}

  for (const c of candidaturas) {
    const e = c.eleicao || 'outros'
    porEleicao[e] = (porEleicao[e] || 0) + 1
  }

  return { candidaturas, total: candidaturas.length, por_eleicao: porEleicao }
}

async function getResumoProcessual(id: number): Promise<ResumoProcessual> {
  const politico = await getPolitico(id)
  const uuid = await getPoliticoUuid(id)
  const cpf = politico.cpf

  let totalProcessos = 0
  let processosAtivos = 0
  let totalDoacoesFeitas = 0
  let valorTotalDoado = 0
  let totalDoacoesRecebidas = 0
  let valorTotalRecebido = 0
  let totalCandidaturas = 0
  let eleicoesVencidas = 0
  let partidos: string[] = []
  let ultimaAtualizacao: string | null = null

  if (uuid) {
    // Processos
    const { data: processos } = await supabase
      .from('processos_judiciais')
      .select('status')
      .eq('politico_id', uuid)

    if (processos) {
      totalProcessos = processos.length
      processosAtivos = processos.filter(p => p.status === 'ativo').length
    }

    // Candidaturas
    const { data: candidaturas } = await supabase
      .from('candidaturas')
      .select('situacao_totalizacao')
      .eq('politico_id', uuid)

    if (candidaturas) {
      totalCandidaturas = candidaturas.length
      eleicoesVencidas = candidaturas.filter(c =>
        c.situacao_totalizacao?.toLowerCase().includes('eleito')
      ).length
    }

    // Filiações
    const { data: filiacoes } = await supabase
      .from('filiacoes_partidarias')
      .select('sigla_partido')
      .eq('politico_id', uuid)

    if (filiacoes) {
      partidos = [...new Set(filiacoes.map(f => f.sigla_partido).filter(Boolean))]
    }

    // Última consulta
    const { data: logs } = await supabase
      .from('consulta_processual_logs')
      .select('iniciado_em')
      .eq('politico_id', uuid)
      .order('iniciado_em', { ascending: false })
      .limit(1)

    if (logs?.[0]) {
      ultimaAtualizacao = logs[0].iniciado_em
    }
  }

  // Doações (usa CPF)
  if (cpf) {
    const { data: doacoesFeitas } = await supabase
      .from('doacoes_eleitorais')
      .select('valor')
      .eq('cpf_doador', cpf)

    if (doacoesFeitas) {
      totalDoacoesFeitas = doacoesFeitas.length
      valorTotalDoado = doacoesFeitas.reduce((acc, d) => acc + parseFloat(d.valor || 0), 0)
    }

    const { data: doacoesRecebidas } = await supabase
      .from('doacoes_eleitorais')
      .select('valor')
      .eq('cpf_candidato', cpf)

    if (doacoesRecebidas) {
      totalDoacoesRecebidas = doacoesRecebidas.length
      valorTotalRecebido = doacoesRecebidas.reduce((acc, d) => acc + parseFloat(d.valor || 0), 0)
    }
  }

  return {
    politico_id: id,
    nome: politico.name,
    cpf: cpf ? `${cpf.slice(0, 3)}***${cpf.slice(-2)}` : null,
    total_processos: totalProcessos,
    processos_ativos: processosAtivos,
    total_doacoes_feitas: totalDoacoesFeitas,
    valor_total_doado: Math.round(valorTotalDoado * 100) / 100,
    total_doacoes_recebidas: totalDoacoesRecebidas,
    valor_total_recebido: Math.round(valorTotalRecebido * 100) / 100,
    total_candidaturas: totalCandidaturas,
    eleicoes_vencidas: eleicoesVencidas,
    historico_partidos: partidos,
    ultima_atualizacao: ultimaAtualizacao,
  }
}

async function getConsultaProcessualLogs(
  politicoId?: number,
  fonte?: string,
  limit = 50
): Promise<ConsultaLog[]> {
  let query = supabase.from('consulta_processual_logs').select('*')

  if (politicoId) {
    const uuid = await getPoliticoUuid(politicoId)
    if (uuid) query = query.eq('politico_id', uuid)
    else return []
  }

  if (fonte) query = query.eq('fonte', fonte)

  const { data, error } = await query
    .order('iniciado_em', { ascending: false })
    .limit(limit)

  if (error) throw new Error(`Erro ao buscar logs: ${error.message}`)
  return data || []
}

// ==================== ADMIN (APENAS LEITURA) ====================

async function getColetaLogs(limit = 50): Promise<ColetaLog[]> {
  const { data, error } = await supabase
    .from('coleta_logs')
    .select('*')
    .order('iniciado_em', { ascending: false })
    .limit(limit)

  if (error) throw new Error(`Erro ao buscar logs: ${error.message}`)
  return data || []
}

// ==================== ANÁLISE DE NOTÍCIA (SIMPLIFICADA) ====================

async function getNoticiaAnalise(noticiaId: string): Promise<{
  noticia_id: string
  pontos: {
    pesos: Record<string, number>
    scores: Record<string, number>
    contribuicoes: Record<string, number>
    relevancia_calculada: number
    detalhes: Record<string, unknown>
  }
  resumo_tecnico: string | null
  porque_pontuou: string[]
  hipoteses: string[]
  alertas: string[]
}> {
  const { data, error } = await supabase
    .from('noticias')
    .select('*')
    .eq('id', noticiaId)
    .single()

  if (error || !data) throw new Error('Notícia não encontrada')

  // Pesos padrão do sistema de relevância
  const pesos = { recencia: 0.25, mencao: 0.35, fonte: 0.25, engajamento: 0.15 }

  const scores = {
    recencia: data.score_recencia || 0,
    mencao: data.score_mencao || 0,
    fonte: data.score_fonte || 0,
    engajamento: data.score_engajamento || 0,
    relevancia_total: data.relevancia_total || 0,
  }

  const contribuicoes = {
    recencia: scores.recencia * pesos.recencia,
    mencao: scores.mencao * pesos.mencao,
    fonte: scores.fonte * pesos.fonte,
    engajamento: scores.engajamento * pesos.engajamento,
  }

  const relevanciaCalculada =
    contribuicoes.recencia + contribuicoes.mencao + contribuicoes.fonte + contribuicoes.engajamento

  const porquePontuou: string[] = []
  if (data.mencao_titulo) porquePontuou.push('Nome mencionado diretamente no título')
  if (data.mencao_conteudo && data.mencao_conteudo > 0)
    porquePontuou.push(`Nome mencionado ${data.mencao_conteudo}x no conteúdo`)
  if (scores.fonte > 70) porquePontuou.push('Fonte de alta confiabilidade')
  if (scores.recencia > 80) porquePontuou.push('Notícia muito recente')

  return {
    noticia_id: noticiaId,
    pontos: {
      pesos,
      scores,
      contribuicoes,
      relevancia_calculada: relevanciaCalculada,
      detalhes: {
        mencao_titulo: data.mencao_titulo,
        mencao_conteudo: data.mencao_conteudo,
        fonte_nome: data.fonte_nome,
      },
    },
    resumo_tecnico: null, // Sem OpenAI no frontend
    porque_pontuou: porquePontuou,
    hipoteses: [],
    alertas: ['Análise simplificada (sem IA). O resumo técnico requer o backend Python.'],
  }
}

// ==================== EXPORTAÇÃO ====================

export const portalApi = {
  // Health (sempre ok para Supabase direto)
  health: async () => ({ status: 'ok', version: 'supabase-direct', timestamp: new Date().toISOString() }),

  // Políticos
  getPoliticos,
  getPolitico,
  getResumo,

  // Notícias
  getNoticiasPolitico,
  getNoticiasPolitica,
  getNoticiasEstado,
  getNoticiasCapital,
  getNoticiasTodasCapitais,
  getNoticiaAnalise,

  // Concorrentes
  getResumoConcorrentes,

  // Instagram
  getInstagramPolitico,

  // Trending
  getTrending,

  // Menções sociais
  getSocialMentionsPolitico,
  getMentionTopicsPolitico,
  getAssuntosPolitico,

  // Consulta processual
  getProcessosPolitico,
  getDoacoesPolitico,
  getFiliacoesPolitico,
  getCandidaturasPolitico,
  getResumoProcessual,
  getConsultaProcessualLogs,

  // Admin (apenas leitura)
  getColetaLogs,
  
  // Funções que NÃO estão disponíveis no frontend direto (requerem backend Python)
  // Estas funções existem apenas para compatibilidade e lançam erro informativo
  getColetaJobs: async () => {
    console.warn('getColetaJobs não disponível: os jobs rodam no servidor Python')
    return []
  },
  executarColeta: async () => {
    throw new Error('Execução de coleta não disponível. Use o servidor Python para executar coletas.')
  },
  executarConsultaProcessual: async () => {
    throw new Error('Consulta processual não disponível. Use o servidor Python para executar consultas.')
  },
  atualizarCpfPolitico: async () => {
    throw new Error('Atualização de CPF não disponível diretamente. Configure RLS no Supabase ou use o servidor Python.')
  },
}
