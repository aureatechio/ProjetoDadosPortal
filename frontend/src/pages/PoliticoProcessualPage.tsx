import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { portalApi } from '../api/portalApi'
import type {
  ResumoProcessual,
  ProcessosResponse,
  DoacoesResponse,
  FiliacoesResponse,
  CandidaturasResponse,
  Politico,
  ConsultaLog,
} from '../types'
import { Loading } from '../components/Loading'
import { ErrorState } from '../components/ErrorState'

type TabType = 'resumo' | 'processos' | 'doacoes' | 'filiacoes' | 'candidaturas'

export function PoliticoProcessualPage() {
  const { id } = useParams()
  const politicoId = Number(id)

  const [politico, setPolitico] = useState<Politico | null>(null)
  const [tab, setTab] = useState<TabType>('resumo')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Data states
  const [resumo, setResumo] = useState<ResumoProcessual | null>(null)
  const [processos, setProcessos] = useState<ProcessosResponse | null>(null)
  const [doacoes, setDoacoes] = useState<DoacoesResponse | null>(null)
  const [filiacoes, setFiliacoes] = useState<FiliacoesResponse | null>(null)
  const [candidaturas, setCandidaturas] = useState<CandidaturasResponse | null>(null)

  // Filtros de doações
  const [doacaoTipo, setDoacaoTipo] = useState<'feitas' | 'recebidas' | 'todas'>('todas')

  // Consulta processual
  const [consultando, setConsultando] = useState(false)
  const [consultaResult, setConsultaResult] = useState<any>(null)
  const [cpfInput, setCpfInput] = useState('')
  const [cpfSaving, setCpfSaving] = useState(false)
  const [cpfMsg, setCpfMsg] = useState<string | null>(null)
  const [consultaLogs, setConsultaLogs] = useState<ConsultaLog[] | null>(null)

  async function loadPolitico() {
    try {
      const p = await portalApi.getPolitico(politicoId)
      setPolitico(p)
    } catch {
      setError('Político não encontrado')
    }
  }

  async function loadResumo() {
    setLoading(true)
    setError(null)
    try {
      const data = await portalApi.getResumoProcessual(politicoId)
      setResumo(data)
      // Se já existe CPF (mascarado), não sobrescreve input; senão limpa mensagem
      setCpfMsg(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar resumo processual')
    } finally {
      setLoading(false)
    }
  }

  async function loadConsultaLogs() {
    try {
      const logs = await portalApi.getConsultaProcessualLogs(politicoId, undefined, 50)
      setConsultaLogs(logs)
    } catch {
      setConsultaLogs(null)
    }
  }

  async function loadProcessos() {
    setLoading(true)
    try {
      const data = await portalApi.getProcessosPolitico(politicoId)
      setProcessos(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar processos')
    } finally {
      setLoading(false)
    }
  }

  async function loadDoacoes(tipo: 'feitas' | 'recebidas' | 'todas' = 'todas') {
    setLoading(true)
    try {
      const data = await portalApi.getDoacoesPolitico(politicoId, tipo)
      setDoacoes(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar doações')
    } finally {
      setLoading(false)
    }
  }

  async function loadFiliacoes() {
    setLoading(true)
    try {
      const data = await portalApi.getFiliacoesPolitico(politicoId)
      setFiliacoes(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar filiações')
    } finally {
      setLoading(false)
    }
  }

  async function loadCandidaturas() {
    setLoading(true)
    try {
      const data = await portalApi.getCandidaturasPolitico(politicoId)
      setCandidaturas(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha ao carregar candidaturas')
    } finally {
      setLoading(false)
    }
  }

  async function executarConsulta() {
    setConsultando(true)
    setConsultaResult(null)
    try {
      const result = await portalApi.executarConsultaProcessual(politicoId, ['TSE', 'TJSP', 'TRF3', 'DOE'])
      setConsultaResult(result)
      // Recarrega resumo após consulta
      await loadResumo()
      await loadConsultaLogs()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Falha na consulta processual')
    } finally {
      setConsultando(false)
    }
  }

  async function salvarCpf() {
    const raw = cpfInput.trim()
    if (!raw) return
    setCpfSaving(true)
    setCpfMsg(null)
    try {
      const res = await portalApi.atualizarCpfPolitico(politicoId, raw)
      setCpfMsg(res.mensagem || 'CPF atualizado.')
      setCpfInput('')
      await loadResumo()
      await loadConsultaLogs()
    } catch (e) {
      setCpfMsg(e instanceof Error ? e.message : 'Falha ao atualizar CPF')
    } finally {
      setCpfSaving(false)
    }
  }

  useEffect(() => {
    if (!Number.isFinite(politicoId)) return
    void loadPolitico()
    void loadResumo()
    void loadConsultaLogs()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [politicoId])

  useEffect(() => {
    if (!Number.isFinite(politicoId)) return
    if (tab === 'processos' && !processos) void loadProcessos()
    if (tab === 'doacoes' && !doacoes) void loadDoacoes(doacaoTipo)
    if (tab === 'filiacoes' && !filiacoes) void loadFiliacoes()
    if (tab === 'candidaturas' && !candidaturas) void loadCandidaturas()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, politicoId])

  useEffect(() => {
    if (tab === 'doacoes') void loadDoacoes(doacaoTipo)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [doacaoTipo])

  if (!Number.isFinite(politicoId)) return <ErrorState title="ID inválido" message="Político não encontrado." />
  if (error && !resumo) return <ErrorState message={error} onRetry={loadResumo} />

  const formatCurrency = (value: number) =>
    new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)

  const formatDate = (dateStr: string | null | undefined) => {
    if (!dateStr) return '—'
    try {
      return new Date(dateStr).toLocaleDateString('pt-BR')
    } catch {
      return dateStr
    }
  }

  return (
    <div className="stack gap-16">
      {/* Header */}
      <section className="hero">
        <div className="row space-between gap-12 wrap">
          <div>
            <div className="row gap-8 align-center">
              <Link to={`/politicos/${politicoId}`} className="link">← Voltar</Link>
            </div>
            <h1>{politico?.name ?? 'Carregando...'}</h1>
            <p className="muted">Consulta Processual e Eleitoral</p>
            {resumo?.cpf && <span className="pill">CPF: {resumo.cpf}</span>}
          </div>
          <div className="stack gap-8" style={{ minWidth: 280 }}>
            <div className="row gap-8 wrap">
              <button className="btn primary" onClick={executarConsulta} disabled={consultando}>
                {consultando ? 'Consultando...' : 'Atualizar Dados'}
              </button>
              <button className="btn secondary" onClick={() => void loadConsultaLogs()} disabled={consultando}>
                Atualizar logs
              </button>
            </div>

            {!resumo?.cpf ? (
              <div className="card" style={{ padding: 10 }}>
                <div className="card-title">Cadastrar CPF (para consulta)</div>
                <div className="row gap-8 wrap" style={{ marginTop: 8 }}>
                  <input
                    className="input"
                    value={cpfInput}
                    onChange={(e) => setCpfInput(e.target.value)}
                    placeholder="CPF (11 dígitos)"
                    aria-label="CPF do político"
                  />
                  <button type="button" className="btn" onClick={() => void salvarCpf()} disabled={cpfSaving || !cpfInput.trim()}>
                    {cpfSaving ? 'Salvando…' : 'Salvar'}
                  </button>
                </div>
                {cpfMsg ? <div className="muted" style={{ marginTop: 8 }}>{cpfMsg}</div> : <div className="muted" style={{ marginTop: 8 }}>O CPF fica armazenado no banco (o resumo exibe mascarado).</div>}
              </div>
            ) : cpfMsg ? (
              <div className="muted">{cpfMsg}</div>
            ) : null}
          </div>
        </div>
      </section>

      {/* Resultado da consulta */}
      {consultaResult && (
        <section className="card">
          <h3>Resultado da Consulta</h3>
          <p className="muted">Fontes consultadas: {consultaResult.fontes_consultadas?.join(', ')}</p>
          
          {consultaResult.urls_pendentes?.length > 0 && (
            <div className="stack gap-8" style={{ marginTop: '12px' }}>
              <strong>URLs para consulta manual (CAPTCHA necessário):</strong>
              {consultaResult.urls_pendentes.map((item: any, idx: number) => (
                <div key={idx} className="card" style={{ padding: '8px' }}>
                  <strong>{item.fonte}</strong>
                  {item.url && (
                    <div>
                      <a href={item.url} target="_blank" rel="noopener noreferrer" className="link">
                        {item.url.substring(0, 80)}...
                      </a>
                    </div>
                  )}
                  {item.urls?.map((url: string, i: number) => (
                    <div key={i}>
                      <a href={url} target="_blank" rel="noopener noreferrer" className="link">
                        {url.substring(0, 80)}...
                      </a>
                    </div>
                  ))}
                </div>
              ))}
            </div>
          )}
        </section>
      )}

      {/* Tabs */}
      <div className="row gap-8 wrap">
        {(['resumo', 'processos', 'doacoes', 'filiacoes', 'candidaturas'] as TabType[]).map((t) => (
          <button
            key={t}
            className={`btn ${tab === t ? 'primary' : ''}`}
            onClick={() => setTab(t)}
          >
            {t.charAt(0).toUpperCase() + t.slice(1)}
          </button>
        ))}
      </div>

      {/* Content */}
      {loading ? (
        <Loading label="Carregando..." />
      ) : (
        <>
          {/* Resumo */}
          {tab === 'resumo' && resumo && (
            <section className="grid two">
              <div className="card">
                <h3>Processos Judiciais</h3>
                <div className="stat">
                  <div className="stat-value">{resumo.total_processos}</div>
                  <div className="muted">total de processos</div>
                </div>
                <div className="stat">
                  <div className="stat-value">{resumo.processos_ativos}</div>
                  <div className="muted">processos ativos</div>
                </div>
              </div>

              <div className="card">
                <h3>Histórico Eleitoral</h3>
                <div className="stat">
                  <div className="stat-value">{resumo.total_candidaturas}</div>
                  <div className="muted">candidaturas</div>
                </div>
                <div className="stat">
                  <div className="stat-value">{resumo.eleicoes_vencidas}</div>
                  <div className="muted">eleições vencidas</div>
                </div>
              </div>

              <div className="card">
                <h3>Doações Feitas</h3>
                <div className="stat">
                  <div className="stat-value">{resumo.total_doacoes_feitas}</div>
                  <div className="muted">doações realizadas</div>
                </div>
                <div className="stat">
                  <div className="stat-value">{formatCurrency(resumo.valor_total_doado)}</div>
                  <div className="muted">valor total doado</div>
                </div>
              </div>

              <div className="card">
                <h3>Doações Recebidas</h3>
                <div className="stat">
                  <div className="stat-value">{resumo.total_doacoes_recebidas}</div>
                  <div className="muted">doações recebidas</div>
                </div>
                <div className="stat">
                  <div className="stat-value">{formatCurrency(resumo.valor_total_recebido)}</div>
                  <div className="muted">valor total recebido</div>
                </div>
              </div>

              <div className="card" style={{ gridColumn: 'span 2' }}>
                <h3>Histórico Partidário</h3>
                <div className="row gap-8 wrap">
                  {resumo.historico_partidos.length > 0 ? (
                    resumo.historico_partidos.map((partido) => (
                      <span key={partido} className="pill">{partido}</span>
                    ))
                  ) : (
                    <span className="muted">Nenhum registro de filiação</span>
                  )}
                </div>
                {resumo.ultima_atualizacao && (
                  <p className="muted" style={{ marginTop: '8px' }}>
                    Última atualização: {formatDate(resumo.ultima_atualizacao)}
                  </p>
                )}
              </div>
            </section>
          )}

          {/* Processos */}
          {tab === 'processos' && processos && (
            <section className="stack gap-12">
              <div className="row space-between">
                <h2>Processos ({processos.total})</h2>
                <div className="row gap-8">
                  {Object.entries(processos.por_tribunal).map(([tribunal, count]) => (
                    <span key={tribunal} className="pill">{tribunal}: {count}</span>
                  ))}
                </div>
              </div>

              {processos.processos.length > 0 ? (
                <div className="stack gap-8">
                  {processos.processos.map((proc) => (
                    <div key={proc.id} className="card">
                      <div className="row space-between">
                        <div>
                          <strong>{proc.numero_processo}</strong>
                          <span className={`pill ${proc.status === 'ativo' ? 'success' : ''}`} style={{ marginLeft: '8px' }}>
                            {proc.status}
                          </span>
                        </div>
                        <span className="pill">{proc.tribunal}</span>
                      </div>
                      <p className="muted">{proc.classe}</p>
                      <p>{proc.assunto}</p>
                      <div className="row gap-8 meta">
                        <span>Vara: {proc.vara ?? '—'}</span>
                        <span>Comarca: {proc.comarca ?? '—'}</span>
                        <span>Distribuição: {formatDate(proc.data_distribuicao)}</span>
                      </div>
                      {proc.url_consulta && (
                        <a href={proc.url_consulta} target="_blank" rel="noopener noreferrer" className="link">
                          Ver no tribunal →
                        </a>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="muted">Nenhum processo encontrado.</div>
              )}
            </section>
          )}

          {/* Doações */}
          {tab === 'doacoes' && doacoes && (
            <section className="stack gap-12">
              <div className="row space-between wrap">
                <h2>Doações Eleitorais ({doacoes.total})</h2>
                <div className="row gap-8">
                  <select 
                    value={doacaoTipo} 
                    onChange={(e) => setDoacaoTipo(e.target.value as any)}
                    className="input"
                  >
                    <option value="todas">Todas</option>
                    <option value="feitas">Feitas</option>
                    <option value="recebidas">Recebidas</option>
                  </select>
                </div>
              </div>

              <div className="row gap-8 wrap">
                <span className="pill">Total: {formatCurrency(doacoes.valor_total)}</span>
                {Object.entries(doacoes.por_eleicao).map(([eleicao, count]) => (
                  <span key={eleicao} className="pill">{eleicao}: {count}</span>
                ))}
              </div>

              {doacoes.doacoes.length > 0 ? (
                <div className="stack gap-8">
                  {doacoes.doacoes.map((doacao) => (
                    <div key={doacao.id} className="card">
                      <div className="row space-between">
                        <strong>{formatCurrency(doacao.valor)}</strong>
                        <span className="pill">{doacao.eleicao}</span>
                      </div>
                      <p>
                        {doacao.nome_doador ? (
                          <>De: {doacao.nome_doador}</>
                        ) : (
                          <>Para: {doacao.nome_candidato}</>
                        )}
                      </p>
                      <div className="row gap-8 meta">
                        <span>{doacao.tipo_doacao}</span>
                        <span>{doacao.cargo}</span>
                        <span>{doacao.partido}</span>
                        <span>{doacao.uf}</span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="muted">Nenhuma doação encontrada.</div>
              )}
            </section>
          )}

          {/* Filiações */}
          {tab === 'filiacoes' && filiacoes && (
            <section className="stack gap-12">
              <h2>Filiações Partidárias ({filiacoes.total})</h2>
              
              <div className="row gap-8 wrap">
                {filiacoes.historico_partidos.map((partido) => (
                  <span key={partido} className="pill">{partido}</span>
                ))}
              </div>

              {filiacoes.filiacoes.length > 0 ? (
                <div className="stack gap-8">
                  {filiacoes.filiacoes.map((filiacao) => (
                    <div key={filiacao.id} className="card">
                      <div className="row space-between">
                        <strong>{filiacao.sigla_partido ?? filiacao.partido}</strong>
                        <span className={`pill ${filiacao.situacao === 'regular' ? 'success' : ''}`}>
                          {filiacao.situacao}
                        </span>
                      </div>
                      <p>{filiacao.partido}</p>
                      <div className="row gap-8 meta">
                        <span>Filiação: {formatDate(filiacao.data_filiacao)}</span>
                        {filiacao.data_desfiliacao && (
                          <span>Desfiliação: {formatDate(filiacao.data_desfiliacao)}</span>
                        )}
                        <span>{filiacao.municipio}, {filiacao.uf}</span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="muted">Nenhuma filiação encontrada.</div>
              )}
            </section>
          )}

          {/* Candidaturas */}
          {tab === 'candidaturas' && candidaturas && (
            <section className="stack gap-12">
              <h2>Candidaturas ({candidaturas.total})</h2>

              <div className="row gap-8 wrap">
                {Object.entries(candidaturas.por_eleicao).map(([eleicao, count]) => (
                  <span key={eleicao} className="pill">{eleicao}: {count}</span>
                ))}
              </div>

              {candidaturas.candidaturas.length > 0 ? (
                <div className="stack gap-8">
                  {candidaturas.candidaturas.map((cand) => (
                    <div key={cand.id} className="card">
                      <div className="row space-between">
                        <div>
                          <strong>{cand.nome_urna ?? cand.nome}</strong>
                          <span className="pill" style={{ marginLeft: '8px' }}>{cand.numero_candidato}</span>
                        </div>
                        <span className="pill">{cand.eleicao}</span>
                      </div>
                      <p>{cand.cargo} - {cand.municipio ?? cand.uf}</p>
                      <div className="row gap-8 meta">
                        <span>{cand.sigla_partido}</span>
                        <span className={cand.situacao_totalizacao?.toLowerCase().includes('eleito') ? 'success' : ''}>
                          {cand.situacao_totalizacao}
                        </span>
                        {cand.total_votos && <span>{cand.total_votos.toLocaleString()} votos</span>}
                      </div>
                      {cand.coligacao && (
                        <p className="muted" style={{ fontSize: '12px' }}>Coligação: {cand.coligacao}</p>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="muted">Nenhuma candidatura encontrada.</div>
              )}
            </section>
          )}
        </>
      )}

      {/* Logs */}
      <section className="card">
        <div className="row space-between gap-12 wrap">
          <div className="card-title">Logs de consulta processual</div>
          <button className="btn secondary" type="button" onClick={() => void loadConsultaLogs()}>
            Atualizar
          </button>
        </div>
        <div className="muted" style={{ marginTop: 8 }}>Mostra as últimas execuções por fonte (status, registros e mensagens).</div>

        <div className="list" style={{ marginTop: 10 }}>
          {consultaLogs && consultaLogs.length ? (
            consultaLogs.map((l) => (
              <div key={l.id} className="list-row">
                <div className="grow">
                  <div className="item-title">
                    {l.fonte} • <span className={`status ${l.status}`}>{l.status}</span>
                  </div>
                  <div className="muted">
                    {l.mensagem ?? '—'} {Number.isFinite(l.registros_encontrados) ? `• ${l.registros_encontrados} registros` : ''}
                  </div>
                </div>
                <div className="pill">{l.iniciado_em ? new Date(l.iniciado_em).toLocaleString('pt-BR') : '—'}</div>
              </div>
            ))
          ) : (
            <div className="muted">Sem logs ainda.</div>
          )}
        </div>
      </section>
    </div>
  )
}
