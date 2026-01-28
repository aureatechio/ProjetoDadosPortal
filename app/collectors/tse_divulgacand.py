"""
Collector para API DivulgaCandContas do TSE.

Este módulo consulta a API REST do portal DivulgaCandContas para obter:
- Informações detalhadas de candidatos
- Bens declarados
- Receitas e despesas de campanha
- Patrimônio declarado

Fonte: https://divulgacandcontas.tse.jus.br/
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import time

import httpx

from app.database import get_supabase

logger = logging.getLogger(__name__)

# Base URL da API
API_BASE_URL = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1"

# Mapeamento de eleições
ELEICOES = {
    "2024": {"id": "2045202024", "tipo": "municipal"},
    "2022": {"id": "2040602022", "tipo": "geral"},
    "2020": {"id": "2030402020", "tipo": "municipal"},
    "2018": {"id": "2022802018", "tipo": "geral"},
    "2016": {"id": "2020602016", "tipo": "municipal"},
    "2014": {"id": "2014", "tipo": "geral"},
}

# UFs brasileiras
UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
    "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
    "RO", "RR", "RS", "SC", "SE", "SP", "TO"
]


class DivulgaCandContasCollector:
    """
    Collector para API DivulgaCandContas do TSE.
    
    Consulta a API REST para obter dados detalhados de candidatos.
    """
    
    def __init__(self):
        self.supabase = get_supabase()
        self.client = httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            }
        )
        
    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
    def _normalizar_cpf(self, cpf: str) -> str:
        """Remove formatação do CPF."""
        if not cpf:
            return ""
        return "".join(filter(str.isdigit, str(cpf)))
    
    def _criar_log(self, politico_id: int, cpf: str, tipo: str) -> str:
        """Cria log de consulta."""
        try:
            result = self.supabase.table("consulta_processual_logs").insert({
                "politico_id": politico_id,
                "cpf": cpf,
                "fonte": "TSE_DIVULGACAND",
                "tipo_consulta": tipo,
                "status": "iniciado"
            }).execute()
            return result.data[0]["id"] if result.data else None
        except Exception as e:
            logger.error(f"Erro ao criar log: {e}")
            return None
    
    def _atualizar_log(self, log_id: str, status: str, registros: int = 0, mensagem: str = None):
        """Atualiza log de consulta."""
        if not log_id:
            return
        try:
            self.supabase.table("consulta_processual_logs").update({
                "status": status,
                "registros_encontrados": registros,
                "mensagem": mensagem,
                "finalizado_em": datetime.utcnow().isoformat()
            }).eq("id", log_id).execute()
        except Exception as e:
            logger.error(f"Erro ao atualizar log: {e}")
    
    def _fazer_requisicao(self, endpoint: str, params: dict = None) -> Optional[Dict]:
        """
        Faz requisição à API com retry.
        
        Args:
            endpoint: Endpoint da API
            params: Parâmetros da requisição
            
        Returns:
            Resposta JSON ou None
        """
        url = f"{API_BASE_URL}/{endpoint}"
        
        for tentativa in range(3):
            try:
                response = self.client.get(url, params=params)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limited, espera e tenta novamente
                    time.sleep(5 * (tentativa + 1))
                    continue
                else:
                    logger.warning(f"API retornou {response.status_code}: {url}")
                    return None
                    
            except Exception as e:
                logger.error(f"Erro na requisição (tentativa {tentativa + 1}): {e}")
                time.sleep(2)
        
        return None
    
    def buscar_candidato_por_nome(
        self, 
        nome: str, 
        eleicao: str = "2024",
        uf: str = None
    ) -> List[Dict[str, Any]]:
        """
        Busca candidatos por nome.
        
        Args:
            nome: Nome do candidato
            eleicao: Ano da eleição
            uf: UF para filtrar (opcional)
            
        Returns:
            Lista de candidatos encontrados
        """
        if eleicao not in ELEICOES:
            logger.error(f"Eleição {eleicao} não disponível")
            return []
        
        eleicao_id = ELEICOES[eleicao]["id"]
        ufs = [uf] if uf else UFS
        candidatos = []
        
        for estado in ufs:
            try:
                # Endpoint de busca por nome
                endpoint = f"candidatura/listar/{eleicao}/{eleicao_id}/{estado}/2045202024/candidato"
                params = {"nomeCompleto": nome}
                
                resultado = self._fazer_requisicao(endpoint, params)
                
                if resultado and "candidatos" in resultado:
                    for cand in resultado["candidatos"]:
                        candidatos.append(self._parse_candidato(cand, eleicao, estado))
                
                # Delay para não sobrecarregar a API
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Erro ao buscar candidato em {estado}: {e}")
                continue
        
        return candidatos
    
    def buscar_candidato_por_sequencial(
        self, 
        sequencial: str, 
        eleicao: str = "2024",
        uf: str = "SP"
    ) -> Optional[Dict[str, Any]]:
        """
        Busca candidato pelo número sequencial.
        
        Args:
            sequencial: Número sequencial do candidato
            eleicao: Ano da eleição
            uf: UF do candidato
            
        Returns:
            Dados do candidato ou None
        """
        if eleicao not in ELEICOES:
            return None
        
        eleicao_id = ELEICOES[eleicao]["id"]
        
        try:
            endpoint = f"candidatura/buscar/{eleicao}/{eleicao_id}/{uf}/{sequencial}/candidato"
            resultado = self._fazer_requisicao(endpoint)
            
            if resultado:
                return self._parse_candidato_detalhado(resultado, eleicao, uf)
            
        except Exception as e:
            logger.error(f"Erro ao buscar candidato {sequencial}: {e}")
        
        return None
    
    def _parse_candidato(self, data: dict, eleicao: str, uf: str) -> Dict[str, Any]:
        """Parse básico de dados do candidato."""
        return {
            "sequencial_candidato": data.get("id", ""),
            "nome": data.get("nomeCompleto", ""),
            "nome_urna": data.get("nomeUrna", ""),
            "numero_candidato": data.get("numero", ""),
            "cargo": data.get("cargo", {}).get("nome", ""),
            "partido": data.get("partido", {}).get("nome", ""),
            "sigla_partido": data.get("partido", {}).get("sigla", ""),
            "situacao_candidatura": data.get("descricaoSituacao", ""),
            "eleicao": eleicao,
            "uf": uf,
            "url_foto": data.get("fotoUrl", ""),
        }
    
    def _parse_candidato_detalhado(self, data: dict, eleicao: str, uf: str) -> Dict[str, Any]:
        """Parse detalhado de dados do candidato."""
        candidato = self._parse_candidato(data, eleicao, uf)
        
        # Adiciona campos extras
        candidato.update({
            "cpf": self._normalizar_cpf(data.get("cpf", "")),
            "ocupacao": data.get("ocupacao", ""),
            "grau_instrucao": data.get("grauInstrucao", ""),
            "estado_civil": data.get("estadoCivil", ""),
            "nacionalidade": data.get("nacionalidade", ""),
            "data_nascimento": data.get("dataDeNascimento", ""),
            "genero": data.get("descricaoSexo", ""),
            "cor_raca": data.get("descricaoCorRaca", ""),
            "email": data.get("emails", [""])[0] if data.get("emails") else "",
            "municipio": data.get("localCandidatura", ""),
            "coligacao": data.get("nomeColigacao", ""),
            "composicao_coligacao": data.get("composicaoColigacao", ""),
            "bens_declarados": self._parse_bens(data.get("bens", [])),
            "total_bens": data.get("totalDeBens", 0),
            "dados_raw": data
        })
        
        return candidato
    
    def _parse_bens(self, bens: list) -> List[Dict]:
        """Parse de bens declarados."""
        return [
            {
                "tipo": bem.get("tipoBem", ""),
                "descricao": bem.get("descricao", ""),
                "valor": bem.get("valor", 0)
            }
            for bem in bens
        ]
    
    def buscar_receitas_candidato(
        self, 
        sequencial: str, 
        eleicao: str = "2024",
        uf: str = "SP"
    ) -> List[Dict[str, Any]]:
        """
        Busca receitas de campanha do candidato.
        
        Args:
            sequencial: Número sequencial do candidato
            eleicao: Ano da eleição
            uf: UF do candidato
            
        Returns:
            Lista de receitas
        """
        if eleicao not in ELEICOES:
            return []
        
        eleicao_id = ELEICOES[eleicao]["id"]
        receitas = []
        
        try:
            endpoint = f"prestador/consulta/receitas/2/{eleicao_id}/{uf}/{sequencial}"
            resultado = self._fazer_requisicao(endpoint)
            
            if resultado:
                for item in resultado if isinstance(resultado, list) else [resultado]:
                    receita = {
                        "sequencial_candidato": sequencial,
                        "eleicao": eleicao,
                        "uf": uf,
                        "tipo_receita": item.get("fonteReceita", ""),
                        "origem_receita": item.get("origemReceita", ""),
                        "especie_recurso": item.get("especieRecurso", ""),
                        "valor": item.get("valorReceita", 0),
                        "cpf_cnpj_doador": item.get("cpfCnpjDoador", ""),
                        "nome_doador": item.get("nomeDoador", ""),
                        "data_receita": item.get("dataReceita", ""),
                        "numero_documento": item.get("numeroDocumento", ""),
                        "descricao": item.get("descricaoReceita", ""),
                    }
                    receitas.append(receita)
                    
        except Exception as e:
            logger.error(f"Erro ao buscar receitas {sequencial}: {e}")
        
        return receitas
    
    def buscar_despesas_candidato(
        self, 
        sequencial: str, 
        eleicao: str = "2024",
        uf: str = "SP"
    ) -> List[Dict[str, Any]]:
        """
        Busca despesas de campanha do candidato.
        
        Args:
            sequencial: Número sequencial do candidato
            eleicao: Ano da eleição
            uf: UF do candidato
            
        Returns:
            Lista de despesas
        """
        if eleicao not in ELEICOES:
            return []
        
        eleicao_id = ELEICOES[eleicao]["id"]
        despesas = []
        
        try:
            endpoint = f"prestador/consulta/despesas/2/{eleicao_id}/{uf}/{sequencial}"
            resultado = self._fazer_requisicao(endpoint)
            
            if resultado:
                for item in resultado if isinstance(resultado, list) else [resultado]:
                    despesa = {
                        "sequencial_candidato": sequencial,
                        "eleicao": eleicao,
                        "uf": uf,
                        "tipo_despesa": item.get("tipoDespesa", ""),
                        "origem_despesa": item.get("origemDespesa", ""),
                        "valor": item.get("valorDespesa", 0),
                        "cpf_cnpj_fornecedor": item.get("cpfCnpjFornecedor", ""),
                        "nome_fornecedor": item.get("nomeFornecedor", ""),
                        "data_despesa": item.get("dataDespesa", ""),
                        "numero_documento": item.get("numeroDocumento", ""),
                        "descricao": item.get("descricaoDespesa", ""),
                    }
                    despesas.append(despesa)
                    
        except Exception as e:
            logger.error(f"Erro ao buscar despesas {sequencial}: {e}")
        
        return despesas
    
    def consulta_completa_candidato(
        self, 
        nome: str = None,
        sequencial: str = None,
        eleicao: str = "2024",
        uf: str = "SP",
        politico_id: int = None
    ) -> Dict[str, Any]:
        """
        Realiza consulta completa de um candidato.
        
        Args:
            nome: Nome do candidato (para busca)
            sequencial: Sequencial do candidato (se conhecido)
            eleicao: Ano da eleição
            uf: UF do candidato
            politico_id: ID do político (opcional)
            
        Returns:
            Dados completos do candidato
        """
        resultado = {
            "candidato": None,
            "receitas": [],
            "despesas": [],
            "total_receitas": 0,
            "total_despesas": 0
        }
        
        log_id = self._criar_log(politico_id, "", "candidato_completo")
        
        try:
            # Se tiver sequencial, busca direto
            if sequencial:
                resultado["candidato"] = self.buscar_candidato_por_sequencial(sequencial, eleicao, uf)
                
            # Senão, busca por nome primeiro
            elif nome:
                candidatos = self.buscar_candidato_por_nome(nome, eleicao, uf)
                if candidatos:
                    # Pega o primeiro resultado e busca detalhes
                    seq = candidatos[0].get("sequencial_candidato")
                    if seq:
                        resultado["candidato"] = self.buscar_candidato_por_sequencial(seq, eleicao, uf)
                    else:
                        resultado["candidato"] = candidatos[0]
            
            # Se encontrou candidato, busca receitas e despesas
            if resultado["candidato"]:
                seq = resultado["candidato"].get("sequencial_candidato")
                if seq:
                    resultado["receitas"] = self.buscar_receitas_candidato(seq, eleicao, uf)
                    resultado["despesas"] = self.buscar_despesas_candidato(seq, eleicao, uf)
                    
                    resultado["total_receitas"] = sum(r.get("valor", 0) for r in resultado["receitas"])
                    resultado["total_despesas"] = sum(d.get("valor", 0) for d in resultado["despesas"])
                    
                    # Salva candidatura no banco
                    self._salvar_candidatura(resultado["candidato"], politico_id)
                    
                    # Salva doações recebidas
                    self._salvar_receitas_como_doacoes(resultado["receitas"], resultado["candidato"], politico_id)
            
            self._atualizar_log(log_id, "sucesso", 1 if resultado["candidato"] else 0)
            
        except Exception as e:
            logger.error(f"Erro na consulta completa: {e}")
            self._atualizar_log(log_id, "erro", 0, str(e))
        
        return resultado
    
    def _salvar_candidatura(self, candidato: dict, politico_id: int = None):
        """Salva candidatura no banco."""
        try:
            dados = {
                "politico_id": politico_id,
                "cpf": candidato.get("cpf"),
                "nome": candidato.get("nome"),
                "nome_urna": candidato.get("nome_urna"),
                "numero_candidato": candidato.get("numero_candidato"),
                "sequencial_candidato": candidato.get("sequencial_candidato"),
                "eleicao": candidato.get("eleicao"),
                "cargo": candidato.get("cargo"),
                "uf": candidato.get("uf"),
                "municipio": candidato.get("municipio"),
                "partido": candidato.get("partido"),
                "sigla_partido": candidato.get("sigla_partido"),
                "coligacao": candidato.get("coligacao"),
                "situacao_candidatura": candidato.get("situacao_candidatura"),
                "ocupacao": candidato.get("ocupacao"),
                "grau_instrucao": candidato.get("grau_instrucao"),
                "estado_civil": candidato.get("estado_civil"),
                "genero": candidato.get("genero"),
                "cor_raca": candidato.get("cor_raca"),
                "email": candidato.get("email"),
                "bens_declarados": candidato.get("total_bens"),
                "url_foto": candidato.get("url_foto"),
                "dados_raw": candidato.get("dados_raw"),
                "fonte": "TSE_DIVULGACAND"
            }
            
            self.supabase.table("candidaturas").upsert(
                dados,
                on_conflict="cpf,eleicao,cargo,turno"
            ).execute()
            
        except Exception as e:
            logger.error(f"Erro ao salvar candidatura: {e}")
    
    def _salvar_receitas_como_doacoes(self, receitas: list, candidato: dict, politico_id: int = None):
        """Salva receitas como doações eleitorais."""
        for receita in receitas:
            try:
                doacao = {
                    "politico_id": politico_id,
                    "cpf_doador": self._normalizar_cpf(receita.get("cpf_cnpj_doador", "")),
                    "nome_doador": receita.get("nome_doador"),
                    "cpf_candidato": candidato.get("cpf"),
                    "nome_candidato": candidato.get("nome"),
                    "valor": receita.get("valor", 0),
                    "tipo_doacao": receita.get("origem_receita"),
                    "tipo_receita": receita.get("tipo_receita"),
                    "eleicao": receita.get("eleicao"),
                    "partido": candidato.get("sigla_partido"),
                    "cargo": candidato.get("cargo"),
                    "uf": receita.get("uf"),
                    "fonte": "TSE_DIVULGACAND",
                    "sequencial_candidato": receita.get("sequencial_candidato"),
                    "numero_documento": receita.get("numero_documento")
                }
                
                self.supabase.table("doacoes_eleitorais").upsert(
                    doacao,
                    on_conflict="cpf_doador,cpf_candidato,eleicao,valor,numero_documento"
                ).execute()
                
            except Exception as e:
                logger.error(f"Erro ao salvar doação: {e}")


# Instância global
divulgacand_collector = DivulgaCandContasCollector()


# Funções de conveniência
def buscar_candidato(nome: str, eleicao: str = "2024", uf: str = None) -> List[Dict]:
    """Busca candidatos por nome."""
    return divulgacand_collector.buscar_candidato_por_nome(nome, eleicao, uf)


def consulta_candidato_completa(nome: str = None, sequencial: str = None, 
                                 eleicao: str = "2024", uf: str = "SP") -> Dict:
    """Consulta completa de candidato."""
    return divulgacand_collector.consulta_completa_candidato(nome, sequencial, eleicao, uf)
