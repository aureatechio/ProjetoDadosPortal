"""
Collector para dados abertos do TSE (Tribunal Superior Eleitoral).

Este módulo baixa e processa CSVs do portal dadosabertos.tse.jus.br
para extrair informações sobre:
- Candidaturas
- Doações eleitorais (receitas)
- Filiações partidárias

Fonte: https://dadosabertos.tse.jus.br/
"""

import os
import logging
import zipfile
import io
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

import httpx
import pandas as pd

from app.config import settings
from app.database import get_supabase

logger = logging.getLogger(__name__)

# Diretório para cache de arquivos baixados
CACHE_DIR = Path("data/tse_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# URLs base do TSE Dados Abertos
TSE_BASE_URL = "https://dadosabertos.tse.jus.br/dataset"

# Mapeamento de eleições disponíveis
ELEICOES_DISPONIVEIS = [
    "2024", "2022", "2020", "2018", "2016", "2014", "2012", "2010"
]


class TSEDadosAbertosCollector:
    """
    Collector para dados abertos do TSE.
    
    Baixa e processa arquivos CSV do portal de dados abertos do TSE.
    """
    
    def __init__(self):
        self.supabase = get_supabase()
        self.client = httpx.Client(timeout=120.0, follow_redirects=True)
        
    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
    def _normalizar_cpf(self, cpf: str) -> str:
        """Remove formatação do CPF, mantendo apenas números."""
        if not cpf:
            return ""
        return "".join(filter(str.isdigit, str(cpf)))
    
    def _formatar_cpf(self, cpf: str) -> str:
        """Formata CPF com pontos e traço."""
        cpf = self._normalizar_cpf(cpf)
        if len(cpf) == 11:
            return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
        return cpf
    
    def _criar_log(self, politico_id: int, cpf: str, fonte: str, tipo: str) -> str:
        """Cria um registro de log de consulta."""
        try:
            result = self.supabase.table("consulta_processual_logs").insert({
                "politico_id": politico_id,
                "cpf": cpf,
                "fonte": fonte,
                "tipo_consulta": tipo,
                "status": "iniciado"
            }).execute()
            return result.data[0]["id"] if result.data else None
        except Exception as e:
            logger.error(f"Erro ao criar log: {e}")
            return None
    
    def _atualizar_log(self, log_id: str, status: str, registros: int = 0, mensagem: str = None):
        """Atualiza um registro de log."""
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
    
    def _baixar_arquivo(self, url: str, nome_arquivo: str) -> Optional[Path]:
        """
        Baixa um arquivo ZIP do TSE e extrai o CSV.
        
        Args:
            url: URL do arquivo ZIP
            nome_arquivo: Nome para salvar o arquivo
            
        Returns:
            Path do arquivo CSV extraído ou None se falhar
        """
        cache_path = CACHE_DIR / nome_arquivo
        
        # Verifica se já existe em cache (válido por 24h)
        if cache_path.exists():
            idade_arquivo = datetime.now().timestamp() - cache_path.stat().st_mtime
            if idade_arquivo < 86400:  # 24 horas
                logger.info(f"Usando arquivo em cache: {cache_path}")
                return cache_path
        
        logger.info(f"Baixando arquivo: {url}")
        
        try:
            response = self.client.get(url)
            response.raise_for_status()
            
            # Se for ZIP, extrai
            if url.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    # Encontra o arquivo CSV dentro do ZIP
                    csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                    if csv_files:
                        csv_content = zf.read(csv_files[0])
                        cache_path.write_bytes(csv_content)
                        return cache_path
            else:
                # Arquivo direto
                cache_path.write_bytes(response.content)
                return cache_path
                
        except Exception as e:
            logger.error(f"Erro ao baixar arquivo {url}: {e}")
            return None
    
    def _ler_csv_tse(self, filepath: Path, encoding: str = 'latin-1') -> Optional[pd.DataFrame]:
        """
        Lê um CSV do TSE com o encoding correto.
        
        Args:
            filepath: Caminho do arquivo CSV
            encoding: Encoding do arquivo (padrão latin-1 para arquivos TSE)
            
        Returns:
            DataFrame pandas ou None se falhar
        """
        try:
            # TSE usa separador ; e encoding latin-1
            df = pd.read_csv(
                filepath,
                sep=';',
                encoding=encoding,
                dtype=str,
                low_memory=False
            )
            return df
        except Exception as e:
            logger.error(f"Erro ao ler CSV {filepath}: {e}")
            # Tenta com utf-8
            try:
                df = pd.read_csv(
                    filepath,
                    sep=';',
                    encoding='utf-8',
                    dtype=str,
                    low_memory=False
                )
                return df
            except Exception as e2:
                logger.error(f"Erro ao ler CSV com utf-8: {e2}")
                return None
    
    # ==================== CANDIDATURAS ====================
    
    def buscar_candidaturas_por_cpf(
        self, 
        cpf: str, 
        politico_id: Optional[int] = None,
        eleicoes: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca candidaturas de uma pessoa pelo CPF nos dados do TSE.
        
        Args:
            cpf: CPF a ser pesquisado
            politico_id: ID do político (opcional)
            eleicoes: Lista de eleições a pesquisar (padrão: todas)
            
        Returns:
            Lista de candidaturas encontradas
        """
        cpf_normalizado = self._normalizar_cpf(cpf)
        if not cpf_normalizado or len(cpf_normalizado) != 11:
            logger.error(f"CPF inválido: {cpf}")
            return []
        
        log_id = self._criar_log(politico_id, cpf_normalizado, "TSE", "candidaturas")
        candidaturas = []
        eleicoes = eleicoes or ELEICOES_DISPONIVEIS
        
        for eleicao in eleicoes:
            try:
                # URL do dataset de candidatos
                # Formato: consulta_cand_{ano}.zip
                url = f"https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_{eleicao}.zip"
                
                arquivo = self._baixar_arquivo(url, f"candidatos_{eleicao}.csv")
                if not arquivo:
                    continue
                
                df = self._ler_csv_tse(arquivo)
                if df is None:
                    continue
                
                # Encontra a coluna de CPF (pode variar entre arquivos)
                cpf_col = None
                for col in ['NR_CPF_CANDIDATO', 'CPF_CANDIDATO', 'NR_CPF']:
                    if col in df.columns:
                        cpf_col = col
                        break
                
                if not cpf_col:
                    logger.warning(f"Coluna de CPF não encontrada em {arquivo}")
                    continue
                
                # Filtra pelo CPF
                df[cpf_col] = df[cpf_col].apply(lambda x: self._normalizar_cpf(str(x)) if pd.notna(x) else "")
                matches = df[df[cpf_col] == cpf_normalizado]
                
                for _, row in matches.iterrows():
                    candidatura = {
                        "cpf": cpf_normalizado,
                        "politico_id": politico_id,
                        "eleicao": eleicao,
                        "nome": row.get("NM_CANDIDATO", row.get("NOME_CANDIDATO", "")),
                        "nome_urna": row.get("NM_URNA_CANDIDATO", ""),
                        "numero_candidato": row.get("NR_CANDIDATO", ""),
                        "sequencial_candidato": row.get("SQ_CANDIDATO", ""),
                        "cargo": row.get("DS_CARGO", row.get("DESCRICAO_CARGO", "")),
                        "uf": row.get("SG_UF", row.get("SIGLA_UF", "")),
                        "municipio": row.get("NM_UE", row.get("NOME_MUNICIPIO", "")),
                        "partido": row.get("NM_PARTIDO", row.get("NOME_PARTIDO", "")),
                        "sigla_partido": row.get("SG_PARTIDO", row.get("SIGLA_PARTIDO", "")),
                        "coligacao": row.get("NM_COLIGACAO", ""),
                        "situacao_candidatura": row.get("DS_SITUACAO_CANDIDATURA", ""),
                        "situacao_totalizacao": row.get("DS_SIT_TOT_TURNO", ""),
                        "total_votos": int(row.get("QT_VOTOS_NOMINAIS", 0) or 0) if pd.notna(row.get("QT_VOTOS_NOMINAIS")) else None,
                        "ocupacao": row.get("DS_OCUPACAO", ""),
                        "grau_instrucao": row.get("DS_GRAU_INSTRUCAO", ""),
                        "estado_civil": row.get("DS_ESTADO_CIVIL", ""),
                        "genero": row.get("DS_GENERO", row.get("DESCRICAO_SEXO", "")),
                        "cor_raca": row.get("DS_COR_RACA", ""),
                        "email": row.get("NM_EMAIL", ""),
                        "fonte": "TSE"
                    }
                    candidaturas.append(candidatura)
                    
            except Exception as e:
                logger.error(f"Erro ao processar candidaturas {eleicao}: {e}")
                continue
        
        # Salva no banco
        if candidaturas:
            self._salvar_candidaturas(candidaturas)
        
        self._atualizar_log(log_id, "sucesso", len(candidaturas))
        return candidaturas
    
    def _salvar_candidaturas(self, candidaturas: List[Dict[str, Any]]):
        """Salva candidaturas no banco de dados."""
        for cand in candidaturas:
            try:
                self.supabase.table("candidaturas").upsert(
                    cand,
                    on_conflict="cpf,eleicao,cargo,turno"
                ).execute()
            except Exception as e:
                logger.error(f"Erro ao salvar candidatura: {e}")
    
    # ==================== DOAÇÕES ELEITORAIS ====================
    
    def buscar_doacoes_por_cpf(
        self, 
        cpf: str, 
        politico_id: Optional[int] = None,
        eleicoes: List[str] = None,
        tipo: str = "doador"  # "doador" ou "candidato"
    ) -> List[Dict[str, Any]]:
        """
        Busca doações eleitorais pelo CPF (como doador ou candidato).
        
        Args:
            cpf: CPF a ser pesquisado
            politico_id: ID do político (opcional)
            eleicoes: Lista de eleições a pesquisar
            tipo: "doador" para doações feitas, "candidato" para recebidas
            
        Returns:
            Lista de doações encontradas
        """
        cpf_normalizado = self._normalizar_cpf(cpf)
        if not cpf_normalizado or len(cpf_normalizado) != 11:
            logger.error(f"CPF inválido: {cpf}")
            return []
        
        log_id = self._criar_log(politico_id, cpf_normalizado, "TSE", f"doacoes_{tipo}")
        doacoes = []
        eleicoes = eleicoes or ELEICOES_DISPONIVEIS
        
        for eleicao in eleicoes:
            try:
                # URL do dataset de receitas
                url = f"https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{eleicao}.zip"
                
                arquivo = self._baixar_arquivo(url, f"receitas_{eleicao}.csv")
                if not arquivo:
                    # Tenta formato alternativo
                    url = f"https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas/receitas_candidatos_{eleicao}.zip"
                    arquivo = self._baixar_arquivo(url, f"receitas_alt_{eleicao}.csv")
                    if not arquivo:
                        continue
                
                df = self._ler_csv_tse(arquivo)
                if df is None:
                    continue
                
                # Define a coluna de busca baseado no tipo
                if tipo == "doador":
                    cpf_col = None
                    for col in ['NR_CPF_CNPJ_DOADOR', 'CPF_CNPJ_DOADOR', 'NR_CPF_DOADOR']:
                        if col in df.columns:
                            cpf_col = col
                            break
                else:
                    cpf_col = None
                    for col in ['NR_CPF_CANDIDATO', 'CPF_CANDIDATO']:
                        if col in df.columns:
                            cpf_col = col
                            break
                
                if not cpf_col:
                    logger.warning(f"Coluna de CPF não encontrada em {arquivo}")
                    continue
                
                # Filtra pelo CPF
                df[cpf_col] = df[cpf_col].apply(lambda x: self._normalizar_cpf(str(x)) if pd.notna(x) else "")
                matches = df[df[cpf_col] == cpf_normalizado]
                
                for _, row in matches.iterrows():
                    valor = row.get("VR_RECEITA", row.get("VALOR_RECEITA", "0"))
                    try:
                        valor = float(str(valor).replace(",", "."))
                    except:
                        valor = 0.0
                    
                    doacao = {
                        "politico_id": politico_id,
                        "cpf_doador": self._normalizar_cpf(str(row.get("NR_CPF_CNPJ_DOADOR", row.get("CPF_CNPJ_DOADOR", "")))),
                        "nome_doador": row.get("NM_DOADOR", row.get("NOME_DOADOR", "")),
                        "cpf_candidato": self._normalizar_cpf(str(row.get("NR_CPF_CANDIDATO", row.get("CPF_CANDIDATO", "")))),
                        "nome_candidato": row.get("NM_CANDIDATO", row.get("NOME_CANDIDATO", "")),
                        "valor": valor,
                        "tipo_doacao": row.get("DS_ORIGEM_RECEITA", row.get("ORIGEM_RECEITA", "")),
                        "tipo_receita": row.get("DS_FONTE_RECEITA", row.get("FONTE_RECEITA", "")),
                        "eleicao": eleicao,
                        "turno": int(row.get("NR_TURNO", 1) or 1),
                        "partido": row.get("SG_PARTIDO", row.get("SIGLA_PARTIDO", "")),
                        "cargo": row.get("DS_CARGO", row.get("DESCRICAO_CARGO", "")),
                        "uf": row.get("SG_UF", row.get("SIGLA_UF", "")),
                        "municipio": row.get("NM_UE", row.get("NOME_MUNICIPIO", "")),
                        "fonte": "TSE",
                        "sequencial_candidato": row.get("SQ_CANDIDATO", ""),
                        "numero_documento": row.get("NR_DOCUMENTO", "")
                    }
                    doacoes.append(doacao)
                    
            except Exception as e:
                logger.error(f"Erro ao processar doações {eleicao}: {e}")
                continue
        
        # Salva no banco
        if doacoes:
            self._salvar_doacoes(doacoes)
        
        self._atualizar_log(log_id, "sucesso", len(doacoes))
        return doacoes
    
    def _salvar_doacoes(self, doacoes: List[Dict[str, Any]]):
        """Salva doações no banco de dados."""
        for doacao in doacoes:
            try:
                self.supabase.table("doacoes_eleitorais").upsert(
                    doacao,
                    on_conflict="cpf_doador,cpf_candidato,eleicao,valor,numero_documento"
                ).execute()
            except Exception as e:
                logger.error(f"Erro ao salvar doação: {e}")
    
    # ==================== FILIAÇÕES PARTIDÁRIAS ====================
    
    def buscar_filiacoes_por_cpf(
        self, 
        cpf: str, 
        politico_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca filiações partidárias pelo CPF.
        
        Args:
            cpf: CPF a ser pesquisado
            politico_id: ID do político (opcional)
            
        Returns:
            Lista de filiações encontradas
        """
        cpf_normalizado = self._normalizar_cpf(cpf)
        if not cpf_normalizado or len(cpf_normalizado) != 11:
            logger.error(f"CPF inválido: {cpf}")
            return []
        
        log_id = self._criar_log(politico_id, cpf_normalizado, "TSE", "filiacoes")
        filiacoes = []
        
        # Lista de partidos para buscar
        # O TSE disponibiliza filiados por partido
        partidos = [
            "avante", "cidadania", "dc", "dem", "mdb", "novo", "patriota",
            "pcb", "pcdob", "pdt", "phs", "pl", "pmb", "pmn", "pode",
            "pp", "ppl", "pros", "prp", "prtb", "psb", "psc", "psd",
            "psdb", "psl", "psol", "pstu", "pt", "ptb", "ptc", "pv",
            "rede", "republicanos", "sd", "solidariedade", "uniao"
        ]
        
        for partido in partidos:
            try:
                # URL do dataset de filiados
                url = f"https://cdn.tse.jus.br/estatistica/sead/odsele/filiacao_partidaria/filiados_{partido.upper()}.zip"
                
                arquivo = self._baixar_arquivo(url, f"filiados_{partido}.csv")
                if not arquivo:
                    continue
                
                df = self._ler_csv_tse(arquivo)
                if df is None:
                    continue
                
                # Encontra a coluna de título de eleitor ou CPF
                # Nota: O TSE geralmente usa título de eleitor, não CPF
                cpf_col = None
                for col in ['NR_CPF', 'CPF', 'NUMERO_INSCRICAO']:
                    if col in df.columns:
                        cpf_col = col
                        break
                
                if not cpf_col:
                    # Tenta buscar por nome se não encontrar CPF
                    continue
                
                # Filtra pelo CPF
                df[cpf_col] = df[cpf_col].apply(lambda x: self._normalizar_cpf(str(x)) if pd.notna(x) else "")
                matches = df[df[cpf_col] == cpf_normalizado]
                
                for _, row in matches.iterrows():
                    filiacao = {
                        "politico_id": politico_id,
                        "cpf": cpf_normalizado,
                        "nome": row.get("NM_FILIADO", row.get("NOME_FILIADO", "")),
                        "titulo_eleitoral": row.get("NR_TITULO_ELEITORAL", ""),
                        "partido": row.get("NM_PARTIDO", row.get("NOME_PARTIDO", partido.upper())),
                        "sigla_partido": row.get("SG_PARTIDO", partido.upper()),
                        "data_filiacao": self._parse_data(row.get("DT_FILIACAO", "")),
                        "data_desfiliacao": self._parse_data(row.get("DT_DESFILIACAO", "")),
                        "data_cancelamento": self._parse_data(row.get("DT_CANCELAMENTO", "")),
                        "data_regularizacao": self._parse_data(row.get("DT_REGULARIZACAO", "")),
                        "situacao": row.get("DS_SITUACAO_REGISTRO", row.get("SITUACAO_REGISTRO", "regular")),
                        "motivo_cancelamento": row.get("DS_MOTIVO_CANCELAMENTO", ""),
                        "uf": row.get("SG_UF", row.get("SIGLA_UF", "")),
                        "municipio": row.get("NM_MUNICIPIO", row.get("NOME_MUNICIPIO", "")),
                        "zona_eleitoral": row.get("NR_ZONA", ""),
                        "secao_eleitoral": row.get("NR_SECAO", ""),
                        "fonte": "TSE"
                    }
                    filiacoes.append(filiacao)
                    
            except Exception as e:
                logger.error(f"Erro ao processar filiações {partido}: {e}")
                continue
        
        # Salva no banco
        if filiacoes:
            self._salvar_filiacoes(filiacoes)
        
        self._atualizar_log(log_id, "sucesso", len(filiacoes))
        return filiacoes
    
    def _parse_data(self, data_str: str) -> Optional[str]:
        """Converte string de data para formato ISO."""
        if not data_str or data_str == "":
            return None
        try:
            # Tenta vários formatos
            for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
                try:
                    dt = datetime.strptime(str(data_str), fmt)
                    return dt.strftime("%Y-%m-%d")
                except:
                    continue
            return None
        except:
            return None
    
    def _salvar_filiacoes(self, filiacoes: List[Dict[str, Any]]):
        """Salva filiações no banco de dados."""
        for filiacao in filiacoes:
            try:
                self.supabase.table("filiacoes_partidarias").upsert(
                    filiacao,
                    on_conflict="cpf,partido,data_filiacao"
                ).execute()
            except Exception as e:
                logger.error(f"Erro ao salvar filiação: {e}")
    
    # ==================== CONSULTA COMPLETA ====================
    
    def consulta_completa_cpf(
        self, 
        cpf: str, 
        politico_id: Optional[int] = None,
        eleicoes: List[str] = None
    ) -> Dict[str, Any]:
        """
        Realiza consulta completa de um CPF em todas as bases do TSE.
        
        Args:
            cpf: CPF a ser pesquisado
            politico_id: ID do político (opcional)
            eleicoes: Lista de eleições (opcional)
            
        Returns:
            Dicionário com todos os dados encontrados
        """
        resultado = {
            "cpf": self._normalizar_cpf(cpf),
            "politico_id": politico_id,
            "candidaturas": [],
            "doacoes_feitas": [],
            "doacoes_recebidas": [],
            "filiacoes": [],
            "resumo": {}
        }
        
        # Busca candidaturas
        logger.info(f"Buscando candidaturas para CPF {cpf[:3]}***")
        resultado["candidaturas"] = self.buscar_candidaturas_por_cpf(cpf, politico_id, eleicoes)
        
        # Busca doações feitas
        logger.info(f"Buscando doações feitas para CPF {cpf[:3]}***")
        resultado["doacoes_feitas"] = self.buscar_doacoes_por_cpf(cpf, politico_id, eleicoes, "doador")
        
        # Busca doações recebidas
        logger.info(f"Buscando doações recebidas para CPF {cpf[:3]}***")
        resultado["doacoes_recebidas"] = self.buscar_doacoes_por_cpf(cpf, politico_id, eleicoes, "candidato")
        
        # Busca filiações
        logger.info(f"Buscando filiações para CPF {cpf[:3]}***")
        resultado["filiacoes"] = self.buscar_filiacoes_por_cpf(cpf, politico_id)
        
        # Monta resumo
        resultado["resumo"] = {
            "total_candidaturas": len(resultado["candidaturas"]),
            "total_doacoes_feitas": len(resultado["doacoes_feitas"]),
            "valor_total_doado": sum(d.get("valor", 0) for d in resultado["doacoes_feitas"]),
            "total_doacoes_recebidas": len(resultado["doacoes_recebidas"]),
            "valor_total_recebido": sum(d.get("valor", 0) for d in resultado["doacoes_recebidas"]),
            "total_filiacoes": len(resultado["filiacoes"]),
            "partidos": list(set(f.get("sigla_partido", "") for f in resultado["filiacoes"]))
        }
        
        return resultado


# Instância global do collector
tse_collector = TSEDadosAbertosCollector()


# Funções de conveniência
def buscar_candidaturas(cpf: str, politico_id: int = None) -> List[Dict]:
    """Busca candidaturas por CPF."""
    return tse_collector.buscar_candidaturas_por_cpf(cpf, politico_id)


def buscar_doacoes(cpf: str, politico_id: int = None, tipo: str = "doador") -> List[Dict]:
    """Busca doações por CPF."""
    return tse_collector.buscar_doacoes_por_cpf(cpf, politico_id, tipo=tipo)


def buscar_filiacoes(cpf: str, politico_id: int = None) -> List[Dict]:
    """Busca filiações por CPF."""
    return tse_collector.buscar_filiacoes_por_cpf(cpf, politico_id)


def consulta_completa(cpf: str, politico_id: int = None) -> Dict:
    """Realiza consulta completa do TSE."""
    return tse_collector.consulta_completa_cpf(cpf, politico_id)
