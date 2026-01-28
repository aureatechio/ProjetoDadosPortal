"""
Collector para consulta processual no TRF-3 (Tribunal Regional Federal da 3ª Região).

Este módulo realiza consultas de processos na Justiça Federal de São Paulo e Mato Grosso do Sul.

IMPORTANTE: Este collector requer resolução de CAPTCHA.
Utiliza estratégia similar ao TJSP com modo semi-automatizado.

URL: https://web.trf3.jus.br/consultas/Internet/ConsultaProcessual
"""

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import urlencode, quote

import httpx
from bs4 import BeautifulSoup

from app.database import get_supabase

logger = logging.getLogger(__name__)

# URLs do TRF-3
TRF3_URLS = {
    "consulta": "https://web.trf3.jus.br/consultas/Internet/ConsultaProcessual",
    "pesquisa": "https://web.trf3.jus.br/consultas/Internet/ConsultaProcessual/Pesquisar",
    "detalhes": "https://web.trf3.jus.br/consultas/Internet/ConsultaProcessual/Processo",
}


class TRF3Collector:
    """
    Collector para consulta de processos no TRF-3.
    
    Realiza scraping do sistema de consulta processual da Justiça Federal.
    """
    
    def __init__(self, captcha_api_key: str = None):
        """
        Inicializa o collector.
        
        Args:
            captcha_api_key: Chave de API para serviço de CAPTCHA (opcional)
        """
        self.supabase = get_supabase()
        self.captcha_api_key = captcha_api_key
        self.client = httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
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
                "fonte": "TRF3",
                "tipo_consulta": tipo,
                "status": "iniciado"
            }).execute()
            return result.data[0]["id"] if result.data else None
        except Exception as e:
            logger.error(f"Erro ao criar log: {e}")
            return None
    
    def _atualizar_log(self, log_id: str, status: str, registros: int = 0, 
                       mensagem: str = None, dados: dict = None):
        """Atualiza log de consulta."""
        if not log_id:
            return
        try:
            update_data = {
                "status": status,
                "registros_encontrados": registros,
                "mensagem": mensagem,
                "finalizado_em": datetime.utcnow().isoformat()
            }
            if dados:
                update_data["dados_resposta"] = dados
                
            self.supabase.table("consulta_processual_logs").update(
                update_data
            ).eq("id", log_id).execute()
        except Exception as e:
            logger.error(f"Erro ao atualizar log: {e}")
    
    def gerar_url_consulta(
        self, 
        cpf: str = None, 
        nome: str = None,
        numero_processo: str = None,
        oab: str = None
    ) -> str:
        """
        Gera URL para consulta manual no TRF-3.
        
        Args:
            cpf: CPF/CNPJ para consulta
            nome: Nome da parte
            numero_processo: Número do processo
            oab: Número OAB do advogado
            
        Returns:
            URL de consulta formatada
        """
        base_url = TRF3_URLS["consulta"]
        
        # O TRF-3 usa parâmetros na URL para pré-preencher campos
        params = {}
        
        if cpf:
            params["CpfCnpj"] = self._normalizar_cpf(cpf)
        if nome:
            params["NomeParte"] = nome
        if numero_processo:
            params["NumeroProcesso"] = numero_processo
        if oab:
            params["Oab"] = oab
        
        if params:
            return f"{base_url}?{urlencode(params)}"
        return base_url
    
    def consultar_por_cpf_semi_auto(
        self, 
        cpf: str, 
        politico_id: int = None
    ) -> Dict[str, Any]:
        """
        Método semi-automatizado: gera URL e instruções para consulta manual.
        
        Args:
            cpf: CPF para consulta
            politico_id: ID do político (opcional)
            
        Returns:
            Dicionário com URL e instruções
        """
        cpf_normalizado = self._normalizar_cpf(cpf)
        log_id = self._criar_log(politico_id, cpf_normalizado, "processos_federais_semi")
        
        url = self.gerar_url_consulta(cpf=cpf_normalizado)
        
        resultado = {
            "metodo": "semi_automatizado",
            "url_consulta": url,
            "cpf": cpf_normalizado,
            "tribunal": "TRF3",
            "cobertura": "Justiça Federal de São Paulo e Mato Grosso do Sul",
            "instrucoes": [
                f"1. Acesse a URL: {url}",
                "2. No campo 'CPF/CNPJ', verifique se o CPF está preenchido",
                "3. Resolva o CAPTCHA se solicitado",
                "4. Clique em 'Pesquisar'",
                "5. Copie o HTML da página de resultados",
                "6. Use o método 'processar_html_resultado' para extrair os processos"
            ],
            "tipos_processo": [
                "Ações contra a União, autarquias e empresas públicas federais",
                "Crimes federais",
                "Execuções fiscais",
                "Mandados de segurança contra autoridades federais",
                "Habeas corpus em matéria federal"
            ],
            "processos": [],
            "status": "aguardando_captcha"
        }
        
        self._atualizar_log(log_id, "parcial", 0, "URL gerada para consulta manual", resultado)
        
        return resultado
    
    def processar_html_resultado(
        self, 
        html: str, 
        cpf: str = None,
        politico_id: int = None
    ) -> List[Dict[str, Any]]:
        """
        Processa HTML de resultado de consulta do TRF-3.
        
        Args:
            html: HTML da página de resultados
            cpf: CPF consultado (para referência)
            politico_id: ID do político (opcional)
            
        Returns:
            Lista de processos extraídos
        """
        processos = []
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # O TRF-3 retorna processos em uma tabela ou divs
            # Tenta diferentes seletores
            
            # Estrutura de tabela
            linhas = soup.select('table.table tbody tr, table#tabelaProcessos tr')
            
            for linha in linhas:
                processo = self._extrair_processo_tabela(linha, cpf, politico_id)
                if processo:
                    processos.append(processo)
            
            # Estrutura de divs (resultados individuais)
            if not processos:
                divs = soup.select('div.processo-item, div.resultado-processo')
                for div in divs:
                    processo = self._extrair_processo_div(div, cpf, politico_id)
                    if processo:
                        processos.append(processo)
            
            # Estrutura de lista
            if not processos:
                items = soup.select('ul.lista-processos li, ol.resultados li')
                for item in items:
                    processo = self._extrair_processo_lista(item, cpf, politico_id)
                    if processo:
                        processos.append(processo)
            
            # Salva processos no banco
            if processos:
                self._salvar_processos(processos)
            
        except Exception as e:
            logger.error(f"Erro ao processar HTML TRF-3: {e}")
        
        return processos
    
    def _extrair_processo_tabela(self, linha, cpf: str, politico_id: int) -> Optional[Dict]:
        """Extrai dados de processo de uma linha de tabela."""
        try:
            colunas = linha.find_all('td')
            if len(colunas) < 2:
                return None
            
            # Primeira coluna geralmente tem o número do processo
            numero_elem = linha.select_one('a[href*="Processo"], a[href*="processo"]')
            if not numero_elem:
                numero_elem = colunas[0]
            
            numero = numero_elem.text.strip()
            if not numero or len(numero) < 10:
                return None
            
            url = ""
            if numero_elem.name == 'a':
                url = numero_elem.get('href', '')
            
            # Tenta extrair classe/assunto das outras colunas
            classe = colunas[1].text.strip() if len(colunas) > 1 else ""
            assunto = colunas[2].text.strip() if len(colunas) > 2 else ""
            vara = colunas[3].text.strip() if len(colunas) > 3 else ""
            data_str = colunas[4].text.strip() if len(colunas) > 4 else ""
            
            return {
                "politico_id": politico_id,
                "cpf": cpf,
                "numero_processo": self._normalizar_numero_processo(numero),
                "tribunal": "TRF3",
                "tipo": self._inferir_tipo_processo(classe, assunto),
                "classe": classe,
                "assunto": assunto,
                "vara": vara,
                "comarca": "Justiça Federal",
                "data_distribuicao": self._parse_data(data_str),
                "status": "ativo",
                "url_consulta": self._construir_url_processo(url, numero),
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair processo de tabela TRF-3: {e}")
            return None
    
    def _extrair_processo_div(self, div, cpf: str, politico_id: int) -> Optional[Dict]:
        """Extrai dados de processo de uma div."""
        try:
            texto = div.get_text()
            
            # Tenta extrair número do processo
            numero_match = re.search(r'\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}', texto)
            if not numero_match:
                # Tenta formato alternativo
                numero_match = re.search(r'\d{10,20}', texto)
            
            if not numero_match:
                return None
            
            numero = numero_match.group()
            
            # Tenta extrair classe
            classe_elem = div.select_one('.classe, .classe-processo')
            classe = classe_elem.text.strip() if classe_elem else ""
            
            return {
                "politico_id": politico_id,
                "cpf": cpf,
                "numero_processo": self._normalizar_numero_processo(numero),
                "tribunal": "TRF3",
                "tipo": "federal",
                "classe": classe,
                "dados_raw": {"texto_original": texto[:500]}
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair processo de div TRF-3: {e}")
            return None
    
    def _extrair_processo_lista(self, item, cpf: str, politico_id: int) -> Optional[Dict]:
        """Extrai dados de processo de um item de lista."""
        return self._extrair_processo_div(item, cpf, politico_id)
    
    def _normalizar_numero_processo(self, numero: str) -> str:
        """Normaliza número do processo (CNJ)."""
        nums = "".join(filter(str.isdigit, numero))
        
        # Formato CNJ: NNNNNNN-DD.AAAA.J.TR.OOOO
        if len(nums) == 20:
            return f"{nums[:7]}-{nums[7:9]}.{nums[9:13]}.{nums[13]}.{nums[14:16]}.{nums[16:]}"
        
        return numero.strip()
    
    def _construir_url_processo(self, url: str, numero: str) -> str:
        """Constrói URL completa para detalhes do processo."""
        if url and url.startswith('http'):
            return url
        if url:
            return f"https://web.trf3.jus.br{url}"
        return f"{TRF3_URLS['detalhes']}?NumeroProcesso={quote(numero)}"
    
    def _inferir_tipo_processo(self, classe: str, assunto: str) -> str:
        """Infere o tipo de processo baseado na classe e assunto."""
        texto = f"{classe} {assunto}".lower()
        
        if any(p in texto for p in ["criminal", "penal", "crime", "inquérito", "habeas corpus"]):
            return "criminal"
        elif any(p in texto for p in ["execução fiscal", "dívida ativa"]):
            return "fiscal"
        elif any(p in texto for p in ["previdenciário", "aposentadoria", "benefício", "inss"]):
            return "previdenciario"
        elif any(p in texto for p in ["tributário", "imposto", "contribuição"]):
            return "tributario"
        elif any(p in texto for p in ["mandado de segurança"]):
            return "mandado_seguranca"
        else:
            return "federal"
    
    def _parse_data(self, data_str: str) -> Optional[str]:
        """Converte string de data para formato ISO."""
        if not data_str:
            return None
        try:
            for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]:
                try:
                    dt = datetime.strptime(data_str.strip(), fmt)
                    return dt.strftime("%Y-%m-%d")
                except:
                    continue
            return None
        except:
            return None
    
    def _salvar_processos(self, processos: List[Dict]):
        """Salva processos no banco de dados."""
        for processo in processos:
            try:
                self.supabase.table("processos_judiciais").upsert(
                    processo,
                    on_conflict="numero_processo,tribunal"
                ).execute()
            except Exception as e:
                logger.error(f"Erro ao salvar processo TRF-3: {e}")
    
    def consultar_por_nome(
        self, 
        nome: str, 
        politico_id: int = None
    ) -> Dict[str, Any]:
        """
        Consulta processos por nome da parte.
        
        Args:
            nome: Nome para consulta
            politico_id: ID do político
            
        Returns:
            Resultado da consulta
        """
        log_id = self._criar_log(politico_id, "", "processos_federais_nome")
        
        url = self.gerar_url_consulta(nome=nome)
        
        resultado = {
            "metodo": "semi_automatizado",
            "url_consulta": url,
            "nome": nome,
            "tribunal": "TRF3",
            "instrucoes": [
                f"1. Acesse a URL: {url}",
                "2. Verifique se o nome está preenchido corretamente",
                "3. Resolva o CAPTCHA se solicitado",
                "4. Clique em 'Pesquisar'",
            ],
            "processos": [],
            "status": "aguardando_captcha"
        }
        
        self._atualizar_log(log_id, "parcial", 0, "URL gerada para consulta manual", resultado)
        
        return resultado


# Instância global
trf3_collector = TRF3Collector()


# Funções de conveniência
def gerar_url_trf3(cpf: str = None, nome: str = None) -> str:
    """Gera URL de consulta no TRF-3."""
    return trf3_collector.gerar_url_consulta(cpf=cpf, nome=nome)


def consultar_trf3(cpf: str, politico_id: int = None) -> Dict:
    """Consulta processos no TRF-3 (modo semi-automatizado)."""
    return trf3_collector.consultar_por_cpf_semi_auto(cpf, politico_id)


def processar_resultado_trf3(html: str, cpf: str = None, politico_id: int = None) -> List[Dict]:
    """Processa HTML de resultado do TRF-3."""
    return trf3_collector.processar_html_resultado(html, cpf, politico_id)
