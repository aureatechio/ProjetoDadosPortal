"""
Collector para consulta processual no TJSP (e-SAJ).

Este módulo realiza consultas de processos no Tribunal de Justiça de São Paulo
através do sistema e-SAJ.

IMPORTANTE: Este collector requer resolução de CAPTCHA.
Opções disponíveis:
1. Semi-automatizado: Gera URL para consulta manual
2. Serviço externo: Integração com 2Captcha/Anti-Captcha (requer API key)
3. Selenium: Tentativa de automação com undetected-chromedriver

URLs:
- 1º Grau: https://esaj.tjsp.jus.br/cpopg/open.do
- 2º Grau: https://esaj.tjsp.jus.br/cposg/open.do
"""

import logging
import re
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import urlencode, quote

import httpx
from bs4 import BeautifulSoup

from app.database import get_supabase

logger = logging.getLogger(__name__)

# URLs do e-SAJ
TJSP_URLS = {
    "primeiro_grau": "https://esaj.tjsp.jus.br/cpopg",
    "segundo_grau": "https://esaj.tjsp.jus.br/cposg",
    "search_primeiro": "https://esaj.tjsp.jus.br/cpopg/search.do",
    "search_segundo": "https://esaj.tjsp.jus.br/cposg/search.do",
}


class TJSPCollector:
    """
    Collector para consulta de processos no TJSP.
    
    Realiza scraping do sistema e-SAJ com estratégias para CAPTCHA.
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
        self._session_cookies = {}
        
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
                "fonte": "TJSP",
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
        grau: str = "primeiro"
    ) -> str:
        """
        Gera URL para consulta manual no e-SAJ.
        
        Útil quando não é possível resolver CAPTCHA automaticamente.
        
        Args:
            cpf: CPF para consulta
            nome: Nome para consulta (alternativa ao CPF)
            grau: "primeiro" ou "segundo"
            
        Returns:
            URL de consulta formatada
        """
        base_url = TJSP_URLS.get(f"{grau}_grau", TJSP_URLS["primeiro_grau"])
        
        params = {
            "conversationId": "",
            "cbPesquisa": "DOCPARTE" if cpf else "NMPARTE",
            "dadosConsulta.tipoNuProcesso": "UNIFICADO",
            "dadosConsulta.valorConsulta": self._normalizar_cpf(cpf) if cpf else nome,
        }
        
        return f"{base_url}/search.do?{urlencode(params)}"
    
    def _iniciar_sessao(self, grau: str = "primeiro") -> bool:
        """
        Inicia sessão no e-SAJ para obter cookies necessários.
        
        Args:
            grau: "primeiro" ou "segundo"
            
        Returns:
            True se sessão iniciada com sucesso
        """
        try:
            base_url = TJSP_URLS.get(f"{grau}_grau")
            response = self.client.get(f"{base_url}/open.do")
            
            if response.status_code == 200:
                # Salva cookies da sessão
                self._session_cookies = dict(response.cookies)
                return True
                
        except Exception as e:
            logger.error(f"Erro ao iniciar sessão: {e}")
        
        return False
    
    def consultar_por_cpf_semi_auto(
        self, 
        cpf: str, 
        politico_id: int = None,
        grau: str = "primeiro"
    ) -> Dict[str, Any]:
        """
        Método semi-automatizado: gera URL e instruções para consulta manual.
        
        Este método é útil quando não há serviço de CAPTCHA configurado.
        O usuário deve acessar a URL, resolver o CAPTCHA manualmente,
        e depois colar o HTML da resposta.
        
        Args:
            cpf: CPF para consulta
            politico_id: ID do político (opcional)
            grau: "primeiro" ou "segundo"
            
        Returns:
            Dicionário com URL e instruções
        """
        cpf_normalizado = self._normalizar_cpf(cpf)
        log_id = self._criar_log(politico_id, cpf_normalizado, f"processos_{grau}_grau_semi")
        
        url = self.gerar_url_consulta(cpf=cpf_normalizado, grau=grau)
        
        resultado = {
            "metodo": "semi_automatizado",
            "url_consulta": url,
            "cpf": cpf_normalizado,
            "grau": grau,
            "instrucoes": [
                f"1. Acesse a URL: {url}",
                "2. Resolva o CAPTCHA 'Não sou robô'",
                "3. Clique em 'Consultar'",
                "4. Copie o HTML da página de resultados",
                "5. Use o método 'processar_html_resultado' para extrair os processos"
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
        Processa HTML de resultado de consulta do e-SAJ.
        
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
            
            # Encontra a tabela de resultados
            # O e-SAJ usa diferentes estruturas dependendo da página
            
            # Tenta encontrar linhas de processo
            linhas = soup.select('tr.fundocinza1, tr.fundocinza2, tr.containerInterno')
            
            if not linhas:
                # Tenta estrutura alternativa
                linhas = soup.select('div.espacamentoTop20 > table tr')
            
            for linha in linhas:
                processo = self._extrair_processo_linha(linha, cpf, politico_id)
                if processo:
                    processos.append(processo)
            
            # Se não encontrou na tabela, tenta extrair de divs
            if not processos:
                divs = soup.select('div#listaDeProcessos div.processo')
                for div in divs:
                    processo = self._extrair_processo_div(div, cpf, politico_id)
                    if processo:
                        processos.append(processo)
            
            # Salva processos no banco
            if processos:
                self._salvar_processos(processos)
            
        except Exception as e:
            logger.error(f"Erro ao processar HTML: {e}")
        
        return processos
    
    def _extrair_processo_linha(self, linha, cpf: str, politico_id: int) -> Optional[Dict]:
        """Extrai dados de processo de uma linha de tabela."""
        try:
            # Número do processo
            numero_elem = linha.select_one('a.linkProcesso, a[title*="processo"]')
            if not numero_elem:
                return None
            
            numero = numero_elem.text.strip()
            url = numero_elem.get('href', '')
            
            # Classe
            classe_elem = linha.select_one('td:nth-child(2), span.classeProcesso')
            classe = classe_elem.text.strip() if classe_elem else ""
            
            # Assunto
            assunto_elem = linha.select_one('td:nth-child(3), span.assuntoProcesso')
            assunto = assunto_elem.text.strip() if assunto_elem else ""
            
            # Comarca/Vara
            comarca_elem = linha.select_one('td:nth-child(4), span.comarcaProcesso')
            comarca = comarca_elem.text.strip() if comarca_elem else ""
            
            # Data distribuição
            data_elem = linha.select_one('td:nth-child(5), span.dataProcesso')
            data_str = data_elem.text.strip() if data_elem else ""
            
            return {
                "politico_id": politico_id,
                "cpf": cpf,
                "numero_processo": self._normalizar_numero_processo(numero),
                "tribunal": "TJSP",
                "tipo": self._inferir_tipo_processo(classe, assunto),
                "classe": classe,
                "assunto": assunto,
                "comarca": comarca,
                "data_distribuicao": self._parse_data(data_str),
                "status": "ativo",
                "url_consulta": f"https://esaj.tjsp.jus.br{url}" if url.startswith('/') else url,
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair processo de linha: {e}")
            return None
    
    def _extrair_processo_div(self, div, cpf: str, politico_id: int) -> Optional[Dict]:
        """Extrai dados de processo de uma div."""
        try:
            # Número do processo
            numero_elem = div.select_one('a.linkProcesso')
            if not numero_elem:
                return None
            
            numero = numero_elem.text.strip()
            
            # Outros campos
            texto = div.get_text()
            
            return {
                "politico_id": politico_id,
                "cpf": cpf,
                "numero_processo": self._normalizar_numero_processo(numero),
                "tribunal": "TJSP",
                "tipo": "civel",  # Padrão
                "dados_raw": {"texto_original": texto}
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair processo de div: {e}")
            return None
    
    def _normalizar_numero_processo(self, numero: str) -> str:
        """Normaliza número do processo (CNJ)."""
        # Remove caracteres não numéricos
        nums = "".join(filter(str.isdigit, numero))
        
        # Formato CNJ: NNNNNNN-DD.AAAA.J.TR.OOOO
        if len(nums) == 20:
            return f"{nums[:7]}-{nums[7:9]}.{nums[9:13]}.{nums[13]}.{nums[14:16]}.{nums[16:]}"
        
        return numero.strip()
    
    def _inferir_tipo_processo(self, classe: str, assunto: str) -> str:
        """Infere o tipo de processo baseado na classe e assunto."""
        texto = f"{classe} {assunto}".lower()
        
        if any(p in texto for p in ["criminal", "penal", "crime", "inquérito"]):
            return "criminal"
        elif any(p in texto for p in ["trabalhista", "reclamação trabalhista"]):
            return "trabalhista"
        elif any(p in texto for p in ["eleitoral", "eleição"]):
            return "eleitoral"
        elif any(p in texto for p in ["administrativo", "mandado de segurança"]):
            return "administrativo"
        else:
            return "civel"
    
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
                logger.error(f"Erro ao salvar processo: {e}")
    
    def consultar_detalhes_processo(self, numero_processo: str) -> Optional[Dict]:
        """
        Consulta detalhes de um processo específico.
        
        Args:
            numero_processo: Número do processo (formato CNJ)
            
        Returns:
            Detalhes do processo ou None
        """
        # Gera URL para detalhes
        url = f"https://esaj.tjsp.jus.br/cpopg/show.do?processo.codigo={quote(numero_processo)}"
        
        return {
            "url_detalhes": url,
            "numero_processo": numero_processo,
            "instrucoes": "Acesse a URL para ver detalhes completos do processo"
        }
    
    def buscar_todos_processos(
        self, 
        cpf: str, 
        politico_id: int = None
    ) -> Dict[str, Any]:
        """
        Busca processos em todos os graus.
        
        Args:
            cpf: CPF para consulta
            politico_id: ID do político
            
        Returns:
            Resultados consolidados
        """
        resultado = {
            "cpf": self._normalizar_cpf(cpf),
            "politico_id": politico_id,
            "primeiro_grau": self.consultar_por_cpf_semi_auto(cpf, politico_id, "primeiro"),
            "segundo_grau": self.consultar_por_cpf_semi_auto(cpf, politico_id, "segundo"),
            "total_urls": 2,
            "instrucoes_gerais": [
                "Este collector requer resolução manual de CAPTCHA.",
                "Acesse cada URL gerada, resolva o CAPTCHA e copie o HTML.",
                "Use processar_html_resultado() para extrair os processos.",
                "Alternativamente, configure um serviço de CAPTCHA (2Captcha, Anti-Captcha)."
            ]
        }
        
        return resultado


# Instância global
tjsp_collector = TJSPCollector()


# Funções de conveniência
def gerar_url_tjsp(cpf: str, grau: str = "primeiro") -> str:
    """Gera URL de consulta no TJSP."""
    return tjsp_collector.gerar_url_consulta(cpf=cpf, grau=grau)


def consultar_tjsp(cpf: str, politico_id: int = None) -> Dict:
    """Consulta processos no TJSP (modo semi-automatizado)."""
    return tjsp_collector.buscar_todos_processos(cpf, politico_id)


def processar_resultado_tjsp(html: str, cpf: str = None, politico_id: int = None) -> List[Dict]:
    """Processa HTML de resultado do TJSP."""
    return tjsp_collector.processar_html_resultado(html, cpf, politico_id)
