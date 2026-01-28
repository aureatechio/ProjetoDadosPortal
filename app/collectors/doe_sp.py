"""
Collector para consulta no Diário Oficial do Estado de São Paulo (DOE-SP).

Este módulo realiza buscas no DOE-SP para encontrar:
- Nomeações e exonerações
- Processos administrativos (PADs)
- Promoções e progressões funcionais
- Multas e penalidades
- Licitações e contratos

Fonte: https://www.imprensaoficial.com.br/

NOTA: A busca é feita por nome, não por CPF.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from urllib.parse import urlencode, quote

import httpx
from bs4 import BeautifulSoup

from app.database import get_supabase

logger = logging.getLogger(__name__)

# URLs da Imprensa Oficial
DOE_URLS = {
    "busca": "https://www.imprensaoficial.com.br/DO/BuscaDO2001Resultado_11_3.aspx",
    "pesquisa": "https://www.imprensaoficial.com.br/DO/GatewayPDF.aspx",
    "home": "https://www.imprensaoficial.com.br/",
}


class DOESPCollector:
    """
    Collector para consulta no Diário Oficial de São Paulo.
    
    Realiza busca por nome em publicações oficiais.
    """
    
    def __init__(self):
        """Inicializa o collector."""
        self.supabase = get_supabase()
        self.client = httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            }
        )
        
    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
    def _criar_log(self, politico_id: int, nome: str, tipo: str) -> str:
        """Cria log de consulta."""
        try:
            result = self.supabase.table("consulta_processual_logs").insert({
                "politico_id": politico_id,
                "cpf": "",
                "fonte": "DOE_SP",
                "tipo_consulta": tipo,
                "status": "iniciado",
                "mensagem": f"Busca por: {nome}"
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
    
    def gerar_url_busca(
        self, 
        nome: str,
        data_inicio: str = None,
        data_fim: str = None,
        caderno: str = "todos"
    ) -> str:
        """
        Gera URL para busca no DOE-SP.
        
        Args:
            nome: Nome para buscar
            data_inicio: Data início (DD/MM/AAAA)
            data_fim: Data fim (DD/MM/AAAA)
            caderno: Tipo de caderno (executivo, legislativo, judicial, todos)
            
        Returns:
            URL de busca formatada
        """
        # Datas padrão: último ano
        if not data_fim:
            data_fim = datetime.now().strftime("%d/%m/%Y")
        if not data_inicio:
            data_inicio = (datetime.now() - timedelta(days=365)).strftime("%d/%m/%Y")
        
        # Mapeamento de cadernos
        cadernos = {
            "executivo": "1",
            "legislativo": "2", 
            "judicial": "3",
            "todos": "0"
        }
        
        params = {
            "txtPalavraChave": nome,
            "txtDataIni": data_inicio,
            "txtDataFim": data_fim,
            "rdoCaderno": cadernos.get(caderno, "0"),
            "tipoBusca": "avancada"
        }
        
        return f"{DOE_URLS['busca']}?{urlencode(params)}"
    
    def buscar_por_nome_semi_auto(
        self, 
        nome: str, 
        politico_id: int = None,
        data_inicio: str = None,
        data_fim: str = None
    ) -> Dict[str, Any]:
        """
        Método semi-automatizado: gera URL e instruções para consulta manual.
        
        Args:
            nome: Nome para buscar
            politico_id: ID do político (opcional)
            data_inicio: Data início (opcional)
            data_fim: Data fim (opcional)
            
        Returns:
            Dicionário com URL e instruções
        """
        log_id = self._criar_log(politico_id, nome, "atos_administrativos")
        
        url = self.gerar_url_busca(nome, data_inicio, data_fim)
        
        resultado = {
            "metodo": "semi_automatizado",
            "url_consulta": url,
            "nome": nome,
            "fonte": "DOE_SP",
            "descricao": "Diário Oficial do Estado de São Paulo",
            "instrucoes": [
                f"1. Acesse a URL: {url}",
                "2. Verifique se o nome está preenchido corretamente",
                "3. Ajuste o período de busca se necessário",
                "4. Clique em 'Pesquisar'",
                "5. Os resultados mostrarão publicações onde o nome aparece",
                "6. Copie o HTML ou anote os resultados manualmente"
            ],
            "tipos_publicacao": [
                "Nomeações e exonerações de servidores públicos",
                "Processos administrativos disciplinares (PADs)",
                "Promoções e progressões funcionais",
                "Multas e penalidades",
                "Licitações e contratos públicos",
                "Aposentadorias e pensões",
                "Designações e substituições"
            ],
            "publicacoes": [],
            "status": "aguardando_consulta"
        }
        
        self._atualizar_log(log_id, "parcial", 0, "URL gerada para consulta manual", resultado)
        
        return resultado
    
    def processar_html_resultado(
        self, 
        html: str, 
        nome: str = None,
        politico_id: int = None
    ) -> List[Dict[str, Any]]:
        """
        Processa HTML de resultado de busca do DOE-SP.
        
        Args:
            html: HTML da página de resultados
            nome: Nome buscado (para referência)
            politico_id: ID do político (opcional)
            
        Returns:
            Lista de publicações extraídas
        """
        publicacoes = []
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # O DOE-SP retorna resultados em diferentes formatos
            # Tenta vários seletores
            
            # Formato de tabela
            linhas = soup.select('table.resultado tr, table#resultados tr')
            
            for linha in linhas:
                pub = self._extrair_publicacao_tabela(linha, nome, politico_id)
                if pub:
                    publicacoes.append(pub)
            
            # Formato de lista/divs
            if not publicacoes:
                divs = soup.select('div.resultado-item, div.publicacao')
                for div in divs:
                    pub = self._extrair_publicacao_div(div, nome, politico_id)
                    if pub:
                        publicacoes.append(pub)
            
            # Tenta extrair de links de PDF
            if not publicacoes:
                links = soup.select('a[href*="GatewayPDF"], a[href*=".pdf"]')
                for link in links:
                    pub = self._extrair_publicacao_link(link, nome, politico_id)
                    if pub:
                        publicacoes.append(pub)
            
            # Classificar publicações
            for pub in publicacoes:
                pub["tipo_ato"] = self._classificar_tipo_ato(pub.get("texto", ""))
            
        except Exception as e:
            logger.error(f"Erro ao processar HTML DOE-SP: {e}")
        
        return publicacoes
    
    def _extrair_publicacao_tabela(self, linha, nome: str, politico_id: int) -> Optional[Dict]:
        """Extrai dados de publicação de uma linha de tabela."""
        try:
            colunas = linha.find_all('td')
            if len(colunas) < 2:
                return None
            
            # Estrutura típica: Data | Caderno | Página | Texto/Link
            data = colunas[0].text.strip() if len(colunas) > 0 else ""
            caderno = colunas[1].text.strip() if len(colunas) > 1 else ""
            pagina = colunas[2].text.strip() if len(colunas) > 2 else ""
            
            # Texto ou link
            texto_elem = colunas[-1] if colunas else None
            texto = texto_elem.text.strip() if texto_elem else ""
            
            # Link para PDF
            link = linha.select_one('a[href]')
            url_pdf = link.get('href', '') if link else ""
            
            if not texto or len(texto) < 10:
                return None
            
            return {
                "politico_id": politico_id,
                "nome_buscado": nome,
                "data_publicacao": self._parse_data(data),
                "caderno": caderno,
                "pagina": pagina,
                "texto": texto[:2000],  # Limita tamanho
                "url_pdf": self._construir_url_pdf(url_pdf),
                "fonte": "DOE_SP"
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair publicação de tabela: {e}")
            return None
    
    def _extrair_publicacao_div(self, div, nome: str, politico_id: int) -> Optional[Dict]:
        """Extrai dados de publicação de uma div."""
        try:
            texto = div.get_text(strip=True)
            if not texto or len(texto) < 20:
                return None
            
            # Tenta extrair data
            data_match = re.search(r'\d{2}/\d{2}/\d{4}', texto)
            data = data_match.group() if data_match else ""
            
            # Link
            link = div.select_one('a[href]')
            url = link.get('href', '') if link else ""
            
            return {
                "politico_id": politico_id,
                "nome_buscado": nome,
                "data_publicacao": self._parse_data(data),
                "texto": texto[:2000],
                "url_pdf": self._construir_url_pdf(url),
                "fonte": "DOE_SP"
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair publicação de div: {e}")
            return None
    
    def _extrair_publicacao_link(self, link, nome: str, politico_id: int) -> Optional[Dict]:
        """Extrai dados de publicação de um link."""
        try:
            texto = link.get_text(strip=True)
            url = link.get('href', '')
            
            # Tenta extrair data do texto ou URL
            data_match = re.search(r'\d{2}/\d{2}/\d{4}', texto + url)
            data = data_match.group() if data_match else ""
            
            return {
                "politico_id": politico_id,
                "nome_buscado": nome,
                "data_publicacao": self._parse_data(data),
                "texto": texto[:500] if texto else "Publicação no DOE-SP",
                "url_pdf": self._construir_url_pdf(url),
                "fonte": "DOE_SP"
            }
            
        except Exception as e:
            logger.error(f"Erro ao extrair publicação de link: {e}")
            return None
    
    def _construir_url_pdf(self, url: str) -> str:
        """Constrói URL completa para o PDF."""
        if not url:
            return ""
        if url.startswith('http'):
            return url
        return f"https://www.imprensaoficial.com.br{url}"
    
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
    
    def _classificar_tipo_ato(self, texto: str) -> str:
        """Classifica o tipo de ato administrativo."""
        texto_lower = texto.lower()
        
        classificacoes = {
            "nomeacao": ["nomear", "nomeação", "nomeado", "designar", "designação"],
            "exoneracao": ["exonerar", "exoneração", "exonerado", "dispensa"],
            "promocao": ["promover", "promoção", "promovido", "progressão"],
            "aposentadoria": ["aposentar", "aposentadoria", "aposentado"],
            "penalidade": ["penalidade", "suspensão", "advertência", "demissão", "cassação"],
            "pad": ["processo administrativo", "pad", "sindicância", "inquérito"],
            "licitacao": ["licitação", "pregão", "concorrência", "tomada de preço"],
            "contrato": ["contrato", "aditivo", "termo de ajuste"],
            "portaria": ["portaria"],
            "decreto": ["decreto"],
            "resolucao": ["resolução"],
        }
        
        for tipo, palavras in classificacoes.items():
            if any(p in texto_lower for p in palavras):
                return tipo
        
        return "outros"
    
    def buscar_atos_servidores(
        self, 
        nome: str, 
        politico_id: int = None
    ) -> Dict[str, Any]:
        """
        Busca específica por atos de servidores públicos.
        
        Args:
            nome: Nome do servidor
            politico_id: ID do político
            
        Returns:
            Resultado da busca
        """
        # Busca no caderno executivo (servidores estaduais)
        url = self.gerar_url_busca(nome, caderno="executivo")
        
        resultado = {
            "metodo": "semi_automatizado",
            "url_consulta": url,
            "nome": nome,
            "fonte": "DOE_SP",
            "caderno": "Executivo (Servidores Estaduais)",
            "instrucoes": [
                "Esta busca foca em atos relacionados a servidores públicos estaduais.",
                f"1. Acesse: {url}",
                "2. Pesquise e anote os resultados",
                "3. Tipos de atos comuns: nomeação, exoneração, promoção, PAD"
            ],
            "publicacoes": [],
            "status": "aguardando_consulta"
        }
        
        return resultado


# Instância global
doe_sp_collector = DOESPCollector()


# Funções de conveniência
def gerar_url_doe_sp(nome: str, data_inicio: str = None, data_fim: str = None) -> str:
    """Gera URL de busca no DOE-SP."""
    return doe_sp_collector.gerar_url_busca(nome, data_inicio, data_fim)


def buscar_doe_sp(nome: str, politico_id: int = None) -> Dict:
    """Busca publicações no DOE-SP (modo semi-automatizado)."""
    return doe_sp_collector.buscar_por_nome_semi_auto(nome, politico_id)


def processar_resultado_doe_sp(html: str, nome: str = None, politico_id: int = None) -> List[Dict]:
    """Processa HTML de resultado do DOE-SP."""
    return doe_sp_collector.processar_html_resultado(html, nome, politico_id)
