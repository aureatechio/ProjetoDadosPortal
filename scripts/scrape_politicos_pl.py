"""
Script para extrair dados de políticos do site do Partido Liberal (PL).
URL: https://partidoliberal.org.br/pl-nos-estados/

Este script faz scraping da página para obter:
- Nome do político
- Cargo (Vereador, Deputado Federal, Deputado Estadual, Senador, Governador, Vice Governador, Prefeito, Presidente)
- Cidade onde exerce a função (se aplicável)
- Estado
- Status de eleição (eleito ou não)

Depois insere/atualiza os dados na tabela 'politico' do Supabase.
"""

import os
import sys
import re
import time
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

# Adiciona o diretório pai ao path para importar módulos do app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase import create_client, Client
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Politico:
    """Estrutura de dados para um político"""
    name: str
    funcao: str  # Cargo: Vereador, Deputado Federal, etc.
    cidade: Optional[str]  # Cidade onde exerce função (None para cargos estaduais/federais)
    estado: str  # Sigla do estado (BA, SP, etc.)
    eleito: bool  # Se foi eleito
    description: str  # Descrição completa do cargo


def get_supabase_client() -> Client:
    """Cria e retorna cliente Supabase"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidos no .env")
    
    return create_client(url, key)


def extrair_info_cargo(descricao: str) -> Tuple[str, Optional[str]]:
    """
    Extrai o cargo e a cidade da descrição.
    
    Exemplos:
    - "Vereador de Teofilândia" -> ("Vereador", "Teofilândia")
    - "Deputado Federal" -> ("Deputado Federal", None)
    - "Prefeito de Cuiabá" -> ("Prefeito", "Cuiabá")
    - "Deputado Estadual" -> ("Deputado Estadual", None)
    - "Governador" -> ("Governador", None)
    """
    descricao = descricao.strip()
    
    # Cargos sem cidade (estaduais/federais)
    cargos_sem_cidade = [
        "Deputado Federal",
        "Deputada Federal",
        "Deputado Estadual",
        "Deputada Estadual",
        "Deputado Distrital",
        "Deputada Distrital",
        "Senador",
        "Senadora",
        "Governador",
        "Governadora",
        "Vice-Governador",
        "Vice-Governadora",
        "Vice Governador",
        "Vice Governadora",
        "Presidente",
    ]
    
    # Verifica se é cargo sem cidade
    for cargo in cargos_sem_cidade:
        if descricao.lower().startswith(cargo.lower()):
            return cargo, None
    
    # Tenta extrair cargo e cidade (formato: "Cargo de Cidade")
    match = re.match(r'^(Vereador[a]?|Prefeito[a]?|Perfeita)\s+(?:de|do|da|dos|das)?\s*(.+)$', descricao, re.IGNORECASE)
    if match:
        cargo = match.group(1).strip()
        cidade = match.group(2).strip()
        # Normaliza cargo
        if cargo.lower() in ['perfeita', 'prefeita']:
            cargo = 'Prefeita'
        elif cargo.lower() == 'prefeito':
            cargo = 'Prefeito'
        elif cargo.lower() == 'vereadora':
            cargo = 'Vereadora'
        elif cargo.lower() == 'vereador':
            cargo = 'Vereador'
        return cargo, cidade
    
    # Se não conseguiu parsear, retorna a descrição como cargo
    return descricao, None


def extrair_estado(texto_estado: str) -> str:
    """
    Extrai a sigla do estado do texto.
    Exemplo: "PL | BA" -> "BA"
    """
    match = re.search(r'\|\s*([A-Z]{2})', texto_estado)
    if match:
        return match.group(1)
    return texto_estado.strip()


def scrape_politicos_pl() -> List[Politico]:
    """
    Faz scraping da página do PL para extrair dados dos políticos.
    """
    url = "https://partidoliberal.org.br/pl-nos-estados/"
    
    logger.info(f"Iniciando scraping de {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Erro ao acessar página: {e}")
        raise
    
    soup = BeautifulSoup(response.content, 'html.parser')
    politicos = []
    
    # Busca todos os elementos de político (geralmente são cards ou list items)
    # Ajuste os seletores conforme a estrutura real da página
    cards = soup.select('li, .politico-card, .member-card, article')
    
    logger.info(f"Encontrados {len(cards)} elementos para analisar")
    
    for card in cards:
        try:
            # Tenta extrair nome
            nome_elem = card.select_one('h2, h3, h4, .nome, .name, strong, a[title]')
            if not nome_elem:
                continue
            
            nome = nome_elem.get_text(strip=True) or nome_elem.get('title', '')
            if not nome or len(nome) < 3:
                continue
            
            # Tenta extrair descrição/cargo
            desc_elem = card.select_one('.cargo, .description, .role, p, span')
            descricao = desc_elem.get_text(strip=True) if desc_elem else ""
            
            # Tenta extrair estado
            estado_elem = card.select_one('.estado, .state, .partido')
            estado_texto = estado_elem.get_text(strip=True) if estado_elem else ""
            
            # Verifica se está eleito
            card_text = card.get_text().lower()
            eleito = 'eleito' in card_text or 'eleita' in card_text
            
            if nome and descricao:
                funcao, cidade = extrair_info_cargo(descricao)
                estado = extrair_estado(estado_texto)
                
                if estado and len(estado) == 2:
                    politico = Politico(
                        name=nome,
                        funcao=funcao,
                        cidade=cidade,
                        estado=estado,
                        eleito=eleito,
                        description=descricao
                    )
                    politicos.append(politico)
                    
        except Exception as e:
            logger.debug(f"Erro ao processar card: {e}")
            continue
    
    logger.info(f"Extraídos {len(politicos)} políticos do site")
    return politicos


def parse_politicos_from_text(html_content: str) -> List[Politico]:
    """
    Parser alternativo que extrai dados baseado em padrões de texto.
    Usado quando o scraping de elementos não funciona bem.
    """
    politicos = []
    
    # Padrão para encontrar blocos de político no HTML
    # Baseado na estrutura observada nos dados externos
    patterns = [
        # Nome seguido de cargo e estado
        r'([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Úa-zà-ú]+)*)\s+(Vereador[a]?|Prefeito[a]?|Perfeita|Deputado[a]?\s+(?:Federal|Estadual|Distrital)|Senador[a]?|Governador[a]?|Vice[\-\s]?Governador[a]?|Presidente)(?:\s+(?:de|do|da|dos|das)\s+)?([^|]*?)?\s*PL\s*\|\s*([A-Z]{2})\s*(Eleito|Eleita)?',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html_content, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            try:
                nome = match[0].strip()
                cargo_base = match[1].strip()
                cidade = match[2].strip() if len(match) > 2 and match[2] else None
                estado = match[3].strip() if len(match) > 3 else ""
                eleito = bool(match[4]) if len(match) > 4 else False
                
                # Constrói descrição
                if cidade:
                    descricao = f"{cargo_base} de {cidade}"
                else:
                    descricao = cargo_base
                
                funcao, cidade_parsed = extrair_info_cargo(descricao)
                
                politico = Politico(
                    name=nome,
                    funcao=funcao,
                    cidade=cidade_parsed or cidade,
                    estado=estado,
                    eleito=eleito,
                    description=descricao
                )
                politicos.append(politico)
                
            except Exception as e:
                logger.debug(f"Erro ao processar match: {e}")
                continue
    
    return politicos


def parse_from_known_data() -> List[Politico]:
    """
    Parser que usa os dados conhecidos da estrutura da página.
    Baseado nos dados já extraídos e disponíveis.
    """
    # Dados extraídos do site (estrutura conhecida)
    # Formato: (nome, cargo_completo, estado, eleito)
    dados_conhecidos = [
        ("Abel do Alecrim", "Vereador de Teofilândia", "BA", False),
        ("Abílio Brunini", "Deputado Federal", "MT", False),
        ("Abilio Jacques Brunini Moumer", "Prefeito de Cuiabá", "MT", True),
        ("Abimael Santos", "Deputado Estadual", "PE", False),
        ("Abraão David Neto", "Prefeito de Nilópolis", "RJ", True),
        # ... adicione mais conforme necessário
    ]
    
    politicos = []
    for nome, cargo_completo, estado, eleito in dados_conhecidos:
        funcao, cidade = extrair_info_cargo(cargo_completo)
        politicos.append(Politico(
            name=nome,
            funcao=funcao,
            cidade=cidade,
            estado=estado,
            eleito=eleito,
            description=cargo_completo
        ))
    
    return politicos


def fazer_scraping_completo() -> List[Politico]:
    """
    Faz o scraping completo usando múltiplas estratégias.
    """
    url = "https://partidoliberal.org.br/pl-nos-estados/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    logger.info(f"Baixando página: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        html = response.text
    except requests.RequestException as e:
        logger.error(f"Erro ao acessar página: {e}")
        raise
    
    soup = BeautifulSoup(html, 'html.parser')
    politicos = []
    
    # Estratégia 1: Buscar elementos específicos
    # Os cards de político geralmente têm uma estrutura consistente
    cards = soup.find_all(['li', 'article', 'div'], class_=lambda x: x and ('member' in str(x).lower() or 'politico' in str(x).lower() or 'card' in str(x).lower()))
    
    if not cards:
        # Tenta buscar por estrutura de lista
        cards = soup.select('ul li')
    
    logger.info(f"Encontrados {len(cards)} cards potenciais")
    
    for card in cards:
        text = card.get_text(separator=' ', strip=True)
        
        # Padrão: Nome | Cargo | PL | UF | [Eleito]
        # Exemplo: "Abílio Brunini Deputado Federal PL | MT"
        
        # Tenta extrair usando regex
        match = re.search(
            r'([A-ZÀ-Ú][a-zà-ú\s\-\']+?)[\s\|]+'
            r'(Vereador[a]?|Prefeito[a]?|Perfeita|Deputado[a]?\s*(?:Federal|Estadual|Distrital)?|Senador[a]?|Governador[a]?|Vice[\-\s]?Governador[a]?|Presidente)'
            r'(?:\s+(?:de|do|da|dos|das)\s+([A-ZÀ-Ú][a-zà-ú\s\-\']+?))?'
            r'.*?PL\s*\|\s*([A-Z]{2})'
            r'(?:.*?(Eleito|Eleita))?',
            text,
            re.IGNORECASE
        )
        
        if match:
            nome = match.group(1).strip()
            cargo = match.group(2).strip()
            cidade = match.group(3).strip() if match.group(3) else None
            estado = match.group(4).strip()
            eleito = bool(match.group(5))
            
            # Monta descrição
            if cidade:
                descricao = f"{cargo} de {cidade}"
            else:
                descricao = cargo
            
            politico = Politico(
                name=nome,
                funcao=cargo,
                cidade=cidade,
                estado=estado,
                eleito=eleito,
                description=descricao
            )
            
            # Evita duplicatas
            if not any(p.name == politico.name for p in politicos):
                politicos.append(politico)
    
    # Se não encontrou nada, tenta parser alternativo no HTML inteiro
    if len(politicos) < 10:
        logger.info("Tentando parser alternativo...")
        politicos_alt = parse_politicos_from_text(html)
        for p in politicos_alt:
            if not any(existing.name == p.name for existing in politicos):
                politicos.append(p)
    
    logger.info(f"Total de políticos extraídos: {len(politicos)}")
    return politicos


def upsert_politicos(client: Client, politicos: List[Politico]) -> Dict[str, int]:
    """
    Insere ou atualiza políticos no banco de dados.
    Retorna estatísticas de inserção/atualização.
    """
    stats = {
        "inseridos": 0,
        "atualizados": 0,
        "erros": 0,
        "ignorados": 0
    }
    
    for politico in politicos:
        try:
            # Verifica se político já existe pelo nome
            existing = client.table("politico")\
                .select("id, name")\
                .ilike("name", f"%{politico.name}%")\
                .execute()
            
            data = {
                "name": politico.name,
                "funcao": politico.funcao,
                "cidade": politico.cidade,
                "estado": politico.estado,
                "eleito": politico.eleito,
                "description": politico.description,
                "active": True  # Marca como ativo
            }
            
            if existing.data and len(existing.data) > 0:
                # Atualiza registro existente
                politico_id = existing.data[0]["id"]
                client.table("politico")\
                    .update(data)\
                    .eq("id", politico_id)\
                    .execute()
                stats["atualizados"] += 1
                logger.debug(f"Atualizado: {politico.name}")
            else:
                # Insere novo registro
                client.table("politico")\
                    .insert(data)\
                    .execute()
                stats["inseridos"] += 1
                logger.debug(f"Inserido: {politico.name}")
                
            # Pequeno delay para evitar rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Erro ao processar {politico.name}: {e}")
            stats["erros"] += 1
    
    return stats


def main():
    """
    Função principal que executa o scraping e atualização do banco.
    """
    logger.info("=" * 60)
    logger.info("Iniciando extração de políticos do site do PL")
    logger.info("=" * 60)
    
    try:
        # Inicializa cliente Supabase
        client = get_supabase_client()
        logger.info("Conexão com Supabase estabelecida")
        
        # Faz scraping
        politicos = fazer_scraping_completo()
        
        if not politicos:
            logger.warning("Nenhum político foi extraído. Verifique a estrutura da página.")
            return
        
        logger.info(f"Extraídos {len(politicos)} políticos")
        
        # Mostra alguns exemplos
        logger.info("\nExemplos de políticos extraídos:")
        for p in politicos[:5]:
            logger.info(f"  - {p.name} | {p.funcao} | {p.cidade or 'N/A'} | {p.estado} | Eleito: {p.eleito}")
        
        # Pergunta confirmação
        resposta = input(f"\nDeseja inserir/atualizar {len(politicos)} políticos no banco? (s/n): ")
        if resposta.lower() != 's':
            logger.info("Operação cancelada pelo usuário")
            return
        
        # Insere/atualiza no banco
        logger.info("\nIniciando inserção/atualização no banco...")
        stats = upsert_politicos(client, politicos)
        
        # Exibe estatísticas
        logger.info("\n" + "=" * 40)
        logger.info("ESTATÍSTICAS FINAIS")
        logger.info("=" * 40)
        logger.info(f"Inseridos: {stats['inseridos']}")
        logger.info(f"Atualizados: {stats['atualizados']}")
        logger.info(f"Erros: {stats['erros']}")
        logger.info(f"Total processados: {len(politicos)}")
        
    except Exception as e:
        logger.error(f"Erro durante execução: {e}")
        raise


def test_scraping():
    """
    Função para testar o scraping sem modificar o banco.
    """
    logger.info("Modo de teste - apenas extração, sem modificar banco")
    
    politicos = fazer_scraping_completo()
    
    logger.info(f"\nTotal extraído: {len(politicos)} políticos")
    
    # Agrupa por estado
    por_estado = {}
    for p in politicos:
        por_estado.setdefault(p.estado, []).append(p)
    
    logger.info("\nDistribuição por estado:")
    for estado, lista in sorted(por_estado.items()):
        logger.info(f"  {estado}: {len(lista)} políticos")
    
    # Agrupa por cargo
    por_cargo = {}
    for p in politicos:
        por_cargo.setdefault(p.funcao, []).append(p)
    
    logger.info("\nDistribuição por cargo:")
    for cargo, lista in sorted(por_cargo.items(), key=lambda x: -len(x[1])):
        logger.info(f"  {cargo}: {len(lista)}")
    
    # Mostra todos os políticos
    logger.info("\n" + "=" * 80)
    logger.info("LISTA COMPLETA DE POLÍTICOS EXTRAÍDOS")
    logger.info("=" * 80)
    for p in sorted(politicos, key=lambda x: (x.estado, x.name)):
        eleito_str = "✓ Eleito" if p.eleito else ""
        cidade_str = f"({p.cidade})" if p.cidade else ""
        logger.info(f"{p.estado} | {p.name} | {p.funcao} {cidade_str} {eleito_str}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extrai políticos do site do PL")
    parser.add_argument("--test", action="store_true", help="Modo teste (não modifica banco)")
    args = parser.parse_args()
    
    if args.test:
        test_scraping()
    else:
        main()
