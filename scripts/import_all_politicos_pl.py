"""
Script para importar TODOS os políticos do PL no banco de dados.
Lê o arquivo JSON extraído do site e insere/atualiza no Supabase.
"""
import os
import sys
import re
import json
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidos")
    return create_client(url, key)


def extrair_info_cargo(descricao: str):
    """Extrai cargo e cidade da descrição."""
    descricao = descricao.strip()
    
    # Remove espaços extras
    descricao = re.sub(r'\s+', ' ', descricao)
    
    cargos_sem_cidade = [
        "Deputado Federal", "Deputada Federal", "Deputado Estadual", "Deputada Estadual",
        "Deputado Distrital", "Deputada Distrital", "Senador", "Senadora",
        "Governador", "Governadora", "Vice-Governador", "Vice-Governadora",
        "Vice Governador", "Vice Governadora", "Presidente"
    ]
    
    for cargo in cargos_sem_cidade:
        if descricao.lower().startswith(cargo.lower()):
            return cargo, None
    
    # Tenta extrair cidade do cargo
    match = re.match(r'^(Vereador[a]?|Prefeito[a]?|Perfeita)\s*(?:de|do|da|dos|das)?\s*(.+)$', descricao, re.IGNORECASE)
    if match:
        cargo = match.group(1).strip()
        cidade = match.group(2).strip()
        # Normaliza
        if cargo.lower() in ['perfeita', 'prefeita']:
            cargo = 'Prefeita'
        elif cargo.lower() == 'prefeito':
            cargo = 'Prefeito'
        return cargo, cidade
    
    return descricao, None


def limpar_nome(nome: str) -> str:
    """Remove sufixos como '– Não está em exercício' do nome."""
    nome = re.sub(r'\s*[–-]\s*(Não está em exercício|Licenciado|Situação Afastamento).*$', '', nome, flags=re.IGNORECASE)
    return nome.strip()


def main():
    logger.info("=" * 60)
    logger.info("IMPORTAÇÃO COMPLETA DE POLÍTICOS DO PL")
    logger.info("=" * 60)
    
    # Carrega dados do JSON
    json_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'politicos_pl_completo.json')
    
    with open(json_path, 'r', encoding='utf-8') as f:
        politicos_raw = json.load(f)
    
    logger.info(f"Total de políticos no arquivo: {len(politicos_raw)}")
    
    client = get_supabase_client()
    logger.info("Conectado ao Supabase")
    
    inseridos = 0
    atualizados = 0
    erros = 0
    
    for i, p in enumerate(politicos_raw):
        try:
            nome = limpar_nome(p['nome'])
            cargo_completo = p['cargo']
            estado = p['estado']
            eleito = p['eleito']
            
            funcao, cidade = extrair_info_cargo(cargo_completo)
            
            dados = {
                "name": nome,
                "funcao": funcao,
                "cidade": cidade,
                "estado": estado,
                "eleito": eleito,
                "description": cargo_completo,
                "active": True
            }
            
            # Verifica se já existe por nome exato
            existing = client.table("politico").select("id").eq("name", nome).execute()
            
            if existing.data:
                # Atualiza
                politico_id = existing.data[0]["id"]
                client.table("politico").update({
                    "funcao": funcao,
                    "cidade": cidade,
                    "estado": estado,
                    "eleito": eleito,
                    "description": cargo_completo
                }).eq("id", politico_id).execute()
                atualizados += 1
            else:
                # Insere novo
                client.table("politico").insert(dados).execute()
                inseridos += 1
            
            # Log a cada 100 registros
            if (i + 1) % 100 == 0:
                logger.info(f"Processados: {i + 1}/{len(politicos_raw)} | Inseridos: {inseridos} | Atualizados: {atualizados}")
            
            time.sleep(0.02)  # Evita rate limiting
            
        except Exception as e:
            logger.error(f"Erro em {p.get('nome', 'N/A')}: {e}")
            erros += 1
    
    logger.info("=" * 60)
    logger.info("RESULTADO FINAL")
    logger.info("=" * 60)
    logger.info(f"Total processados: {len(politicos_raw)}")
    logger.info(f"Inseridos: {inseridos}")
    logger.info(f"Atualizados: {atualizados}")
    logger.info(f"Erros: {erros}")


if __name__ == "__main__":
    main()
