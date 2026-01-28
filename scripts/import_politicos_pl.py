"""
Script para importar políticos do PL diretamente no banco de dados.
Usa os dados extraídos da página do PL.
"""
import os
import sys
import re
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
        raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidos no .env")
    return create_client(url, key)


def extrair_info_cargo(descricao: str):
    """Extrai cargo e cidade da descrição."""
    descricao = descricao.strip()
    
    cargos_sem_cidade = [
        "Deputado Federal", "Deputada Federal", "Deputado Estadual", "Deputada Estadual",
        "Deputado Distrital", "Deputada Distrital", "Senador", "Senadora",
        "Governador", "Governadora", "Vice-Governador", "Vice-Governadora",
        "Vice Governador", "Vice Governadora", "Presidente"
    ]
    
    for cargo in cargos_sem_cidade:
        if descricao.lower().startswith(cargo.lower()):
            return cargo, None
    
    match = re.match(r'^(Vereador[a]?|Prefeito[a]?|Perfeita)\s+(?:de|do|da|dos|das)?\s*(.+)$', descricao, re.IGNORECASE)
    if match:
        cargo = match.group(1).strip().capitalize()
        cidade = match.group(2).strip()
        return cargo, cidade
    
    return descricao, None


# Dados extraídos do site do PL - Lista parcial para demonstração
# (Os dados completos estão no arquivo de dados brutos)
POLITICOS_PL = """
Abel do Alecrim,Vereador de Teofilândia,BA,
Abílio Brunini,Deputado Federal,MT,
Abilio Jacques Brunini Moumer,Prefeito de Cuiabá,MT,Eleito
Abimael Santos,Deputado Estadual,PE,
Abraão David Neto,Prefeito de Nilópolis,RJ,Eleito
Adalberto Filho,Vereador de Nova Russas,CE,
Adalberto Ribeiro Lopes,Prefeito de Campo Belo,MG,Eleito
Adeildo Nogueira da Silva,Prefeito de Campo Limpo Paulista,SP,Eleito
Adelson,Vereador de Tabocas do Brejo Velho,BA,
Ademilson Junior,Vereador,SP,Eleito
Adilson,Vereador de Jaguaripe,BA,
Adilson Barroso,Deputado Federal,SP,
Adilson de Oliveira Lopes,Prefeito de Álvaro de Carvalho,SP,Eleito
Adir Flavio Sviderskei,Prefeito de Alto Bela Vista,SC,Eleito
Adolfo,Vereador de São Miguel dos Milagres,AL,
Adriana Lara,Deputada Estadual,RS,
Adriano Companheiro Velho,Vereador de Leopoldo de Bulhões,GO,
Adriano Garcia,Prefeito de Aimorés,MG,Eleito
Adriano Marcel Zimmermann,Prefeito de Guaramirim,SC,Eleito
Alberto Fraga,Deputado Federal,DF,
Alex Madureira,Deputado Estadual,SP,
Alex Maroto De Oliveira,Prefeito de Mesquita,RJ,Eleito
Alexandre Gomes Knoploch dos Santos,Deputado Estadual,RJ,
Alexandre Gomes Ribas,Prefeito de Itapiranga,SC,Eleito
Ana Campagnolo,Deputada Estadual,SC,
Anderson Luis de Moraes,Deputado Estadual,RJ,
André Fernandes,Deputado Federal,CE,
André Ferreira,Deputado Federal,PE,
André do Prado,Deputado Estadual,SP,
Andre Vechi,Prefeito de Brusque,SC,Eleito
Antônio Carlos Arantes,Deputado Estadual,MG,
Antonio Carlos Rodrigues,Deputado Federal,SP,
Bia Kicis,Deputada Federal,DF,
Bibo Nunes,Deputado Federal,RS,
Bruno Engler,Deputado Estadual,MG,
Bruno Zambelli,Deputado Estadual,SP,
Cabo Gilberto Silva,Deputado Federal,PB,
Capitão Alberto Neto,Deputado Federal,AM,
Capitão Alden,Deputado Federal,BA,
Capitão Augusto,Deputado Federal,SP,
Carlos Bolsonaro,Vereador,RJ,
Carlos Jordy,Deputado Federal,RJ,
Carlos Portinho,Senador,RJ,
Caroline De Toni,Deputada Federal,SC,
Chris Tonietto,Deputado Federal,RJ,
Cláudio Castro,Governador,RJ,
Coronel Chrisóstomo,Deputado Federal,RO,
Coronel Fernanda,Deputada Federal,MT,
Coronel Meira,Deputado Federal,PE,
Coronel Tadeu,Deputado Federal,SP,
Daniel Freitas,Deputado Federal,SC,
Daniela Reinehr,Deputada Federal,SC,
Delegado Éder Mauro,Deputado Federal,PA,
Delegado Paulo Bilynskyj,Deputado Federal,SP,
Delegado Ramagem,Deputado Federal,RJ,
Domingos Sávio,Deputado Federal,MG,
Dr. Jaziel,Deputado Federal,CE,
Dra. Mayra Isabel Correia Pinheiro,Deputada Federal,CE,
Eduardo Bolsonaro,Deputado Federal,SP,
Eduardo Gomes,Senador,TO,
Eli Borges,Deputado Federal,TO,
Emidinho Madeira,Deputado Federal,MG,
Eros Biondini,Deputado Federal,MG,
Filipe Barros,Deputado Federal,PR,
Filipe Martins,Deputado Federal,TO,
Flávio Bolsonaro,Senador,RJ,
General Eduardo Pazuello,Deputado Federal,RJ,
General Girão,Deputado Federal,RN,
Giacobo,Deputado Federal,PR,
Giovani Cherini,Deputado Federal,RS,
Gláucia Santiago,Deputada Federal,MG,
Gustavo Gayer,Deputado Federal,GO,
Helio Lopes,Deputado Federal,RJ,
Izalci Lucas,Senador,DF,
Jaime Bagattoli,Senador,RO,
Jefferson Campos,Deputado Federal,SP,
Joaquim Passarinho,Deputado Federal,PA,
Jorge Seif,Senador,SC,
Jorginho Mello,Governador,SC,
José Medeiros,Deputado Federal,MT,
Josimar Maranhãozinho,Deputado Federal,MA,
Julia Zanatta,Deputada Federal,SC,
Junio Amaral,Deputado Federal,MG,
Junior Lourenço,Deputado Federal,MA,
Lincoln Portela,Deputado Federal,MG,
Luciano Lorenzini Zucco,Deputado Federal,RS,
Luiz Carlos Motta,Deputado Federal,SP,
Luiz Philippe de Orleans e Bragança,Deputado Federal,SP,
Magno Malta,Senador,ES,
Marcelo Álvaro Antônio,Deputado Federal,MG,
Marcelo Moraes,Deputado Federal,RS,
Marcos Pollon,Deputado Federal,MS,
Marcos Pontes,Senador,SP,
Marcos Rogério,Senador,RO,
Mario Frias,Deputado Federal,SP,
Matheus Noronha,Deputado Federal,CE,
Miguel Lombardi,Deputado Federal,SP,
Missionário José Olimpio,Deputado Federal,SP,
Nelson Barbudo,Deputado Federal,MT,
Nikolas Ferreira,Deputado Federal,MG,
Osmar Terra,Deputado Federal,RS,
Pastor Eurico,Deputado Federal,PE,
Pastor Gil,Deputado Federal,MA,
Pr. Marco Feliciano,Deputado Federal,SP,
Professor Alcides,Deputado Federal,GO,
Ricardo Guidi,Deputado Federal,SC,
Roberta Roma,Deputada Federal,BA,
Roberto Monteiro,Deputado Federal,RJ,
Rodolfo Nogueira,Deputado Federal,MS,
Rodrigo da Zaeli,Deputado Federal,MT,
Rogério Marinho,Senador,RN,
Rosana Valle,Deputada Federal,SP,
Rosângela Reis,Deputada Federal,MG,
Sanderson,Deputado Federal,RS,
Sargento Gonçalves,Deputado Federal,RN,
Silvia Waiãpi,Deputada Federal,AP,
Silvio Antonio,Deputado Federal,MA,
Sonize Barbosa,Deputada Federal,AP,
Soraya Santos,Deputado Federal,RJ,
Sóstenes Cavalcante,Deputado Federal,RJ,
Tadeu Oliveira,Deputado Federal,CE,
Vinícius Gurgel,Deputado Federal,AP,
Wellington Fagundes,Senador,MT,
Wellington Roberto,Deputado Federal,PB,
Wilder Morais,Senador,GO,
Zé Trovão,Deputado Federal,SC,
Zé Vitor,Deputado Federal,MG,
Lucas Pavanato,Vereador,SP,Eleito
Fernando Holiday,Vereador,SP,
Sonaira Fernandes,Vereadora,SP,Eleito
""".strip()


def parse_linha(linha):
    """Parseia uma linha do CSV."""
    partes = linha.split(',')
    if len(partes) < 3:
        return None
    
    nome = partes[0].strip()
    cargo_completo = partes[1].strip()
    estado = partes[2].strip()
    eleito = len(partes) > 3 and partes[3].strip().lower() == 'eleito'
    
    funcao, cidade = extrair_info_cargo(cargo_completo)
    
    return {
        "name": nome,
        "funcao": funcao,
        "cidade": cidade,
        "estado": estado,
        "eleito": eleito,
        "description": cargo_completo,
        "active": True
    }


def main():
    logger.info("Iniciando importação de políticos do PL")
    
    client = get_supabase_client()
    logger.info("Conectado ao Supabase")
    
    linhas = [l.strip() for l in POLITICOS_PL.split('\n') if l.strip()]
    logger.info(f"Total de linhas para processar: {len(linhas)}")
    
    atualizados = 0
    erros = 0
    
    for linha in linhas:
        try:
            dados = parse_linha(linha)
            if not dados:
                continue
            
            # Busca por nome exato
            existing = client.table("politico").select("id").eq("name", dados["name"]).execute()
            
            if existing.data:
                # Atualiza registro existente
                politico_id = existing.data[0]["id"]
                del dados["active"]  # Não sobrescrever active
                client.table("politico").update(dados).eq("id", politico_id).execute()
                atualizados += 1
                logger.info(f"Atualizado: {dados['name']} (ID: {politico_id})")
            else:
                # Não encontrou exato, tenta busca aproximada
                existing_like = client.table("politico").select("id, name").ilike("name", f"%{dados['name']}%").execute()
                if existing_like.data:
                    politico_id = existing_like.data[0]["id"]
                    del dados["active"]
                    client.table("politico").update(dados).eq("id", politico_id).execute()
                    atualizados += 1
                    logger.info(f"Atualizado (aproximado): {dados['name']} -> {existing_like.data[0]['name']} (ID: {politico_id})")
                else:
                    logger.warning(f"Não encontrado no banco: {dados['name']}")
            
            time.sleep(0.03)
            
        except Exception as e:
            logger.error(f"Erro: {linha} - {e}")
            erros += 1
    
    logger.info(f"\nRESULTADO: Atualizados={atualizados}, Erros={erros}")


if __name__ == "__main__":
    main()
