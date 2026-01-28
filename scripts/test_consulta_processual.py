#!/usr/bin/env python3
"""
Script de teste para consulta processual.
Testa a coleta de dados do TSE para um CPF específico.

Uso:
    python scripts/test_consulta_processual.py <cpf> [politico_id]
    
Exemplo:
    python scripts/test_consulta_processual.py 14882895730 1
"""

import sys
import os
import json
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Carrega variáveis de ambiente
from dotenv import load_dotenv
load_dotenv()

from app.collectors.tse_dados_abertos import tse_collector
from app.collectors.tse_divulgacand import divulgacand_collector
from app.collectors.tjsp_esaj import tjsp_collector
from app.collectors.trf3_consulta import trf3_collector
from app.collectors.doe_sp import doe_sp_collector


def test_tse_dados_abertos(cpf: str, politico_id: int = None):
    """Testa coleta de dados abertos do TSE."""
    print("\n" + "="*60)
    print("TESTE: TSE Dados Abertos")
    print("="*60)
    
    try:
        resultado = tse_collector.consulta_completa_cpf(cpf, politico_id)
        
        print(f"\nResumo:")
        print(f"  - Candidaturas: {resultado['resumo'].get('total_candidaturas', 0)}")
        print(f"  - Doações feitas: {resultado['resumo'].get('total_doacoes_feitas', 0)}")
        print(f"  - Valor total doado: R$ {resultado['resumo'].get('valor_total_doado', 0):.2f}")
        print(f"  - Doações recebidas: {resultado['resumo'].get('total_doacoes_recebidas', 0)}")
        print(f"  - Valor total recebido: R$ {resultado['resumo'].get('valor_total_recebido', 0):.2f}")
        print(f"  - Filiações: {resultado['resumo'].get('total_filiacoes', 0)}")
        print(f"  - Partidos: {resultado['resumo'].get('partidos', [])}")
        
        return resultado
        
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_divulgacand(nome: str, uf: str = "SP", politico_id: int = None):
    """Testa API DivulgaCandContas."""
    print("\n" + "="*60)
    print("TESTE: DivulgaCandContas")
    print("="*60)
    
    try:
        resultado = divulgacand_collector.consulta_completa_candidato(
            nome=nome, uf=uf, politico_id=politico_id
        )
        
        if resultado.get("candidato"):
            print(f"\nCandidato encontrado: {resultado['candidato'].get('nome')}")
            print(f"  - Eleição: {resultado['candidato'].get('eleicao')}")
            print(f"  - Cargo: {resultado['candidato'].get('cargo')}")
            print(f"  - Partido: {resultado['candidato'].get('sigla_partido')}")
            print(f"  - Total receitas: R$ {resultado.get('total_receitas', 0):.2f}")
            print(f"  - Total despesas: R$ {resultado.get('total_despesas', 0):.2f}")
        else:
            print("\nNenhum candidato encontrado com esse nome.")
        
        return resultado
        
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_tjsp(cpf: str, politico_id: int = None):
    """Testa geração de URLs do TJSP."""
    print("\n" + "="*60)
    print("TESTE: TJSP (URLs para consulta manual)")
    print("="*60)
    
    try:
        resultado = tjsp_collector.buscar_todos_processos(cpf, politico_id)
        
        print(f"\nURLs geradas para consulta manual (requer CAPTCHA):")
        print(f"\n  1º Grau: {resultado['primeiro_grau']['url_consulta']}")
        print(f"\n  2º Grau: {resultado['segundo_grau']['url_consulta']}")
        
        return resultado
        
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_trf3(cpf: str, politico_id: int = None):
    """Testa geração de URLs do TRF-3."""
    print("\n" + "="*60)
    print("TESTE: TRF-3 (URL para consulta manual)")
    print("="*60)
    
    try:
        resultado = trf3_collector.consultar_por_cpf_semi_auto(cpf, politico_id)
        
        print(f"\nURL gerada para consulta manual (requer CAPTCHA):")
        print(f"\n  {resultado['url_consulta']}")
        
        return resultado
        
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_doe_sp(nome: str, politico_id: int = None):
    """Testa geração de URLs do DOE-SP."""
    print("\n" + "="*60)
    print("TESTE: DOE-SP (URL para consulta manual)")
    print("="*60)
    
    try:
        resultado = doe_sp_collector.buscar_por_nome_semi_auto(nome, politico_id)
        
        print(f"\nURL gerada para consulta manual:")
        print(f"\n  {resultado['url_consulta']}")
        
        return resultado
        
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    cpf = sys.argv[1]
    politico_id = int(sys.argv[2]) if len(sys.argv) > 2 else None
    nome = sys.argv[3] if len(sys.argv) > 3 else "Rosana Valle"  # Nome padrão para teste
    
    print(f"\n{'#'*60}")
    print(f"# TESTE DE CONSULTA PROCESSUAL")
    print(f"# CPF: {cpf[:3]}***{cpf[-2:]}")
    print(f"# Político ID: {politico_id or 'N/A'}")
    print(f"# Nome: {nome}")
    print(f"{'#'*60}")
    
    # Testa TSE Dados Abertos
    test_tse_dados_abertos(cpf, politico_id)
    
    # Testa DivulgaCandContas
    test_divulgacand(nome, "SP", politico_id)
    
    # Testa TJSP (URLs apenas)
    test_tjsp(cpf, politico_id)
    
    # Testa TRF-3 (URLs apenas)
    test_trf3(cpf, politico_id)
    
    # Testa DOE-SP (URLs apenas)
    test_doe_sp(nome, politico_id)
    
    print("\n" + "="*60)
    print("TESTES CONCLUÍDOS")
    print("="*60)
    print("\nNota: Os dados do TSE são baixados de arquivos CSV grandes.")
    print("A primeira execução pode demorar alguns minutos para baixar os arquivos.")
    print("\nPara TJSP, TRF-3 e DOE-SP, acesse as URLs geradas manualmente")
    print("e resolva o CAPTCHA para obter os resultados.")


if __name__ == "__main__":
    main()
