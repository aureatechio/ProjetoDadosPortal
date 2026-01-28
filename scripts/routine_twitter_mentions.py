#!/usr/bin/env python3
"""
ROTINA DEDICADA: Coleta de Menções do Twitter/X

Esta rotina coleta o que as pessoas estão comentando sobre os políticos
no Twitter/X. Deve ser executada periodicamente (ex: a cada 6 horas).

Uso:
    python scripts/routine_twitter_mentions.py

Configuração via variáveis de ambiente ou argumentos:
    --tweets-per-politico: Quantidade de tweets por político (default: 30)
    --days-back: Quantos dias para trás buscar (default: 3)
    
Requer no .env:
    - SUPABASE_URL
    - SUPABASE_KEY  
    - APIFY_TOKEN
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path


def main():
    """Executa a rotina de coleta de menções do Twitter."""
    
    script_dir = Path(__file__).parent
    collector_script = script_dir / "collect_twitter_mentions_apify.py"
    
    print("=" * 60)
    print("ROTINA: Coleta de Menções do Twitter/X")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    # Configurações da rotina
    config = {
        "limit_politicos": 50,      # Processar todos os políticos
        "limit_tweets": 30,          # 30 tweets por político
        "days_back": 3,              # Últimos 3 dias
        "search_mode": "latest",     # Tweets mais recentes
        "min_engagement": 0,         # Sem filtro de engajamento mínimo
        "skip_if_recent": 20,        # Pular se já tiver 20+ menções nas últimas 24h
    }
    
    # Monta o comando
    cmd = [
        sys.executable,
        str(collector_script),
        "--apply",
        f"--limit-politicos={config['limit_politicos']}",
        f"--limit-tweets={config['limit_tweets']}",
        f"--days-back={config['days_back']}",
        f"--search-mode={config['search_mode']}",
        f"--min-engagement={config['min_engagement']}",
        f"--skip-if-recent={config['skip_if_recent']}",
    ]
    
    print(f"Executando: {' '.join(cmd)}")
    print()
    
    # Executa o coletor
    result = subprocess.run(cmd, cwd=script_dir.parent)
    
    print()
    print("=" * 60)
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Status: {'SUCESSO' if result.returncode == 0 else 'ERRO'}")
    print("=" * 60)
    
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
