#!/usr/bin/env python3
"""
ROTINA DEDICADA: Insights de Twitter/X para CONCORRENTES

Gera (ou atualiza) um snapshot diário em `public.concorrente_twitter_insights`
para cada concorrente referenciado em `public.politico_concorrentes`, com:
- followers_count (se APIFY_TOKEN estiver configurado)
- top 3 menções mais engajadas no Twitter/X (extraídas via Apify; e opcionalmente gravadas em `public.social_mentions`)

Uso:
  python scripts/routine_concorrentes_twitter_insights.py

Requer no .env:
  - SUPABASE_URL
  - SUPABASE_KEY

Opcional (followers):
  - APIFY_TOKEN
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path


def main() -> int:
    script_dir = Path(__file__).parent
    collector_script = script_dir / "collect_concorrentes_twitter_insights.py"

    print("=" * 60)
    print("ROTINA: Insights de Twitter/X para CONCORRENTES")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()

    config = {
        "days_back": 7,
        "limit_concorrentes": 200,
    }

    cmd = [
        sys.executable,
        str(collector_script),
        "--apply",
        "--save-mentions",
        f"--days-back={config['days_back']}",
        f"--limit-concorrentes={config['limit_concorrentes']}",
    ]

    print(f"Executando: {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd, cwd=script_dir.parent)

    print()
    print("=" * 60)
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Status: {'SUCESSO' if result.returncode == 0 else 'ERRO'}")
    print("=" * 60)

    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())

