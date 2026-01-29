"""
Script para verificar os logs de coleta no Supabase.
Mostra as últimas execuções e identifica erros.
"""
import os
import sys
from pathlib import Path
from datetime import datetime

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

from supabase import create_client

def main():
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("❌ Faltam SUPABASE_URL ou SUPABASE_KEY no .env")
        return
    
    supabase = create_client(supabase_url, supabase_key)
    
    print("=" * 70)
    print("LOGS DE COLETA - ÚLTIMAS 20 EXECUÇÕES")
    print("=" * 70)
    
    # Busca os últimos logs
    response = supabase.table("coleta_logs")\
        .select("*")\
        .order("iniciado_em", desc=True)\
        .limit(20)\
        .execute()
    
    logs = response.data or []
    
    if not logs:
        print("Nenhum log de coleta encontrado.")
        return
    
    # Agrupa por tipo de coleta
    trending_logs = []
    outros_logs = []
    
    for log in logs:
        tipo = log.get("tipo_coleta", "")
        if "trending" in tipo.lower():
            trending_logs.append(log)
        else:
            outros_logs.append(log)
    
    # Mostra logs de trending primeiro
    if trending_logs:
        print("\n🔥 LOGS DE TRENDING:")
        print("-" * 70)
        for log in trending_logs:
            status_icon = "✅" if log.get("status") == "sucesso" else "⚠️" if log.get("status") == "parcial" else "❌"
            print(f"{status_icon} {log.get('tipo_coleta', 'N/A')}")
            print(f"   Status: {log.get('status', 'N/A')}")
            print(f"   Iniciado: {log.get('iniciado_em', 'N/A')}")
            print(f"   Finalizado: {log.get('finalizado_em', 'N/A')}")
            print(f"   Registros: {log.get('registros_coletados', 0)}")
            if log.get("mensagem"):
                print(f"   Mensagem: {log.get('mensagem')}")
            print()
    else:
        print("\n⚠️  Nenhum log de TRENDING encontrado!")
    
    # Mostra outros logs
    print("\n📰 OUTROS LOGS:")
    print("-" * 70)
    for log in outros_logs[:10]:  # Limita a 10
        status_icon = "✅" if log.get("status") == "sucesso" else "⚠️" if log.get("status") == "parcial" else "❌"
        print(f"{status_icon} {log.get('tipo_coleta', 'N/A')}")
        print(f"   Status: {log.get('status', 'N/A')}")
        print(f"   Iniciado: {log.get('iniciado_em', 'N/A')}")
        print(f"   Registros: {log.get('registros_coletados', 0)}")
        if log.get("mensagem"):
            print(f"   Mensagem: {log.get('mensagem')[:100]}...")
        print()
    
    # Verifica trending topics no banco
    print("\n" + "=" * 70)
    print("TRENDING TOPICS NO BANCO")
    print("=" * 70)
    
    for category in ["politica", "twitter", "google"]:
        response = supabase.table("portal_trending_topics")\
            .select("*")\
            .eq("category", category)\
            .order("rank")\
            .limit(5)\
            .execute()
        
        topics = response.data or []
        print(f"\n📊 {category.upper()}: {len(topics)} topics")
        for topic in topics[:3]:
            print(f"   #{topic.get('rank', '?')} {topic.get('title', 'N/A')}")


if __name__ == "__main__":
    main()
