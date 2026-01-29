#!/usr/bin/env python3
"""
Script para fazer scraping dos trending topics de política do Google Trends.
Usa Playwright para renderizar a página JavaScript e extrair os dados.
URL: https://trends.google.com.br/trending?geo=BR&category=14
Categoria 14 = Legislação e governo (Política)
"""
import asyncio
import json
import sys
from datetime import datetime
from typing import List, Dict, Any

# Adiciona o diretório raiz ao path
sys.path.insert(0, '/Users/arthurcavallini/Downloads/ProjetoDadosPortal')


async def run_collector():
    """
    Executa o coletor de trending topics do projeto com scraping.
    """
    print("=" * 70)
    print("🇧🇷 EXECUTANDO COLETOR DE TRENDING COM SCRAPING")
    print(f"📅 Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 70)
    print()
    
    try:
        from app.collectors.trending import trending_collector
        
        # Coleta trending de POLÍTICA (usa scraping como fonte primária)
        print("📰 Coletando trending topics de POLÍTICA (scraping)...")
        print("-" * 70)
        
        topics = await trending_collector.identificar_trending_topics(max_topics=15, use_scraping=True)
        
        if topics:
            print(f"\n✅ {len(topics)} trending topics de política encontrados:\n")
            for topic in topics:
                print(f"  #{topic['rank']} {topic['title']}")
                if topic.get('subtitle'):
                    print(f"      └─ {topic['subtitle'][:80]}...")
            
            # Salva no banco
            count = await trending_collector.executar_coleta()
            print(f"\n💾 Salvos {count} no banco de dados!")
        else:
            print("⚠️ Nenhum trending de política encontrado.")
        
        print("\n" + "=" * 70)
        print("✅ COLETA FINALIZADA!")
        print("=" * 70)
        
        return topics
        
    except ImportError as e:
        print(f"❌ Erro de importação: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"❌ Erro durante a coleta: {e}")
        import traceback
        traceback.print_exc()
    
    return []


async def scrape_only():
    """
    Faz apenas o scraping sem salvar no banco.
    """
    from playwright.async_api import async_playwright
    
    url = "https://trends.google.com.br/trending?geo=BR&category=14"
    topics = []
    
    print("=" * 70)
    print("🇧🇷 GOOGLE TRENDS - SCRAPING DE TRENDING TOPICS DE POLÍTICA")
    print(f"📅 Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"📡 URL: {url}")
    print("=" * 70)
    print()
    
    async with async_playwright() as p:
        print("🚀 Iniciando navegador...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            locale='pt-BR',
            timezone_id='America/Sao_Paulo',
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        )
        page = await context.new_page()
        
        try:
            print(f"📄 Acessando página...")
            await page.goto(url, wait_until='networkidle', timeout=60000)
            
            print("⏳ Aguardando carregamento...")
            try:
                await page.wait_for_selector('table', timeout=15000)
                print("✅ Tabela encontrada!")
            except:
                print("⚠️ Tabela não encontrada")
            
            await asyncio.sleep(2)
            
            print("🔍 Extraindo dados...")
            
            extract_script = """
            () => {
                const topics = [];
                const rows = document.querySelectorAll('table tbody tr');
                
                rows.forEach((row, index) => {
                    let titulo = '';
                    const titleEl = row.querySelector('.mZ3RIc, .title, [data-title], a');
                    if (titleEl) {
                        titulo = titleEl.textContent.trim();
                    } else {
                        const cells = row.querySelectorAll('td, [role="cell"]');
                        if (cells.length > 0) {
                            titulo = cells[0].textContent.trim();
                        }
                    }
                    
                    let buscas = 'N/A';
                    const volumeEl = row.querySelector('.lqv0Cb, .volume, [data-volume]');
                    if (volumeEl) {
                        buscas = volumeEl.textContent.trim();
                    } else {
                        const cells = row.querySelectorAll('td, [role="cell"]');
                        if (cells.length > 1) {
                            buscas = cells[1].textContent.trim();
                        }
                    }
                    
                    if (titulo && titulo.length > 1) {
                        topics.push({
                            rank: index + 1,
                            titulo: titulo,
                            buscas: buscas
                        });
                    }
                });
                
                return topics.slice(0, 30);
            }
            """
            
            extracted = await page.evaluate(extract_script)
            
            if extracted:
                topics = extracted
                print(f"✅ Extraídos {len(topics)} trending topics!\n")
                
                print("-" * 70)
                print("📊 TRENDING TOPICS DE POLÍTICA:")
                print("-" * 70 + "\n")
                
                for topic in topics[:20]:
                    print(f"#{topic['rank']:2d} 📌 {topic['titulo']}")
                    if topic.get('buscas') and topic['buscas'] != 'N/A':
                        print(f"     🔥 Buscas: {topic['buscas']}")
                    print()
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()
            print("🔒 Navegador fechado.")
    
    # Salva em JSON
    if topics:
        output_file = '/Users/arthurcavallini/Downloads/ProjetoDadosPortal/trending_politica_scrape.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'coletado_em': datetime.now().isoformat(),
                'fonte': 'Google Trends (scraping)',
                'categoria': 'politica',
                'url': url,
                'total': len(topics),
                'topics': topics
            }, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Dados salvos em: {output_file}")
    
    print("\n" + "=" * 70)
    print(f"📋 RESUMO: {len(topics)} trending topics extraídos")
    print("=" * 70)
    
    return topics


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Scraping de trending topics de política')
    parser.add_argument('--collector', action='store_true', help='Usa o coletor completo do projeto')
    parser.add_argument('--scrape-only', action='store_true', help='Apenas scraping, sem salvar no banco')
    args = parser.parse_args()
    
    if args.collector:
        asyncio.run(run_collector())
    else:
        asyncio.run(scrape_only())
