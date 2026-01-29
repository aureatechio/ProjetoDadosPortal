"""
Coletor de trending topics políticos e gerais.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from collections import Counter
import re

from gnews import GNews
from app.database import db
from app.relevance.analyzer import ContentAnalyzer

# Tenta importar pytrends para trending geral
try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError:
    PYTRENDS_AVAILABLE = False

# Tenta importar Playwright para scraping
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logger = logging.getLogger(__name__)


class TrendingCollector:
    """
    Coleta e identifica trending topics políticos baseado em:
    1. Google Trends RSS filtrado por termos políticos (fonte primária)
    2. Google News RSS de política Brasil
    3. GNews como fallback
    """
    
    def __init__(self):
        self.gnews = GNews(language="pt", country="BR", max_results=50)
        self.analyzer = ContentAnalyzer()
        
        # URL do RSS do Google Trends (geral, será filtrado por termos políticos)
        self.google_trends_rss = "https://trends.google.com.br/trending/rss?geo=BR"
        
        # URLs de RSS do Google News para política
        self.rss_urls = [
            "https://news.google.com/rss/search?q=política+brasil+governo&hl=pt-BR&gl=BR&ceid=BR:pt-419",
            "https://news.google.com/rss/search?q=congresso+câmara+senado&hl=pt-BR&gl=BR&ceid=BR:pt-419",
            "https://news.google.com/rss/search?q=lula+bolsonaro+eleição&hl=pt-BR&gl=BR&ceid=BR:pt-419",
        ]
        
        # Palavras-chave para identificar trending político
        self.political_keywords = {
            # Políticos brasileiros famosos
            'lula', 'bolsonaro', 'tarcísio', 'haddad', 'ciro', 'simone tebet', 'marina silva',
            'flávio bolsonaro', 'eduardo bolsonaro', 'carlos bolsonaro', 'michelle bolsonaro',
            'lira', 'pacheco', 'moraes', 'barroso', 'mendonça', 'dino', 'zanin',
            'nikolas ferreira', 'kim kataguiri', 'tabata amaral', 'erika hilton',
            'pablo marçal', 'guilherme boulos', 'datena', 'nunes',
            # Partidos
            'pt', 'pl', 'mdb', 'pp', 'psd', 'psdb', 'psol', 'novo', 'republicanos', 'união brasil',
            # Instituições
            'stf', 'tse', 'trf', 'mpf', 'pgr', 'tcu', 'cgu', 'pf', 'polícia federal',
            'congresso', 'senado', 'câmara', 'planalto', 'governo federal',
            # Termos políticos
            'eleição', 'eleições', 'candidato', 'candidatura', 'votação', 'urna',
            'impeachment', 'cassação', 'inelegível', 'inelegibilidade',
            'cpi', 'comissão', 'investigação', 'indiciamento', 'denúncia',
            'reforma', 'pec', 'projeto de lei', 'mp', 'medida provisória',
            'orçamento', 'arcabouço fiscal', 'dívida pública',
            'ministro', 'ministério', 'secretário', 'governador', 'prefeito',
            'pesquisa eleitoral', 'datafolha', 'quaest', 'ipec', 'atlas',
            # Contextos políticos
            'golpe', 'golpista', '8 de janeiro', 'atos antidemocráticos',
            'anistia', 'indulto', 'prisão', 'condenação', 'absolvição',
        }
        
        # Palavras a ignorar (stop words políticas)
        self.stop_words = {
            'o', 'a', 'os', 'as', 'um', 'uma', 'uns', 'umas',
            'de', 'da', 'do', 'das', 'dos', 'em', 'na', 'no', 'nas', 'nos',
            'para', 'por', 'com', 'sem', 'sobre', 'entre',
            'que', 'qual', 'quais', 'como', 'quando', 'onde',
            'e', 'ou', 'mas', 'se', 'não', 'sim',
            'é', 'são', 'foi', 'foram', 'ser', 'será', 'seria',
            'tem', 'teve', 'ter', 'tinha', 'terá',
            'pode', 'podem', 'poderia', 'poderá',
            'diz', 'disse', 'afirma', 'afirmou', 'segundo',
            'após', 'antes', 'durante', 'até', 'desde',
            'ano', 'anos', 'dia', 'dias', 'vez', 'vezes',
            'novo', 'nova', 'novos', 'novas',
            'brasil', 'brasileiro', 'brasileira', 'país',
            'google', 'notícias', 'news', 'fonte', 'jornal'
        }
        
        # Termos políticos importantes para destacar
        self.termos_politicos = {
            'reforma', 'tributária', 'administrativa', 'previdência',
            'pec', 'projeto', 'lei', 'votação', 'aprovação',
            'cpi', 'investigação', 'denúncia', 'corrupção',
            'eleição', 'eleições', 'candidato', 'candidatura',
            'governo', 'oposição', 'base', 'aliados',
            'ministro', 'ministério', 'secretário', 'secretaria',
            'stf', 'tse', 'trf', 'mpf', 'pgr',
            'orçamento', 'fiscal', 'inflação', 'economia',
            'saúde', 'educação', 'segurança', 'meio ambiente',
            'lula', 'bolsonaro', 'tarcísio', 'congresso', 'câmara', 'senado'
        }
    
    def _extract_relevant_terms(self, text: str) -> List[str]:
        """
        Extrai termos relevantes de um texto.
        
        Args:
            text: Texto para análise
            
        Returns:
            Lista de termos relevantes
        """
        if not text:
            return []
        
        # Normaliza e tokeniza
        text_lower = self.analyzer.normalize_text(text)
        
        # Extrai palavras (apenas letras, min 3 caracteres)
        words = re.findall(r'\b[a-záàâãéèêíìîóòôõúùûç]{3,}\b', text_lower)
        
        # Filtra stop words
        relevant = [w for w in words if w not in self.stop_words]
        
        return relevant
    
    def _extract_named_entities(self, text: str) -> List[str]:
        """
        Extrai entidades nomeadas (nomes próprios) do texto.
        Versão simplificada baseada em padrões.
        
        Args:
            text: Texto original (com capitalização)
            
        Returns:
            Lista de entidades encontradas
        """
        if not text:
            return []
        
        # Padrão para nomes próprios (palavras capitalizadas consecutivas)
        pattern = r'\b([A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛ][a-záàâãéèêíìîóòôõúùû]+(?:\s+[A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛ][a-záàâãéèêíìîóòôõúùû]+)*)\b'
        
        matches = re.findall(pattern, text)
        
        # Filtra nomes muito curtos ou comuns
        entities = []
        for match in matches:
            words = match.split()
            # Pelo menos 2 palavras ou uma palavra > 5 caracteres
            if len(words) >= 2 or (len(words) == 1 and len(words[0]) > 5):
                entities.append(match)
        
        return entities
    
    def _is_political_topic(self, title: str, news_titles: List[str] = None) -> bool:
        """
        Verifica se um trending topic é relacionado a política.
        O título do trending DEVE conter termos políticos (não apenas as notícias).
        
        Args:
            title: Título do trending topic
            news_titles: Lista de títulos de notícias relacionadas ao topic
            
        Returns:
            True se o topic é político
        """
        title_lower = title.lower()
        
        # O título do trending deve conter termos políticos diretamente
        # (não basta as notícias relacionadas mencionarem política)
        for keyword in self.political_keywords:
            kw_lower = keyword.lower()
            if kw_lower in title_lower:
                return True
        
        # Se o título não tem termo político, verifica se TODAS as notícias são políticas
        # (indica que o topic é sobre política mesmo com título genérico)
        if news_titles and len(news_titles) >= 2:
            political_news_count = 0
            for news_title in news_titles:
                news_lower = news_title.lower()
                for keyword in self.political_keywords:
                    if keyword.lower() in news_lower:
                        political_news_count += 1
                        break
            
            # Pelo menos 50% das notícias devem ser políticas
            if political_news_count >= len(news_titles) * 0.5:
                return True
        
        return False
    
    async def _fetch_google_trends_political(self, max_topics: int = 10) -> List[Dict[str, Any]]:
        """
        Busca trending do Google Trends RSS e filtra apenas os políticos.
        """
        try:
            import httpx
            from xml.etree import ElementTree
        except ImportError:
            logger.warning("httpx não disponível para RSS")
            return []
        
        topics = []
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    self.google_trends_rss,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                )
                response.raise_for_status()
                
                # Parse XML
                root = ElementTree.fromstring(response.text)
                ns = {'ht': 'https://trends.google.com/trending/rss'}
                
                items = root.findall('.//item')
                
                for item in items:
                    title_elem = item.find('title')
                    traffic_elem = item.find('ht:approx_traffic', ns)
                    
                    if title_elem is None or not title_elem.text:
                        continue
                    
                    title = title_elem.text.strip()
                    
                    # Coleta títulos de notícias relacionadas
                    news_titles = []
                    for news_item in item.findall('ht:news_item', ns):
                        news_title = news_item.find('ht:news_item_title', ns)
                        if news_title is not None and news_title.text:
                            news_titles.append(news_title.text.strip())
                    
                    # Verifica se é político
                    if self._is_political_topic(title, news_titles):
                        traffic = ""
                        if traffic_elem is not None and traffic_elem.text:
                            traffic = traffic_elem.text.strip()
                        
                        # Pega a primeira notícia como descrição
                        subtitle = news_titles[0] if news_titles else f"Em alta no Google - {traffic} buscas"
                        
                        topics.append({
                            "title": title,
                            "subtitle": subtitle[:120] + "..." if len(subtitle) > 120 else subtitle,
                            "traffic": traffic,
                            "news_count": len(news_titles)
                        })
                        
                        if len(topics) >= max_topics:
                            break
                
                if topics:
                    logger.info(f"Encontrados {len(topics)} trending políticos do Google Trends RSS")
                    
        except Exception as e:
            logger.warning(f"Erro ao buscar Google Trends RSS para política: {e}")
        
        return topics
    
    async def _scrape_google_trends_politics(self, max_topics: int = 15) -> List[Dict[str, Any]]:
        """
        Faz scraping da página de trending topics de política do Google Trends.
        Usa Playwright para renderizar JavaScript.
        URL: https://trends.google.com.br/trending?geo=BR&category=14
        Categoria 14 = Legislação e governo (Política)
        """
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright não disponível para scraping")
            return []
        
        url = "https://trends.google.com.br/trending?geo=BR&category=14"
        topics = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    locale='pt-BR',
                    timezone_id='America/Sao_Paulo',
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                )
                page = await context.new_page()
                
                try:
                    logger.info(f"Scraping Google Trends política: {url}")
                    # Usa domcontentloaded em vez de networkidle para ser mais rápido
                    await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    
                    # Aguarda a tabela carregar
                    try:
                        await page.wait_for_selector('table', timeout=20000)
                    except:
                        logger.warning("Tabela não encontrada no Google Trends")
                    
                    await asyncio.sleep(3)  # Aguarda JavaScript carregar dados
                    
                    # Script JavaScript para extrair dados
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
                                    title: titulo,
                                    traffic: buscas
                                });
                            }
                        });
                        
                        return topics.slice(0, 30);
                    }
                    """
                    
                    extracted = await page.evaluate(extract_script)
                    
                    if extracted:
                        for item in extracted[:max_topics]:
                            topics.append({
                                "title": item.get("title", ""),
                                "subtitle": f"Buscas: {item.get('traffic', 'N/A')}",
                                "traffic": item.get("traffic", ""),
                                "rank": item.get("rank", 0)
                            })
                        
                        logger.info(f"Scraping: encontrados {len(topics)} trending de política")
                    
                except Exception as e:
                    logger.error(f"Erro no scraping do Google Trends: {e}")
                finally:
                    await browser.close()
                    
        except Exception as e:
            logger.error(f"Erro ao iniciar Playwright: {e}")
        
        return topics
    
    async def _fetch_political_news_rss(self) -> List[Dict[str, Any]]:
        """
        Busca notícias políticas via Google News RSS (fonte mais confiável).
        """
        try:
            import httpx
            from xml.etree import ElementTree
        except ImportError:
            logger.warning("httpx não disponível para RSS de política")
            return []
        
        todas_noticias = []
        
        for rss_url in self.rss_urls:
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(
                        rss_url,
                        headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                        }
                    )
                    response.raise_for_status()
                    
                    # Parse XML
                    root = ElementTree.fromstring(response.text)
                    items = root.findall('.//item')
                    
                    for item in items:
                        title_elem = item.find('title')
                        source_elem = item.find('source')
                        
                        if title_elem is not None and title_elem.text:
                            noticia = {
                                "title": title_elem.text.strip(),
                                "description": title_elem.text.strip(),
                                "publisher": {
                                    "title": source_elem.text if source_elem is not None else "Google News"
                                }
                            }
                            todas_noticias.append(noticia)
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"Erro ao buscar RSS de política: {e}")
        
        if todas_noticias:
            logger.info(f"Coletadas {len(todas_noticias)} notícias de política via RSS")
        
        return todas_noticias
    
    async def _fetch_political_news(self) -> List[Dict[str, Any]]:
        """
        Busca notícias políticas recentes.
        Usa RSS do Google News como fonte primária, GNews como fallback.
        """
        # Tenta RSS primeiro (mais confiável)
        noticias = await self._fetch_political_news_rss()
        
        if noticias:
            return noticias
        
        # Fallback para GNews
        logger.info("RSS falhou, usando GNews como fallback para política")
        
        queries = [
            "política Brasil hoje",
            "Congresso Nacional",
            "governo federal",
            "eleições Brasil"
        ]
        
        todas_noticias = []
        
        loop = asyncio.get_event_loop()
        
        for query in queries:
            try:
                results = await loop.run_in_executor(
                    None,
                    self.gnews.get_news,
                    query
                )
                if results:
                    todas_noticias.extend(results)
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"Erro ao buscar notícias para trending: {e}")
        
        return todas_noticias
    
    def _get_context_description(self, topic_title: str, noticias: List[Dict[str, Any]]) -> str:
        """
        Gera uma descrição contextual do que está sendo discutido sobre o tópico.
        
        Args:
            topic_title: Título do tópico trending
            noticias: Lista de notícias para buscar contexto
            
        Returns:
            Descrição contextual do tópico
        """
        topic_lower = topic_title.lower()
        related_headlines = []
        
        # Encontra notícias que mencionam o tópico
        for noticia in noticias:
            titulo = noticia.get("title", "")
            if topic_lower in titulo.lower():
                # Remove o nome da fonte do título (geralmente após " - ")
                clean_title = titulo.split(" - ")[0].strip()
                if clean_title and len(clean_title) > 20:
                    related_headlines.append(clean_title)
        
        if not related_headlines:
            return f"Assunto em destaque nas notícias políticas"
        
        # Pega a manchete mais relevante como descrição
        # Ordena por tamanho (manchetes mais completas são melhores)
        related_headlines.sort(key=len, reverse=True)
        
        # Usa a primeira manchete como base da descrição
        main_headline = related_headlines[0]
        
        # Se tiver mais de uma manchete, adiciona contexto
        if len(related_headlines) > 1:
            # Tenta encontrar palavras-chave adicionais nas outras manchetes
            keywords = set()
            for headline in related_headlines[1:3]:
                words = headline.lower().split()
                for word in words:
                    if len(word) > 4 and word not in self.stop_words and word != topic_lower:
                        keywords.add(word.capitalize())
            
            if keywords:
                extra_context = ", ".join(list(keywords)[:2])
                return f"{main_headline[:100]}{'...' if len(main_headline) > 100 else ''}"
        
        # Retorna a manchete principal (truncada se necessário)
        if len(main_headline) > 120:
            return main_headline[:117] + "..."
        return main_headline
    
    async def identificar_trending_topics(self, max_topics: int = 10, use_scraping: bool = True) -> List[Dict[str, Any]]:
        """
        Identifica os principais trending topics políticos.
        
        Usa múltiplas fontes em ordem de prioridade:
        1. Scraping da página do Google Trends categoria política (Playwright)
        2. Google Trends RSS filtrado por termos políticos
        3. Google News RSS de política
        4. Análise de frequência de termos em notícias
        
        Args:
            max_topics: Número máximo de topics a retornar
            use_scraping: Se True, tenta usar scraping com Playwright como fonte primária
            
        Returns:
            Lista de trending topics com rank, título e descrição contextual
        """
        logger.info("Identificando trending topics políticos")
        
        # FONTE 0: Scraping da página do Google Trends com categoria política (mais precisa)
        if use_scraping and PLAYWRIGHT_AVAILABLE:
            logger.info("Tentando scraping do Google Trends (categoria política)...")
            scrape_topics = await self._scrape_google_trends_politics(max_topics)
            
            if scrape_topics and len(scrape_topics) >= 3:
                topics = []
                for rank, topic in enumerate(scrape_topics, 1):
                    topics.append({
                        "rank": rank,
                        "title": topic["title"],
                        "subtitle": topic.get("subtitle", f"Em alta - {topic.get('traffic', 'N/A')} buscas")
                    })
                
                logger.info(f"Usando {len(topics)} trending políticos do scraping Google Trends")
                return topics
            else:
                logger.info("Scraping retornou poucos resultados, tentando RSS...")
        
        # FONTE 1: Google Trends RSS filtrado por política (só usa se tiver pelo menos 5 topics)
        google_trends_topics = await self._fetch_google_trends_political(max_topics)
        
        if google_trends_topics and len(google_trends_topics) >= 5:
            # Se encontrou trending políticos suficientes no Google Trends, usa esses
            topics = []
            for rank, topic in enumerate(google_trends_topics, 1):
                topics.append({
                    "rank": rank,
                    "title": topic["title"],
                    "subtitle": topic["subtitle"]
                })
            
            logger.info(f"Usando {len(topics)} trending políticos do Google Trends RSS")
            return topics
        
        # FONTE 2/3: Google News RSS + análise de frequência
        # Usa quando Google Trends não tem trending políticos suficientes
        if google_trends_topics:
            logger.info(f"Apenas {len(google_trends_topics)} trending políticos no Google Trends (mínimo 5), usando Google News")
        else:
            logger.info("Nenhum trending político no Google Trends, usando Google News")
        
        # Busca notícias recentes
        noticias = await self._fetch_political_news()
        
        if not noticias:
            logger.warning("Nenhuma notícia encontrada para trending")
            return []
        
        # Conta frequência de termos e entidades
        term_counter = Counter()
        entity_counter = Counter()
        entity_headlines = {}  # Guarda manchetes relacionadas a cada entidade
        
        for noticia in noticias:
            titulo = noticia.get("title", "")
            descricao = noticia.get("description", "")
            texto = f"{titulo} {descricao}"
            
            # Extrai termos
            termos = self._extract_relevant_terms(texto)
            term_counter.update(termos)
            
            # Extrai entidades e guarda manchetes relacionadas
            entidades = self._extract_named_entities(texto)
            entity_counter.update(entidades)
            
            # Associa manchete a cada entidade encontrada
            for entity in entidades:
                if entity not in entity_headlines:
                    entity_headlines[entity] = []
                entity_headlines[entity].append(titulo)
        
        # Combina termos políticos importantes com entidades
        trending = []
        
        # Prioriza entidades (nomes de pessoas, instituições)
        for entity, count in entity_counter.most_common(max_topics * 2):
            if count >= 2:  # Aparece em pelo menos 2 notícias
                trending.append({
                    "title": entity,
                    "count": count,
                    "type": "entity",
                    "headlines": entity_headlines.get(entity, [])
                })
        
        # Adiciona termos políticos relevantes
        for term, count in term_counter.most_common(50):
            normalized = self.analyzer.normalize_text(term)
            if normalized in self.termos_politicos and count >= 3:
                # Verifica se não é duplicata
                if not any(t["title"].lower() == term.lower() for t in trending):
                    trending.append({
                        "title": term.capitalize(),
                        "count": count,
                        "type": "term",
                        "headlines": []
                    })
        
        # Ordena por contagem e pega os top N
        trending_sorted = sorted(trending, key=lambda x: x["count"], reverse=True)[:max_topics]
        
        # Formata para o banco de dados com descrições contextuais
        topics = []
        for rank, topic in enumerate(trending_sorted, 1):
            # Gera descrição contextual baseada nas manchetes
            description = self._get_context_description(topic["title"], noticias)
            
            topics.append({
                "rank": rank,
                "title": topic["title"],
                "subtitle": description
            })
        
        logger.info(f"Identificados {len(topics)} trending topics via Google News")
        
        return topics
    
    async def executar_coleta(self) -> int:
        """
        Executa coleta e atualização dos trending topics políticos.
        
        Returns:
            Número de topics atualizados
        """
        topics = await self.identificar_trending_topics()
        
        if topics:
            count = db.update_trending_topics(topics, category="politica")
            logger.info(f"Atualizados {count} trending topics políticos")
            return count
        
        return 0


class GeneralTrendingCollector:
    """
    Coleta trending topics gerais do Brasil.
    Usa GNews para buscar as principais notícias do momento em várias categorias.
    """
    
    def __init__(self):
        self.gnews = GNews(language="pt", country="BR", max_results=30)
        self._pytrend: Optional['TrendReq'] = None
    
    @property
    def pytrends_disponivel(self) -> bool:
        """Verifica se o pytrends está disponível"""
        return PYTRENDS_AVAILABLE
    
    def _get_pytrend(self) -> Optional['TrendReq']:
        """Retorna instância do pytrends (lazy loading)"""
        if not PYTRENDS_AVAILABLE:
            return None
        if self._pytrend is None:
            self._pytrend = TrendReq(
                hl='pt-BR',
                tz=-180,
                timeout=(10, 25)
            )
        return self._pytrend
    
    async def _fetch_gnews_trending(self, max_topics: int = 10) -> List[Dict[str, Any]]:
        """
        Busca trending topics usando GNews (notícias em alta).
        Mais confiável que pytrends que pode retornar 404.
        """
        queries = [
            "notícias Brasil hoje",
            "últimas notícias",
            "notícias do dia"
        ]
        
        todas_noticias = []
        loop = asyncio.get_event_loop()
        
        for query in queries:
            try:
                results = await loop.run_in_executor(
                    None,
                    self.gnews.get_news,
                    query
                )
                if results:
                    todas_noticias.extend(results)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Erro ao buscar GNews para trending geral: {e}")
        
        # Também busca top news
        try:
            top_news = await loop.run_in_executor(
                None,
                self.gnews.get_top_news
            )
            if top_news:
                todas_noticias.extend(top_news)
        except Exception as e:
            logger.warning(f"Erro ao buscar top news: {e}")
        
        if not todas_noticias:
            return []
        
        # Extrai tópicos únicos dos títulos
        seen_topics = set()
        topics = []
        
        for noticia in todas_noticias:
            titulo = noticia.get("title", "")
            if not titulo:
                continue
            
            # Limpa o título (remove fonte após " - ")
            clean_title = titulo.split(" - ")[0].strip()
            
            # Pula títulos muito curtos ou duplicados
            if len(clean_title) < 20:
                continue
            
            # Verifica duplicatas (normalizado)
            normalized = clean_title.lower()[:50]
            if normalized in seen_topics:
                continue
            seen_topics.add(normalized)
            
            topics.append({
                "title": clean_title[:100],
                "subtitle": f"Fonte: {noticia.get('publisher', {}).get('title', 'Google News')}"
            })
            
            if len(topics) >= max_topics:
                break
        
        # Adiciona rank
        for i, topic in enumerate(topics, 1):
            topic["rank"] = i
        
        return topics
    
    async def _fetch_pytrends_trending(self, max_topics: int = 10) -> List[Dict[str, Any]]:
        """Tenta buscar trending do pytrends (pode falhar com 404)"""
        if not PYTRENDS_AVAILABLE:
            return []
        
        pytrend = self._get_pytrend()
        if not pytrend:
            return []
        
        topics = []
        
        try:
            loop = asyncio.get_event_loop()
            
            trending_df = await loop.run_in_executor(
                None,
                lambda: pytrend.trending_searches(pn='brazil')
            )
            
            if trending_df is not None and not trending_df.empty:
                trending_list = trending_df[0].tolist()[:max_topics]
                
                for rank, title in enumerate(trending_list, 1):
                    topics.append({
                        "rank": rank,
                        "title": str(title),
                        "subtitle": "Em alta no Google Brasil"
                    })
                
                logger.info(f"Encontrados {len(topics)} trending via pytrends")
                
        except Exception as e:
            logger.warning(f"pytrends falhou (esperado): {e}")
        
        return topics
    
    async def identificar_trending_topics(self, max_topics: int = 10) -> List[Dict[str, Any]]:
        """
        Identifica os principais trending topics gerais do Brasil.
        Tenta pytrends primeiro, depois usa GNews como fallback.
        
        Args:
            max_topics: Número máximo de topics a retornar
            
        Returns:
            Lista de trending topics com rank, título e descrição
        """
        logger.info("Identificando trending topics gerais")
        
        # Tenta pytrends primeiro (pode retornar 404)
        topics = await self._fetch_pytrends_trending(max_topics)
        
        # Se pytrends falhou, usa GNews
        if not topics:
            logger.info("Usando GNews como fallback para trending geral")
            topics = await self._fetch_gnews_trending(max_topics)
        
        if topics:
            logger.info(f"Identificados {len(topics)} trending topics gerais")
        else:
            logger.warning("Nenhum trending topic geral encontrado")
        
        return topics
    
    async def executar_coleta(self) -> int:
        """
        Executa coleta e atualização dos trending topics gerais.
        
        Returns:
            Número de topics atualizados
        """
        topics = await self.identificar_trending_topics()
        
        if topics:
            count = db.update_trending_topics(topics, category="geral")
            logger.info(f"Atualizados {count} trending topics gerais")
            return count
        
        return 0


class TwitterTrendingCollector:
    """
    Coleta trending topics do Twitter/X no Brasil.
    Faz scraping do trends24.in/brazil para obter os trending em tempo real.
    """
    
    def __init__(self):
        self.url = "https://trends24.in/brazil/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        }
    
    async def identificar_trending_topics(self, max_topics: int = 10) -> List[Dict[str, Any]]:
        """
        Identifica os trending topics do Twitter/X no Brasil via trends24.in.
        
        Args:
            max_topics: Número máximo de topics a retornar
            
        Returns:
            Lista de trending topics com rank, título e descrição
        """
        logger.info("Identificando trending topics do Twitter/X (trends24.in)")
        
        try:
            import httpx
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("httpx ou beautifulsoup4 não instalados. Execute: pip install httpx beautifulsoup4")
            return []
        
        topics = []
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(self.url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Busca a primeira lista de trending (mais recente)
                # O site organiza por hora, pegamos o bloco mais recente
                trend_cards = soup.find_all('ol', class_='trend-card__list')
                
                if trend_cards:
                    # Pega a primeira lista (mais recente)
                    first_list = trend_cards[0]
                    items = first_list.find_all('li')
                    
                    for rank, item in enumerate(items[:max_topics], 1):
                        # Extrai o texto do trending
                        link = item.find('a')
                        if link:
                            title = link.get_text(strip=True)
                            if title:
                                topics.append({
                                    "rank": rank,
                                    "title": title,
                                    "subtitle": "Em alta no Twitter/X Brasil"
                                })
                
                if not topics:
                    # Tenta método alternativo
                    all_links = soup.find_all('a', href=lambda x: x and 'twitter.com/search' in str(x))
                    seen = set()
                    for link in all_links:
                        title = link.get_text(strip=True)
                        if title and title not in seen and len(title) > 1:
                            seen.add(title)
                            topics.append({
                                "rank": len(topics) + 1,
                                "title": title,
                                "subtitle": "Em alta no Twitter/X Brasil"
                            })
                            if len(topics) >= max_topics:
                                break
                
                logger.info(f"Encontrados {len(topics)} trending topics do Twitter")
                
        except Exception as e:
            logger.error(f"Erro ao buscar trending do Twitter: {e}")
        
        return topics
    
    async def executar_coleta(self) -> int:
        """
        Executa coleta e atualização dos trending topics do Twitter.
        
        Returns:
            Número de topics atualizados
        """
        topics = await self.identificar_trending_topics()
        
        if topics:
            count = db.update_trending_topics(topics, category="twitter")
            logger.info(f"Atualizados {count} trending topics do Twitter")
            return count
        
        return 0


class GoogleTrendingCollector:
    """
    Coleta trending topics do Google Trends Brasil.
    Usa múltiplas fontes em ordem de prioridade:
    1. Scraping da página do Google Trends (Playwright) - fonte primária
    2. RSS do Google Trends
    3. pytrends trending_searches
    """
    
    def __init__(self):
        self._pytrend: Optional['TrendReq'] = None
        # URL do RSS de trending do Google
        self.rss_url = "https://trends.google.com.br/trending/rss?geo=BR"
        # URL da página para scraping
        self.scrape_url = "https://trends.google.com.br/trending?geo=BR"
    
    @property
    def disponivel(self) -> bool:
        """Verifica se o pytrends está disponível"""
        return PYTRENDS_AVAILABLE
    
    def _get_pytrend(self) -> Optional['TrendReq']:
        """Retorna instância do pytrends (lazy loading)"""
        if not PYTRENDS_AVAILABLE:
            return None
        if self._pytrend is None:
            self._pytrend = TrendReq(
                hl='pt-BR',
                tz=-180,
                timeout=(10, 25)
            )
        return self._pytrend
    
    async def _scrape_google_trends(self, max_topics: int = 15) -> List[Dict[str, Any]]:
        """
        Faz scraping da página de trending topics do Google Trends Brasil.
        Usa Playwright para renderizar JavaScript.
        URL: https://trends.google.com.br/trending?geo=BR
        """
        if not PLAYWRIGHT_AVAILABLE:
            logger.warning("Playwright não disponível para scraping do Google Trends geral")
            return []
        
        topics = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    locale='pt-BR',
                    timezone_id='America/Sao_Paulo',
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                )
                page = await context.new_page()
                
                try:
                    logger.info(f"Scraping Google Trends geral: {self.scrape_url}")
                    # Usa domcontentloaded em vez de networkidle para ser mais rápido
                    await page.goto(self.scrape_url, wait_until='domcontentloaded', timeout=30000)
                    
                    # Aguarda a tabela carregar
                    try:
                        await page.wait_for_selector('table', timeout=20000)
                    except:
                        logger.warning("Tabela não encontrada no Google Trends geral")
                    
                    await asyncio.sleep(3)  # Aguarda JavaScript carregar dados
                    
                    # Script JavaScript para extrair dados
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
                                    title: titulo,
                                    traffic: buscas
                                });
                            }
                        });
                        
                        return topics.slice(0, 30);
                    }
                    """
                    
                    extracted = await page.evaluate(extract_script)
                    
                    if extracted:
                        for item in extracted[:max_topics]:
                            traffic = item.get("traffic", "N/A")
                            subtitle = f"Buscas: {traffic}" if traffic and traffic != "N/A" else "Pesquisa em alta no Google Brasil"
                            topics.append({
                                "rank": item.get("rank", 0),
                                "title": item.get("title", ""),
                                "subtitle": subtitle
                            })
                        
                        logger.info(f"Scraping Google geral: encontrados {len(topics)} trending")
                    
                except Exception as e:
                    logger.error(f"Erro no scraping do Google Trends geral: {e}")
                finally:
                    await browser.close()
                    
        except Exception as e:
            logger.error(f"Erro ao iniciar Playwright para Google Trends geral: {e}")
        
        return topics
    
    async def _fetch_rss_trending(self, max_topics: int = 10) -> List[Dict[str, Any]]:
        """
        Busca trending via RSS do Google Trends (método mais confiável).
        """
        try:
            import httpx
            from xml.etree import ElementTree
        except ImportError:
            logger.warning("httpx não disponível para RSS")
            return []
        
        topics = []
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    self.rss_url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                )
                response.raise_for_status()
                
                # Parse XML
                root = ElementTree.fromstring(response.text)
                
                # Namespace do RSS
                ns = {'ht': 'https://trends.google.com.br/trends/trendingsearches/daily'}
                
                items = root.findall('.//item')
                
                for rank, item in enumerate(items[:max_topics], 1):
                    title_elem = item.find('title')
                    traffic_elem = item.find('ht:approx_traffic', ns)
                    
                    if title_elem is not None and title_elem.text:
                        title = title_elem.text.strip()
                        traffic = ""
                        if traffic_elem is not None and traffic_elem.text:
                            traffic = traffic_elem.text.strip()
                        
                        subtitle = f"Pesquisa em alta - {traffic} buscas" if traffic else "Pesquisa em alta no Google Brasil"
                        
                        topics.append({
                            "rank": rank,
                            "title": title,
                            "subtitle": subtitle
                        })
                
                if topics:
                    logger.info(f"Encontrados {len(topics)} trending do Google via RSS")
                    
        except Exception as e:
            logger.warning(f"Erro ao buscar RSS do Google Trends: {e}")
        
        return topics
    
    async def _fetch_pytrends_trending(self, max_topics: int = 10) -> List[Dict[str, Any]]:
        """Busca trending via pytrends (pode falhar com 404/429)"""
        if not PYTRENDS_AVAILABLE:
            return []
        
        pytrend = self._get_pytrend()
        if not pytrend:
            return []
        
        topics = []
        loop = asyncio.get_event_loop()
        
        try:
            # Tenta trending_searches primeiro
            trending_df = await loop.run_in_executor(
                None,
                lambda: pytrend.trending_searches(pn='brazil')
            )
            
            if trending_df is not None and not trending_df.empty:
                trending_list = trending_df[0].tolist()[:max_topics]
                
                for rank, title in enumerate(trending_list, 1):
                    topics.append({
                        "rank": rank,
                        "title": str(title),
                        "subtitle": "Pesquisa em alta no Google Brasil"
                    })
                
                logger.info(f"Encontrados {len(topics)} trending via pytrends")
                
        except Exception as e:
            logger.warning(f"Erro em pytrends trending_searches: {e}")
            
            # Fallback: tenta realtime_trending_searches
            try:
                realtime_df = await loop.run_in_executor(
                    None,
                    lambda: pytrend.realtime_trending_searches(pn='BR')
                )
                
                if realtime_df is not None and not realtime_df.empty:
                    if 'title' in realtime_df.columns:
                        titles = realtime_df['title'].head(max_topics).tolist()
                    else:
                        titles = realtime_df.iloc[:, 0].head(max_topics).tolist()
                    
                    for rank, title in enumerate(titles, 1):
                        topics.append({
                            "rank": rank,
                            "title": str(title),
                            "subtitle": "Pesquisa em alta no Google Brasil"
                        })
                    
                    logger.info(f"Encontrados {len(topics)} trending via pytrends (realtime)")
                    
            except Exception as e2:
                logger.warning(f"Erro em pytrends realtime: {e2}")
        
        return topics
    
    async def identificar_trending_topics(self, max_topics: int = 10, use_scraping: bool = True) -> List[Dict[str, Any]]:
        """
        Identifica os trending topics do Google Trends Brasil.
        Usa múltiplas fontes em ordem de prioridade:
        1. Scraping da página do Google Trends (Playwright) - fonte primária
        2. RSS do Google Trends
        3. pytrends trending_searches
        
        Args:
            max_topics: Número máximo de topics a retornar
            use_scraping: Se True, tenta usar scraping com Playwright como fonte primária
            
        Returns:
            Lista de trending topics com rank, título e descrição
        """
        logger.info("Identificando trending topics do Google Trends")
        
        topics = []
        
        # FONTE 1: Scraping da página do Google Trends (mais precisa)
        if use_scraping and PLAYWRIGHT_AVAILABLE:
            logger.info("Tentando scraping do Google Trends geral...")
            topics = await self._scrape_google_trends(max_topics)
            
            if topics and len(topics) >= 3:
                logger.info(f"Usando {len(topics)} trending do scraping Google Trends")
                return topics
            else:
                logger.info("Scraping retornou poucos resultados, tentando RSS...")
        
        # FONTE 2: RSS do Google Trends
        if not topics:
            topics = await self._fetch_rss_trending(max_topics)
        
        # FONTE 3: pytrends como fallback
        if not topics:
            logger.info("RSS falhou, tentando pytrends como fallback")
            topics = await self._fetch_pytrends_trending(max_topics)
        
        if topics:
            logger.info(f"Total: {len(topics)} trending topics do Google")
        else:
            logger.warning("Nenhum trending do Google encontrado em todas as fontes")
        
        return topics
    
    async def executar_coleta(self) -> int:
        """
        Executa coleta e atualização dos trending topics do Google.
        
        Returns:
            Número de topics atualizados
        """
        topics = await self.identificar_trending_topics()
        
        if topics:
            count = db.update_trending_topics(topics, category="google")
            logger.info(f"Atualizados {count} trending topics do Google")
            return count
        
        return 0


# Instâncias globais dos coletores
trending_collector = TrendingCollector()
general_trending_collector = GeneralTrendingCollector()
twitter_trending_collector = TwitterTrendingCollector()
google_trending_collector = GoogleTrendingCollector()
