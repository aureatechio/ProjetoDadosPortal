"""
Coleta menções do Twitter/X sobre políticos com usar_diretoriaja = TRUE
usando Apify (X Posts Search Scraper).

Busca tweets que mencionam:
- O nome do político
- O @username do Twitter

Salva as menções na tabela `public.social_mentions` com plataforma='twitter'.

Requer variáveis de ambiente (no .env ou exportadas):
- SUPABASE_URL
- SUPABASE_KEY
- APIFY_TOKEN

Opcional:
- APIFY_TWITTER_SEARCH_ACTOR_ID (default: scraper_one/x-posts-search)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from supabase import create_client

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


def ensure_env_loaded(project_root: Path) -> None:
    if load_dotenv is not None:
        load_dotenv(project_root / ".env", override=False)


def looks_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def pick_int(d: Dict[str, Any], *keys: str, default: int = 0) -> int:
    """Extrai um valor inteiro de um dicionário, tentando múltiplas chaves."""
    for k in keys:
        v = d.get(k)
        if isinstance(v, bool):
            continue
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str) and v.isdigit():
            return int(v)
    return default


def pick_str(d: Dict[str, Any], *keys: str, default: str = "") -> str:
    """Extrai uma string de um dicionário, tentando múltiplas chaves."""
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default


def clean_tweet_text(text: str) -> str:
    """
    Limpa e formata o texto do tweet para melhor legibilidade.
    - Remove menções excessivas no início (@user @user2 @user3...)
    - Mantém no máximo 2 menções no início
    - Remove URLs de mídia (t.co)
    - Remove espaços extras
    """
    if not text:
        return ""
    
    # Remove URLs t.co no final
    text = re.sub(r'\s*https://t\.co/\w+\s*', ' ', text)
    
    # Encontra menções no início do texto
    # Pattern: começa com @mentions separados por espaço
    match = re.match(r'^((?:@\w+\s*)+)', text)
    
    if match:
        mentions_part = match.group(1)
        rest_of_text = text[len(mentions_part):].strip()
        
        # Extrai todas as menções
        mentions = re.findall(r'@\w+', mentions_part)
        
        # Mantém no máximo 2 menções no início
        if len(mentions) > 2:
            kept_mentions = ' '.join(mentions[:2])
            text = f"{kept_mentions} [...] {rest_of_text}"
        elif rest_of_text:
            text = f"{mentions_part.strip()} {rest_of_text}"
    
    # Remove espaços extras
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def build_search_queries(politico: Dict[str, Any]) -> List[str]:
    """
    Constrói queries de busca para o político.
    Retorna lista de queries (uma para nome, outra para @username).
    O actor tem limite de 30 caracteres por query.
    """
    name = (politico.get("name") or "").strip()
    twitter_username = (politico.get("twitter_username") or "").strip()
    
    # Remove títulos comuns do nome
    name_clean = re.sub(
        r"^(dr\.?|dra\.?|prof\.?|dep\.?|sen\.?|vereador|vereadora|deputado|deputada|senador|senadora)\s+",
        "",
        name,
        flags=re.I
    ).strip()
    
    queries = []
    
    # Query por @username (menções diretas) - prioridade
    if twitter_username:
        username_query = f"@{twitter_username}"
        if len(username_query) <= 30:
            queries.append(username_query)
    
    # Query por nome (se couber no limite)
    if name_clean:
        # Tenta nome completo entre aspas
        name_query = f'"{name_clean}"'
        if len(name_query) <= 30:
            queries.append(name_query)
        else:
            # Se não couber, usa apenas o primeiro e último nome
            parts = name_clean.split()
            if len(parts) >= 2:
                short_name = f"{parts[0]} {parts[-1]}"
                short_query = f'"{short_name}"'
                if len(short_query) <= 30:
                    queries.append(short_query)
                else:
                    # Último recurso: só o primeiro nome
                    queries.append(parts[0])
            else:
                queries.append(name_clean[:28])  # Trunca se necessário
    
    return queries


def normalize_tweet(tweet: Dict[str, Any], politico: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza um tweet do Apify para o formato da tabela social_mentions.
    
    Campos do actor xtdata/twitter-x-scraper:
    - full_text - texto completo do tweet
    - url, twitterUrl - URL do tweet
    - id - ID do tweet
    - author: {screen_name, name, ...}
    - favorite_count, retweet_count, reply_count, quote_count
    - created_at - data de criação
    """
    # Extrai campos do tweet (xtdata usa full_text, id, etc.)
    tweet_id = pick_str(tweet, "id", "postId", "post_id", "tweetId", "tweet_id")
    raw_content = pick_str(tweet, "full_text", "text", "postText", "fullText", "content")
    content = clean_tweet_text(raw_content)  # Limpa e formata o texto
    
    # Autor - xtdata usa author.screen_name e author.name
    author_name = ""
    author_username = ""
    
    # Autor vem como objeto aninhado no xtdata
    if isinstance(tweet.get("author"), dict):
        author_obj = tweet["author"]
        author_username = pick_str(author_obj, "screen_name", "screenName", "username", "userName", "handle")
        author_name = pick_str(author_obj, "name", "displayName", "fullName")
    
    # Fallback para campos diretos
    if not author_username:
        author_username = pick_str(tweet, "screen_name", "screenName", "username", "authorUsername")
    if not author_name:
        author_name = pick_str(tweet, "authorName", "author_name", "userName", "name")
    
    if isinstance(tweet.get("user"), dict):
        user_obj = tweet["user"]
        if not author_username:
            author_username = pick_str(user_obj, "screen_name", "screenName", "username")
        if not author_name:
            author_name = pick_str(user_obj, "name", "displayName")
    
    # URL do tweet (xtdata usa url ou twitterUrl)
    url = pick_str(tweet, "url", "twitterUrl", "postUrl", "tweet_url", "link")
    if not url and tweet_id and author_username:
        url = f"https://x.com/{author_username}/status/{tweet_id}"
    
    # Métricas de engajamento (xtdata usa favorite_count, retweet_count, etc.)
    likes = pick_int(tweet, "favorite_count", "favouriteCount", "favoriteCount", "likeCount", "likes")
    reposts = pick_int(tweet, "retweet_count", "repostCount", "retweetCount", "retweets")
    replies = pick_int(tweet, "reply_count", "replyCount", "replies", "commentCount")
    quotes = pick_int(tweet, "quote_count", "quoteCount", "quotes")
    views = pick_int(tweet, "view_count", "viewCount", "views", "impressionCount")
    
    # Score de engajamento = likes + reposts*2 + replies + quotes
    engagement = likes + (reposts * 2) + replies + quotes
    
    # Data de publicação
    posted_at = pick_str(tweet, "timestamp", "createdAt", "created_at", "date", "postedAt", "publishedAt")
    
    # Metadata com informações extras
    metadata = {
        "source": "apify",
        "actor": "x-posts-search",
        "politico_name": politico.get("name"),
        "politico_twitter": politico.get("twitter_username"),
        "views": views,
        "quotes": quotes,
        "raw_keys": list(tweet.keys())[:30],  # Para debug
    }
    
    # Adiciona campos extras se disponíveis
    if tweet.get("lang"):
        metadata["language"] = tweet.get("lang")
    if tweet.get("isRetweet") or tweet.get("is_retweet"):
        metadata["is_retweet"] = True
    if tweet.get("isReply") or tweet.get("is_reply"):
        metadata["is_reply"] = True
    
    return {
        "plataforma": "twitter",
        "mention_id": tweet_id or None,
        "autor": author_name or None,
        "autor_username": author_username or None,
        "conteudo": content or None,
        "url": url or None,
        "assunto": None,  # Pode ser preenchido depois com análise de IA
        "assunto_detalhe": None,
        "sentimento": None,  # Pode ser preenchido depois com análise de IA
        "likes": likes,
        "reposts": reposts,
        "replies": replies,
        "engagement_score": float(engagement),
        "posted_at": posted_at or None,
        "metadata": metadata,
        "politico_id": politico.get("uuid"),
    }


class ApifyClient:
    """Cliente para a API do Apify."""
    
    def __init__(self, token: str, base_url: str = "https://api.apify.com") -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")
        # Timeout longo para execução síncrona do actor
        self._http = httpx.Client(timeout=httpx.Timeout(600.0, connect=30.0))
    
    def close(self) -> None:
        self._http.close()
    
    def run_sync_get_items(
        self,
        actor_id: str,
        actor_input: Dict[str, Any],
        *,
        limit: int = 100,
        timeout_s: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Executa um actor do Apify de forma síncrona e retorna os itens do dataset.
        """
        url = f"{self._base_url}/v2/acts/{actor_id}/run-sync-get-dataset-items"
        
        try:
            r = self._http.post(
                url,
                params={
                    "token": self._token,
                    "format": "json",
                    "limit": str(limit),
                },
                json=actor_input,
                timeout=timeout_s,
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            body = ""
            try:
                body = e.response.text or ""
            except Exception:
                pass
            raise RuntimeError(f"Apify HTTP {e.response.status_code}: {body[:2000]}") from e
        except httpx.TimeoutException as e:
            raise RuntimeError(f"Apify timeout após {timeout_s}s") from e
        
        data = r.json()
        
        # O response pode ser uma lista direta ou um objeto com "items"
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
        
        return []


def fetch_politicos_diretoriaja(supabase: Any) -> List[Dict[str, Any]]:
    """
    Busca políticos com usar_diretoriaja = true e twitter_username preenchido.
    """
    result = (
        supabase.table("politico")
        .select("id,uuid,name,twitter_username,cidade,estado,funcao,active,usar_diretoriaja")
        .eq("usar_diretoriaja", True)
        .not_.is_("twitter_username", "null")
        .neq("twitter_username", "")
        .order("name")
        .execute()
    )
    return result.data or []


def count_existing_mentions(supabase: Any, politico_uuid: str, hours: int = 24) -> int:
    """
    Conta quantas menções já existem para o político nas últimas X horas.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    
    result = (
        supabase.table("social_mentions")
        .select("id", count="exact")
        .eq("politico_id", politico_uuid)
        .eq("plataforma", "twitter")
        .gte("collected_at", cutoff)
        .execute()
    )
    
    return int(getattr(result, "count", 0) or 0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Coleta menções do Twitter/X sobre políticos via Apify"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Se setado, grava no Supabase. Caso contrário, apenas simula (dry-run).",
    )
    parser.add_argument(
        "--limit-politicos",
        type=int,
        default=50,
        help="Máximo de políticos a processar por execução.",
    )
    parser.add_argument(
        "--limit-tweets",
        type=int,
        default=50,
        help="Máximo de tweets a coletar por político.",
    )
    parser.add_argument(
        "--search-mode",
        type=str,
        choices=["top", "latest"],
        default="latest",
        help="Modo de busca: 'top' para mais relevantes, 'latest' para mais recentes.",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Quantos dias para trás buscar tweets.",
    )
    parser.add_argument(
        "--min-engagement",
        type=int,
        default=0,
        help="Engajamento mínimo para salvar o tweet.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Delay entre políticos (segundos).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Timeout para cada busca no Apify (segundos).",
    )
    parser.add_argument(
        "--only-ids",
        type=str,
        default=None,
        help="Opcional: processa apenas esses IDs de políticos (ex: 15,16,17).",
    )
    parser.add_argument(
        "--skip-if-recent",
        type=int,
        default=0,
        help="Pula político se já tiver >= N menções nas últimas 24h. 0 = não pula.",
    )
    args = parser.parse_args()
    
    # Carrega variáveis de ambiente
    project_root = Path(__file__).resolve().parents[1]
    ensure_env_loaded(project_root)
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    apify_token = os.getenv("APIFY_TOKEN")
    
    if not supabase_url or not supabase_key:
        raise SystemExit("Faltam SUPABASE_URL/SUPABASE_KEY no ambiente.")
    if not apify_token:
        raise SystemExit("Falta APIFY_TOKEN no ambiente.")
    
    # Actor do Apify para busca de tweets
    # xtdata~twitter-x-scraper - retorna texto completo dos tweets
    # https://apify.com/xtdata/twitter-x-scraper
    twitter_actor = os.getenv("APIFY_TWITTER_SEARCH_ACTOR_ID", "xtdata~twitter-x-scraper")
    
    supabase = create_client(supabase_url, supabase_key)
    apify = ApifyClient(apify_token)
    
    # Diretório para logs de auditoria
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    audit_path = logs_dir / f"twitter_mentions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    
    # Busca políticos alvo
    politicos = fetch_politicos_diretoriaja(supabase)
    
    # Filtra por IDs específicos se solicitado
    if args.only_ids:
        wanted = {int(x.strip()) for x in args.only_ids.split(",") if x.strip().isdigit()}
        politicos = [p for p in politicos if int(p.get("id") or 0) in wanted]
    
    # Limita quantidade
    politicos = politicos[: args.limit_politicos]
    
    print(f"[INFO] Encontrados {len(politicos)} políticos com usar_diretoriaja=true e twitter_username")
    print(f"[INFO] Modo: {'APLICAR' if args.apply else 'DRY-RUN'}")
    print(f"[INFO] Actor: {twitter_actor}")
    print(f"[INFO] Busca: {args.search_mode}, últimos {args.days_back} dias")
    print()
    
    # Estatísticas
    stats = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mode": "APLICAR" if args.apply else "DRY-RUN",
        "politicos_total": len(politicos),
        "politicos_processed": 0,
        "politicos_skipped": 0,
        "tweets_found": 0,
        "tweets_saved": 0,
        "errors": [],
    }
    
    try:
        for idx, politico in enumerate(politicos, 1):
            pid = int(politico["id"])
            puuid = politico.get("uuid")
            name = (politico.get("name") or "").strip()
            twitter_username = (politico.get("twitter_username") or "").strip()
            
            print(f"[{idx}/{len(politicos)}] {name} (@{twitter_username})")
            
            # Verifica se deve pular por já ter menções recentes
            if args.skip_if_recent > 0:
                existing = count_existing_mentions(supabase, puuid, hours=24)
                if existing >= args.skip_if_recent:
                    print(f"  -> Pulando: já tem {existing} menções nas últimas 24h")
                    stats["politicos_skipped"] += 1
                    continue
            
            # Constrói as queries de busca (pode retornar múltiplas)
            search_queries = build_search_queries(politico)
            if not search_queries:
                print(f"  -> Pulando: não foi possível construir query de busca")
                stats["politicos_skipped"] += 1
                continue
            
            print(f"  -> Queries: {search_queries}")
            
            # Coleta tweets de todas as queries
            all_tweets = []
            seen_ids = set()
            
            for search_query in search_queries:
                # Input para o actor do Apify (xtdata/twitter-x-scraper)
                # Documentação: https://apify.com/xtdata/twitter-x-scraper
                actor_input = {
                    "searchTerms": [search_query],  # lista de termos de busca
                    "maxItems": int(args.limit_tweets),
                    "tweetLanguage": "pt",  # foco em tweets em português
                }
                
                # Modo de busca (live = mais recentes)
                if args.search_mode == "latest":
                    actor_input["searchMode"] = "live"
                
                # Executa a busca
                try:
                    tweets = apify.run_sync_get_items(
                        twitter_actor,
                        actor_input,
                        limit=int(args.limit_tweets),
                        timeout_s=float(args.timeout),
                    )
                    
                    # Deduplica por ID do tweet
                    for t in tweets:
                        tid = pick_str(t, "id", "tweetId", "tweet_id", "postId")
                        if tid and tid not in seen_ids:
                            seen_ids.add(tid)
                            all_tweets.append(t)
                    
                except Exception as e:
                    error_msg = f"Erro na query '{search_query}': {str(e)}"
                    print(f"  -> ERRO: {error_msg}")
                    stats["errors"].append({"politico_id": pid, "query": search_query, "error": str(e)})
                
                # Pequeno delay entre queries
                time.sleep(0.5)
            
            tweets = all_tweets
            print(f"  -> Encontrados: {len(tweets)} tweets (únicos)")
            stats["tweets_found"] += len(tweets)
            
            # Normaliza e filtra tweets
            mentions_to_save = []
            for tweet in tweets:
                mention = normalize_tweet(tweet, politico)
                
                # Filtra por engajamento mínimo
                if mention["engagement_score"] < args.min_engagement:
                    continue
                
                # Pula se não tiver conteúdo
                if not mention.get("conteudo") and not mention.get("url"):
                    continue
                
                mentions_to_save.append(mention)
            
            print(f"  -> Após filtros: {len(mentions_to_save)} menções válidas")
            
            # Salva no Supabase
            saved_count = 0
            if args.apply and mentions_to_save:
                for mention in mentions_to_save:
                    try:
                        # Usa upsert para evitar duplicatas
                        # Constraint: social_mentions_unique_mention_per_politico (politico_id, plataforma, mention_id)
                        supabase.table("social_mentions").upsert(
                            mention,
                            on_conflict="politico_id,plataforma,mention_id"
                        ).execute()
                        saved_count += 1
                    except Exception as e:
                        # Se falhar o upsert, tenta insert normal
                        try:
                            supabase.table("social_mentions").insert(mention).execute()
                            saved_count += 1
                        except Exception:
                            # Provavelmente duplicata, ignora silenciosamente
                            pass
                
                print(f"  -> Salvos: {saved_count} menções")
            
            stats["tweets_saved"] += saved_count
            stats["politicos_processed"] += 1
            
            # Log de auditoria
            audit_row = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "politico_id": pid,
                "politico_uuid": puuid,
                "politico_name": name,
                "twitter_username": twitter_username,
                "search_queries": search_queries,
                "tweets_found": len(tweets),
                "tweets_filtered": len(mentions_to_save),
                "tweets_saved": saved_count,
                "apply": args.apply,
            }
            with audit_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(audit_row, ensure_ascii=False) + "\n")
            
            # Delay entre políticos
            time.sleep(float(args.sleep))
    
    finally:
        apify.close()
    
    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    stats["audit_file"] = str(audit_path)
    
    print()
    print("=" * 60)
    print("RESULTADO FINAL")
    print("=" * 60)
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
