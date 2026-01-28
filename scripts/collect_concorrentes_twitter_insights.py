"""
Coleta INSIGHTS de Twitter/X para concorrentes (tabela public.concorrentes).

Objetivo:
- Mapear os concorrentes cadastrados em `public.concorrentes`
- Para cada concorrente, salvar um snapshot diário em `public.concorrente_twitter_insights` com:
  - followers_count (via Apify / actor do Twitter user scraper)
  - top 3 menções mais engajadas no Twitter/X (coletadas via Apify; fallback em `public.social_mentions`)

Requer variáveis de ambiente (no .env ou exportadas):
- SUPABASE_URL
- SUPABASE_KEY

Opcional (para followers_count via Apify):
- APIFY_TOKEN
- APIFY_BASE_URL=https://api.apify.com
- APIFY_TWITTER_USER_ACTOR_ID=V38PZzpEgOfeeWvZY (apidojo/twitter-user-scraper)
Opcional (para menções no Twitter/X via Apify):
- APIFY_TWITTER_SEARCH_ACTOR_ID=xtdata~twitter-x-scraper

Uso:
  python scripts/collect_concorrentes_twitter_insights.py               # dry-run
  python scripts/collect_concorrentes_twitter_insights.py --apply       # grava no Supabase
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


def _parse_int_like(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # remove separadores comuns (1,234 / 1.234 / 1 234)
        s = re.sub(r"[,\.\s]", "", s)
        if s.isdigit():
            try:
                return int(s)
            except Exception:
                return None
    return None


def pick_int(d: Dict[str, Any], *keys: str) -> Optional[int]:
    for k in keys:
        v = d.get(k)
        parsed = _parse_int_like(v)
        if parsed is not None:
            return parsed
    return None


def pick_str(d: Dict[str, Any], *keys: str, default: str = "") -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default


def clean_tweet_text(text: str) -> str:
    """
    Limpa texto do tweet para exibição:
    - remove urls t.co
    - reduz menções repetidas no começo
    """
    if not text:
        return ""
    text = re.sub(r"\s*https://t\.co/\w+\s*", " ", text)
    match = re.match(r"^((?:@\w+\s*)+)", text)
    if match:
        mentions_part = match.group(1)
        rest = text[len(mentions_part) :].strip()
        mentions = re.findall(r"@\w+", mentions_part)
        if len(mentions) > 2:
            kept = " ".join(mentions[:2])
            text = f"{kept} [...] {rest}".strip()
        elif rest:
            text = f"{mentions_part.strip()} {rest}".strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_search_queries(name: str, twitter_username: str) -> List[str]:
    """
    Monta queries curtas (<=30 chars) para o actor de busca.
    Prioriza @username e depois nome (entre aspas se couber).
    """
    name = (name or "").strip()
    tw = (twitter_username or "").strip().lstrip("@")

    # remove prefixos comuns no começo do nome
    name_clean = re.sub(
        r"^(dr\.?|dra\.?|prof\.?|dep\.?|sen\.?|vereador|vereadora|deputado|deputada|senador|senadora)\s+",
        "",
        name,
        flags=re.I,
    ).strip()

    queries: List[str] = []
    if tw:
        q = f"@{tw}"
        if len(q) <= 30:
            queries.append(q)

    if name_clean:
        q = f"\"{name_clean}\""
        if len(q) <= 30:
            queries.append(q)
        else:
            parts = name_clean.split()
            if len(parts) >= 2:
                short = f"\"{parts[0]} {parts[-1]}\""
                if len(short) <= 30:
                    queries.append(short)
                else:
                    queries.append(parts[0][:28])
            else:
                queries.append(name_clean[:28])

    return queries


def normalize_tweet_to_mention(
    tweet: Dict[str, Any],
    *,
    politico_uuid: Optional[str],
    politico_name: str,
    politico_tw: str,
) -> Dict[str, Any]:
    """
    Normaliza o output do actor xtdata/twitter-x-scraper para o formato da tabela social_mentions.
    """
    tweet_id = pick_str(tweet, "id", "postId", "post_id", "tweetId", "tweet_id")
    raw_content = pick_str(tweet, "full_text", "text", "postText", "fullText", "content")
    content = clean_tweet_text(raw_content)

    author_name = ""
    author_username = ""
    if isinstance(tweet.get("author"), dict):
        author_obj = tweet["author"]
        author_username = pick_str(author_obj, "screen_name", "screenName", "username", "userName", "handle")
        author_name = pick_str(author_obj, "name", "displayName", "fullName")
    if not author_username:
        author_username = pick_str(tweet, "screen_name", "screenName", "username", "authorUsername")
    if not author_name:
        author_name = pick_str(tweet, "authorName", "author_name", "userName", "name")

    url = pick_str(tweet, "url", "twitterUrl", "postUrl", "tweet_url", "link")
    if not url and tweet_id and author_username:
        url = f"https://x.com/{author_username}/status/{tweet_id}"

    likes = pick_int(tweet, "favorite_count", "favouriteCount", "favoriteCount", "likeCount", "likes") or 0
    reposts = pick_int(tweet, "retweet_count", "repostCount", "retweetCount", "retweets") or 0
    replies = pick_int(tweet, "reply_count", "replyCount", "replies", "commentCount") or 0
    quotes = pick_int(tweet, "quote_count", "quoteCount", "quotes") or 0
    views = pick_int(tweet, "view_count", "viewCount", "views", "impressionCount") or 0

    engagement = int(likes) + (int(reposts) * 2) + int(replies) + int(quotes)
    posted_at = pick_str(tweet, "timestamp", "createdAt", "created_at", "date", "postedAt", "publishedAt")

    metadata = {
        "source": "apify",
        "actor": "xtdata~twitter-x-scraper",
        "politico_name": politico_name,
        "politico_twitter": politico_tw,
        "views": views,
        "quotes": quotes,
        "raw_keys": list(tweet.keys())[:30],
    }

    return {
        "plataforma": "twitter",
        "mention_id": tweet_id or None,
        "autor": author_name or None,
        "autor_username": author_username or None,
        "conteudo": content or None,
        "url": url or None,
        "assunto": None,
        "assunto_detalhe": None,
        "sentimento": None,
        "likes": int(likes),
        "reposts": int(reposts),
        "replies": int(replies),
        "engagement_score": float(engagement),
        "posted_at": posted_at or None,
        "metadata": metadata,
        "politico_id": politico_uuid or None,
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_mentions_from_apify(
    apify: ApifyClient,
    *,
    actor_id: str,
    politico_uuid: Optional[str],
    politico_name: str,
    politico_tw: str,
    max_items: int,
    search_mode: str,
    timeout_s: float,
) -> List[Dict[str, Any]]:
    """
    Coleta tweets do Twitter/X via Apify (xtdata~twitter-x-scraper) e normaliza.
    """
    queries = build_search_queries(politico_name, politico_tw)
    if not queries:
        return []

    all_tweets: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    for q in queries:
        actor_input: Dict[str, Any] = {
            "searchTerms": [q],
            "maxItems": int(max_items),
            "tweetLanguage": "pt",
        }
        if search_mode == "latest":
            actor_input["searchMode"] = "live"

        tweets = apify.run_sync_get_items(actor_id, actor_input, limit=int(max_items), timeout_s=float(timeout_s))
        for t in tweets:
            tid = pick_str(t, "id", "tweetId", "tweet_id", "postId")
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                all_tweets.append(t)

        time.sleep(0.3)

    mentions = [
        normalize_tweet_to_mention(t, politico_uuid=politico_uuid, politico_name=politico_name, politico_tw=politico_tw)
        for t in all_tweets
    ]
    mentions = [m for m in mentions if m.get("mention_id") and (m.get("conteudo") or m.get("url"))]
    mentions.sort(key=lambda m: float(m.get("engagement_score", 0) or 0), reverse=True)
    return mentions


class ApifyClient:
    """Cliente mínimo para Apify (run-sync-get-dataset-items)."""

    def __init__(self, token: str, base_url: str = "https://api.apify.com") -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._http = httpx.Client(timeout=httpx.Timeout(310.0, connect=30.0))

    def close(self) -> None:
        self._http.close()

    def run_sync_get_items(
        self,
        actor_id: str,
        actor_input: Dict[str, Any],
        *,
        limit: int = 10,
        timeout_s: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        url = f"{self._base_url}/v2/acts/{actor_id}/run-sync-get-dataset-items"
        try:
            r = self._http.post(
                url,
                params={"token": self._token, "format": "json", "limit": str(limit)},
                json=actor_input,
                timeout=timeout_s,
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            body = ""
            try:
                body = e.response.text or ""
            except Exception:
                body = ""
            raise RuntimeError(f"Apify HTTP {e.response.status_code}: {body[:2000]}") from e
        data = r.json()
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
        return []


def fetch_concorrentes(supabase: Any) -> List[Dict[str, Any]]:
    """Retorna concorrentes cadastrados em public.concorrentes."""
    page_size = 1000
    offset = 0
    out: List[Dict[str, Any]] = []

    while True:
        resp = (
            supabase.table("concorrentes")
            .select("id,name,twitter_username,politico_id")
            .order("created_at", desc=False)
            .limit(page_size)
            .offset(offset)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if not rows:
            break
        out.extend([r for r in rows if isinstance(r, dict)])
        if len(rows) < page_size:
            break
        offset += page_size

    return out


def fetch_top_mentions(
    supabase: Any,
    *,
    politico_uuid: str,
    days_back: int,
    limit: int = 3,
) -> List[Dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=int(days_back))).isoformat()
    resp = (
        supabase.table("social_mentions")
        .select(
            "mention_id,url,conteudo,autor,autor_username,posted_at,likes,reposts,replies,engagement_score,metadata,collected_at"
        )
        .eq("politico_id", politico_uuid)
        .eq("plataforma", "twitter")
        .gte("collected_at", cutoff)
        .order("engagement_score", desc=True)
        .limit(int(limit))
        .execute()
    )
    return getattr(resp, "data", None) or []


def fetch_followers_count_via_apify(
    apify: ApifyClient,
    *,
    twitter_actor_id: str,
    twitter_username: str,
) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    Tenta extrair followers_count do actor apidojo/twitter-user-scraper.
    O output varia por versão; fazemos parsing robusto.
    """
    u = (twitter_username or "").strip().lstrip("@")
    if not u:
        return None, {"reason": "missing_username"}

    # Nota importante (actor API Dojo / pay-per-result):
    # há uma exigência mínima de ~5 resultados para cobrir custos.
    # Para coletar somente o perfil (sem followers/following), repetimos o handle 5x.
    actor_input = {
        "twitterHandles": [u] * 5,
        "maxItems": 5,
        "getFollowers": False,
        "getFollowing": False,
        "getRetweeters": False,
        "includeUnavailableUsers": False,
    }
    items = apify.run_sync_get_items(twitter_actor_id, actor_input, limit=5, timeout_s=120.0)
    if not items:
        return None, {"reason": "no_items"}

    it = items[0] if isinstance(items[0], dict) else {}

    # Campos observados/possíveis em outputs do Apify (varia):
    # - followersCount / followers_count / followers
    # - user: { followersCount / followers_count }
    user_obj = it.get("user") if isinstance(it.get("user"), dict) else {}
    followers = pick_int(
        it,
        "followersCount",
        "followers_count",
        "followers",
        "followersTotal",
        "followers_total",
    )
    if followers is None and isinstance(user_obj, dict):
        followers = pick_int(
            user_obj,
            "followersCount",
            "followers_count",
            "followers",
            "followersTotal",
            "followers_total",
        )

    meta = {
        "actor": twitter_actor_id,
        "raw_keys": list(it.keys())[:40],
        "raw_user_keys": list(user_obj.keys())[:40] if isinstance(user_obj, dict) else [],
    }
    return followers, meta


def main() -> None:
    parser = argparse.ArgumentParser(description="Coleta insights de Twitter/X para concorrentes")
    parser.add_argument("--apply", action="store_true", help="Se setado, grava no Supabase; senão, dry-run.")
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Janela de dias para buscar menções em social_mentions (default: 7).",
    )
    parser.add_argument(
        "--limit-concorrentes",
        type=int,
        default=200,
        help="Máximo de concorrentes a processar por execução (default: 200).",
    )
    parser.add_argument(
        "--no-fetch-twitter",
        action="store_true",
        help="Se setado, NÃO faz extração de menções via Apify (usa apenas social_mentions).",
    )
    parser.add_argument(
        "--save-mentions",
        action="store_true",
        help="Se setado junto com --apply, grava as menções coletadas no Supabase (tabela social_mentions).",
    )
    parser.add_argument(
        "--limit-tweets",
        type=int,
        default=50,
        help="Máximo de tweets a coletar por query (default: 50).",
    )
    parser.add_argument(
        "--search-mode",
        type=str,
        choices=["top", "latest"],
        default="top",
        help="Modo de busca no Twitter/X via Apify: 'top' ou 'latest' (default: top).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Delay entre concorrentes (segundos).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Timeout por chamada ao Apify (segundos).",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    ensure_env_loaded(project_root)

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    apify_token = os.getenv("APIFY_TOKEN")
    apify_base_url = os.getenv("APIFY_BASE_URL", "https://api.apify.com")
    twitter_user_actor = os.getenv("APIFY_TWITTER_USER_ACTOR_ID", "V38PZzpEgOfeeWvZY")
    twitter_search_actor = os.getenv("APIFY_TWITTER_SEARCH_ACTOR_ID", "xtdata~twitter-x-scraper")

    if not supabase_url or not supabase_key:
        raise SystemExit("Faltam SUPABASE_URL/SUPABASE_KEY no ambiente.")

    supabase = create_client(supabase_url, supabase_key)
    apify: Optional[ApifyClient] = None
    if apify_token:
        apify = ApifyClient(apify_token, base_url=apify_base_url)

    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    audit_path = logs_dir / f"concorrentes_twitter_insights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

    stats: Dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mode": "APLICAR" if args.apply else "DRY-RUN",
        "days_back": int(args.days_back),
        "concorrentes_total": 0,
        "concorrentes_processed": 0,
        "snapshots_upserted": 0,
        "errors": [],
        "apify_enabled": bool(apify is not None),
        "apify_actor": twitter_user_actor if apify is not None else None,
    }

    try:
        concorrentes = fetch_concorrentes(supabase)
        concorrentes = concorrentes[: int(args.limit_concorrentes)]
        stats["concorrentes_total"] = len(concorrentes)

        print(f"[INFO] Concorrentes: {len(concorrentes)}")
        print(f"[INFO] Modo: {'APLICAR' if args.apply else 'DRY-RUN'}")
        print(f"[INFO] Janela menções: {int(args.days_back)} dias")
        print(f"[INFO] Apify: {'SIM' if apify is not None else 'NÃO (APIFY_TOKEN ausente)'}")
        print(f"[INFO] Fetch Twitter via Apify: {'NÃO' if args.no_fetch_twitter else 'SIM'}")
        print()

        for idx, c in enumerate(concorrentes, 1):
            concorrente_id = (c.get("id") or "").strip()
            politico_uuid = (c.get("politico_id") or "").strip()
            name = (c.get("name") or "").strip()
            tw = (c.get("twitter_username") or "").strip()

            label = f"[{idx}/{len(concorrentes)}] {name} ({concorrente_id})"
            if tw:
                label += f" {tw if tw.startswith('@') else '@' + tw}"
            if politico_uuid:
                label += f" politico={politico_uuid}"
            print(label)

            if not concorrente_id:
                stats["errors"].append({"concorrente": name, "error": "concorrente.id ausente"})
                continue

            # 1) Menções (preferência: extração via Twitter/Apify; fallback: social_mentions)
            top_mentions: List[Dict[str, Any]] = []

            if apify is not None and not args.no_fetch_twitter:
                try:
                    mentions_live = fetch_mentions_from_apify(
                        apify,
                        actor_id=twitter_search_actor,
                        politico_uuid=politico_uuid or None,
                        politico_name=name,
                        politico_tw=tw,
                        max_items=int(args.limit_tweets),
                        search_mode=str(args.search_mode),
                        timeout_s=float(args.timeout),
                    )
                    top_mentions = mentions_live[:3]

                    # opcional: persistir menções no Supabase (social_mentions)
                    if args.apply and args.save_mentions and mentions_live:
                        for m in mentions_live:
                            # garante politico_id (pode ser NULL) e collected_at
                            if not politico_uuid:
                                m["politico_id"] = None
                            try:
                                supabase.table("social_mentions").upsert(
                                    m,
                                    on_conflict="politico_id,plataforma,mention_id",
                                ).execute()
                            except Exception:
                                # se falhar (ex.: duplicata/constraint), ignora
                                pass
                except Exception as e:
                    stats["errors"].append({"concorrente_id": concorrente_id, "error": f"apify_mentions: {str(e)}"})

            if not top_mentions:
                if politico_uuid:
                    top_mentions = fetch_top_mentions(supabase, politico_uuid=politico_uuid, days_back=int(args.days_back), limit=3)
                else:
                    top_mentions = []

            # 2) Followers count (opcional via Apify)
            followers_count: Optional[int] = None
            followers_meta: Dict[str, Any] = {}
            if apify is not None and not looks_empty(tw):
                try:
                    followers_count, followers_meta = fetch_followers_count_via_apify(
                        apify,
                        twitter_actor_id=twitter_user_actor,
                        twitter_username=tw,
                    )
                except Exception as e:
                    followers_meta = {"error": str(e)}

            now = datetime.now(timezone.utc)
            row = {
                "concorrente_id": concorrente_id,
                "politico_id": politico_uuid or None,
                "concorrente_politico_int_id": None,
                "twitter_username": tw or None,
                "followers_count": int(followers_count) if isinstance(followers_count, int) else None,
                "top_mentions": top_mentions or [],
                "mentions_window_days": int(args.days_back),
                "computed_date": now.date().isoformat(),
                "computed_at": now.isoformat(),
                "source": "apify+supabase",
            }

            audit_row = {
                "ts": now.isoformat(),
                "apply": bool(args.apply),
                "concorrente_id": concorrente_id,
                "politico_id": politico_uuid or None,
                "concorrente_name": name,
                "twitter_username": tw,
                "followers_count": followers_count,
                "followers_meta": followers_meta,
                "top_mentions_count": len(top_mentions or []),
            }
            with audit_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(audit_row, ensure_ascii=False) + "\n")

            if args.apply:
                try:
                    supabase.table("concorrente_twitter_insights").upsert(
                        row,
                        on_conflict="concorrente_id,mentions_window_days,computed_date",
                    ).execute()
                    stats["snapshots_upserted"] += 1
                except Exception as e:
                    stats["errors"].append({"concorrente_id": concorrente_id, "error": str(e)})

            stats["concorrentes_processed"] += 1
            time.sleep(float(args.sleep))

    finally:
        if apify is not None:
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

