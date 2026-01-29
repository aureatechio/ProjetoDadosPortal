"""
Sincroniza políticos com `public.politico.usar_diretoriaja = true`:
- Preenche `instagram_username` / `twitter_username` (quando vazios) usando Apify
- Se `politico.image` estiver vazio, coloca a foto do perfil do Instagram (profilePicUrlHD/profilePicUrl)
- Coleta e grava as 3 postagens mais engajadas no `public.social_media_posts` (plataforma=instagram)

Requer .env/variáveis de ambiente:
- SUPABASE_URL
- SUPABASE_KEY
- APIFY_TOKEN

Actors padrão:
- Instagram Search Scraper (apify/instagram-search-scraper): DrF9mzPPEuVizVF4l
- Instagram Scraper (apify/instagram-scraper): shu8hvrXbJbY3Eb9W
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
from supabase import create_client

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

# Configurações de storage
DEFAULT_BUCKET = "portal"
DOWNLOAD_TIMEOUT = 30.0
IMAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}


TITLE_PREFIXES = (
    "dr",
    "dra",
    "prof",
    "prof.",
    "capitão",
    "capitao",
    "tenente",
    "coronel",
    "delegada",
    "delegado",
    "pastor",
    "padre",
    "vereador",
    "vereadora",
)

POL_HINT_RE = re.compile(r"\b(deputad|senador|prefeit|vereador|parlament|gov|governad|dep\.)\b", re.I)
BR_HINT_RE = re.compile(r"\b(brasil|brasileir|br)\b", re.I)


def ensure_env_loaded(project_root: Path) -> None:
    if load_dotenv is not None:
        load_dotenv(project_root / ".env", override=False)


# ============ Funções de Upload para Supabase Storage ============

def _guess_content_type(url: str, response: httpx.Response) -> str:
    """Determina o content-type da imagem."""
    ct = (response.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ct.startswith("image/"):
        return ct
    url_lower = url.lower()
    if ".png" in url_lower:
        return "image/png"
    if ".webp" in url_lower:
        return "image/webp"
    if ".gif" in url_lower:
        return "image/gif"
    return "image/jpeg"


def _ext_from_content_type(ct: str) -> str:
    """Retorna extensão baseada no content-type."""
    if "png" in ct:
        return "png"
    if "webp" in ct:
        return "webp"
    if "gif" in ct:
        return "gif"
    return "jpg"


def _is_already_in_storage(url: str, bucket: str = DEFAULT_BUCKET) -> bool:
    """Verifica se a URL já aponta para o Supabase Storage."""
    if not url:
        return False
    return f"/storage/v1/object/public/{bucket}/" in url


def upload_image_to_storage(
    supabase: Any,
    image_url: str,
    folder: str,
    filename: str,
    bucket: str = DEFAULT_BUCKET,
    http_client: Optional[httpx.Client] = None,
) -> Optional[str]:
    """
    Baixa uma imagem de URL externa e faz upload para o Supabase Storage.
    
    Args:
        supabase: Cliente Supabase
        image_url: URL da imagem a ser baixada
        folder: Pasta no bucket (ex: "politicos", "instagram")
        filename: Nome do arquivo (sem extensão)
        bucket: Nome do bucket
        http_client: Cliente HTTP opcional (se None, cria um novo)
        
    Returns:
        URL pública do Storage ou a URL original em caso de erro
    """
    if not image_url:
        return None
    
    # Verifica se já está no storage
    if _is_already_in_storage(image_url, bucket):
        return image_url
    
    try:
        # Download da imagem
        if http_client:
            response = http_client.get(image_url, timeout=DOWNLOAD_TIMEOUT)
        else:
            with httpx.Client(timeout=DOWNLOAD_TIMEOUT, follow_redirects=True, headers=IMAGE_HEADERS) as client:
                response = client.get(image_url)
        
        response.raise_for_status()
        image_data = response.content
        
        if not image_data:
            return image_url
        
        content_type = _guess_content_type(image_url, response)
        ext = _ext_from_content_type(content_type)
        
        # Gera hash para unicidade
        url_hash = hashlib.md5(image_url.encode()).hexdigest()[:8]
        safe_filename = f"{filename}_{url_hash}.{ext}"
        path = f"{folder}/{safe_filename}"
        
        # Upload para o storage
        with tempfile.NamedTemporaryFile(delete=True, suffix=f".{ext}") as tmp:
            tmp.write(image_data)
            tmp.flush()
            
            supabase.storage.from_(bucket).upload(
                path,
                tmp.name,
                {"content-type": content_type, "upsert": "true"},
            )
        
        # Retorna URL pública
        public_url = supabase.storage.from_(bucket).get_public_url(path)
        return public_url
        
    except Exception as e:
        # Em caso de erro, retorna URL original
        print(f"[WARN] Erro ao fazer upload de imagem {image_url}: {e}")
        return image_url


def looks_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def normalize_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    parts = n.split()
    if parts and parts[0].strip(".").lower() in TITLE_PREFIXES:
        n = " ".join(parts[1:])
    n = re.sub(r"[^\w\sÀ-ÿ]", " ", n, flags=re.UNICODE)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def token_set(s: str) -> set[str]:
    s = normalize_name(s).lower()
    return {t for t in s.split() if len(t) >= 2}


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def first_non_empty(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def coerce_username(v: Any) -> str:
    s = (v or "")
    if not isinstance(s, str):
        s = str(s)
    s = s.strip().lstrip("@")
    s = re.sub(r"^https?://(www\.)?(instagram\.com|x\.com|twitter\.com)/", "", s, flags=re.I)
    s = s.split("?")[0].strip("/")
    if "/" in s:
        s = s.split("/")[0]
    return s


def extract_instagram_username(item: Dict[str, Any]) -> str:
    return coerce_username(first_non_empty(item.get("username"), item.get("url")))


def extract_profile_pic(item: Dict[str, Any]) -> str:
    return first_non_empty(item.get("profilePicUrlHD"), item.get("profilePicUrl"), item.get("profilePicUrlHd"))


def _iter_external_urls(instagram_item: Dict[str, Any]) -> Iterable[str]:
    ext = instagram_item.get("externalUrl")
    if isinstance(ext, str) and ext.strip():
        yield ext.strip()
    ext_list = instagram_item.get("externalUrls")
    if isinstance(ext_list, list):
        for it in ext_list:
            if isinstance(it, str) and it.strip():
                yield it.strip()
            if isinstance(it, dict):
                u = it.get("url")
                if isinstance(u, str) and u.strip():
                    yield u.strip()


def _extract_twitter_handle_from_text(text: str) -> Optional[str]:
    m = re.search(r"https?://(?:www\.)?(?:twitter\.com|x\.com)/([A-Za-z0-9_]{1,30})", text)
    if m:
        return m.group(1)
    return None


def infer_twitter_from_instagram_item(http: httpx.Client, ig_item: Dict[str, Any]) -> Optional[str]:
    # 1) links diretos (x.com/twitter.com)
    for u in _iter_external_urls(ig_item):
        handle = _extract_twitter_handle_from_text(u)
        if handle:
            return handle

    # 2) Linktree / l.instagram.com -> segue e busca twitter.com no HTML final
    allow_hosts = ("linktr.ee", "www.linktr.ee", "l.instagram.com")
    for u in _iter_external_urls(ig_item):
        try:
            parsed = httpx.URL(u)
            if parsed.host not in allow_hosts:
                continue
        except Exception:
            continue
        try:
            r = http.get(u, follow_redirects=True)
            if r.status_code >= 400:
                continue
            handle = _extract_twitter_handle_from_text(r.text or "")
            if handle:
                return handle
        except Exception:
            continue
    return None


def score_profile(politico_name: str, ig_item: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    pn = token_set(politico_name)
    full_name = first_non_empty(ig_item.get("fullName"), ig_item.get("name"))
    fn = token_set(full_name)
    bio = first_non_empty(ig_item.get("biography"), ig_item.get("bio"), ig_item.get("description"))
    verified = bool(ig_item.get("verified")) if isinstance(ig_item.get("verified"), bool) else False

    sim = jaccard(pn, fn)  # 0..1
    score = int(round(sim * 70))
    if verified:
        score += 15
    if POL_HINT_RE.search(bio):
        score += 10
    if BR_HINT_RE.search(bio):
        score += 5
    score = max(0, min(100, score))
    return score, {
        "full_name": full_name,
        "verified": verified,
        "sim": sim,
        "score": score,
    }


def pick_best_ig(politico_name: str, items: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], int, Dict[str, Any]]:
    best = None
    best_score = -1
    best_meta: Dict[str, Any] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        username = extract_instagram_username(it)
        if not username:
            continue
        sc, meta = score_profile(politico_name, it)
        if sc > best_score:
            best = it
            best_score = sc
            best_meta = meta
    if best_score < 0:
        best_score = 0
    return best, best_score, best_meta


class Apify:
    def __init__(self, token: str) -> None:
        self.token = token
        self.http = httpx.Client(timeout=httpx.Timeout(310.0, connect=30.0))

    def close(self) -> None:
        self.http.close()

    def run_sync_items(
        self,
        actor_id: str,
        actor_input: Dict[str, Any],
        *,
        limit: int,
        timeout_s: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items"
        r = self.http.post(
            url,
            params={"token": self.token, "format": "json", "limit": str(limit)},
            json=actor_input,
            timeout=timeout_s,
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
        return []


def pick_int(d: Dict[str, Any], *keys: str, default: int = 0) -> int:
    for k in keys:
        v = d.get(k)
        if isinstance(v, bool):
            continue
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    return default


def pick_str(d: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def normalize_post(row: Dict[str, Any]) -> Dict[str, Any]:
    likes = pick_int(row, "likesCount", "likes", "likes_count", default=0)
    comments = pick_int(row, "commentsCount", "comments", "comments_count", default=0)
    engagement = likes + comments

    post_url = pick_str(row, "url", "postUrl", "post_url")
    shortcode = pick_str(row, "shortCode", "shortcode", "postShortcode", "code")
    caption = pick_str(row, "caption", "text", "description")
    media_type = pick_str(row, "type", "mediaType", "media_type")
    thumb = pick_str(row, "displayUrl", "thumbnailUrl", "thumbnail_url", "imageUrl", "image_url")
    posted_at = pick_str(row, "timestamp", "createdAt", "postedAt", "date")
    if posted_at:
        posted_at = posted_at

    return {
        "post_id": shortcode or None,
        "post_url": post_url or None,
        "conteudo": caption or None,
        "likes": likes,
        "comments": comments,
        "shares": 0,
        "views": 0,
        "engagement_score": float(engagement),
        "media_type": media_type or None,
        "media_url": thumb or None,
        "posted_at": posted_at or None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Se setado, grava no Supabase.")
    parser.add_argument("--sleep", type=float, default=1.0, help="Delay entre políticos (segundos).")
    parser.add_argument("--min-score", type=int, default=80, help="Score mínimo para aceitar perfil do Instagram.")
    parser.add_argument("--ig-search-limit", type=int, default=15, help="Máximo de candidatos do Instagram Search.")
    parser.add_argument("--ig-posts-fetch", type=int, default=30, help="Quantos posts buscar do perfil para rankear.")
    parser.add_argument("--top-posts", type=int, default=3, help="Quantos posts mais engajados gravar.")
    parser.add_argument("--posts-timeout", type=float, default=70.0, help="Timeout (s) para scraping de posts por perfil.")
    parser.add_argument(
        "--only-ids",
        type=str,
        default=None,
        help="Opcional: processa apenas esses IDs (ex: 15,16,86).",
    )
    parser.add_argument(
        "--skip-posts-if-exists",
        action="store_true",
        help="Se já existir >= top-posts em social_media_posts, não refaz a coleta.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    ensure_env_loaded(project_root)

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    apify_token = os.getenv("APIFY_TOKEN")
    if not supabase_url or not supabase_key:
        raise SystemExit("Faltam SUPABASE_URL/SUPABASE_KEY no ambiente.")
    if not apify_token:
        raise SystemExit("Falta APIFY_TOKEN no ambiente.")

    IG_SEARCH_ACTOR = os.getenv("APIFY_INSTAGRAM_SEARCH_ACTOR_ID", "DrF9mzPPEuVizVF4l")
    IG_SCRAPER_ACTOR = os.getenv("APIFY_INSTAGRAM_SCRAPER_ACTOR_ID", "shu8hvrXbJbY3Eb9W")

    supabase = create_client(supabase_url, supabase_key)
    apify = Apify(apify_token)

    audit_dir = project_root / "logs"
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_path = audit_dir / f"diretoriaja_sync_{datetime.now().strftime('%Y%m%d')}.jsonl"

    # pega os políticos alvo
    politicos = (
        supabase.table("politico")
        .select("id,uuid,name,image,instagram_username,twitter_username,active,usar_diretoriaja")
        .eq("usar_diretoriaja", True)
        .order("id")
        .execute()
        .data
        or []
    )
    if args.only_ids:
        wanted = {int(x.strip()) for x in args.only_ids.split(",") if x.strip().isdigit()}
        politicos = [p for p in politicos if int(p.get("id") or 0) in wanted]

    updated_politicos = 0
    upserted_posts = 0

    try:
        with httpx.Client(timeout=httpx.Timeout(20.0, connect=10.0), headers={"User-Agent": "Mozilla/5.0"}) as h:
            for p in politicos:
                pid = int(p["id"])
                puuid = p.get("uuid")
                name = (p.get("name") or "").strip()
                if not name:
                    continue

                missing_ig = looks_empty(p.get("instagram_username"))
                missing_tw = looks_empty(p.get("twitter_username"))
                missing_img = looks_empty(p.get("image"))

                best_item: Optional[Dict[str, Any]] = None
                best_score = 0
                best_meta: Dict[str, Any] = {}

                # fallback 0: se existir outro político com mesmo nome e redes preenchidas, reaproveita
                if missing_ig or missing_tw:
                    try:
                        sibling = (
                            supabase.table("politico")
                            .select("id,instagram_username,twitter_username")
                            .ilike("name", name)
                            .neq("id", pid)
                            .limit(1)
                            .execute()
                            .data
                        )
                        if sibling:
                            sib = sibling[0]
                            if missing_ig and not looks_empty(sib.get("instagram_username")):
                                p["instagram_username"] = sib.get("instagram_username")
                                missing_ig = False
                            if missing_tw and not looks_empty(sib.get("twitter_username")):
                                p["twitter_username"] = sib.get("twitter_username")
                                missing_tw = False
                    except Exception:
                        pass

                # se faltar algo, tenta achar o perfil via IG Search
                if missing_ig or missing_tw or missing_img:
                    search_input = {
                        "search": normalize_name(name),
                        "searchType": "user",
                        "searchLimit": int(args.ig_search_limit),
                    }
                    ig_candidates = apify.run_sync_items(
                        IG_SEARCH_ACTOR,
                        search_input,
                        limit=int(args.ig_search_limit),
                        timeout_s=90.0,
                    )
                    best_item, best_score, best_meta = pick_best_ig(name, ig_candidates)

                chosen_ig = p.get("instagram_username")
                chosen_tw = p.get("twitter_username")
                chosen_img = p.get("image")

                if best_item and best_score >= int(args.min_score):
                    if missing_ig:
                        chosen_ig = extract_instagram_username(best_item) or chosen_ig
                    if missing_img:
                        pic = extract_profile_pic(best_item)
                        if pic:
                            # Upload da foto de perfil para o Supabase Storage
                            ig_username = extract_instagram_username(best_item) or f"politico_{pid}"
                            chosen_img = upload_image_to_storage(
                                supabase=supabase,
                                image_url=pic,
                                folder="politicos",
                                filename=f"{pid}_{ig_username}",
                                http_client=h,
                            )
                    if missing_tw:
                        inferred = infer_twitter_from_instagram_item(h, best_item)
                        if inferred:
                            chosen_tw = inferred

                # update politico (somente o que estiver faltando)
                payload: Dict[str, Any] = {}
                if missing_ig and chosen_ig and not looks_empty(chosen_ig):
                    payload["instagram_username"] = chosen_ig
                if missing_tw and chosen_tw and not looks_empty(chosen_tw):
                    payload["twitter_username"] = chosen_tw
                if missing_img and chosen_img and not looks_empty(chosen_img):
                    payload["image"] = chosen_img

                did_update = False
                if payload and args.apply:
                    supabase.table("politico").update(payload).eq("id", pid).execute()
                    did_update = True
                    updated_politicos += 1

                # posts: exige instagram_username
                ig_user = (chosen_ig or p.get("instagram_username") or "").strip()
                top_rows: List[Dict[str, Any]] = []
                if ig_user:
                    if args.skip_posts_if_exists:
                        try:
                            existing = (
                                supabase.table("social_media_posts")
                                .select("id", count="exact")
                                .eq("politico_id", puuid)
                                .eq("plataforma", "instagram")
                                .execute()
                            )
                            total_existing = int(getattr(existing, "count", 0) or 0)
                            if total_existing >= int(args.top_posts):
                                top_rows = []
                                # ainda assim registra no audit, mas pula scraping
                                audit_row = {
                                    "ts": datetime.now(timezone.utc).isoformat(),
                                    "apply": bool(args.apply),
                                    "politico_id": pid,
                                    "politico_uuid": puuid,
                                    "politico_name": name,
                                    "missing": {"instagram": missing_ig, "twitter": missing_tw, "image": missing_img},
                                    "ig_search": {"score": best_score, "meta": best_meta},
                                    "chosen": {"instagram_username": chosen_ig, "twitter_username": chosen_tw, "image": chosen_img},
                                    "did_update_politico": did_update,
                                    "top_posts_count": 0,
                                    "top_posts": [],
                                    "posts_skipped": True,
                                    "posts_existing": total_existing,
                                }
                                with audit_path.open("a", encoding="utf-8") as f:
                                    f.write(json.dumps(audit_row, ensure_ascii=False) + "\n")
                                time.sleep(float(args.sleep))
                                continue
                        except Exception:
                            pass

                        # Se não tem posts suficientes, mas outro político com o mesmo instagram_username já tem,
                        # copia os TOP posts (evita custo/timeout no Apify).
                        try:
                            siblings = (
                                supabase.table("politico")
                                .select("uuid")
                                .eq("instagram_username", ig_user)
                                .neq("uuid", puuid)
                                .limit(5)
                                .execute()
                                .data
                                or []
                            )
                            for s in siblings:
                                ouuid = s.get("uuid")
                                if not ouuid:
                                    continue
                                src_posts = (
                                    supabase.table("social_media_posts")
                                    .select("*")
                                    .eq("politico_id", ouuid)
                                    .eq("plataforma", "instagram")
                                    .order("engagement_score", desc=True)
                                    .limit(int(args.top_posts))
                                    .execute()
                                    .data
                                    or []
                                )
                                if len(src_posts) >= int(args.top_posts):
                                    if args.apply:
                                        for sp in src_posts:
                                            sp2 = dict(sp)
                                            sp2.pop("id", None)  # deixa o UUID ser gerado
                                            sp2["politico_id"] = puuid
                                            md = sp2.get("metadata") if isinstance(sp2.get("metadata"), dict) else {}
                                            md = dict(md)
                                            md["copied_from_politico_uuid"] = ouuid
                                            sp2["metadata"] = md
                                            supabase.table("social_media_posts").upsert(
                                                sp2, on_conflict="politico_id,plataforma,post_id"
                                            ).execute()
                                            upserted_posts += 1
                                    audit_row = {
                                        "ts": datetime.now(timezone.utc).isoformat(),
                                        "apply": bool(args.apply),
                                        "politico_id": pid,
                                        "politico_uuid": puuid,
                                        "politico_name": name,
                                        "missing": {"instagram": missing_ig, "twitter": missing_tw, "image": missing_img},
                                        "ig_search": {"score": best_score, "meta": best_meta},
                                        "chosen": {"instagram_username": chosen_ig, "twitter_username": chosen_tw, "image": chosen_img},
                                        "did_update_politico": did_update,
                                        "top_posts_count": len(src_posts),
                                        "top_posts": [{"post_id": r.get("post_id"), "engagement_score": r.get("engagement_score")} for r in src_posts],
                                        "posts_copied": True,
                                        "posts_copied_from": ouuid,
                                    }
                                    with audit_path.open("a", encoding="utf-8") as f:
                                        f.write(json.dumps(audit_row, ensure_ascii=False) + "\n")
                                    time.sleep(float(args.sleep))
                                    # pula scraping
                                    raise StopIteration
                        except StopIteration:
                            continue
                        except Exception:
                            pass

                    profile_url = f"https://www.instagram.com/{ig_user}"
                    posts_input = {
                        "directUrls": [profile_url],
                        "resultsType": "posts",
                        "resultsLimit": int(args.ig_posts_fetch),
                        "addParentData": True,
                    }
                    try:
                        raw_posts = apify.run_sync_items(
                            IG_SCRAPER_ACTOR,
                            posts_input,
                            limit=int(args.ig_posts_fetch),
                            timeout_s=float(args.posts_timeout),
                        )
                    except httpx.TimeoutException:
                        raw_posts = []
                    norm = [normalize_post(r) for r in raw_posts]
                    norm = [r for r in norm if r.get("post_id") and (r.get("post_url") or r.get("conteudo"))]
                    norm.sort(key=lambda r: float(r.get("engagement_score") or 0), reverse=True)
                    top_rows = norm[: max(1, int(args.top_posts))]

                    if args.apply and top_rows:
                        for r in top_rows:
                            # Upload do thumbnail para o Supabase Storage
                            media_url_original = r.get("media_url")
                            media_url = media_url_original
                            if media_url_original:
                                post_id = r.get("post_id", "")
                                media_url = upload_image_to_storage(
                                    supabase=supabase,
                                    image_url=media_url_original,
                                    folder="instagram",
                                    filename=f"post_{post_id}" if post_id else f"post_{ig_user}",
                                    http_client=h,
                                )
                            
                            row = {
                                "politico_id": puuid,
                                "plataforma": "instagram",
                                "post_id": r["post_id"],
                                "post_url": r.get("post_url"),
                                "conteudo": r.get("conteudo"),
                                "likes": int(r.get("likes") or 0),
                                "comments": int(r.get("comments") or 0),
                                "shares": 0,
                                "views": 0,
                                "engagement_score": float(r.get("engagement_score") or 0),
                                "media_type": r.get("media_type"),
                                "media_url": media_url,
                                "posted_at": r.get("posted_at"),
                                "metadata": {
                                    "source": "apify",
                                    "actor_id": IG_SCRAPER_ACTOR,
                                    "instagram_username": ig_user,
                                },
                            }
                            supabase.table("social_media_posts").upsert(
                                row, on_conflict="politico_id,plataforma,post_id"
                            ).execute()
                            upserted_posts += 1

                audit_row = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "apply": bool(args.apply),
                    "politico_id": pid,
                    "politico_uuid": puuid,
                    "politico_name": name,
                    "missing": {"instagram": missing_ig, "twitter": missing_tw, "image": missing_img},
                    "ig_search": {"score": best_score, "meta": best_meta},
                    "chosen": {"instagram_username": chosen_ig, "twitter_username": chosen_tw, "image": chosen_img},
                    "did_update_politico": did_update,
                    "top_posts_count": len(top_rows),
                    "top_posts": [{"post_id": r.get("post_id"), "engagement_score": r.get("engagement_score")} for r in top_rows],
                }
                with audit_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(audit_row, ensure_ascii=False) + "\n")

                time.sleep(float(args.sleep))

    finally:
        apify.close()

    print(
        json.dumps(
            {
                "apply": bool(args.apply),
                "politicos_total": len(politicos),
                "politicos_updated": updated_politicos,
                "social_media_posts_upserted": upserted_posts,
                "audit_file": str(audit_path),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

