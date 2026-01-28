"""
Preenche `public.politico.instagram_username` e/ou `public.politico.twitter_username`
no Supabase usando Actors do Apify (busca por nome).

Loop 1 por 1:
- Sempre pega o próximo político com pelo menos um campo vazio (NULL ou string vazia)
- Tenta encontrar candidatos via Apify
- Só grava se o score de confiança atingir um limiar
- Audit log em `logs/apify_social_search_YYYYMMDD.jsonl`

Requer variáveis de ambiente (no .env ou exportadas):
- SUPABASE_URL
- SUPABASE_KEY
- APIFY_TOKEN

Opcional (defaults razoáveis):
- APIFY_BASE_URL=https://api.apify.com
- APIFY_INSTAGRAM_ACTOR_ID=DrF9mzPPEuVizVF4l   (apify/instagram-search-scraper)
- APIFY_TWITTER_ACTOR_ID=V38PZzpEgOfeeWvZY     (apidojo/twitter-user-scraper)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
from supabase import create_client

try:
    # opcional, mas recomendado (já está no requirements do projeto)
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


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
UF_RE = re.compile(r"\b([A-Z]{2})\b")


def ensure_env_loaded(project_root: Path) -> None:
    if load_dotenv is not None:
        load_dotenv(project_root / ".env", override=False)


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


def first_non_empty(*vals: Optional[str]) -> str:
    for v in vals:
        if v is None:
            continue
        vv = str(v).strip()
        if vv:
            return vv
    return ""


def looks_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def coerce_username(v: Any) -> str:
    s = (v or "")
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    # remove @ e prefixos comuns
    s = s.lstrip("@")
    s = re.sub(r"^https?://(www\.)?(instagram\.com|x\.com|twitter\.com)/", "", s, flags=re.I)
    s = s.split("?")[0].strip("/")
    # username/handle costuma ser a primeira parte do path
    if "/" in s:
        s = s.split("/")[0]
    return s


def extract_instagram_username(item: Dict[str, Any]) -> str:
    # tentativa: campos comuns
    return coerce_username(
        first_non_empty(
            item.get("username"),
            item.get("userName"),
            item.get("handle"),
            item.get("user", {}).get("username") if isinstance(item.get("user"), dict) else None,
            item.get("profile", {}).get("username") if isinstance(item.get("profile"), dict) else None,
            item.get("url"),
            item.get("profileUrl"),
        )
    )


def extract_twitter_username(item: Dict[str, Any]) -> str:
    return coerce_username(
        first_non_empty(
            item.get("username"),
            item.get("screenName"),
            item.get("handle"),
            item.get("userName"),
            item.get("url"),
            item.get("profileUrl"),
        )
    )


def _iter_external_urls(instagram_item: Dict[str, Any]) -> Iterable[str]:
    """
    Extrai URLs externas de um item do Instagram Search Scraper.
    Campos observados: externalUrl (string) e externalUrls (lista com dicts contendo 'url').
    """
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


def _extract_twitter_from_html(html: str) -> Optional[str]:
    m = re.search(r"https?://(?:www\.)?(?:twitter\.com|x\.com)/([A-Za-z0-9_]{1,30})", html)
    if not m:
        return None
    return m.group(1)


def try_infer_twitter_from_instagram_links(http: httpx.Client, instagram_item: Dict[str, Any]) -> Optional[str]:
    """
    Workaround: muitos perfis oficiais colocam Linktree/links externos no Instagram.
    Se a coleta via Actor do Twitter não for possível (plano/atores pagos), tentamos inferir
    o Twitter/X a partir dessas URLs externas.
    """
    # allowlist simples para evitar seguir qualquer URL arbitrária
    allow_hosts = ("linktr.ee", "www.linktr.ee", "l.instagram.com")

    for u in _iter_external_urls(instagram_item):
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
            handle = _extract_twitter_from_html(r.text or "")
            if handle:
                return handle
        except Exception:
            continue
    return None


def guess_verified(item: Dict[str, Any]) -> bool:
    for key in (
        "isVerified",
        "verified",
        "is_blue_verified",
        "isBlueVerified",
        "blueVerified",
        "is_business",
        "isBusiness",
    ):
        v = item.get(key)
        if isinstance(v, bool):
            return v
    # alguns outputs trazem string "true"/"false"
    for key in ("verified", "isVerified"):
        v = item.get(key)
        if isinstance(v, str) and v.strip().lower() in ("true", "false"):
            return v.strip().lower() == "true"
    return False


def score_candidate(
    politico_name: str,
    item: Dict[str, Any],
    *,
    platform: str,
    cidade: Optional[str] = None,
    estado: Optional[str] = None,
    funcao: Optional[str] = None,
) -> Tuple[int, Dict[str, Any]]:
    """
    Score 0..100 baseado em:
    - similaridade entre nome do político e nome exibido do candidato
    - bônus para verificado
    - hints em bio/descrição (política / brasil)
    - hints de UF/cidade (fraco, só desempate)
    """
    pn_tokens = token_set(politico_name)

    display_name = first_non_empty(
        item.get("fullName"),
        item.get("full_name"),
        item.get("name"),
        item.get("displayName"),
        item.get("display_name"),
    )
    dn_tokens = token_set(display_name)

    bio = first_non_empty(
        item.get("bio"),
        item.get("biography"),
        item.get("description"),
        item.get("profileBio"),
    )

    verified = guess_verified(item)

    sim = jaccard(pn_tokens, dn_tokens)  # 0..1
    score = int(round(sim * 70))

    if verified:
        score += 15

    if POL_HINT_RE.search(bio):
        score += 10
    if BR_HINT_RE.search(bio):
        score += 5

    # hints fracos com cidade/estado/funcao
    if estado:
        if re.search(rf"\\b{re.escape(estado.upper())}\\b", bio):
            score += 3
        else:
            # às vezes vem como "SP - Brasil"
            if UF_RE.search(bio) and estado.upper() in UF_RE.findall(bio):
                score += 2
    if cidade and cidade.strip():
        if re.search(rf"\\b{re.escape(cidade.strip())}\\b", bio, flags=re.I):
            score += 2
    if funcao and funcao.strip():
        if re.search(rf"\\b{re.escape(funcao.strip())}\\b", bio, flags=re.I):
            score += 2

    score = max(0, min(100, score))
    meta = {
        "platform": platform,
        "display_name": display_name,
        "verified": verified,
        "sim_jaccard": sim,
        "bio_hint_politica": bool(POL_HINT_RE.search(bio)),
        "bio_hint_br": bool(BR_HINT_RE.search(bio)),
        "score": score,
    }
    return score, meta


@dataclass(frozen=True)
class PickResult:
    username: Optional[str]
    score: int
    meta: Dict[str, Any]
    raw_item: Optional[Dict[str, Any]] = None


def pick_best(
    politico: Dict[str, Any],
    items: List[Dict[str, Any]],
    *,
    platform: str,
) -> PickResult:
    best_user: Optional[str] = None
    best_meta: Dict[str, Any] = {}
    best_score = -1
    best_item: Optional[Dict[str, Any]] = None

    for it in items:
        sc, meta = score_candidate(
            politico.get("name") or "",
            it,
            platform=platform,
            cidade=politico.get("cidade"),
            estado=politico.get("estado"),
            funcao=politico.get("funcao"),
        )

        if platform == "instagram":
            u = extract_instagram_username(it)
        else:
            u = extract_twitter_username(it)

        if not u:
            continue

        if sc > best_score:
            best_score = sc
            best_user = u
            best_item = it if isinstance(it, dict) else None
            best_meta = {"candidate_meta": meta, "raw_item_keys": list(it.keys())[:40]}

    if best_score < 0:
        best_score = 0
    raw_subset: Optional[Dict[str, Any]] = None
    if best_item and platform == "instagram":
        raw_subset = {
            k: best_item.get(k)
            for k in (
                "username",
                "fullName",
                "verified",
                "biography",
                "url",
                "externalUrl",
                "externalUrls",
            )
            if k in best_item
        }
    return PickResult(username=best_user, score=best_score, meta=best_meta, raw_item=raw_subset)


class ApifyClient:
    def __init__(self, token: str, base_url: str) -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")
        # O endpoint sync pode levar até ~300s; deixe folga para evitar timeouts prematuros.
        self._http = httpx.Client(timeout=httpx.Timeout(310.0, connect=30.0))

    def close(self) -> None:
        self._http.close()

    def run_sync_get_items(self, actor_id: str, actor_input: Dict[str, Any], *, limit: int = 20) -> List[Dict[str, Any]]:
        url = f"{self._base_url}/v2/acts/{actor_id}/run-sync-get-dataset-items"
        try:
            r = self._http.post(
                url,
                params={"token": self._token, "format": "json", "limit": str(limit)},
                json=actor_input,
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
        # alguns endpoints retornam {"items":[...]}
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
        return []


def fetch_next_politico(
    supabase: Any,
    *,
    include_inactive: bool,
) -> Optional[Dict[str, Any]]:
    q = (
        supabase.table("politico")
        .select("id,name,cidade,estado,funcao,instagram_username,twitter_username,active")
        .or_("instagram_username.is.null,instagram_username.eq.,twitter_username.is.null,twitter_username.eq.")
        # Evita placeholder comum (id=0). O Apify sempre pesquisa por nome; o ID é só para update.
        .neq("id", 0)
        .order("id")
        .limit(1)
    )
    if not include_inactive:
        q = q.eq("active", True)
    res = q.execute()
    rows = getattr(res, "data", None) or []
    return rows[0] if rows else None


def refresh_politico(supabase: Any, politico_id: int) -> Optional[Dict[str, Any]]:
    res = supabase.table("politico").select("id,instagram_username,twitter_username").eq("id", politico_id).single().execute()
    return getattr(res, "data", None)


def update_socials_if_missing(
    supabase: Any,
    politico_id: int,
    *,
    instagram_username: Optional[str],
    twitter_username: Optional[str],
) -> bool:
    current = refresh_politico(supabase, politico_id)
    if not current:
        return False

    payload: Dict[str, Any] = {}
    if instagram_username and looks_empty(current.get("instagram_username")):
        payload["instagram_username"] = instagram_username
    if twitter_username and looks_empty(current.get("twitter_username")):
        payload["twitter_username"] = twitter_username

    if not payload:
        return False

    supabase.table("politico").update(payload).eq("id", politico_id).execute()
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="Quantos políticos processar por execução.")
    parser.add_argument("--apply", action="store_true", help="Se setado, grava no Supabase.")
    parser.add_argument("--min-score", type=int, default=85, help="Score mínimo para gravar.")
    parser.add_argument("--sleep", type=float, default=2.0, help="Delay entre iterações (segundos).")
    parser.add_argument("--apify-items-limit", type=int, default=10, help="Quantos candidatos pedir por plataforma (máx).")
    parser.add_argument("--only-missing-instagram", action="store_true", help="Só tenta preencher Instagram.")
    parser.add_argument("--only-missing-twitter", action="store_true", help="Só tenta preencher Twitter.")
    parser.add_argument("--include-inactive", action="store_true", help="Inclui políticos inativos.")
    args = parser.parse_args()

    if args.only_missing_instagram and args.only_missing_twitter:
        raise SystemExit("Use no máximo um: --only-missing-instagram ou --only-missing-twitter.")

    project_root = Path(__file__).resolve().parents[1]
    ensure_env_loaded(project_root)

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    apify_token = os.getenv("APIFY_TOKEN")
    apify_base_url = os.getenv("APIFY_BASE_URL", "https://api.apify.com")
    ig_actor = os.getenv("APIFY_INSTAGRAM_ACTOR_ID", "DrF9mzPPEuVizVF4l")
    tw_actor = os.getenv("APIFY_TWITTER_ACTOR_ID", "V38PZzpEgOfeeWvZY")

    if not supabase_url or not supabase_key:
        raise SystemExit("Faltam SUPABASE_URL/SUPABASE_KEY no ambiente.")
    if not apify_token:
        raise SystemExit("Falta APIFY_TOKEN no ambiente.")

    supabase = create_client(supabase_url, supabase_key)
    apify = ApifyClient(apify_token, apify_base_url)

    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    audit_path = logs_dir / f"apify_social_search_{datetime.now().strftime('%Y%m%d')}.jsonl"

    processed = 0
    updated = 0
    started = datetime.now(timezone.utc).isoformat()

    try:
        while processed < args.limit:
            politico = fetch_next_politico(supabase, include_inactive=bool(args.include_inactive))
            if not politico:
                break

            pid = int(politico["id"])
            name = (politico.get("name") or "").strip()
            if not name:
                # evita loop infinito em registro inválido
                processed += 1
                time.sleep(args.sleep)
                continue

            missing_ig = looks_empty(politico.get("instagram_username"))
            missing_tw = looks_empty(politico.get("twitter_username"))

            if args.only_missing_instagram:
                missing_tw = False
            if args.only_missing_twitter:
                missing_ig = False

            chosen_ig: Optional[str] = None
            chosen_tw: Optional[str] = None
            pick_meta: Dict[str, Any] = {}

            if missing_ig:
                ig_input = {
                    "search": normalize_name(name),
                    "searchType": "user",
                    "searchLimit": int(args.apify_items_limit),
                }
                try:
                    ig_items = apify.run_sync_get_items(ig_actor, ig_input, limit=int(args.apify_items_limit))
                except Exception as e:
                    ig_items = []
                    pick_meta["instagram_error"] = str(e)
                ig_pick = pick_best(politico, ig_items, platform="instagram")
                chosen_ig = ig_pick.username
                pick_meta["instagram"] = {
                    "score": ig_pick.score,
                    "meta": ig_pick.meta,
                    "items": len(ig_items),
                    "raw": ig_pick.raw_item,
                }

            if missing_tw:
                try:
                    tw_input = {
                        "searchTerms": [normalize_name(name)],
                        "maxItems": int(args.apify_items_limit),
                        "getFollowers": False,
                        "getFollowing": False,
                        "getRetweeters": False,
                    }
                    tw_items = apify.run_sync_get_items(tw_actor, tw_input, limit=int(args.apify_items_limit))
                    tw_pick = pick_best(politico, tw_items, platform="twitter")
                    chosen_tw = tw_pick.username
                    pick_meta["twitter"] = {"score": tw_pick.score, "meta": tw_pick.meta, "items": len(tw_items)}
                except Exception as e:
                    pick_meta["twitter_error"] = str(e)

                # Fallback: inferir Twitter/X via links externos do Instagram (ex.: Linktree)
                if not chosen_tw:
                    raw_ig = (pick_meta.get("instagram") or {}).get("raw")
                    if isinstance(raw_ig, dict):
                        try:
                            with httpx.Client(
                                timeout=httpx.Timeout(20.0, connect=10.0),
                                headers={"User-Agent": "Mozilla/5.0"},
                            ) as h:
                                inferred = try_infer_twitter_from_instagram_links(h, raw_ig)
                            if inferred:
                                chosen_tw = inferred
                                pick_meta["twitter_inferred_from_instagram"] = True
                                pick_meta["twitter_inferred_username"] = inferred
                        except Exception as e:
                            pick_meta["twitter_infer_error"] = str(e)

            # aplica limiar
            if "instagram" in pick_meta:
                if (pick_meta["instagram"]["score"] or 0) < args.min_score:
                    chosen_ig = None
            if "twitter" in pick_meta:
                if (pick_meta["twitter"]["score"] or 0) < args.min_score:
                    chosen_tw = None

            audit_row = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "mode": "APLICOU" if args.apply else "DRY-RUN",
                "politico_id": pid,
                "politico_name": name,
                "missing_instagram": bool(missing_ig),
                "missing_twitter": bool(missing_tw),
                "chosen_instagram_username": chosen_ig,
                "chosen_twitter_username": chosen_tw,
                "min_score": int(args.min_score),
                "actors": {"instagram": ig_actor, "twitter": tw_actor},
                "details": pick_meta,
            }
            with audit_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(audit_row, ensure_ascii=False) + "\n")

            did_update = False
            if args.apply and (chosen_ig or chosen_tw):
                did_update = update_socials_if_missing(
                    supabase,
                    pid,
                    instagram_username=chosen_ig,
                    twitter_username=chosen_tw,
                )
                if did_update:
                    updated += 1

            processed += 1
            time.sleep(args.sleep)

    finally:
        apify.close()

    print(
        json.dumps(
            {
                "started_at": started,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "mode": "APLICOU" if args.apply else "DRY-RUN",
                "processed": processed,
                "updated": updated,
                "audit_file": str(audit_path),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

