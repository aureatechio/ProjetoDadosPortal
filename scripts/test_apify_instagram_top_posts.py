"""
Rotina de teste: extrair posts MAIS ENGAJADOS do Instagram via Apify e (opcionalmente) gravar no Supabase.

Usa o Actor oficial "Instagram Scraper" (apify/instagram-scraper):
- Input: directUrls=[https://www.instagram.com/<username>] + resultsType=posts + resultsLimit

Requer variáveis de ambiente:
- APIFY_TOKEN
- SUPABASE_URL / SUPABASE_KEY (apenas se --apply)

Exemplo (dry-run):
  python scripts/test_apify_instagram_top_posts.py --politico-id 1 --top 5

Aplicar (insere/atualiza em public.social_media_posts):
  python scripts/test_apify_instagram_top_posts.py --politico-id 1 --top 5 --apply
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from supabase import create_client
from openai import OpenAI

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


IG_SCRAPER_ACTOR_ID_DEFAULT = "shu8hvrXbJbY3Eb9W"  # apify/instagram-scraper


def ensure_env_loaded(project_root: Path) -> None:
    if load_dotenv is not None:
        load_dotenv(project_root / ".env", override=False)


def apify_run_sync_get_items(token: str, actor_id: str, actor_input: Dict[str, Any], *, limit: int) -> List[Dict[str, Any]]:
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items"
    with httpx.Client(timeout=httpx.Timeout(310.0, connect=30.0)) as c:
        r = c.post(url, params={"token": token, "format": "json", "limit": str(limit)}, json=actor_input)
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


def parse_datetime(s: str) -> Optional[str]:
    # Mantém ISO se vier pronto; se não, tenta converter epoch-like
    if not s:
        return None
    try:
        # já é ISO?
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return s
    except Exception:
        return None


def normalize_post(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza um item do Instagram Scraper para o formato da tabela public.instagram_posts.
    Observação: o output do actor pode variar; então usamos chaves comuns e fallback.
    """
    likes = pick_int(row, "likesCount", "likes", "likes_count", default=0)
    comments = pick_int(row, "commentsCount", "comments", "comments_count", default=0)
    engagement = likes + comments

    post_url = pick_str(row, "url", "postUrl", "post_url")
    shortcode = pick_str(row, "shortCode", "shortcode", "postShortcode", "code")
    caption = pick_str(row, "caption", "text", "description")
    media_type = pick_str(row, "type", "mediaType", "media_type")
    thumb = pick_str(row, "displayUrl", "thumbnailUrl", "thumbnail_url", "imageUrl", "image_url")
    posted_at = parse_datetime(pick_str(row, "timestamp", "createdAt", "postedAt", "date"))

    return {
        "post_shortcode": shortcode or None,
        "post_url": post_url or None,
        "caption": caption or None,
        "likes": likes,
        "comments": comments,
        "engagement_score": float(engagement),
        "media_type": media_type or None,
        "thumbnail_url": thumb or None,
        "posted_at": posted_at,
    }


def normalize_comment(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza item de comentário do Instagram Scraper (resultsType=comments).
    O schema varia; então tentamos chaves comuns e mantemos texto/autor/data/likes.
    """
    text = pick_str(row, "text", "comment", "commentText", "content")
    author = pick_str(row, "ownerUsername", "username", "author", "userName")
    likes = pick_int(row, "likesCount", "likes", default=0)
    created_at = parse_datetime(pick_str(row, "timestamp", "createdAt", "date"))
    return {"text": text, "author": author, "likes": likes, "created_at": created_at}


_STOPWORDS_PT = {
    "a","o","os","as","um","uma","uns","umas","de","do","da","dos","das","em","no","na","nos","nas","por","pra","pro",
    "com","sem","e","ou","que","é","ser","não","sim","mais","menos","muito","muita","muitos","muitas","já","tb","também",
    "vc","vcs","vocês","voce","eu","tu","ele","ela","eles","elas","me","te","se","lhe","isso","isto","aquilo","aqui","ai",
    "lá","pra","para","porque","pq","quando","onde","como","sobre","até","bem","só","vai","vou","foi","tem","tá","ta",
}

_POS_PT = {"parabéns","parabens","apoio","boa","bom","ótimo","otimo","excelente","show","top","linda","lindo","orgulho","mito","Deus","abençoa","abencoe","merece"}
_NEG_PT = {"vergonha","lixo","ridículo","ridiculo","mentira","mentiroso","corrupto","corrupta","roubo","roubou","bandido","bandidos","genocida","odiado","ódio","odio","péssimo","pessimo"}


def summarize_comments_pt(comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Resumo simples (heurístico) do sentimento + temas (palavras mais frequentes).
    Não usa IA. Serve como “sinal” para teste.
    """
    words: Dict[str, int] = {}
    pos = 0
    neg = 0
    total = 0

    for c in comments:
        text = (c.get("text") or "").strip()
        if not text:
            continue
        total += 1
        low = text.lower()
        if any(w.lower() in low for w in _POS_PT):
            pos += 1
        if any(w.lower() in low for w in _NEG_PT):
            neg += 1

        for tok in re.findall(r"[a-zA-ZÀ-ÿ]{2,}", low):
            if tok in _STOPWORDS_PT:
                continue
            words[tok] = words.get(tok, 0) + 1

    top_terms = sorted(words.items(), key=lambda kv: kv[1], reverse=True)[:12]
    return {
        "comments_total_used": total,
        "sentiment": {"positive_hits": pos, "negative_hits": neg},
        "top_terms": [{"term": t, "count": n} for t, n in top_terms],
    }

def summarize_comments_ai(
    *,
    api_key: str,
    model: str,
    post_caption: str,
    comments: List[Dict[str, Any]],
) -> str:
    """
    Gera um resumo em português do que as pessoas mais estão falando nos comentários.
    Retorna texto curto (para salvar em coluna text).
    """
    # reduz custo: manda só os comentários com mais likes + pequenos trechos
    lines: List[str] = []
    for c in comments[:25]:
        txt = (c.get("text") or "").strip().replace("\n", " ")
        if not txt:
            continue
        likes = int(c.get("likes") or 0)
        author = (c.get("author") or "").strip()
        if author:
            lines.append(f"- ({likes} likes) {author}: {txt[:240]}")
        else:
            lines.append(f"- ({likes} likes): {txt[:240]}")

    prompt = (
        "Você é um analista de opinião pública.\n"
        "Tarefa: resumir, em português do Brasil, o que as pessoas mais estão falando nos comentários.\n"
        "Regras:\n"
        "- Não invente fatos.\n"
        "- Seja conciso (3 a 6 frases).\n"
        "- Se houver polarização, cite de forma neutra.\n"
        "- Não inclua dados pessoais.\n\n"
        f"Legenda do post (contexto):\n{post_caption[:800]}\n\n"
        "Comentários (amostra, ordenada por likes):\n"
        + "\n".join(lines[:60])
    )

    client = OpenAI(api_key=api_key)
    resp = client.responses.create(
        model=model,
        instructions="Responda somente com o resumo final (texto).",
        input=prompt,
    )
    return (resp.output_text or "").strip()


def to_social_media_post(politico_id: int, p: Dict[str, Any], *, actor_id: str) -> Dict[str, Any]:
    return {
        "politico_id": politico_id,
        "plataforma": "instagram",
        "post_id": p.get("post_shortcode"),
        "post_url": p.get("post_url"),
        "conteudo": p.get("caption"),
        "likes": int(p.get("likes") or 0),
        "comments": int(p.get("comments") or 0),
        "shares": 0,
        "views": 0,
        "engagement_score": float(p.get("engagement_score") or 0),
        "media_type": p.get("media_type"),
        "media_url": p.get("thumbnail_url"),
        "posted_at": p.get("posted_at"),
        "comment_summary_ai": None,
        "metadata": {
            "source": "apify",
            "actor_id": actor_id,
            "post_shortcode": p.get("post_shortcode"),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--politico-id", type=int, required=True)
    parser.add_argument(
        "--instagram-username",
        type=str,
        default=None,
        help="Se fornecido, não consulta o Supabase para descobrir o username.",
    )
    parser.add_argument(
        "--politico-name",
        type=str,
        default=None,
        help="Opcional (apenas para melhorar o output quando --instagram-username é usado).",
    )
    parser.add_argument("--top", type=int, default=5, help="Quantos posts mais engajados retornar.")
    parser.add_argument("--perfis-limit", type=int, default=20, help="Quantos posts puxar do perfil para ranquear.")
    parser.add_argument("--apply", action="store_true", help="Se setado, grava no Supabase.")
    parser.add_argument("--actor-id", type=str, default=IG_SCRAPER_ACTOR_ID_DEFAULT)
    parser.add_argument(
        "--dest-table",
        type=str,
        default="social_media_posts",
        choices=["social_media_posts", "instagram_posts"],
        help="Tabela destino para --apply.",
    )
    parser.add_argument("--with-comments", action="store_true", help="Busca comentários dos Top posts (via Apify) e gera resumo.")
    parser.add_argument("--comments-limit", type=int, default=30, help="Máximo de comentários por post (o actor limita).")
    parser.add_argument("--with-ai-summary", action="store_true", help="Gera resumo por IA dos comentários e salva no Supabase.")
    parser.add_argument("--openai-model", type=str, default=os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    ensure_env_loaded(project_root)

    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        raise SystemExit("Falta APIFY_TOKEN no ambiente.")

    openai_key = os.getenv("OPENAI_API_KEY")
    if args.with_ai_summary and not openai_key:
        raise SystemExit("Para --with-ai-summary, defina OPENAI_API_KEY no ambiente (.env).")

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if args.apply and (not supabase_url or not supabase_key):
        raise SystemExit("Para --apply, defina SUPABASE_URL e SUPABASE_KEY.")

    supabase = create_client(supabase_url, supabase_key) if args.apply else None

    # busca username no supabase (a menos que venha por CLI)
    pol: Dict[str, Any] = {"id": args.politico_id, "name": args.politico_name, "instagram_username": args.instagram_username}
    ig_user = (args.instagram_username or "").strip()
    if not ig_user:
        if not supabase_url or not supabase_key:
            raise SystemExit("Para ler o político, defina SUPABASE_URL e SUPABASE_KEY (ou passe --instagram-username).")
        supa_ro = create_client(supabase_url, supabase_key)
        pol = supa_ro.table("politico").select("id,name,instagram_username").eq("id", args.politico_id).single().execute().data
        ig_user = (pol.get("instagram_username") or "").strip()
    if not ig_user:
        raise SystemExit(f"Político {args.politico_id} não tem instagram_username preenchido.")

    profile_url = f"https://www.instagram.com/{ig_user}"
    actor_input = {
        "directUrls": [profile_url],
        "resultsType": "posts",
        "resultsLimit": int(args.perfis_limit),
        "addParentData": True,
    }

    items = apify_run_sync_get_items(apify_token, args.actor_id, actor_input, limit=int(args.perfis_limit))
    normalized = [normalize_post(it) for it in items]
    normalized = [p for p in normalized if p.get("post_url") or p.get("post_shortcode")]
    normalized.sort(key=lambda p: float(p.get("engagement_score") or 0), reverse=True)
    top_posts = normalized[: max(1, int(args.top))]

    # opcional: comentários e resumo
    comments_by_shortcode: Dict[str, List[Dict[str, Any]]] = {}
    comment_summary_by_shortcode: Dict[str, Dict[str, Any]] = {}
    ai_summary_by_shortcode: Dict[str, str] = {}
    if args.with_comments:
        for p in top_posts:
            sc = p.get("post_shortcode")
            url = p.get("post_url")
            if not sc or not url:
                continue
            c_input = {
                "directUrls": [url],
                "resultsType": "comments",
                "resultsLimit": int(max(1, args.comments_limit)),
            }
            raw_comments = apify_run_sync_get_items(apify_token, args.actor_id, c_input, limit=int(max(1, args.comments_limit)))
            norm_comments = [normalize_comment(rc) for rc in raw_comments]
            # prioriza comentários por likes (mais relevantes)
            norm_comments.sort(key=lambda c: int(c.get("likes") or 0), reverse=True)
            norm_comments = [c for c in norm_comments if c.get("text")]
            comments_by_shortcode[sc] = norm_comments[: int(max(1, args.comments_limit))]
            comment_summary_by_shortcode[sc] = summarize_comments_pt(comments_by_shortcode[sc])
            if args.with_ai_summary and openai_key:
                ai_summary_by_shortcode[sc] = summarize_comments_ai(
                    api_key=openai_key,
                    model=args.openai_model,
                    post_caption=p.get("caption") or "",
                    comments=comments_by_shortcode.get(sc, []),
                )

    out = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "politico": {"id": pol.get("id"), "name": pol.get("name"), "instagram_username": ig_user},
        "fetched": len(items),
        "usable": len(normalized),
        "top": top_posts,
        "apply": bool(args.apply),
        "dest_table": args.dest_table,
        "with_comments": bool(args.with_comments),
    }
    if args.with_comments:
        out["comment_summaries"] = comment_summary_by_shortcode
        if args.with_ai_summary:
            out["comment_summaries_ai"] = ai_summary_by_shortcode

    if args.apply and supabase is not None:
        inserted = 0
        if args.dest_table == "instagram_posts":
            for p in top_posts:
                if not p.get("post_shortcode"):
                    continue
                row = dict(p)
                row["politico_id"] = args.politico_id
                supabase.table("instagram_posts").upsert(row, on_conflict="post_shortcode").execute()
                inserted += 1
        else:
            for p in top_posts:
                if not p.get("post_shortcode"):
                    continue
                row = to_social_media_post(args.politico_id, p, actor_id=args.actor_id)
                if args.with_comments:
                    sc = p.get("post_shortcode")
                    if sc and sc in comment_summary_by_shortcode:
                        row["metadata"] = dict(row.get("metadata") or {})
                        row["metadata"]["comment_summary"] = comment_summary_by_shortcode[sc]
                        if args.with_ai_summary and openai_key and sc in ai_summary_by_shortcode:
                            ai_text = ai_summary_by_shortcode[sc]
                            row["comment_summary_ai"] = ai_text
                            row["metadata"]["comment_summary_ai"] = ai_text
                # projeto já usa on_conflict="plataforma,post_id" para social_media_posts
                supabase.table("social_media_posts").upsert(row, on_conflict="plataforma,post_id").execute()
                inserted += 1
        out["inserted"] = inserted

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()

