"""
Preenche `public.politico.instagram_username` no Supabase fazendo busca direta no Instagram
via Instaloader TopSearchResults (scraping).

Regras de segurança:
- Não sobrescreve `instagram_username` se já estiver preenchido.
- Só grava quando o score de confiança atinge um limiar alto.
- Mantém modo dry-run por padrão (use --apply para gravar).

Requer variáveis de ambiente (no .env ou exportadas):
- SUPABASE_URL
- SUPABASE_KEY
- (opcional) INSTAGRAM_USERNAME / INSTAGRAM_PASSWORD para login (melhora resultados/limites)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import instaloader
from instaloader import Profile, TopSearchResults
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


def normalize_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    # remove títulos no começo
    parts = n.split()
    if parts and parts[0].strip(".").lower() in TITLE_PREFIXES:
        n = " ".join(parts[1:])
    # remove pontuação simples
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


def score_profile(politico_name: str, profile: Profile) -> Tuple[int, Dict[str, Any]]:
    """
    Score de confiança 0..100 baseado em:
    - similaridade entre nome do político e full_name do perfil
    - hints na bio
    - username parecido
    """
    pn_tokens = token_set(politico_name)
    full_name = getattr(profile, "full_name", "") or ""
    fn_tokens = token_set(full_name)

    bio = getattr(profile, "biography", "") or ""
    username = (getattr(profile, "username", "") or "").lower()

    sim = jaccard(pn_tokens, fn_tokens)  # 0..1
    score = int(round(sim * 70))

    # bônus por hints
    if POL_HINT_RE.search(bio):
        score += 15
    if BR_HINT_RE.search(bio):
        score += 5

    # bônus se username contém sobrenome/parte do nome
    pn_join = "".join(token_set(politico_name))
    if pn_join and pn_join[:6].lower() in username:
        score += 5

    score = max(0, min(100, score))
    meta = {
        "profile_username": getattr(profile, "username", None),
        "profile_full_name": full_name,
        "sim_jaccard": sim,
        "bio_hint_politica": bool(POL_HINT_RE.search(bio)),
        "bio_hint_br": bool(BR_HINT_RE.search(bio)),
        "score": score,
    }
    return score, meta


def pick_best_profile(politico_name: str, profiles: List[Profile]) -> Tuple[Optional[Profile], Optional[Dict[str, Any]]]:
    best: Optional[Profile] = None
    best_meta: Optional[Dict[str, Any]] = None
    best_score = -1

    for prof in profiles:
        sc, meta = score_profile(politico_name, prof)
        if sc > best_score:
            best, best_meta, best_score = prof, meta, sc

    if best_meta:
        best_meta["best_score"] = best_score
    return best, best_meta


def ensure_env_loaded(project_root: Path) -> None:
    if load_dotenv is not None:
        load_dotenv(project_root / ".env", override=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="Quantos políticos processar por execução.")
    parser.add_argument("--apply", action="store_true", help="Se setado, grava no Supabase.")
    parser.add_argument("--min-score", type=int, default=85, help="Score mínimo para gravar.")
    parser.add_argument("--sleep", type=float, default=2.5, help="Delay entre buscas no Instagram (segundos).")
    parser.add_argument(
        "--session-user",
        type=str,
        default=None,
        help="Username do Instagram para carregar sessão salva do Instaloader (recomendado).",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    ensure_env_loaded(project_root)

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise SystemExit("Faltam SUPABASE_URL/SUPABASE_KEY no ambiente.")

    ig_user = os.getenv("INSTAGRAM_USERNAME")
    ig_pass = os.getenv("INSTAGRAM_PASSWORD")
    session_user = args.session_user or os.getenv("INSTAGRAM_SESSION_USER")

    supabase = create_client(supabase_url, supabase_key)

    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
    )

    logged_in = False
    # Preferir sessão salva (não expõe senha)
    if session_user:
        try:
            L.load_session_from_file(session_user)
            logged_in = True
        except Exception:
            logged_in = False
    # Fallback para login via usuário/senha (se existirem)
    if not logged_in and ig_user and ig_pass:
        try:
            L.login(ig_user, ig_pass)
            logged_in = True
        except Exception:
            logged_in = False

    if not logged_in:
        raise SystemExit(
            "Instagram está retornando 401 no endpoint de busca. "
            "Para fazer 'search por nome' você precisa estar autenticado.\n"
            "Opções:\n"
            "1) Criar sessão do Instaloader e rodar com --session-user SEU_USER\n"
            "   Ex: venv/bin/instaloader --login SEU_USER (vai salvar uma sessão)\n"
            "2) Definir INSTAGRAM_USERNAME/INSTAGRAM_PASSWORD no ambiente.\n"
        )

    # Busca políticos sem instagram_username
    rows = (
        supabase.table("politico")
        .select("id,name,instagram_username,active")
        .eq("active", True)
        .or_("instagram_username.is.null,instagram_username.eq.")
        .order("id")
        .limit(args.limit)
        .execute()
        .data
    )

    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    audit_path = logs_dir / f"instagram_name_search_{datetime.now().strftime('%Y%m%d')}.jsonl"

    updated = 0
    scanned = 0

    for r in rows:
        pid = r["id"]
        name = r.get("name") or ""
        if not name or pid == 0:
            continue
        scanned += 1

        query = normalize_name(name)
        try:
            ts = TopSearchResults(L.context, query)
            # pega alguns perfis do top search
            profiles = []
            for i, p in enumerate(ts.get_profiles()):
                profiles.append(p)
                if i >= 9:
                    break

            best, meta = pick_best_profile(name, profiles)
            chosen = getattr(best, "username", None) if best else None
            score = int(meta.get("best_score", 0)) if meta else 0

            payload = {
                "politico_id": pid,
                "politico_name": name,
                "query": query,
                "chosen_username": chosen,
                "score": score,
                "apply": bool(args.apply),
                "logged_in": logged_in,
                "ts": datetime.now(timezone.utc).isoformat(),
                "meta": meta,
            }

            with audit_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")

            if chosen and score >= args.min_score:
                if args.apply:
                    # não sobrescreve se já tem
                    supabase.table("politico").update({"instagram_username": chosen}).eq("id", pid).or_(
                        "instagram_username.is.null,instagram_username.eq."
                    ).execute()
                updated += 1

        except Exception:
            # falhas/rate limit entram no audit via ausência de chosen_username (sem quebrar execução)
            pass

        time.sleep(args.sleep)

    mode = "APLICOU" if args.apply else "DRY-RUN"
    print(
        json.dumps(
            {
                "mode": mode,
                "logged_in": logged_in,
                "scanned": scanned,
                "candidates_above_threshold": updated,
                "audit_file": str(audit_path),
                "min_score": args.min_score,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

