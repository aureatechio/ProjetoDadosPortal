"""
Workaround para imagens que não carregam por hotlink/CDN:
Baixa a imagem atual e salva como data URI em `public.politico.image`.

Isso evita depender de Storage (caso o bucket/policies não permitam upload via script).

Requer:
- SUPABASE_URL
- SUPABASE_KEY
"""

from __future__ import annotations

import argparse
import base64
import os
from pathlib import Path

import httpx
from supabase import create_client

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


def ensure_env_loaded(project_root: Path) -> None:
    if load_dotenv is not None:
        load_dotenv(project_root / ".env", override=False)


def guess_mime(resp: httpx.Response, url: str) -> str:
    ct = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ct.startswith("image/"):
        return ct
    u = url.lower()
    if ".png" in u:
        return "image/png"
    return "image/jpeg"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ids", required=True, help="IDs inteiros (ex: 272,1657,1658)")
    parser.add_argument("--max-bytes", type=int, default=250_000, help="Limite de bytes para evitar imagens gigantes.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    ensure_env_loaded(project_root)

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise SystemExit("Faltam SUPABASE_URL/SUPABASE_KEY.")

    supabase = create_client(supabase_url, supabase_key)
    ids = [int(x.strip()) for x in args.ids.split(",") if x.strip().isdigit()]
    if not ids:
        raise SystemExit("Nenhum id válido em --ids.")

    with httpx.Client(timeout=60.0, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as http:
        for pid in ids:
            pol = supabase.table("politico").select("id,name,image").eq("id", pid).single().execute().data
            img = (pol.get("image") or "").strip()
            if not img:
                print(f"{pid}: sem image, pulando")
                continue
            if img.startswith("data:image/"):
                print(f"{pid}: já é data URI, ok")
                continue

            r = http.get(img)
            r.raise_for_status()

            if len(r.content) > int(args.max_bytes):
                print(f"{pid}: imagem muito grande ({len(r.content)} bytes), pulando")
                continue

            mime = guess_mime(r, img)
            b64 = base64.b64encode(r.content).decode("ascii")
            data_uri = f"data:{mime};base64,{b64}"

            supabase.table("politico").update({"image": data_uri}).eq("id", pid).execute()
            print(f"{pid}: atualizado image para data URI ({mime}, {len(r.content)} bytes)")


if __name__ == "__main__":
    main()

