"""
Corrige `public.politico.image` quando está apontando para CDN do Instagram (links instáveis).

Para cada político alvo:
- baixa a imagem atual (URL)
- faz upload no bucket público `thumbnail` do Supabase (com upsert)
- atualiza `politico.image` para o public URL do Supabase Storage

Requer:
- SUPABASE_URL
- SUPABASE_KEY
"""

from __future__ import annotations

import argparse
import os
import tempfile
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


def guess_content_type(url: str, resp: httpx.Response) -> str:
    ct = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ct.startswith("image/"):
        return ct
    u = url.lower()
    if ".png" in u:
        return "image/png"
    return "image/jpeg"


def ext_from_content_type(ct: str) -> str:
    if ct.endswith("png"):
        return "png"
    if ct.endswith("webp"):
        return "webp"
    return "jpg"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ids", required=True, help="IDs inteiros (ex: 1657,1658)")
    # neste projeto, o bucket público existente é "portal"
    parser.add_argument("--bucket", default="portal")
    parser.add_argument("--prefix", default="politico_thumbs")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    ensure_env_loaded(project_root)

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise SystemExit("Faltam SUPABASE_URL/SUPABASE_KEY.")

    supabase = create_client(url, key)
    ids = [int(x.strip()) for x in args.ids.split(",") if x.strip().isdigit()]
    if not ids:
        raise SystemExit("Nenhum id válido em --ids.")

    with httpx.Client(timeout=60.0, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as http:
        for pid in ids:
            pol = supabase.table("politico").select("id,name,image,instagram_username").eq("id", pid).single().execute().data
            img = (pol.get("image") or "").strip()
            ig = (pol.get("instagram_username") or "").strip()
            if not img:
                print(f"{pid}: sem image, pulando")
                continue
            # Se já estiver no storage DESTE projeto e bucket, não precisa mexer
            if f"/storage/v1/object/public/{args.bucket}/" in img:
                print(f"{pid}: já está no storage, ok")
                continue

            r = http.get(img)
            r.raise_for_status()
            ct = guess_content_type(img, r)
            ext = ext_from_content_type(ct)

            safe_ig = ig or f"politico_{pid}"
            path = f"/{args.prefix}/{pid}_{safe_ig}.{ext}"

            # upload (upsert). Esta versão do storage client espera um caminho de arquivo.
            with tempfile.NamedTemporaryFile(prefix=f"politico_{pid}_", suffix=f".{ext}", delete=True) as tmp:
                tmp.write(r.content)
                tmp.flush()
                resp = supabase.storage.from_(args.bucket).upload(
                    path,
                    tmp.name,
                    {"content-type": ct, "upsert": "true"},
                )
            _ = resp  # só para evitar lint de variável não usada

            public_url = supabase.storage.from_(args.bucket).get_public_url(path)
            supabase.table("politico").update({"image": public_url}).eq("id", pid).execute()

            print(f"{pid}: atualizado image -> {public_url}")


if __name__ == "__main__":
    main()

