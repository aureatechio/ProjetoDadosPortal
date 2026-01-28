"""
Coletor para preencher usernames de redes sociais (Instagram e X/Twitter)
na tabela `politico`, usando fontes oficiais (prioridade: Wikidata via SPARQL).
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx

from app.config import settings
from app.database import db

logger = logging.getLogger(__name__)


_RE_INVALID_CHARS = re.compile(r"\s|/|\\")
_RE_IG_URL = re.compile(r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9._]+)/?", re.IGNORECASE)
_RE_X_URL = re.compile(
    r"(?:https?://)?(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/?",
    re.IGNORECASE,
)
_RE_IG_USERNAME = re.compile(r"^[A-Za-z0-9._]{1,30}$")
_RE_X_USERNAME = re.compile(r"^[A-Za-z0-9_]{1,15}$")


@dataclass(frozen=True)
class FonteIds:
    politico_id: int
    wikidata_qid: Optional[str] = None
    camara_id: Optional[str] = None
    senado_id: Optional[str] = None


class SocialsCollector:
    """
    Preenche `instagram_username` e `twitter_username` via fontes oficiais.

    Por padrão, só tenta preencher quando existir um mapeamento (arquivo CSV) que ligue
    `politico.id` a um identificador oficial (ex: Wikidata QID).
    """

    WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"

    def __init__(self):
        self.delay = settings.delay_entre_requisicoes
        self._base_dir = Path(__file__).resolve().parents[2]
        self._mapping_path = self._base_dir / "data" / "politicos_fontes.csv"
        self._logs_dir = self._base_dir / "logs"

    # =========================
    # Public API
    # =========================

    async def executar_preenchimento(
        self,
        dry_run: bool = True,
        mapping_path: Optional[str] = None,
        log_jsonl: bool = True,
    ) -> Dict[str, Any]:
        """
        Executa o preenchimento de redes sociais para políticos ativos.

        Args:
            dry_run: se True, não grava no banco; apenas gera relatório.
            mapping_path: caminho opcional para o CSV de mapeamento.
            log_jsonl: se True, grava auditoria em `logs/social_fill_YYYYMMDD.jsonl`.
        """
        mapping_file = Path(mapping_path) if mapping_path else self._mapping_path
        fonte_ids = self._load_mapping(mapping_file)

        politicos = db.get_politicos_ativos()
        total_ativos = len(politicos)

        stats = {
            "status": "ok",
            "dry_run": dry_run,
            "politicos_ativos": total_ativos,
            "politicos_com_mapeamento": 0,
            "consultas_wikidata": 0,
            "atualizacoes_planejadas": 0,
            "atualizacoes_aplicadas": 0,
            "pulados_sem_mapeamento": 0,
            "pulados_sem_dados": 0,
            "pulados_sem_mudanca": 0,
            "erros": 0,
            "erros_detalhe": [],
            "alteracoes": [],
        }

        # Auditoria
        audit_path = None
        if log_jsonl:
            self._logs_dir.mkdir(parents=True, exist_ok=True)
            audit_path = self._logs_dir / f"social_fill_{datetime.now().strftime('%Y%m%d')}.jsonl"

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0),
            headers={
                # Wikidata exige User-Agent identificável
                "User-Agent": "ProjetoDadosPortal/1.0 (socials collector; contact: admin@local)",
                "Accept": "application/sparql-results+json",
            },
        ) as client:
            for politico in politicos:
                try:
                    pid = int(politico["id"])
                    mapping = fonte_ids.get(pid)
                    if not mapping:
                        stats["pulados_sem_mapeamento"] += 1
                        continue

                    stats["politicos_com_mapeamento"] += 1

                    candidato_ig = None
                    candidato_x = None
                    fonte = None
                    confianca = None

                    # 1) Wikidata (preferencial)
                    if mapping.wikidata_qid:
                        stats["consultas_wikidata"] += 1
                        ig, x = await self._fetch_from_wikidata(client, mapping.wikidata_qid)
                        if ig or x:
                            candidato_ig, candidato_x = ig, x
                            fonte = "wikidata"
                            confianca = "alta"

                    if not candidato_ig and not candidato_x:
                        stats["pulados_sem_dados"] += 1
                        continue

                    novo_ig = self._normalize_instagram(candidato_ig)
                    novo_x = self._normalize_x(candidato_x)

                    if novo_ig is None and novo_x is None:
                        stats["pulados_sem_dados"] += 1
                        continue

                    atual_ig = politico.get("instagram_username")
                    atual_x = politico.get("twitter_username")

                    # Regra de não sobrescrever:
                    # - preenche apenas se vazio/null, ou se o valor atual for claramente inválido.
                    aplicar_ig = self._should_update_instagram(atual_ig, novo_ig)
                    aplicar_x = self._should_update_x(atual_x, novo_x)

                    if not aplicar_ig and not aplicar_x:
                        stats["pulados_sem_mudanca"] += 1
                        continue

                    payload_update = {
                        "politico_id": pid,
                        "instagram_username": novo_ig if aplicar_ig else None,
                        "twitter_username": novo_x if aplicar_x else None,
                        "fonte": fonte,
                        "confianca": confianca,
                        "dry_run": dry_run,
                        "old": {"instagram_username": atual_ig, "twitter_username": atual_x},
                        "new": {
                            "instagram_username": novo_ig if aplicar_ig else atual_ig,
                            "twitter_username": novo_x if aplicar_x else atual_x,
                        },
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }

                    stats["atualizacoes_planejadas"] += int(bool(aplicar_ig)) + int(bool(aplicar_x))
                    stats["alteracoes"].append(payload_update)

                    if log_jsonl and audit_path:
                        self._append_jsonl(audit_path, payload_update)

                    if not dry_run:
                        updated = db.update_politico_socials(
                            politico_id=pid,
                            instagram_username=novo_ig if aplicar_ig else None,
                            twitter_username=novo_x if aplicar_x else None,
                        )
                        if updated:
                            stats["atualizacoes_aplicadas"] += int(bool(aplicar_ig)) + int(bool(aplicar_x))

                except Exception as e:
                    stats["erros"] += 1
                    msg = f"Erro ao processar politico_id={politico.get('id')}: {e}"
                    logger.exception(msg)
                    stats["erros_detalhe"].append(msg)

        return stats

    # =========================
    # Mapping
    # =========================

    def _load_mapping(self, path: Path) -> Dict[int, FonteIds]:
        if not path.exists():
            logger.warning(f"Arquivo de mapeamento não encontrado: {path}")
            return {}

        out: Dict[int, FonteIds] = {}
        with path.open("r", encoding="utf-8", newline="") as f:
            # ignora linhas comentadas
            lines = (line for line in f if not line.lstrip().startswith("#") and line.strip())
            reader = csv.DictReader(lines)
            for row in reader:
                try:
                    pid_raw = (row.get("politico_id") or "").strip()
                    if not pid_raw:
                        continue
                    pid = int(pid_raw)
                    qid = self._normalize_qid(row.get("wikidata_qid"))
                    out[pid] = FonteIds(
                        politico_id=pid,
                        wikidata_qid=qid,
                        camara_id=(row.get("camara_id") or "").strip() or None,
                        senado_id=(row.get("senado_id") or "").strip() or None,
                    )
                except Exception as e:
                    logger.warning(f"Linha inválida no mapeamento: {row} ({e})")
        return out

    def _normalize_qid(self, qid: Optional[str]) -> Optional[str]:
        if not qid:
            return None
        q = qid.strip()
        if not q:
            return None
        if not re.fullmatch(r"Q[0-9]+", q):
            return None
        return q

    # =========================
    # Wikidata
    # =========================

    async def _fetch_from_wikidata(
        self,
        client: httpx.AsyncClient,
        qid: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        # P2003 = Instagram username
        # P2002 = Twitter username
        query = f"""
        SELECT ?instagram ?twitter WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P2003 ?instagram. }}
          OPTIONAL {{ wd:{qid} wdt:P2002 ?twitter. }}
        }} LIMIT 1
        """.strip()

        resp = await client.get(
            self.WIKIDATA_SPARQL_URL,
            params={"query": query, "format": "json"},
        )
        resp.raise_for_status()
        data = resp.json()

        try:
            bindings = data["results"]["bindings"]
            if not bindings:
                return None, None
            b = bindings[0]
            ig = (b.get("instagram") or {}).get("value")
            tw = (b.get("twitter") or {}).get("value")
            return ig, tw
        except Exception:
            return None, None

    # =========================
    # Normalização + decisão de update
    # =========================

    def _normalize_instagram(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        v = value.strip()
        if not v:
            return None
        v = v.lstrip("@")
        m = _RE_IG_URL.search(v)
        if m:
            v = m.group(1)
        v = v.strip().lstrip("@")
        if _RE_INVALID_CHARS.search(v):
            return None
        if not _RE_IG_USERNAME.fullmatch(v):
            return None
        return v

    def _normalize_x(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        v = value.strip()
        if not v:
            return None
        v = v.lstrip("@")
        m = _RE_X_URL.search(v)
        if m:
            v = m.group(1)
        v = v.strip().lstrip("@")
        if _RE_INVALID_CHARS.search(v):
            return None
        if not _RE_X_USERNAME.fullmatch(v):
            return None
        return v

    def _is_valid_instagram(self, value: Optional[str]) -> bool:
        return self._normalize_instagram(value) is not None

    def _is_valid_x(self, value: Optional[str]) -> bool:
        return self._normalize_x(value) is not None

    def _should_update_instagram(self, current: Optional[str], new_value: Optional[str]) -> bool:
        if not new_value:
            return False
        if not current:
            return True
        if not self._is_valid_instagram(current):
            # valor atual é inválido (ex: veio como URL ou com espaços)
            return True
        return False

    def _should_update_x(self, current: Optional[str], new_value: Optional[str]) -> bool:
        if not new_value:
            return False
        if not current:
            return True
        if not self._is_valid_x(current):
            return True
        return False

    # =========================
    # Auditoria
    # =========================

    def _append_jsonl(self, path: Path, payload: Dict[str, Any]) -> None:
        # Sanitiza para JSON
        line = json.dumps(payload, ensure_ascii=False)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


# Instância global
socials_collector = SocialsCollector()

