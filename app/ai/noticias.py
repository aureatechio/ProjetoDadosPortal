import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from openai import OpenAI

from app.config import settings

logger = logging.getLogger(__name__)


PESO_RECENCIA = 0.25
PESO_MENCAO = 0.35
PESO_FONTE = 0.25
PESO_ENGAJAMENTO = 0.15


def _score(v: Any) -> float:
  try:
    if v is None:
      return 0.0
    return float(v)
  except Exception:
    return 0.0


def calcular_pontos(noticia: Dict[str, Any]) -> Dict[str, Any]:
  """Retorna breakdown (0-100) e contribuições ponderadas (0-100)."""
  rec = _score(noticia.get("score_recencia"))
  men = _score(noticia.get("score_mencao"))
  fon = _score(noticia.get("score_fonte"))
  eng = _score(noticia.get("score_engajamento"))

  contrib = {
    "recencia": round(rec * PESO_RECENCIA, 2),
    "mencao": round(men * PESO_MENCAO, 2),
    "fonte": round(fon * PESO_FONTE, 2),
    "engajamento": round(eng * PESO_ENGAJAMENTO, 2),
  }
  soma = round(sum(contrib.values()), 2)

  return {
    "pesos": {
      "recencia": PESO_RECENCIA,
      "mencao": PESO_MENCAO,
      "fonte": PESO_FONTE,
      "engajamento": PESO_ENGAJAMENTO,
    },
    "scores": {
      "recencia": rec,
      "mencao": men,
      "fonte": fon,
      "engajamento": eng,
      "relevancia_total": _score(noticia.get("relevancia_total")),
    },
    "contribuicoes": contrib,
    "relevancia_calculada": soma,
    "detalhes": {
      "mencao_titulo": bool(noticia.get("mencao_titulo", False)),
      "mencao_conteudo": int(noticia.get("mencao_conteudo") or 0),
      "fonte_nome": noticia.get("fonte_nome"),
      "tipo": noticia.get("tipo"),
    },
  }


@dataclass
class AnaliseNoticia:
  resumo_tecnico: str
  porque_pontuou: list
  hipoteses: list
  alertas: list


def gerar_resumo_tecnico(
  noticia: Dict[str, Any],
  politico_nome: Optional[str] = None,
) -> Optional[AnaliseNoticia]:
  """
  Gera um resumo técnico via OpenAI.
  Retorna None se a chave não estiver configurada.
  """
  titulo = (noticia.get("titulo") or "").strip()
  descricao = (noticia.get("descricao") or "").strip()
  conteudo = (noticia.get("conteudo_completo") or "").strip()
  conteudo = conteudo[:3500]  # evita payload enorme

  pontos = calcular_pontos(noticia)
  now = datetime.now(timezone.utc).isoformat()

  payload = {
    "agora_utc": now,
    "politico_nome": politico_nome,
    "noticia": {
      "titulo": titulo,
      "descricao": descricao,
      "fonte_nome": noticia.get("fonte_nome"),
      "publicado_em": noticia.get("publicado_em"),
      "tipo": noticia.get("tipo"),
      "url": noticia.get("url"),
      "conteudo_completo_truncado": conteudo,
    },
    "pontos": pontos,
  }

  prompt = (
    "Você é um analista técnico de monitoramento político.\n"
    "Gere um resumo técnico conciso em pt-BR e explique, tecnicamente, por que essa notícia recebeu essa pontuação.\n"
    "Responda SOMENTE em JSON válido, com as chaves:\n"
    '- "resumo_tecnico": string com 4-6 bullets (use \\n- ...), focando em: tema, atores, contexto, possível impacto.\n'
    '- "porque_pontuou": array de 4-8 strings, explicando o score (recência, menção, fonte, engajamento) e sinais no texto.\n'
    '- "hipoteses": array de 2-4 strings, hipóteses/testes para validar (ex.: checar fontes adicionais, confirmar menções, etc.).\n'
    '- "alertas": array de 0-3 strings (ex.: conteúdo incompleto, título genérico, baixa confiabilidade).\n'
    "Não invente fatos que não estejam no texto fornecido. Se faltarem dados, diga isso nos alertas."
  )

  try:
    client = OpenAI(api_key=settings.openai_api_key) if settings.openai_api_key else OpenAI()
    resp = client.with_options(timeout=12.0).chat.completions.create(
      model=settings.openai_model,
      messages=[
        {"role": "system", "content": prompt},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
      ],
      temperature=0.2,
    )

    content = resp.choices[0].message.content or ""
    data = json.loads(content)
    return AnaliseNoticia(
      resumo_tecnico=str(data.get("resumo_tecnico") or "").strip(),
      porque_pontuou=list(data.get("porque_pontuou") or []),
      hipoteses=list(data.get("hipoteses") or []),
      alertas=list(data.get("alertas") or []),
    )
  except Exception as e:
    # Não logar payload; só o erro
    logger.warning(f"Falha ao gerar resumo técnico (OpenAI): {e}")
    return None

