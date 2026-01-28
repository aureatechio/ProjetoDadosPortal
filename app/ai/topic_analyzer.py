"""
Analisador de tópicos usando OpenAI para classificar menções por assunto.
"""
import json
import logging
from typing import List, Dict, Any, Optional
from openai import AsyncOpenAI

from app.config import settings

logger = logging.getLogger(__name__)


class TopicAnalyzer:
    """Classifica menções por assunto usando OpenAI"""
    
    CATEGORIAS = [
        "Saúde",
        "Educação", 
        "Segurança",
        "Economia",
        "Infraestrutura",
        "Meio Ambiente",
        "Corrupção",
        "Política",
        "Social",
        "Cultura",
        "Tecnologia",
        "Agronegócio",
        "Outro"
    ]
    
    def __init__(self):
        self.client = None
        if settings.openai_api_key:
            self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model
    
    @property
    def is_available(self) -> bool:
        """Verifica se o analisador está disponível"""
        return self.client is not None
    
    async def classificar_mencao(
        self, 
        conteudo: str, 
        nome_politico: str
    ) -> Dict[str, Any]:
        """
        Classifica uma menção e retorna assunto + sentimento.
        
        Args:
            conteudo: Texto da menção
            nome_politico: Nome do político para contexto
            
        Returns:
            {
                "assunto": "Saúde",
                "assunto_detalhe": "Discussão sobre inauguração de hospital",
                "sentimento": "positivo"
            }
        """
        if not self.is_available:
            logger.warning("OpenAI não configurado, usando classificação padrão")
            return self._classificacao_padrao()
        
        if not conteudo or len(conteudo.strip()) < 10:
            return self._classificacao_padrao()
        
        try:
            prompt = f"""Analise esta menção sobre o político {nome_politico}:

"{conteudo[:500]}"

Classifique em:
1. ASSUNTO: Uma das categorias: {', '.join(self.CATEGORIAS)}
2. DETALHE: Breve descrição do contexto específico (máximo 100 caracteres)
3. SENTIMENTO: positivo, negativo ou neutro

Responda APENAS em JSON com as chaves: assunto, assunto_detalhe, sentimento"""

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "Você é um analista político brasileiro. Classifique menções em redes sociais sobre políticos. Responda apenas em JSON válido."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                max_tokens=150,
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Valida e normaliza resultado
            return self._normalizar_resultado(result)
            
        except Exception as e:
            logger.error(f"Erro ao classificar menção: {e}")
            return self._classificacao_padrao()
    
    async def classificar_batch(
        self, 
        mencoes: List[Dict[str, Any]], 
        nome_politico: str,
        batch_size: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Classifica múltiplas menções de forma otimizada.
        
        Args:
            mencoes: Lista de menções com campo 'conteudo'
            nome_politico: Nome do político
            batch_size: Quantas menções processar por request
            
        Returns:
            Lista de menções com campos de classificação adicionados
        """
        if not self.is_available:
            logger.warning("OpenAI não configurado, aplicando classificação padrão em batch")
            for mencao in mencoes:
                classificacao = self._classificacao_padrao()
                mencao.update(classificacao)
            return mencoes
        
        # Processa em batches para economizar tokens
        for i in range(0, len(mencoes), batch_size):
            batch = mencoes[i:i + batch_size]
            
            try:
                classificacoes = await self._classificar_batch_interno(batch, nome_politico)
                
                for j, mencao in enumerate(batch):
                    if j < len(classificacoes):
                        mencao.update(classificacoes[j])
                    else:
                        mencao.update(self._classificacao_padrao())
                        
            except Exception as e:
                logger.error(f"Erro ao classificar batch: {e}")
                for mencao in batch:
                    mencao.update(self._classificacao_padrao())
        
        return mencoes
    
    async def _classificar_batch_interno(
        self, 
        batch: List[Dict[str, Any]], 
        nome_politico: str
    ) -> List[Dict[str, Any]]:
        """Classifica um batch de menções em uma única chamada"""
        
        # Monta texto das menções
        mencoes_texto = []
        for idx, m in enumerate(batch):
            conteudo = (m.get("conteudo") or "")[:300]
            if conteudo:
                mencoes_texto.append(f"{idx + 1}. \"{conteudo}\"")
        
        if not mencoes_texto:
            return [self._classificacao_padrao() for _ in batch]
        
        prompt = f"""Analise estas {len(mencoes_texto)} menções sobre o político {nome_politico}:

{chr(10).join(mencoes_texto)}

Para cada menção, classifique:
- ASSUNTO: Uma das categorias: {', '.join(self.CATEGORIAS)}
- DETALHE: Breve descrição (máx 80 caracteres)
- SENTIMENTO: positivo, negativo ou neutro

Responda em JSON com array "classificacoes" contendo objetos com: assunto, assunto_detalhe, sentimento"""

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": "Você é um analista político brasileiro. Classifique menções em redes sociais. Responda em JSON válido."
                },
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            max_tokens=500,
            temperature=0.3
        )
        
        result = json.loads(response.choices[0].message.content)
        classificacoes = result.get("classificacoes", [])
        
        return [self._normalizar_resultado(c) for c in classificacoes]
    
    def _normalizar_resultado(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Normaliza e valida o resultado da classificação"""
        assunto = result.get("assunto", "Outro")
        if assunto not in self.CATEGORIAS:
            assunto = "Outro"
        
        sentimento = result.get("sentimento", "neutro").lower()
        if sentimento not in ["positivo", "negativo", "neutro"]:
            sentimento = "neutro"
        
        detalhe = result.get("assunto_detalhe", "")
        if len(detalhe) > 150:
            detalhe = detalhe[:147] + "..."
        
        return {
            "assunto": assunto,
            "assunto_detalhe": detalhe,
            "sentimento": sentimento
        }
    
    def _classificacao_padrao(self) -> Dict[str, Any]:
        """Retorna classificação padrão quando não é possível analisar"""
        return {
            "assunto": "Outro",
            "assunto_detalhe": "",
            "sentimento": "neutro"
        }


# Instância global
topic_analyzer = TopicAnalyzer()
