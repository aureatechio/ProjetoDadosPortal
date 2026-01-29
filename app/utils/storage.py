"""
Utilitários para upload de imagens ao Supabase Storage.

Este módulo centraliza a lógica de:
- Download de imagens de URLs externas
- Upload para o bucket do Supabase Storage
- Geração de URLs públicas do Storage

Uso:
    from app.utils.storage import upload_image_from_url, upload_image_from_url_async
    
    # Síncrono
    storage_url = upload_image_from_url(
        image_url="https://example.com/foto.jpg",
        folder="noticias",
        filename="noticia_123"
    )
    
    # Assíncrono
    storage_url = await upload_image_from_url_async(
        image_url="https://example.com/foto.jpg",
        folder="noticias",
        filename="noticia_123"
    )
"""

import asyncio
import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse

import httpx

from app.database import get_supabase

logger = logging.getLogger(__name__)

# Nome do bucket público no Supabase Storage
DEFAULT_BUCKET = "portal"

# Timeout para download de imagens
DOWNLOAD_TIMEOUT = 30.0

# Headers padrão para evitar bloqueios
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}


def _guess_content_type(url: str, response: httpx.Response) -> str:
    """
    Determina o content-type da imagem.
    
    Prioriza o header da resposta, com fallback para extensão da URL.
    """
    ct = (response.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ct.startswith("image/"):
        return ct
    
    # Fallback baseado na URL
    url_lower = url.lower()
    if ".png" in url_lower:
        return "image/png"
    if ".webp" in url_lower:
        return "image/webp"
    if ".gif" in url_lower:
        return "image/gif"
    if ".svg" in url_lower:
        return "image/svg+xml"
    
    return "image/jpeg"  # Default


def _ext_from_content_type(content_type: str) -> str:
    """Retorna extensão de arquivo baseada no content-type."""
    ct = content_type.lower()
    if "png" in ct:
        return "png"
    if "webp" in ct:
        return "webp"
    if "gif" in ct:
        return "gif"
    if "svg" in ct:
        return "svg"
    return "jpg"


def _generate_filename(original_url: str, prefix: str, content_type: str) -> str:
    """
    Gera um nome de arquivo único baseado na URL original.
    
    Usa hash MD5 da URL para evitar duplicatas e garantir unicidade.
    """
    url_hash = hashlib.md5(original_url.encode()).hexdigest()[:12]
    ext = _ext_from_content_type(content_type)
    return f"{prefix}_{url_hash}.{ext}"


def _is_already_in_storage(url: str, bucket: str = DEFAULT_BUCKET) -> bool:
    """Verifica se a URL já aponta para o Supabase Storage."""
    if not url:
        return False
    return f"/storage/v1/object/public/{bucket}/" in url


def get_storage_url(bucket: str, path: str) -> str:
    """
    Retorna a URL pública completa do Supabase Storage.
    
    Args:
        bucket: Nome do bucket
        path: Caminho do arquivo no bucket
        
    Returns:
        URL pública do arquivo
    """
    supabase = get_supabase()
    return supabase.storage.from_(bucket).get_public_url(path)


def upload_image_to_storage(
    image_data: bytes,
    path: str,
    content_type: str = "image/jpeg",
    bucket: str = DEFAULT_BUCKET,
) -> Optional[str]:
    """
    Faz upload de dados de imagem diretamente para o Supabase Storage.
    
    Args:
        image_data: Bytes da imagem
        path: Caminho completo no bucket (ex: "noticias/img_abc123.jpg")
        content_type: Tipo MIME da imagem
        bucket: Nome do bucket (default: "portal")
        
    Returns:
        URL pública da imagem ou None em caso de erro
    """
    if not image_data:
        logger.warning("upload_image_to_storage: dados vazios")
        return None
    
    try:
        supabase = get_supabase()
        
        # Usa arquivo temporário para o upload (API do supabase-py espera caminho)
        with tempfile.NamedTemporaryFile(delete=True, suffix=f".{_ext_from_content_type(content_type)}") as tmp:
            tmp.write(image_data)
            tmp.flush()
            
            # Upload com upsert (sobrescreve se existir)
            supabase.storage.from_(bucket).upload(
                path,
                tmp.name,
                {"content-type": content_type, "upsert": "true"},
            )
        
        # Retorna URL pública
        public_url = supabase.storage.from_(bucket).get_public_url(path)
        logger.debug(f"Upload bem-sucedido: {path}")
        return public_url
        
    except Exception as e:
        logger.error(f"Erro ao fazer upload para storage: {e}")
        return None


def download_image(url: str, timeout: float = DOWNLOAD_TIMEOUT) -> Tuple[Optional[bytes], str]:
    """
    Baixa uma imagem de uma URL.
    
    Args:
        url: URL da imagem
        timeout: Timeout em segundos
        
    Returns:
        Tupla (dados da imagem, content-type) ou (None, "") em caso de erro
    """
    if not url:
        return None, ""
    
    try:
        with httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers=DEFAULT_HEADERS,
        ) as client:
            response = client.get(url)
            response.raise_for_status()
            
            content_type = _guess_content_type(url, response)
            return response.content, content_type
            
    except httpx.TimeoutException:
        logger.warning(f"Timeout ao baixar imagem: {url}")
    except httpx.HTTPStatusError as e:
        logger.warning(f"HTTP {e.response.status_code} ao baixar imagem: {url}")
    except Exception as e:
        logger.warning(f"Erro ao baixar imagem {url}: {e}")
    
    return None, ""


async def download_image_async(url: str, timeout: float = DOWNLOAD_TIMEOUT) -> Tuple[Optional[bytes], str]:
    """
    Baixa uma imagem de uma URL de forma assíncrona.
    
    Args:
        url: URL da imagem
        timeout: Timeout em segundos
        
    Returns:
        Tupla (dados da imagem, content-type) ou (None, "") em caso de erro
    """
    if not url:
        return None, ""
    
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=DEFAULT_HEADERS,
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            
            content_type = _guess_content_type(url, response)
            return response.content, content_type
            
    except httpx.TimeoutException:
        logger.warning(f"Timeout ao baixar imagem: {url}")
    except httpx.HTTPStatusError as e:
        logger.warning(f"HTTP {e.response.status_code} ao baixar imagem: {url}")
    except Exception as e:
        logger.warning(f"Erro ao baixar imagem {url}: {e}")
    
    return None, ""


def upload_image_from_url(
    image_url: str,
    folder: str,
    filename: Optional[str] = None,
    bucket: str = DEFAULT_BUCKET,
    fallback_to_original: bool = True,
) -> Optional[str]:
    """
    Baixa uma imagem de URL externa e faz upload para o Supabase Storage.
    
    Esta é a função principal para uso síncrono. Baixa a imagem da URL,
    faz upload para o bucket do Supabase e retorna a nova URL pública.
    
    Args:
        image_url: URL da imagem a ser baixada
        folder: Pasta no bucket (ex: "noticias", "instagram", "candidatos")
        filename: Nome do arquivo (sem extensão). Se None, gera automaticamente
        bucket: Nome do bucket (default: "portal")
        fallback_to_original: Se True, retorna URL original em caso de erro
        
    Returns:
        URL pública do Supabase Storage, ou URL original se fallback=True,
        ou None em caso de erro
        
    Exemplo:
        >>> url = upload_image_from_url(
        ...     "https://example.com/foto.jpg",
        ...     folder="noticias",
        ...     filename="noticia_123"
        ... )
        >>> print(url)
        "https://xxx.supabase.co/storage/v1/object/public/portal/noticias/noticia_123_abc.jpg"
    """
    if not image_url:
        return None
    
    # Verifica se já está no storage
    if _is_already_in_storage(image_url, bucket):
        logger.debug(f"Imagem já está no storage: {image_url}")
        return image_url
    
    try:
        # Download da imagem
        image_data, content_type = download_image(image_url)
        
        if not image_data:
            logger.warning(f"Não foi possível baixar imagem: {image_url}")
            return image_url if fallback_to_original else None
        
        # Gera nome do arquivo
        if filename:
            ext = _ext_from_content_type(content_type)
            safe_filename = f"{filename}.{ext}"
        else:
            safe_filename = _generate_filename(image_url, "img", content_type)
        
        # Caminho completo no bucket
        path = f"{folder}/{safe_filename}"
        
        # Upload
        storage_url = upload_image_to_storage(
            image_data=image_data,
            path=path,
            content_type=content_type,
            bucket=bucket,
        )
        
        if storage_url:
            logger.info(f"Imagem migrada para storage: {path}")
            return storage_url
        
        return image_url if fallback_to_original else None
        
    except Exception as e:
        logger.error(f"Erro ao processar imagem {image_url}: {e}")
        return image_url if fallback_to_original else None


async def upload_image_from_url_async(
    image_url: str,
    folder: str,
    filename: Optional[str] = None,
    bucket: str = DEFAULT_BUCKET,
    fallback_to_original: bool = True,
) -> Optional[str]:
    """
    Versão assíncrona de upload_image_from_url.
    
    Baixa uma imagem de URL externa e faz upload para o Supabase Storage.
    O download é feito de forma assíncrona, mas o upload ainda é síncrono
    (limitação da biblioteca supabase-py).
    
    Args:
        image_url: URL da imagem a ser baixada
        folder: Pasta no bucket (ex: "noticias", "instagram", "candidatos")
        filename: Nome do arquivo (sem extensão). Se None, gera automaticamente
        bucket: Nome do bucket (default: "portal")
        fallback_to_original: Se True, retorna URL original em caso de erro
        
    Returns:
        URL pública do Supabase Storage, ou URL original se fallback=True,
        ou None em caso de erro
    """
    if not image_url:
        return None
    
    # Verifica se já está no storage
    if _is_already_in_storage(image_url, bucket):
        logger.debug(f"Imagem já está no storage: {image_url}")
        return image_url
    
    try:
        # Download assíncrono da imagem
        image_data, content_type = await download_image_async(image_url)
        
        if not image_data:
            logger.warning(f"Não foi possível baixar imagem: {image_url}")
            return image_url if fallback_to_original else None
        
        # Gera nome do arquivo
        if filename:
            ext = _ext_from_content_type(content_type)
            safe_filename = f"{filename}.{ext}"
        else:
            safe_filename = _generate_filename(image_url, "img", content_type)
        
        # Caminho completo no bucket
        path = f"{folder}/{safe_filename}"
        
        # Upload (síncrono, executado em thread)
        loop = asyncio.get_event_loop()
        storage_url = await loop.run_in_executor(
            None,
            lambda: upload_image_to_storage(
                image_data=image_data,
                path=path,
                content_type=content_type,
                bucket=bucket,
            )
        )
        
        if storage_url:
            logger.info(f"Imagem migrada para storage: {path}")
            return storage_url
        
        return image_url if fallback_to_original else None
        
    except Exception as e:
        logger.error(f"Erro ao processar imagem {image_url}: {e}")
        return image_url if fallback_to_original else None


# Funções de conveniência para diferentes tipos de conteúdo

async def upload_noticia_image_async(image_url: str, noticia_id: Optional[str] = None) -> Optional[str]:
    """Upload de imagem de notícia."""
    filename = f"noticia_{noticia_id}" if noticia_id else None
    return await upload_image_from_url_async(image_url, folder="noticias", filename=filename)


async def upload_instagram_thumbnail_async(image_url: str, shortcode: str) -> Optional[str]:
    """Upload de thumbnail do Instagram."""
    return await upload_image_from_url_async(image_url, folder="instagram", filename=f"post_{shortcode}")


async def upload_candidato_foto_async(image_url: str, cpf: str, eleicao: str) -> Optional[str]:
    """Upload de foto de candidato do TSE."""
    filename = f"candidato_{cpf}_{eleicao}"
    return await upload_image_from_url_async(image_url, folder="candidatos", filename=filename)


def upload_politico_image(image_url: str, politico_id: int, instagram_username: Optional[str] = None) -> Optional[str]:
    """Upload de foto de perfil de político."""
    suffix = instagram_username or f"politico_{politico_id}"
    return upload_image_from_url(image_url, folder="politicos", filename=f"{politico_id}_{suffix}")


async def upload_politico_image_async(image_url: str, politico_id: int, instagram_username: Optional[str] = None) -> Optional[str]:
    """Upload assíncrono de foto de perfil de político."""
    suffix = instagram_username or f"politico_{politico_id}"
    return await upload_image_from_url_async(image_url, folder="politicos", filename=f"{politico_id}_{suffix}")
