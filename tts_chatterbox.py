"""
╔══════════════════════════════════════════════════════════════════╗
║   TTS CHATTERBOX — vía Hugging Face API (100% gratuito)         ║
║   El modelo corre en los servidores de HF, no en Render.        ║
║   Tu bot solo hace una llamada HTTP y recibe el audio.          ║
║   Sin ffmpeg · Sin sudo · Sin RAM extra · Sin GPU               ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import io
import logging
import asyncio
import hashlib
import aiohttp
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ──────────────────────────────────────────────────
HF_TOKEN   = os.getenv("HF_TOKEN", "")
HF_API_URL = "https://api-inference.huggingface.co/models/ResembleAI/chatterbox"

CACHE_DIR = Path(os.getenv("TTS_CACHE_DIR", "/tmp/tts_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE  = os.getenv("TTS_USE_CACHE", "true").lower() == "true"

EXAGGERATION = float(os.getenv("TTS_EXAGGERATION", "0.35"))
CFG_WEIGHT   = float(os.getenv("TTS_CFG_WEIGHT",   "0.30"))


def _cache_key(texto: str) -> str:
    return hashlib.md5(texto.encode()).hexdigest()


async def _llamar_hf_api(texto: str, exaggeration: float, cfg_weight: float) -> bytes:
    headers = {
        "Authorization": f"Bearer {HF_TOKEN}",
        "Content-Type":  "application/json",
    }
    payload = {
        "inputs": texto,
        "parameters": {"exaggeration": exaggeration, "cfg_weight": cfg_weight}
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            HF_API_URL, headers=headers, json=payload,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status == 503:
                data  = await resp.json()
                espera = min(data.get("estimated_time", 20), 25)
                logger.info(f"⏳ HF cargando modelo, esperando {espera:.0f}s...")
                await asyncio.sleep(espera)
                async with session.post(
                    HF_API_URL, headers=headers, json=payload,
                    timeout=aiohttp.ClientTimeout(total=45)
                ) as retry:
                    if retry.status == 200:
                        return await retry.read()
                    raise Exception(f"HF reintento: {retry.status}")
            if resp.status != 200:
                raise Exception(f"HF API {resp.status}: {await resp.text()}")
            return await resp.read()


def _limpiar_texto(texto: str) -> str:
    import re
    texto = re.sub(r"[*_`]", "", texto)
    texto = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", texto)
    texto = re.sub(r"[^\w\s,.!?;:()\-áéíóúüñÁÉÍÓÚÜÑ]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def _wav_a_ogg_sin_ffmpeg(wav_bytes: bytes) -> bytes:
    try:
        import soundfile as sf
        import numpy as np
        buf_in = io.BytesIO(wav_bytes)
        data, sr = sf.read(buf_in)
        buf_out  = io.BytesIO()
        sf.write(buf_out, data, sr, format="OGG", subtype="VORBIS")
        return buf_out.getvalue()
    except Exception:
        return wav_bytes   # WAV directo — Telegram también lo acepta


async def _edge_tts_fallback(texto: str) -> Optional[bytes]:
    try:
        import edge_tts
        communicate = edge_tts.Communicate(texto, "es-CL-CatalinaNeural",
                                           rate="-8%", pitch="-2Hz")
        buf = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                buf.write(chunk["data"])
        return buf.getvalue()
    except Exception as e:
        logger.error(f"edge-TTS falló: {e}")
        return None


async def texto_a_voz(
    texto: str,
    estilo: str = "normal",
    usar_chatterbox: bool = True
) -> Optional[bytes]:
    """
    Función principal — reemplaza edge-TTS, misma firma.
    Llama a Chatterbox en Hugging Face (gratis), sin usar RAM de Render.
    """
    if not texto or not texto.strip():
        return None

    texto_limpio = _limpiar_texto(texto)
    if not texto_limpio:
        return None

    estilos = {
        "normal":       {"exaggeration": 0.35, "cfg_weight": 0.30},
        "bienvenida":   {"exaggeration": 0.45, "cfg_weight": 0.35},
        "alerta":       {"exaggeration": 0.55, "cfg_weight": 0.40},
        "celebracion":  {"exaggeration": 0.60, "cfg_weight": 0.38},
    }
    cfg = estilos.get(estilo, estilos["normal"])

    if USE_CACHE:
        clave = _cache_key(f"{texto_limpio}_{estilo}")
        f_cache = CACHE_DIR / f"{clave}.ogg"
        if f_cache.exists():
            return f_cache.read_bytes()

    if usar_chatterbox and HF_TOKEN:
        try:
            wav  = await _llamar_hf_api(texto_limpio, cfg["exaggeration"], cfg["cfg_weight"])
            ogg  = _wav_a_ogg_sin_ffmpeg(wav)
            if USE_CACHE:
                f_cache.write_bytes(ogg)
            return ogg
        except Exception as e:
            logger.warning(f"Chatterbox HF falló ({e}) — usando edge-TTS")
            return await _edge_tts_fallback(texto_limpio)
    else:
        if not HF_TOKEN:
            logger.warning("HF_TOKEN no configurado — usando edge-TTS")
        return await _edge_tts_fallback(texto_limpio)


async def limpiar_cache_tts(dias: int = 7):
    import time
    ahora = time.time()
    borrados = sum(
        1 for f in CACHE_DIR.glob("*.ogg")
        if ahora - f.stat().st_mtime > dias * 86400 and f.unlink() is None
    )
    logger.info(f"🗑️ Caché TTS: {borrados} archivos eliminados.")
