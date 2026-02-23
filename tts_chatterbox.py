"""
╔══════════════════════════════════════════════════════════════════╗
║   TTS CHATTERBOX — vía Gradio Space de Hugging Face             ║
║   Voz ultra-natural · 100% gratuito · Sin instalar nada         ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import io
import logging
import asyncio
import hashlib
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ──────────────────────────────────────────────────
HF_TOKEN  = os.getenv("HF_TOKEN", "")
HF_SPACE  = "ResembleAI/Chatterbox"

CACHE_DIR = Path(os.getenv("TTS_CACHE_DIR", "/tmp/tts_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE = os.getenv("TTS_USE_CACHE", "true").lower() == "true"

# Cliente Gradio global (se crea una sola vez)
_gradio_client = None
_client_lock   = asyncio.Lock()


def _cache_key(texto: str, estilo: str) -> str:
    return hashlib.md5(f"{texto}_{estilo}".encode()).hexdigest()


def _limpiar_texto(texto: str) -> str:
    import re
    texto = re.sub(r"[*_`]", "", texto)
    texto = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", texto)
    texto = re.sub(r"[^\w\s,.!?;:()\-áéíóúüñÁÉÍÓÚÜÑ]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


async def _get_cliente():
    """Crea el cliente Gradio una sola vez y lo reutiliza."""
    global _gradio_client
    if _gradio_client is not None:
        return _gradio_client

    async with _client_lock:
        if _gradio_client is not None:
            return _gradio_client

        logger.info("🔌 Conectando a Chatterbox en Hugging Face...")
        loop = asyncio.get_event_loop()

        def _crear():
            from gradio_client import Client
            kwargs = {"src": HF_SPACE}
            if HF_TOKEN:
                kwargs["hf_token"] = HF_TOKEN
            return Client(**kwargs)

        _gradio_client = await loop.run_in_executor(None, _crear)
        logger.info("✅ Conectado a Chatterbox.")

    return _gradio_client


async def _generar_con_gradio(texto: str, exaggeration: float, cfg_weight: float) -> bytes:
    """Llama al Space de Chatterbox y devuelve bytes de audio WAV."""
    cliente = await _get_cliente()
    loop    = asyncio.get_event_loop()

    def _llamar():
        resultado = cliente.predict(
            text         = texto,
            audio_prompt = None,
            exaggeration = exaggeration,
            cfg_weight   = cfg_weight,
            api_name     = "/generate"
        )
        # El Space devuelve la ruta al archivo WAV temporal
        ruta = Path(resultado) if isinstance(resultado, str) else Path(resultado[0])
        return ruta.read_bytes()

    return await loop.run_in_executor(None, _llamar)


def _wav_a_ogg(wav_bytes: bytes) -> bytes:
    """Convierte WAV a OGG sin ffmpeg usando soundfile."""
    try:
        import soundfile as sf
        buf_in  = io.BytesIO(wav_bytes)
        data, sr = sf.read(buf_in)
        buf_out  = io.BytesIO()
        sf.write(buf_out, data, sr, format="OGG", subtype="VORBIS")
        return buf_out.getvalue()
    except Exception:
        return wav_bytes  # Telegram también acepta WAV directo


async def _edge_tts_fallback(texto: str) -> Optional[bytes]:
    """Fallback a Catalina mejorada si Chatterbox no responde."""
    try:
        import edge_tts
        communicate = edge_tts.Communicate(
            texto,
            "es-CL-CatalinaNeural",
            rate   = "-12%",
            pitch  = "-4Hz",
            volume = "+8%"
        )
        buf = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                buf.write(chunk["data"])
        logger.info("🔈 Audio generado con edge-TTS (fallback).")
        return buf.getvalue()
    except Exception as e:
        logger.error(f"edge-TTS también falló: {e}")
        return None


async def texto_a_voz(
    texto: str,
    estilo: str = "normal",
    usar_chatterbox: bool = True
) -> Optional[bytes]:
    """
    Función principal de TTS.
    Intenta Chatterbox primero. Si falla, usa Catalina mejorada.
    """
    if not texto or not texto.strip():
        return None

    texto_limpio = _limpiar_texto(texto)
    if not texto_limpio:
        return None

    estilos = {
        "normal":       {"exaggeration": 0.35, "cfg_weight": 0.30},
        "bienvenida":   {"exaggeration": 0.50, "cfg_weight": 0.35},
        "alerta":       {"exaggeration": 0.60, "cfg_weight": 0.40},
        "celebracion":  {"exaggeration": 0.65, "cfg_weight": 0.38},
    }
    cfg = estilos.get(estilo, estilos["normal"])

    # Revisar caché primero
    if USE_CACHE:
        clave   = _cache_key(texto_limpio, estilo)
        f_cache = CACHE_DIR / f"{clave}.ogg"
        if f_cache.exists():
            logger.debug("🎵 Audio desde caché.")
            return f_cache.read_bytes()

    # Intentar Chatterbox
    if usar_chatterbox and HF_TOKEN:
        try:
            wav = await _generar_con_gradio(
                texto_limpio,
                cfg["exaggeration"],
                cfg["cfg_weight"]
            )
            ogg = _wav_a_ogg(wav)
            if USE_CACHE:
                f_cache.write_bytes(ogg)
            logger.info("🎙️ Audio Chatterbox generado con éxito. ✅")
            return ogg
        except Exception as e:
            logger.warning(f"⚠️ Chatterbox falló ({e}) → usando Catalina mejorada")
            return await _edge_tts_fallback(texto_limpio)
    else:
        if not HF_TOKEN:
            logger.warning("⚠️ HF_TOKEN no configurado → usando Catalina mejorada")
        return await _edge_tts_fallback(texto_limpio)


async def limpiar_cache_tts(dias: int = 7):
    """Limpia audios del caché más antiguos que N días."""
    import time
    ahora    = time.time()
    borrados = 0
    for f in CACHE_DIR.glob("*.ogg"):
        if ahora - f.stat().st_mtime > dias * 86400:
            f.unlink()
            borrados += 1
    logger.info(f"🗑️ Caché TTS: {borrados} archivos eliminados.")
