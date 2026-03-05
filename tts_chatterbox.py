"""
╔══════════════════════════════════════════════════════════════════╗
║   TTS CHATTERBOX — vía Gradio Space de Hugging Face             ║
║   Voz de referencia personalizada vía TTS_VOICE_URL             ║
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
HF_TOKEN       = os.getenv("HF_TOKEN", "")
HF_SPACE       = "ResembleAI/Chatterbox"
TTS_VOICE_URL  = os.getenv("TTS_VOICE_URL", "")   # URL del WAV de referencia

CACHE_DIR      = Path(os.getenv("TTS_CACHE_DIR", "/tmp/tts_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE      = os.getenv("TTS_USE_CACHE", "true").lower() == "true"

VOICE_REF_PATH = Path("/tmp/cofradia_voz_ref.wav")

_gradio_client  = None
_client_lock    = asyncio.Lock()
_voice_ref_lock = asyncio.Lock()


def _cache_key(texto: str, estilo: str) -> str:
    return hashlib.md5(f"{texto}_{estilo}".encode()).hexdigest()


def _limpiar_texto(texto: str) -> str:
    import re
    texto = re.sub(r"[*_`]", "", texto)
    texto = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", texto)
    texto = re.sub(r"[^\w\s,.!?;:()\-áéíóúüñÁÉÍÓÚÜÑ]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


async def _preparar_voz_referencia() -> Optional[str]:
    """
    Obtiene la voz de referencia en este orden de prioridad:
    1. Desde TTS_VOICE_URL (tu archivo WAV personalizado) ← MEJOR OPCIÓN
    2. Generada con edge-TTS (Catalina) como fallback
    Retorna la ruta local al archivo WAV o None si todo falla.
    """
    async with _voice_ref_lock:
        # Si ya está descargada y pesa más de 5KB, reutilizarla
        if VOICE_REF_PATH.exists() and VOICE_REF_PATH.stat().st_size > 5000:
            print(f"✅ [TTS] Usando voz de referencia en disco ({VOICE_REF_PATH.stat().st_size} bytes)")
            return str(VOICE_REF_PATH)

        # ── OPCIÓN 1: Descargar desde TTS_VOICE_URL ───────────────────
        if TTS_VOICE_URL:
            print(f"⬇️  [TTS] Descargando voz de referencia desde: {TTS_VOICE_URL}")
            try:
                import requests
                r = requests.get(TTS_VOICE_URL, timeout=20)
                if r.status_code == 200 and len(r.content) > 5000:
                    VOICE_REF_PATH.write_bytes(r.content)
                    print(f"✅ [TTS] Voz personalizada descargada: {len(r.content)} bytes")
                    return str(VOICE_REF_PATH)
                else:
                    print(f"⚠️  [TTS] Descarga falló (status={r.status_code}, size={len(r.content)})")
            except Exception as e:
                print(f"⚠️  [TTS] Error descargando voz: {e}")

        # ── OPCIÓN 2: Generar con edge-TTS como fallback ──────────────
        print("🎙️ [TTS] Generando voz de referencia con edge-TTS (Catalina)...")
        try:
            import edge_tts
            import soundfile as sf

            texto_ref = (
                "Hola, bienvenido a la Cofradía de Networking. "
                "Es un placer acompañarte en este espacio profesional "
                "donde conectamos a grandes personas."
            )
            communicate = edge_tts.Communicate(
                texto_ref, "es-CL-CatalinaNeural",
                rate="-5%", pitch="-2Hz"
            )
            buf = io.BytesIO()
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    buf.write(chunk["data"])

            audio_bytes = buf.getvalue()
            if len(audio_bytes) > 1000:
                # Intentar convertir MP3→WAV con soundfile
                try:
                    buf_in = io.BytesIO(audio_bytes)
                    data, sr = sf.read(buf_in)
                    data = data[:sr * 8]   # máximo 8 segundos
                    sf.write(str(VOICE_REF_PATH), data, sr)
                except Exception:
                    VOICE_REF_PATH.write_bytes(audio_bytes)
                print(f"✅ [TTS] Voz referencia generada con edge-TTS: {VOICE_REF_PATH.stat().st_size} bytes")
                return str(VOICE_REF_PATH)
        except Exception as e:
            print(f"❌ [TTS] edge-TTS también falló: {e}")

        return None


async def _get_cliente():
    global _gradio_client
    if _gradio_client is not None:
        return _gradio_client

    async with _client_lock:
        if _gradio_client is not None:
            return _gradio_client

        print("🔌 [TTS] Conectando a Chatterbox en Hugging Face...")
        loop = asyncio.get_event_loop()

        def _crear():
            from gradio_client import Client
            if HF_TOKEN:
                os.environ["GRADIO_HF_HUB_TOKEN"] = HF_TOKEN
            return Client(HF_SPACE)

        _gradio_client = await loop.run_in_executor(None, _crear)
        print("✅ [TTS] Conectado a Chatterbox.")
        logger.info("✅ [TTS] Conectado a Chatterbox.")

    return _gradio_client


async def _generar_con_gradio(texto: str, exaggeration: float, cfg_weight: float) -> bytes:
    voz_ref = await _preparar_voz_referencia()
    cliente = await _get_cliente()
    loop    = asyncio.get_event_loop()

    def _llamar():
        print(f"🚀 [TTS] Enviando a Chatterbox | ref={'SÍ' if voz_ref else 'NO'} | exag={exaggeration} | cfg={cfg_weight}")
        try:
            resultado = cliente.predict(
                texto, voz_ref, exaggeration, cfg_weight, 0,
                api_name="/generate"
            )
        except Exception as e1:
            print(f"⚠️ [TTS] Reintentando sin api_name... ({e1})")
            resultado = cliente.predict(
                texto, voz_ref, exaggeration, cfg_weight, 0
            )

        ruta = Path(resultado[0]) if isinstance(resultado, (list, tuple)) else Path(resultado)
        print(f"✅ [TTS] WAV recibido: {ruta} ({ruta.stat().st_size if ruta.exists() else '?'} bytes)")
        return ruta.read_bytes()

    return await loop.run_in_executor(None, _llamar)


def _wav_a_ogg(wav_bytes: bytes) -> bytes:
    try:
        import soundfile as sf
        buf_in   = io.BytesIO(wav_bytes)
        data, sr = sf.read(buf_in)
        buf_out  = io.BytesIO()
        sf.write(buf_out, data, sr, format="OGG", subtype="VORBIS")
        return buf_out.getvalue()
    except Exception as e:
        print(f"⚠️ [TTS] Conversión OGG falló ({e}), usando WAV directo")
        return wav_bytes


async def _edge_tts_fallback(texto: str) -> Optional[bytes]:
    try:
        import edge_tts
        communicate = edge_tts.Communicate(
            texto, "es-CL-CatalinaNeural",
            rate="-12%", pitch="-4Hz", volume="+8%"
        )
        buf = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                buf.write(chunk["data"])
        print("🔈 [TTS] Fallback: Catalina mejorada.")
        logger.info("🔈 [TTS] Fallback: Catalina mejorada.")
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
    Función principal de TTS.
    Usa Chatterbox clonando la voz del archivo TTS_VOICE_URL.
    Fallback automático a Catalina si falla.
    """
    if not texto or not texto.strip():
        return None

    texto_limpio = _limpiar_texto(texto)
    if not texto_limpio:
        return None

    print(f"🎤 [TTS] HF_TOKEN={'SÍ' if HF_TOKEN else 'NO'} | VOICE_URL={'SÍ' if TTS_VOICE_URL else 'NO'}")
    logger.info(f"🎤 [TTS] HF_TOKEN={'SÍ' if HF_TOKEN else 'NO'} | VOICE_URL={'SÍ' if TTS_VOICE_URL else 'NO'}")

    estilos = {
        "normal":       {"exaggeration": 0.50, "cfg_weight": 0.25},
        "bienvenida":   {"exaggeration": 0.58, "cfg_weight": 0.28},
        "alerta":       {"exaggeration": 0.62, "cfg_weight": 0.32},
        "celebracion":  {"exaggeration": 0.68, "cfg_weight": 0.30},
    }
    cfg = estilos.get(estilo, estilos["normal"])

    if USE_CACHE:
        clave   = _cache_key(texto_limpio, estilo)
        f_cache = CACHE_DIR / f"{clave}.ogg"
        if f_cache.exists():
            print("🎵 [TTS] Audio desde caché.")
            return f_cache.read_bytes()

    if usar_chatterbox and HF_TOKEN:
        try:
            wav = await _generar_con_gradio(
                texto_limpio, cfg["exaggeration"], cfg["cfg_weight"]
            )
            ogg = _wav_a_ogg(wav)
            if USE_CACHE:
                f_cache.write_bytes(ogg)
            print("🎙️ [TTS] ¡CHATTERBOX EXITOSO! ✅")
            logger.info("🎙️ [TTS] ¡CHATTERBOX EXITOSO! ✅")
            return ogg
        except Exception as e:
            print(f"❌ [TTS] Chatterbox falló: {e}")
            logger.warning(f"❌ [TTS] Chatterbox falló: {e}")
            return await _edge_tts_fallback(texto_limpio)
    else:
        print("⏭️ [TTS] Sin Chatterbox → Catalina mejorada")
        return await _edge_tts_fallback(texto_limpio)


async def limpiar_cache_tts(dias: int = 7):
    import time
    ahora    = time.time()
    borrados = 0
    for f in CACHE_DIR.glob("*.ogg"):
        if ahora - f.stat().st_mtime > dias * 86400:
            f.unlink()
            borrados += 1
    # Forzar regeneración de la voz de referencia
    if VOICE_REF_PATH.exists():
        VOICE_REF_PATH.unlink()
        print("🗑️ [TTS] Voz de referencia eliminada para regenerar.")
    logger.info(f"🗑️ Caché TTS: {borrados} archivos eliminados.")
