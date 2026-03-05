"""
╔══════════════════════════════════════════════════════════════════╗
║   TTS CHATTERBOX — vía Gradio Space de Hugging Face             ║
║   Voz femenina cálida · Referencia generada con edge-TTS        ║
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

# Archivo WAV de referencia de voz femenina (se genera automáticamente)
VOICE_REF_PATH = Path("/tmp/cofradia_voz_ref.wav")

_gradio_client = None
_client_lock   = asyncio.Lock()
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
    Genera un archivo WAV de referencia con voz femenina chilena
    usando edge-TTS. Este archivo se usa para que Chatterbox
    clone la voz y siempre suene femenina y consistente.
    """
    async with _voice_ref_lock:
        # Si ya existe y pesa más de 5KB, usarlo directamente
        if VOICE_REF_PATH.exists() and VOICE_REF_PATH.stat().st_size > 5000:
            print(f"✅ [TTS] Voz de referencia ya existe: {VOICE_REF_PATH}")
            return str(VOICE_REF_PATH)

        print("🎙️ [TTS] Generando voz de referencia femenina con edge-TTS...")
        try:
            import edge_tts
            import soundfile as sf
            import numpy as np

            # Texto de referencia — frase natural y expresiva en español chileno
            texto_ref = (
                "Hola, bienvenido a la Cofradía de Networking. "
                "Es un placer acompañarte en este espacio profesional "
                "donde conectamos a grandes personas."
            )

            communicate = edge_tts.Communicate(
                texto_ref,
                "es-CL-CatalinaNeural",
                rate   = "-5%",
                pitch  = "-2Hz",
            )

            # Recopilar el audio MP3
            buf = io.BytesIO()
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    buf.write(chunk["data"])

            audio_mp3 = buf.getvalue()
            if len(audio_mp3) < 1000:
                raise Exception("Audio de referencia demasiado corto")

            # Convertir MP3 → WAV usando soundfile
            try:
                buf_mp3 = io.BytesIO(audio_mp3)
                data, sr = sf.read(buf_mp3)
                # Tomar solo los primeros 8 segundos
                max_samples = sr * 8
                if len(data) > max_samples:
                    data = data[:max_samples]
                sf.write(str(VOICE_REF_PATH), data, sr)
            except Exception:
                # Si soundfile no puede leer MP3, guardar directo
                VOICE_REF_PATH.write_bytes(audio_mp3)

            print(f"✅ [TTS] Voz de referencia generada: {VOICE_REF_PATH.stat().st_size} bytes")
            return str(VOICE_REF_PATH)

        except Exception as e:
            print(f"❌ [TTS] Error generando voz de referencia: {e}")
            return None


async def _get_cliente():
    """Crea el cliente Gradio una sola vez y lo reutiliza."""
    global _gradio_client
    if _gradio_client is not None:
        return _gradio_client

    async with _client_lock:
        if _gradio_client is not None:
            return _gradio_client

        print("🔌 [TTS] Conectando a Chatterbox en Hugging Face...")
        logger.info("🔌 [TTS] Conectando a Chatterbox en Hugging Face...")
        loop = asyncio.get_event_loop()

        def _crear():
            from gradio_client import Client
            if HF_TOKEN:
                os.environ["GRADIO_HF_HUB_TOKEN"] = HF_TOKEN
            c = Client(HF_SPACE)
            try:
                endpoints = [str(e) for e in c.endpoints]
                print(f"📋 [TTS] Endpoints: {endpoints}")
            except Exception:
                pass
            return c

        _gradio_client = await loop.run_in_executor(None, _crear)
        print("✅ [TTS] Conectado a Chatterbox.")
        logger.info("✅ [TTS] Conectado a Chatterbox.")

    return _gradio_client


async def _generar_con_gradio(
    texto: str,
    exaggeration: float,
    cfg_weight: float
) -> bytes:
    """Llama al Space de Chatterbox con voz de referencia femenina."""

    # Preparar la voz de referencia ANTES de llamar a Gradio
    voz_ref = await _preparar_voz_referencia()

    cliente = await _get_cliente()
    loop    = asyncio.get_event_loop()

    def _llamar():
        if voz_ref and Path(voz_ref).exists():
            print(f"🎙️ [TTS] Clonando voz femenina de referencia...")
        else:
            print("⚠️ [TTS] Sin referencia — voz aleatoria")

        try:
            resultado = cliente.predict(
                texto,
                voz_ref,       # ← voz femenina de referencia
                exaggeration,
                cfg_weight,
                0,             # seed
                api_name="/generate"
            )
        except Exception as e1:
            print(f"⚠️ [TTS] Intento con api_name falló ({e1}), reintentando...")
            resultado = cliente.predict(
                texto,
                voz_ref,
                exaggeration,
                cfg_weight,
                0,
            )

        ruta = Path(resultado[0]) if isinstance(resultado, (list, tuple)) else Path(resultado)
        print(f"✅ [TTS] Chatterbox generó: {ruta}")
        return ruta.read_bytes()

    return await loop.run_in_executor(None, _llamar)


def _wav_a_ogg(wav_bytes: bytes) -> bytes:
    """Convierte WAV a OGG usando soundfile (sin ffmpeg)."""
    try:
        import soundfile as sf
        buf_in   = io.BytesIO(wav_bytes)
        data, sr = sf.read(buf_in)
        buf_out  = io.BytesIO()
        sf.write(buf_out, data, sr, format="OGG", subtype="VORBIS")
        return buf_out.getvalue()
    except Exception as e:
        print(f"⚠️ [TTS] Conversión OGG falló ({e}), usando WAV")
        return wav_bytes


async def _edge_tts_fallback(texto: str) -> Optional[bytes]:
    """Fallback a Catalina mejorada si Chatterbox falla."""
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
        print("🔈 [TTS] Audio con Catalina mejorada (fallback).")
        logger.info("🔈 [TTS] Audio con Catalina mejorada (fallback).")
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
    Usa Chatterbox con voz femenina chilena de referencia.
    Fallback automático a Catalina mejorada si falla.
    """
    if not texto or not texto.strip():
        return None

    texto_limpio = _limpiar_texto(texto)
    if not texto_limpio:
        return None

    print(f"🎤 [TTS] Solicitud de audio. HF_TOKEN={'SÍ' if HF_TOKEN else 'NO'}")
    logger.info(f"🎤 [TTS] Solicitud de audio. HF_TOKEN={'SÍ' if HF_TOKEN else 'NO'}")

    # exaggeration: expresividad y calidez (0.0=plana → 1.0=muy expresiva)
    # cfg_weight:   velocidad (0.0=muy pausado → 1.0=rápido)
    # Para voz femenina cálida: exaggeration 0.45-0.55, cfg_weight 0.25-0.30
    estilos = {
        "normal":       {"exaggeration": 0.50, "cfg_weight": 0.25},
        "bienvenida":   {"exaggeration": 0.58, "cfg_weight": 0.28},
        "alerta":       {"exaggeration": 0.62, "cfg_weight": 0.32},
        "celebracion":  {"exaggeration": 0.68, "cfg_weight": 0.30},
    }
    cfg = estilos.get(estilo, estilos["normal"])

    # Revisar caché
    if USE_CACHE:
        clave   = _cache_key(texto_limpio, estilo)
        f_cache = CACHE_DIR / f"{clave}.ogg"
        if f_cache.exists():
            print("🎵 [TTS] Audio desde caché.")
            return f_cache.read_bytes()

    if usar_chatterbox and HF_TOKEN:
        try:
            print("🚀 [TTS] Llamando a Chatterbox con voz femenina...")
            wav = await _generar_con_gradio(
                texto_limpio,
                cfg["exaggeration"],
                cfg["cfg_weight"]
            )
            ogg = _wav_a_ogg(wav)
            if USE_CACHE:
                f_cache.write_bytes(ogg)
            print("🎙️ [TTS] ¡CHATTERBOX EXITOSO con voz femenina! ✅")
            logger.info("🎙️ [TTS] ¡CHATTERBOX EXITOSO con voz femenina! ✅")
            return ogg
        except Exception as e:
            print(f"❌ [TTS] Chatterbox falló: {e}")
            logger.warning(f"❌ [TTS] Chatterbox falló: {e}")
            return await _edge_tts_fallback(texto_limpio)
    else:
        print(f"⏭️ [TTS] Sin Chatterbox → Catalina mejorada")
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
    # También limpiar la voz de referencia para que se regenere
    if VOICE_REF_PATH.exists():
        VOICE_REF_PATH.unlink()
    logger.info(f"🗑️ Caché TTS: {borrados} archivos eliminados.")
