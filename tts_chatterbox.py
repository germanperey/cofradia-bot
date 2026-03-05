"""
╔══════════════════════════════════════════════════════════════════╗
║   TTS CHATTERBOX — vía Gradio Space de Hugging Face             ║
║   Versión corregida con endpoint verificado y logs visibles     ║
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
            # Pasar el token via variable de entorno (compatible con todas las versiones)
            if HF_TOKEN:
                os.environ["GRADIO_HF_HUB_TOKEN"] = HF_TOKEN
            c = Client(HF_SPACE)
            # Mostrar endpoints disponibles para diagnóstico
            try:
                endpoints = [str(e) for e in c.endpoints]
                print(f"📋 [TTS] Endpoints disponibles: {endpoints}")
            except Exception:
                pass
            return c

        _gradio_client = await loop.run_in_executor(None, _crear)
        print("✅ [TTS] Conectado a Chatterbox.")
        logger.info("✅ [TTS] Conectado a Chatterbox.")

    return _gradio_client


# ─── VOZ DE REFERENCIA FEMENINA ─────────────────────────────────────
# URL de una muestra de voz femenina cálida en español (Creative Commons)
# Puedes reemplazar esta URL por tu propia grabación de 5-10 segundos
VOICE_REF_URL  = os.getenv(
    "TTS_VOICE_URL",
    "https://huggingface.co/datasets/mozilla-foundation/common_voice_13_0/resolve/main/audio/es/validated/common_voice_es_19822797.mp3"
)
VOICE_REF_PATH = Path("/tmp/tts_voice_ref.wav")


def _obtener_voz_referencia() -> str | None:
    """
    Descarga la voz de referencia femenina si no está en disco.
    Retorna la ruta al archivo WAV o None si falla.
    """
    if VOICE_REF_PATH.exists() and VOICE_REF_PATH.stat().st_size > 1000:
        return str(VOICE_REF_PATH)
    try:
        import requests
        import soundfile as sf
        import numpy as np

        print("⬇️  [TTS] Descargando voz de referencia femenina...")
        r = requests.get(VOICE_REF_URL, timeout=15)
        if r.status_code != 200:
            print(f"⚠️  [TTS] No se pudo descargar la voz de referencia: {r.status_code}")
            return None

        # Guardar como WAV (soundfile puede leer MP3 si tiene libsndfile)
        try:
            buf = io.BytesIO(r.content)
            data, sr = sf.read(buf)
            # Tomar solo los primeros 8 segundos para la referencia
            max_samples = sr * 8
            if len(data) > max_samples:
                data = data[:max_samples]
            sf.write(str(VOICE_REF_PATH), data, sr)
            print(f"✅ [TTS] Voz de referencia guardada: {VOICE_REF_PATH}")
            return str(VOICE_REF_PATH)
        except Exception as e:
            # Si soundfile no puede leer MP3, guardar el archivo crudo
            VOICE_REF_PATH.write_bytes(r.content)
            print(f"✅ [TTS] Voz de referencia guardada (raw): {VOICE_REF_PATH}")
            return str(VOICE_REF_PATH)

    except Exception as e:
        print(f"⚠️  [TTS] Error descargando voz de referencia: {e}")
        return None


async def _generar_con_gradio(texto: str, exaggeration: float, cfg_weight: float) -> bytes:
    cliente = await _get_cliente()
    loop    = asyncio.get_event_loop()

    def _llamar():
        # Obtener voz de referencia femenina
        voz_ref = _obtener_voz_referencia()
        if voz_ref:
            print(f"🎙️  [TTS] Usando voz de referencia femenina: {voz_ref}")
        else:
            print("⚠️  [TTS] Sin voz de referencia — Chatterbox elegirá una voz aleatoria")

        try:
            resultado = cliente.predict(
                texto,
                voz_ref,      # audio_prompt → voz femenina de referencia
                exaggeration,
                cfg_weight,
                0,            # seed
                api_name="/generate"
            )
            print(f"✅ [TTS] Chatterbox respondió: {type(resultado)}")
        except Exception as e1:
            print(f"⚠️ [TTS] Intento 1 falló ({e1}), probando sin nombre de endpoint...")
            resultado = cliente.predict(
                texto,
                voz_ref,
                exaggeration,
                cfg_weight,
                0,
            )
            print(f"✅ [TTS] Chatterbox respondió (intento 2): {type(resultado)}")

        if isinstance(resultado, (list, tuple)):
            ruta = Path(resultado[0])
        else:
            ruta = Path(resultado)

        print(f"🎵 [TTS] Archivo WAV en: {ruta}")
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
    if not texto or not texto.strip():
        return None

    texto_limpio = _limpiar_texto(texto)
    if not texto_limpio:
        return None

    # Log siempre visible para diagnóstico
    print(f"🎤 [TTS] Solicitando audio. HF_TOKEN={'SÍ' if HF_TOKEN else 'NO'} | Chatterbox={'SÍ' if usar_chatterbox else 'NO'}")
    logger.info(f"🎤 [TTS] Solicitando audio. HF_TOKEN={'SÍ' if HF_TOKEN else 'NO'}")

    # exaggeration: calidez y expresividad (0.0=plana, 1.0=muy dramática)
    # cfg_weight: velocidad/pace (0.0=muy lento, 1.0=rápido)
    # Para voz femenina cálida: exaggeration ~0.45-0.55, cfg_weight ~0.25-0.35
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
            print("🚀 [TTS] Llamando a Chatterbox en HF...")
            wav = await _generar_con_gradio(
                texto_limpio,
                cfg["exaggeration"],
                cfg["cfg_weight"]
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
        print(f"⏭️ [TTS] Saltando Chatterbox (HF_TOKEN={'vacío' if not HF_TOKEN else 'ok'}, usar_chatterbox={usar_chatterbox})")
        return await _edge_tts_fallback(texto_limpio)


async def limpiar_cache_tts(dias: int = 7):
    import time
    ahora    = time.time()
    borrados = 0
    for f in CACHE_DIR.glob("*.ogg"):
        if ahora - f.stat().st_mtime > dias * 86400:
            f.unlink()
            borrados += 1
    logger.info(f"🗑️ Caché TTS: {borrados} archivos eliminados.")
