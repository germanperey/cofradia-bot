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
            kwargs = {"src": HF_SPACE}
            if HF_TOKEN:
                kwargs["hf_token"] = HF_TOKEN
            c = Client(**kwargs)
            # Mostrar endpoints disponibles para diagnóstico
            try:
                print(f"📋 [TTS] Endpoints disponibles: {[e for e in c.endpoints]}")
            except Exception:
                pass
            return c

        _gradio_client = await loop.run_in_executor(None, _crear)
        print("✅ [TTS] Conectado a Chatterbox.")
        logger.info("✅ [TTS] Conectado a Chatterbox.")

    return _gradio_client


async def _generar_con_gradio(texto: str, exaggeration: float, cfg_weight: float) -> bytes:
    cliente = await _get_cliente()
    loop    = asyncio.get_event_loop()

    def _llamar():
        # Intentar con el endpoint correcto del Space de Chatterbox
        # El Space ResembleAI/Chatterbox usa estos parámetros posicionales:
        # 1: text (str)
        # 2: audio_prompt (file | None)  
        # 3: exaggeration (float)
        # 4: cfg_weight (float)
        # 5: seed (int) — opcional
        try:
            # Intento 1: endpoint /generate con parámetros nombrados
            resultado = cliente.predict(
                texto,      # text
                None,       # audio_prompt (sin clonar voz)
                exaggeration,
                cfg_weight,
                0,          # seed = 0 (aleatorio)
                api_name="/generate"
            )
            print(f"✅ [TTS] Chatterbox respondió: {type(resultado)}")
        except Exception as e1:
            print(f"⚠️ [TTS] Intento 1 falló ({e1}), probando endpoint alternativo...")
            try:
                # Intento 2: sin nombre de endpoint (usa el primero disponible)
                resultado = cliente.predict(
                    texto,
                    None,
                    exaggeration,
                    cfg_weight,
                    0,
                )
                print(f"✅ [TTS] Chatterbox respondió (intento 2): {type(resultado)}")
            except Exception as e2:
                raise Exception(f"Ambos intentos fallaron. E1: {e1} | E2: {e2}")

        # El Space devuelve la ruta al archivo WAV temporal
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

    estilos = {
        "normal":       {"exaggeration": 0.35, "cfg_weight": 0.30},
        "bienvenida":   {"exaggeration": 0.50, "cfg_weight": 0.35},
        "alerta":       {"exaggeration": 0.60, "cfg_weight": 0.40},
        "celebracion":  {"exaggeration": 0.65, "cfg_weight": 0.38},
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
