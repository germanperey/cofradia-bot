"""
╔══════════════════════════════════════════════════════════════════╗
║   TTS CHATTERBOX — vía Gradio Space de Hugging Face             ║
║   Corrección: archivo WAV enviado con handle_file()             ║
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
HF_TOKEN      = os.getenv("HF_TOKEN", "")
HF_SPACE      = "ResembleAI/Chatterbox"
TTS_VOICE_URL = os.getenv("TTS_VOICE_URL", "")

CACHE_DIR      = Path(os.getenv("TTS_CACHE_DIR", "/tmp/tts_cache_v3"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE      = os.getenv("TTS_USE_CACHE", "true").lower() == "true"
VOICE_REF_PATH = Path("/tmp/cofradia_voz_ref_v3.wav")

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


def _descargar_voz_referencia() -> Optional[str]:
    """
    Descarga el WAV de referencia desde TTS_VOICE_URL.
    Retorna la ruta local o None si falla.
    """
    if VOICE_REF_PATH.exists() and VOICE_REF_PATH.stat().st_size > 5000:
        print(f"✅ [TTS] Voz de referencia ya en disco ({VOICE_REF_PATH.stat().st_size} bytes)")
        return str(VOICE_REF_PATH)

    if not TTS_VOICE_URL:
        print("⚠️ [TTS] TTS_VOICE_URL no configurada")
        return None

    try:
        import requests
        print(f"⬇️  [TTS] Descargando voz desde: {TTS_VOICE_URL}")
        r = requests.get(TTS_VOICE_URL, timeout=20)
        if r.status_code == 200 and len(r.content) > 5000:
            VOICE_REF_PATH.write_bytes(r.content)
            print(f"✅ [TTS] Voz descargada: {len(r.content)} bytes → {VOICE_REF_PATH}")
            return str(VOICE_REF_PATH)
        else:
            print(f"❌ [TTS] Descarga falló: status={r.status_code} size={len(r.content)}")
            return None
    except Exception as e:
        print(f"❌ [TTS] Error descargando voz: {e}")
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
    # Descargar la voz de referencia
    voz_path = _descargar_voz_referencia()
    cliente  = await _get_cliente()
    loop     = asyncio.get_event_loop()

    def _llamar():
        from gradio_client import handle_file

        # Preparar el audio prompt correctamente
        # handle_file() es la forma correcta de enviar archivos a Gradio
        if voz_path and Path(voz_path).exists():
            audio_prompt = handle_file(voz_path)
            print(f"🎙️ [TTS] Enviando voz de referencia con handle_file()")
        else:
            audio_prompt = None
            print(f"⚠️ [TTS] Sin voz de referencia — Chatterbox usará voz aleatoria")

        print(f"🚀 [TTS] Generando audio | exag={exaggeration} | cfg={cfg_weight}")

        try:
            resultado = cliente.predict(
                texto,
                audio_prompt,   # ← handle_file() en vez de string
                exaggeration,
                cfg_weight,
                0,              # seed
                api_name="/generate"
            )
            print(f"✅ [TTS] Respuesta recibida: {type(resultado)}")
        except Exception as e1:
            print(f"⚠️ [TTS] Reintentando sin api_name... ({e1})")
            resultado = cliente.predict(
                texto,
                audio_prompt,
                exaggeration,
                cfg_weight,
                0,
            )

        ruta = Path(resultado[0]) if isinstance(resultado, (list, tuple)) else Path(resultado)
        print(f"✅ [TTS] Audio generado: {ruta}")
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
    if not texto or not texto.strip():
        return None

    texto_limpio = _limpiar_texto(texto)
    if not texto_limpio:
        return None

    print(f"🎤 [TTS] HF_TOKEN={'SÍ' if HF_TOKEN else 'NO'} | VOICE_URL={'SÍ' if TTS_VOICE_URL else 'NO'}")
    logger.info(f"🎤 [TTS] HF_TOKEN={'SÍ' if HF_TOKEN else 'NO'} | VOICE_URL={'SÍ' if TTS_VOICE_URL else 'NO'}")

    # cfg_weight=0 → Chatterbox sigue ÚNICAMENTE la voz de referencia
    # Esto es lo que recomienda la documentación oficial de Chatterbox
    # para preservar el acento y estilo de la voz de referencia al 100%
    # exaggeration=0.3 → natural, sin dramatismo excesivo
    estilos = {
        "normal":       {"exaggeration": 0.30, "cfg_weight": 0.0},
        "bienvenida":   {"exaggeration": 0.38, "cfg_weight": 0.0},
        "alerta":       {"exaggeration": 0.45, "cfg_weight": 0.0},
        "celebracion":  {"exaggeration": 0.50, "cfg_weight": 0.0},
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
            # Borrar voz de referencia para que se re-descargue la próxima vez
            if VOICE_REF_PATH.exists():
                VOICE_REF_PATH.unlink()
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
    if VOICE_REF_PATH.exists():
        VOICE_REF_PATH.unlink()
    logger.info(f"🗑️ Caché TTS: {borrados} archivos eliminados.")
