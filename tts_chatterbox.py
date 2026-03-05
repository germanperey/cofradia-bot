"""
╔══════════════════════════════════════════════════════════════════╗
║   TTS — Google Cloud Text-to-Speech                             ║
║   Voz: es-US-Neural2-A (femenina, latinoamericana, natural)     ║
║   Gratuito: 1,000,000 caracteres/mes para siempre               ║
║   Fallback: edge-TTS Catalina mejorada                          ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import io
import logging
import asyncio
import hashlib
import base64
import requests
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ──────────────────────────────────────────────────
GOOGLE_TTS_KEY = os.getenv("GOOGLE_TTS_KEY", "")
GOOGLE_TTS_URL = "https://texttospeech.googleapis.com/v1/text:synthesize"

# Voz seleccionada: femenina, español latinoamericano, ultra-natural
# Opciones disponibles (todas gratuitas 1M chars/mes):
#   es-US-Neural2-A  → mujer, neutro latinoamericano ← RECOMENDADA
#   es-US-Neural2-C  → mujer, tono más cálido
#   es-US-Wavenet-A  → mujer, muy natural
#   es-US-Wavenet-F  → mujer, tono suave
VOICE_NAME     = os.getenv("GOOGLE_TTS_VOICE", "es-US-Neural2-A")
VOICE_LANGUAGE = "es-US"
SPEAKING_RATE  = float(os.getenv("GOOGLE_TTS_RATE", "0.92"))  # 0.92 = levemente más pausada
PITCH          = float(os.getenv("GOOGLE_TTS_PITCH", "-1.5")) # -1.5 = tono ligeramente más cálido

CACHE_DIR = Path(os.getenv("TTS_CACHE_DIR", "/tmp/tts_cache_gtts"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE = os.getenv("TTS_USE_CACHE", "true").lower() == "true"


def _cache_key(texto: str) -> str:
    return hashlib.md5(f"{texto}_{VOICE_NAME}_{SPEAKING_RATE}_{PITCH}".encode()).hexdigest()


def _limpiar_texto(texto: str) -> str:
    import re
    texto = re.sub(r"[*_`]", "", texto)
    texto = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", texto)
    texto = re.sub(r"[^\w\s,.!?;:()\-áéíóúüñÁÉÍÓÚÜÑ]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def _llamar_google_tts(texto: str) -> Optional[bytes]:
    """
    Llama a Google Cloud TTS y retorna bytes de audio MP3.
    Gratuito hasta 1,000,000 caracteres/mes (WaveNet/Neural2).
    """
    url = f"{GOOGLE_TTS_URL}?key={GOOGLE_TTS_KEY}"
    payload = {
        "input": {"text": texto},
        "voice": {
            "languageCode": VOICE_LANGUAGE,
            "name": VOICE_NAME,
        },
        "audioConfig": {
            "audioEncoding": "MP3",
            "speakingRate": SPEAKING_RATE,
            "pitch": PITCH,
            "effectsProfileId": ["headphone-class-device"],
        }
    }
    try:
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code == 200:
            data = response.json()
            audio_b64 = data.get("audioContent", "")
            if audio_b64:
                audio_bytes = base64.b64decode(audio_b64)
                print(f"✅ [TTS] Google Neural2 generó {len(audio_bytes):,} bytes")
                logger.info(f"✅ [TTS] Google Neural2 OK ({len(audio_bytes):,} bytes)")
                return audio_bytes
        else:
            print(f"❌ [TTS] Google TTS error {response.status_code}: {response.text[:200]}")
            logger.warning(f"Google TTS error {response.status_code}: {response.text[:200]}")
    except Exception as e:
        print(f"❌ [TTS] Google TTS excepción: {e}")
        logger.warning(f"Google TTS excepción: {e}")
    return None


async def _edge_tts_fallback(texto: str) -> Optional[bytes]:
    """Fallback a Catalina mejorada si Google TTS no está disponible."""
    try:
        import edge_tts
        communicate = edge_tts.Communicate(
            texto,
            "es-CL-CatalinaNeural",
            rate   = "-10%",
            pitch  = "-3Hz",
            volume = "+8%"
        )
        buf = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                buf.write(chunk["data"])
        print("🔈 [TTS] Fallback: Catalina mejorada")
        logger.info("🔈 [TTS] Fallback: Catalina mejorada")
        return buf.getvalue()
    except Exception as e:
        logger.error(f"edge-TTS falló: {e}")
        return None


async def texto_a_voz(
    texto: str,
    estilo: str = "normal",
    usar_chatterbox: bool = True   # parámetro mantenido por compatibilidad
) -> Optional[bytes]:
    """
    Función principal de TTS.
    Usa Google Cloud Neural2 (voz latinoamericana natural).
    Fallback a Catalina si GOOGLE_TTS_KEY no está configurada.
    """
    if not texto or not texto.strip():
        return None

    texto_limpio = _limpiar_texto(texto)
    if not texto_limpio:
        return None

    # Limitar largo (Google TTS acepta hasta 5,000 chars)
    if len(texto_limpio) > 4500:
        texto_limpio = texto_limpio[:4497] + "..."

    print(f"🎤 [TTS] GOOGLE_TTS_KEY={'SÍ' if GOOGLE_TTS_KEY else 'NO'} | voz={VOICE_NAME}")
    logger.info(f"🎤 [TTS] GOOGLE_TTS_KEY={'SÍ' if GOOGLE_TTS_KEY else 'NO'}")

    # Revisar caché
    if USE_CACHE:
        clave   = _cache_key(texto_limpio)
        f_cache = CACHE_DIR / f"{clave}.mp3"
        if f_cache.exists():
            print("🎵 [TTS] Audio desde caché")
            return f_cache.read_bytes()

    if GOOGLE_TTS_KEY:
        # Ejecutar en thread para no bloquear asyncio
        loop = asyncio.get_event_loop()
        audio = await loop.run_in_executor(None, _llamar_google_tts, texto_limpio)
        if audio:
            if USE_CACHE:
                f_cache.write_bytes(audio)
            return audio
        else:
            print("⚠️ [TTS] Google TTS falló → Catalina mejorada")
            return await _edge_tts_fallback(texto_limpio)
    else:
        print("⏭️ [TTS] Sin GOOGLE_TTS_KEY → Catalina mejorada")
        logger.warning("⚠️ GOOGLE_TTS_KEY no configurada — usando edge-TTS")
        return await _edge_tts_fallback(texto_limpio)


async def limpiar_cache_tts(dias: int = 7):
    """Limpia audios del caché más antiguos que N días."""
    import time
    ahora    = time.time()
    borrados = 0
    for f in CACHE_DIR.glob("*.mp3"):
        if ahora - f.stat().st_mtime > dias * 86400:
            f.unlink()
            borrados += 1
    logger.info(f"🗑️ Caché TTS: {borrados} archivos eliminados.")
