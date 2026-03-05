"""
TTS — Google Cloud Text-to-Speech
Voz: es-US-Neural2-A (femenina, latinoamericana, natural)
Gratuito: 1,000,000 caracteres/mes
"""
import os, io, logging, asyncio, hashlib, base64, requests
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

GOOGLE_TTS_KEY = os.getenv("GOOGLE_TTS_KEY", "")
GOOGLE_TTS_URL = "https://texttospeech.googleapis.com/v1/text:synthesize"
VOICE_NAME     = os.getenv("GOOGLE_TTS_VOICE", "es-US-Neural2-A")
VOICE_LANGUAGE = "es-US"
SPEAKING_RATE  = float(os.getenv("GOOGLE_TTS_RATE",  "0.90"))
PITCH          = float(os.getenv("GOOGLE_TTS_PITCH", "-2.0"))

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
    url = f"{GOOGLE_TTS_URL}?key={GOOGLE_TTS_KEY}"
    payload = {
        "input": {"text": texto},
        "voice": {
            "languageCode": VOICE_LANGUAGE,
            "name": VOICE_NAME,
        },
        "audioConfig": {
            "audioEncoding":    "MP3",
            "speakingRate":     SPEAKING_RATE,
            "pitch":            PITCH,
            "volumeGainDb":     1.0,
            "effectsProfileId": ["headphone-class-device"],
        }
    }
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            audio = base64.b64decode(r.json().get("audioContent", ""))
            if audio:
                print(f"✅ [TTS] Google Neural2 OK ({len(audio):,} bytes)")
                logger.info(f"✅ [TTS] Google Neural2 OK ({len(audio):,} bytes)")
                return audio
        else:
            print(f"❌ [TTS] Google TTS {r.status_code}: {r.text[:200]}")
            logger.warning(f"Google TTS {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"❌ [TTS] Google TTS error: {e}")
        logger.warning(f"Google TTS error: {e}")
    return None


async def _edge_tts_fallback(texto: str) -> Optional[bytes]:
    try:
        import edge_tts
        communicate = edge_tts.Communicate(
            texto, "es-CL-CatalinaNeural",
            rate="-10%", pitch="-3Hz", volume="+8%"
        )
        buf = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                buf.write(chunk["data"])
        print("🔈 [TTS] Fallback: Catalina mejorada")
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
    if len(texto_limpio) > 4500:
        texto_limpio = texto_limpio[:4497] + "..."

    print(f"🎤 [TTS] GOOGLE_TTS_KEY={'SÍ' if GOOGLE_TTS_KEY else 'NO'} | voz={VOICE_NAME}")
    logger.info(f"🎤 [TTS] GOOGLE_TTS_KEY={'SÍ' if GOOGLE_TTS_KEY else 'NO'}")

    if USE_CACHE:
        f_cache = CACHE_DIR / f"{_cache_key(texto_limpio)}.mp3"
        if f_cache.exists():
            print("🎵 [TTS] Audio desde caché")
            return f_cache.read_bytes()

    if GOOGLE_TTS_KEY:
        loop  = asyncio.get_event_loop()
        audio = await loop.run_in_executor(None, _llamar_google_tts, texto_limpio)
        if audio:
            if USE_CACHE:
                f_cache.write_bytes(audio)
            return audio
        return await _edge_tts_fallback(texto_limpio)
    else:
        print("⏭️ [TTS] Sin GOOGLE_TTS_KEY → Catalina")
        logger.warning("⚠️ GOOGLE_TTS_KEY no configurada")
        return await _edge_tts_fallback(texto_limpio)


async def limpiar_cache_tts(dias: int = 7):
    import time
    ahora, borrados = time.time(), 0
    for f in CACHE_DIR.glob("*.mp3"):
        if ahora - f.stat().st_mtime > dias * 86400:
            f.unlink(); borrados += 1
    logger.info(f"🗑️ Caché TTS: {borrados} eliminados.")
