"""
TTS — Google Cloud Text-to-Speech
Voz: es-US-Neural2-A (femenina, latinoamericana, natural)
Gratuito: 1,000,000 caracteres/mes

FASE 7 (NUEVO, OPCIONAL): SSML profesional para mayor naturalidad.
- Activacion: env var GOOGLE_TTS_SSML=true (default: false, mantiene comportamiento v91)
- Mejoras al activarlo: pausas naturales, abreviaturas chilenas (UF, IPC, TPM),
  números/montos pronunciados correctamente, énfasis en saludos.
- Si SSML falla, fallback automatico a texto plano sin que el usuario lo note.
- Cache blindado: cada texto+voz+config se cachea para minimizar llamadas a Google.
"""
import os, io, logging, asyncio, hashlib, base64, requests, re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

GOOGLE_TTS_KEY = os.getenv("GOOGLE_TTS_KEY", "")
GOOGLE_TTS_URL = "https://texttospeech.googleapis.com/v1/text:synthesize"
# es-US-Wavenet-A: voz femenina más cálida y natural que Neural2
VOICE_NAME     = os.getenv("GOOGLE_TTS_VOICE", "es-US-Wavenet-A")
VOICE_LANGUAGE = "es-US"
SPEAKING_RATE  = float(os.getenv("GOOGLE_TTS_RATE",  "0.85"))  # más pausada = más natural
PITCH          = float(os.getenv("GOOGLE_TTS_PITCH", "-4.0"))  # más grave = más cálida

# FASE 7: SSML toggle ROBUSTO — acepta multiples valores afirmativos
# Antes: solo "true" minusculas matcheaba. Ahora acepta: true/True/TRUE/1/yes/si/on
_ssml_raw = os.getenv("GOOGLE_TTS_SSML", "false").strip().lower()
USE_SSML = _ssml_raw in ("true", "1", "yes", "si", "on", "y", "s")

# LOG INMEDIATO al cargar el modulo: visible al iniciar bot
print(f"━━━ [TTS_CHATTERBOX] CONFIG ━━━")
print(f"  GOOGLE_TTS_KEY: {'CONFIGURADA' if GOOGLE_TTS_KEY else 'NO CONFIGURADA'}")
print(f"  VOICE_NAME    : {VOICE_NAME}")
print(f"  SPEAKING_RATE : {SPEAKING_RATE}")
print(f"  PITCH         : {PITCH}")
print(f"  GOOGLE_TTS_SSML (raw env): '{os.getenv('GOOGLE_TTS_SSML', '<no_set>')}'")
print(f"  USE_SSML detectado: {USE_SSML}  {'✅ SSML ACTIVADO' if USE_SSML else '⚠️ SSML DESACTIVADO (modo v91 plano)'}")
print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
logger.info(f"[TTS] SSML={USE_SSML} (env raw='{os.getenv('GOOGLE_TTS_SSML', '<no_set>')}')")

CACHE_DIR = Path(os.getenv("TTS_CACHE_DIR", "/tmp/tts_cache_gtts"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE = os.getenv("TTS_USE_CACHE", "true").lower() == "true"


def _cache_key(texto: str) -> str:
    # FASE 7: incluir SSML en clave para no mezclar caches
    return hashlib.md5(f"{texto}_{VOICE_NAME}_{SPEAKING_RATE}_{PITCH}_{USE_SSML}".encode()).hexdigest()


def _limpiar_texto(texto: str) -> str:
    texto = re.sub(r"[*_`]", "", texto)
    texto = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", texto)
    texto = re.sub(r"[^\w\s,.!?;:()\-áéíóúüñÁÉÍÓÚÜÑ%$/]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def _texto_a_ssml(texto: str) -> str:
    """FASE 7: Convierte texto plano a SSML enriquecido para Google TTS.
    
    Aplica:
    - Expansión de abreviaturas chilenas (UF→"U F", IPC→"I P C", etc.)
    - Pronunciación correcta de números/montos con <say-as>
    - Pausas naturales en puntuación (<break>)
    - Énfasis cálido en saludos iniciales
    """
    if not texto:
        return texto
    
    # 1. ESCAPE caracteres XML (importante!)
    t = texto.replace('&', ' y ').replace('<', '').replace('>', '')
    
    # 2. EXPANSIONES DE ABREVIATURAS CHILENAS
    abreviaturas = [
        (r'\bUF\b', 'U F'),
        (r'\bCLP\b', 'pesos chilenos'),
        (r'\bUSD\b', 'dólares'),
        (r'\bEUR\b', 'euros'),
        (r'\bUTM\b', 'U T M'),
        (r'\bIPC\b', 'I P C'),
        (r'\bTPM\b', 'T P M'),
        (r'\bAFP\b', 'A F P'),
        (r'\bAPV\b', 'A P V'),
        (r'\bTMC\b', 'T M C'),
        (r'\bCMF\b', 'C M F'),
        (r'\bINE\b', 'I N E'),
        (r'\bBCCh\b', 'Banco Central'),
        (r'\bSII\b', 'S I I'),
        (r'\bIVA\b', 'IVA'),
        (r'\bPYME[Ss]?\b', 'PYMES'),
        (r'\bRUT\b', 'RUT'),
    ]
    for patt, repl in abreviaturas:
        t = re.sub(patt, repl, t)
    
    # 3. NÚMEROS Y MONTOS — usar <say-as> para pronunciación correcta
    def _format_monto(m):
        num = m.group(1).replace('.', '').replace(',', '')
        return f'<say-as interpret-as="cardinal">{num}</say-as> pesos'
    t = re.sub(r'\$\s*([\d.,]+)', _format_monto, t)
    
    def _format_pct(m):
        num = m.group(1).replace(',', '.')
        try:
            return f'<say-as interpret-as="cardinal">{num}</say-as> por ciento'
        except Exception:
            return m.group(0)
    t = re.sub(r'(\d+[,\.]?\d*)\s*%', _format_pct, t)
    
    # 4. PAUSAS NATURALES con <break>
    t = t.replace('. ', '. <break time="500ms"/> ')
    t = t.replace(': ', ': <break time="400ms"/> ')
    t = t.replace('; ', '; <break time="400ms"/> ')
    t = t.replace(', ', ', <break time="200ms"/> ')
    t = t.replace('? ', '? <break time="500ms"/> ')
    t = t.replace('! ', '! <break time="500ms"/> ')
    
    # 5. ÉNFASIS CÁLIDO en saludos iniciales
    if any(t.startswith(s) for s in ('Hola', 'Estimado', 'Buenos', '¡Hola', '¡Buenos')):
        first_period = t.find('.')
        if 5 < first_period < 100:
            t = f'<prosody rate="0.95" pitch="+1st">{t[:first_period+1]}</prosody>{t[first_period+1:]}'
    
    # 6. WRAP en <speak> con prosody global
    ssml = (
        f'<speak>'
        f'<prosody rate="{SPEAKING_RATE}" pitch="{int(PITCH)}st">'
        f'{t}'
        f'</prosody>'
        f'</speak>'
    )
    return ssml


def _llamar_google_tts(texto: str) -> Optional[bytes]:
    """FASE 7: Si USE_SSML=true, intenta SSML primero. Fallback automatico a texto plano."""
    url = f"{GOOGLE_TTS_URL}?key={GOOGLE_TTS_KEY}"
    
    # FASE 7: Intentar SSML si está activado
    if USE_SSML:
        try:
            ssml = _texto_a_ssml(texto)
            payload_ssml = {
                "input": {"ssml": ssml},
                "voice": {"languageCode": VOICE_LANGUAGE, "name": VOICE_NAME},
                "audioConfig": {
                    "audioEncoding":    "MP3",
                    "speakingRate":     SPEAKING_RATE,
                    "pitch":            PITCH,
                    "volumeGainDb":     1.0,
                    "effectsProfileId": ["headphone-class-device"],
                }
            }
            r = requests.post(url, json=payload_ssml, timeout=15)
            if r.status_code == 200:
                audio = base64.b64decode(r.json().get("audioContent", ""))
                if audio:
                    print(f"✅ [TTS-SSML] Google {VOICE_NAME} OK ({len(audio):,} bytes)")
                    logger.info(f"✅ [TTS-SSML] Google {VOICE_NAME} OK ({len(audio):,} bytes)")
                    return audio
            # SSML rechazado → caer a texto plano sin alarmar
            print(f"⚠️ [TTS-SSML] Google rechazó SSML ({r.status_code}), usando texto plano")
            logger.warning(f"Google TTS SSML rechazado {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"⚠️ [TTS-SSML] error: {e}, usando texto plano")
            logger.warning(f"Google TTS SSML error: {e}")
    
    # MODO TEXTO PLANO (default v91 si SSML=false, o fallback si SSML falló)
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
                print(f"✅ [TTS] Google {VOICE_NAME} OK ({len(audio):,} bytes)")
                logger.info(f"✅ [TTS] Google {VOICE_NAME} OK ({len(audio):,} bytes)")
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

    print(f"🎤 [TTS] GOOGLE_TTS_KEY={'SÍ' if GOOGLE_TTS_KEY else 'NO'} | voz={VOICE_NAME} | SSML={USE_SSML}")
    logger.info(f"🎤 [TTS] GOOGLE_TTS_KEY={'SÍ' if GOOGLE_TTS_KEY else 'NO'} | SSML={USE_SSML}")

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
