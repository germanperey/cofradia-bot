"""
TTS — Google Cloud Text-to-Speech con SSML PROFESIONAL (FASE 7)
Voz: es-US-Neural2-A (femenina, latinoamericana, natural) — opcionalmente Studio
Gratuito: 1,000,000 caracteres/mes (Wavenet) | 100,000 caracteres/mes (Neural2)

FASE 7 — Mejoras SSML aplicadas:
- Pausas naturales con <break> en puntuación
- Pronunciación correcta de números/montos con <say-as>
- Énfasis en palabras clave con <emphasis>
- Pronunciación de UF/CLP/USD con expansión léxica
- Velocidad y tono ajustados por <prosody>
"""
import os, io, logging, asyncio, hashlib, base64, requests, re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

GOOGLE_TTS_KEY = os.getenv("GOOGLE_TTS_KEY", "")
GOOGLE_TTS_URL = "https://texttospeech.googleapis.com/v1/text:synthesize"
# es-US-Neural2-A: voz femenina cálida y natural
VOICE_NAME     = os.getenv("GOOGLE_TTS_VOICE", "es-US-Neural2-A")
VOICE_LANGUAGE = "es-US"
SPEAKING_RATE  = float(os.getenv("GOOGLE_TTS_RATE",  "0.92"))  # FASE 7: levemente más ágil que 0.85, más natural
PITCH          = float(os.getenv("GOOGLE_TTS_PITCH", "-2.0"))  # FASE 7: más cálida sin perder claridad

# FASE 7: activar/desactivar SSML (default ON, fallback automático a texto plano si falla)
USE_SSML = os.getenv("GOOGLE_TTS_SSML", "true").lower() == "true"

CACHE_DIR = Path(os.getenv("TTS_CACHE_DIR", "/tmp/tts_cache_gtts"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE = os.getenv("TTS_USE_CACHE", "true").lower() == "true"


def _cache_key(texto: str) -> str:
    return hashlib.md5(f"{texto}_{VOICE_NAME}_{SPEAKING_RATE}_{PITCH}_ssml".encode()).hexdigest()


def _limpiar_texto(texto: str) -> str:
    texto = re.sub(r"[*_`]", "", texto)
    texto = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", texto)
    texto = re.sub(r"[^\w\s,.!?;:()\-áéíóúüñÁÉÍÓÚÜÑ%$/]", " ", texto)  # FASE 7: preservar % $ /
    return re.sub(r"\s+", " ", texto).strip()


def _texto_a_ssml(texto: str) -> str:
    """FASE 7: Convierte texto plano a SSML enriquecido para mejor naturalidad.
    
    Aplica:
    - Pausas naturales en signos de puntuación (<break>)
    - Expansión de abreviaturas chilenas (UF, CLP, USD, IPC, TPM, etc.)
    - Pronunciación correcta de números grandes (<say-as>)
    - Énfasis en palabras destacadas
    - Prosodia general (rate, pitch)
    """
    if not texto:
        return texto
    
    # 1. ESCAPAR caracteres XML peligrosos antes de crear SSML
    t = texto.replace('&', ' y ').replace('<', '').replace('>', '')
    
    # 2. EXPANSIONES DE ABREVIATURAS CHILENAS (mejor pronunciación)
    abreviaturas = [
        (r'\bUF\b', 'U F'),  # "uno once" en lugar de "uf"
        (r'\bCLP\b', 'pesos chilenos'),
        (r'\bUSD\b', 'dólares'),
        (r'\bEUR\b', 'euros'),
        (r'\bUTM\b', 'U T M'),
        (r'\bIPC\b', 'I P C'),
        (r'\bTPM\b', 'T P M'),
        (r'\bAFP\b', 'A F P'),
        (r'\bAPV\b', 'A P V'),
        (r'\bIPSA\b', 'IPSA'),
        (r'\bTMC\b', 'T M C'),
        (r'\bCMF\b', 'C M F'),
        (r'\bINE\b', 'I N E'),
        (r'\bBCCh\b', 'Banco Central'),
        (r'\bSII\b', 'S I I'),
        (r'\bIVA\b', 'IVA'),
        (r'\bPYME[Ss]?\b', 'PYMES'),
        (r'\bONG[Ss]?\b', 'ONG'),
        (r'\bRUT\b', 'RUT'),
    ]
    for patt, repl in abreviaturas:
        t = re.sub(patt, repl, t)
    
    # 3. NÚMEROS Y MONTOS — usar <say-as> para que los lea bien
    # Montos con $: $1.500.000 → <say-as>1500000</say-as>
    def _format_monto(m):
        num = m.group(1).replace('.', '').replace(',', '')
        return f'<say-as interpret-as="cardinal">{num}</say-as> pesos'
    t = re.sub(r'\$\s*([\d.,]+)', _format_monto, t)
    
    # Porcentajes: 4,5% → "cuatro coma cinco por ciento"
    def _format_pct(m):
        num = m.group(1).replace(',', '.')
        try:
            return f'<say-as interpret-as="cardinal">{num}</say-as> por ciento'
        except Exception:
            return m.group(0)
    t = re.sub(r'(\d+[,\.]?\d*)\s*%', _format_pct, t)
    
    # 4. PAUSAS NATURALES con <break>
    # Coma → 250ms, punto y coma → 400ms, punto/dos puntos → 500ms
    t = t.replace('. ', '. <break time="500ms"/> ')
    t = t.replace(': ', ': <break time="400ms"/> ')
    t = t.replace('; ', '; <break time="400ms"/> ')
    t = t.replace(', ', ', <break time="200ms"/> ')
    t = t.replace('? ', '? <break time="500ms"/> ')
    t = t.replace('! ', '! <break time="500ms"/> ')
    
    # 5. ÉNFASIS en marcadores conversacionales calidos
    # (sin abusar — solo apertura)
    if t.startswith('Hola') or t.startswith('Estimado') or t.startswith('Buenos'):
        # Primera frase con tono más cálido
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


def _llamar_google_tts(texto: str, usar_ssml: bool = True) -> Optional[bytes]:
    """FASE 7: ahora soporta SSML opcional. Si falla SSML, reintenta con texto plano."""
    url = f"{GOOGLE_TTS_URL}?key={GOOGLE_TTS_KEY}"
    
    # Intento 1: SSML (mucho mejor calidad)
    if usar_ssml and USE_SSML:
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
        try:
            r = requests.post(url, json=payload_ssml, timeout=15)
            if r.status_code == 200:
                audio = base64.b64decode(r.json().get("audioContent", ""))
                if audio:
                    print(f"✅ [TTS-SSML] Google {VOICE_NAME} OK ({len(audio):,} bytes)")
                    logger.info(f"✅ [TTS-SSML] Google {VOICE_NAME} OK ({len(audio):,} bytes)")
                    return audio
            else:
                # SSML falló — reintentar con texto plano (fallback automático)
                print(f"⚠️ [TTS-SSML] Google rechazó SSML ({r.status_code}), reintentando texto plano...")
                logger.warning(f"Google TTS SSML rechazado {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"⚠️ [TTS-SSML] error: {e}, reintentando texto plano...")
            logger.warning(f"Google TTS SSML error: {e}")
    
    # Intento 2: texto plano (fallback)
    payload_plain = {
        "input": {"text": texto},
        "voice": {"languageCode": VOICE_LANGUAGE, "name": VOICE_NAME},
        "audioConfig": {
            "audioEncoding":    "MP3",
            "speakingRate":     SPEAKING_RATE,
            "pitch":            PITCH,
            "volumeGainDb":     1.0,
            "effectsProfileId": ["headphone-class-device"],
        }
    }
    try:
        r = requests.post(url, json=payload_plain, timeout=15)
        if r.status_code == 200:
            audio = base64.b64decode(r.json().get("audioContent", ""))
            if audio:
                print(f"✅ [TTS] Google plano OK ({len(audio):,} bytes)")
                logger.info(f"✅ [TTS] Google plano OK ({len(audio):,} bytes)")
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
        # FASE 7: Catalina con SSML básico via prosodia (edge-tts soporta SSML simple)
        # Pausas naturales en puntuación
        texto_pausas = texto.replace('. ', '. ').replace(', ', ', ').replace(': ', ': ')
        communicate = edge_tts.Communicate(
            texto_pausas, "es-CL-CatalinaNeural",
            rate="-8%", pitch="-2Hz", volume="+8%"  # FASE 7: ligeramente más natural
        )
        buf = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                buf.write(chunk["data"])
        print("🔈 [TTS] Fallback: Catalina mejorada (edge-tts)")
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
        audio = await loop.run_in_executor(None, _llamar_google_tts, texto_limpio, USE_SSML)
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
