"""
TTS — Google Cloud Text-to-Speech
Voz: es-US-Neural2-A (femenina, latinoamericana, natural)
Gratuito: 1,000,000 caracteres/mes

FASE 11 (FIX CRITICO SSML): Resuelve problemas de SSML detectados:
- Eliminado prosody ANIDADO (Google rechazaba el SSML silenciosamente)
- audioConfig.speakingRate/pitch NO se duplica con SSML (causaba doble efecto y
  rechazos)
- effectsProfileId solo se usa en modo texto plano (incompatible con SSML en
  algunos casos)
- Voz default Neural2-A (mejor para SSML que Wavenet)
- Si SSML falla, fallback automatico a texto plano sin que el usuario lo note
- Cache blindado: cada texto+voz+config se cachea para minimizar llamadas a Google
"""
import os, io, logging, asyncio, hashlib, base64, requests, re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

GOOGLE_TTS_KEY = os.getenv("GOOGLE_TTS_KEY", "")
GOOGLE_TTS_URL = "https://texttospeech.googleapis.com/v1/text:synthesize"
# FASE 12 (USUARIO ELIGIÓ): Default Neural2-C masculina, la voz más natural y cálida
# para Cofradía según pruebas comparativas del comando /test_tts.
# Otras opciones disponibles cambiando GOOGLE_TTS_VOICE en Render:
#   - es-US-Neural2-A: femenina latina natural
#   - es-US-Neural2-C: MASCULINA cálida (default actual elegido por usuario) ✅
#   - es-US-Neural2-B: masculina alternativa
#   - es-ES-Neural2-D: femenina con acento España
# IMPORTANTE: Studio voices NO existen en español todavía. Para máxima naturalidad
# en español, Neural2 es la opción más alta. Wavenet sonaba más robótica.
VOICE_NAME     = os.getenv("GOOGLE_TTS_VOICE", "es-US-Neural2-C")
VOICE_LANGUAGE = os.getenv("GOOGLE_TTS_LANG", "es-US")
# FASE 12: Parámetros ajustados a valores que Google TTS pronuncia con mayor naturalidad
# Velocidad 1.0 = velocidad normal humana. 0.95 = ligeramente pausada (recomendado para naturalidad)
SPEAKING_RATE  = float(os.getenv("GOOGLE_TTS_RATE",  "0.95"))
# Pitch 0 = neutral. Valores negativos = más grave; positivos = más agudo.
# FASE 12 (USUARIO ELIGIÓ Neural2-C masculina): pitch 0.0 = tono natural masculino.
# Si quieres ajustar, cambia GOOGLE_TTS_PITCH en Render:
#   0.0 = natural (recomendado para Neural2-C)
#   +1.0 a +2.0 = ligeramente más agudo (suaviza voz masculina)
#   -1.0 a -2.0 = más grave (autoritario)
PITCH          = float(os.getenv("GOOGLE_TTS_PITCH", "0.0"))

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

# FASE 12: VERSION_TAG — invalida automaticamente caches antiguos
# Cada vez que se cambia este valor, el _cache_key generara hashes nuevos
# y los audios viejos (Wavenet, sin SSML, etc) NO se reusan.
TTS_VERSION_TAG = "v12-neural2-ssml-2026-05-03"

# FASE 12: AUTO-PURGAR caches antiguos al iniciar (los del sistema viejo)
# Esto FUERZA que la primera vez genere audio nuevo con Neural2 + SSML
try:
    import time as _t_purge
    _ahora_purge = _t_purge.time()
    _purgados = 0
    for _f in CACHE_DIR.glob("*.mp3"):
        # Si el archivo es de antes del deploy de Fase 12 → eliminar
        if _ahora_purge - _f.stat().st_mtime > 0:  # cualquier archivo previo
            try:
                _f.unlink()
                _purgados += 1
            except Exception:
                pass
    if _purgados > 0:
        print(f"🗑️ [TTS] Caché viejo purgado: {_purgados} archivos eliminados")
        logger.info(f"🗑️ [TTS] Caché viejo purgado: {_purgados} archivos al iniciar")
except Exception as _e_purge:
    pass


def _cache_key(texto: str) -> str:
    # FASE 12: incluir VERSION_TAG en clave para invalidar caches viejos
    return hashlib.md5(
        f"{TTS_VERSION_TAG}_{texto}_{VOICE_NAME}_{SPEAKING_RATE}_{PITCH}_{USE_SSML}".encode()
    ).hexdigest()


def _limpiar_texto(texto: str) -> str:
    texto = re.sub(r"[*_`]", "", texto)
    texto = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", texto)
    texto = re.sub(r"[^\w\s,.!?;:()\-áéíóúüñÁÉÍÓÚÜÑ%$/]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def _texto_a_ssml(texto: str) -> str:
    """FASE 11: SSML SIMPLIFICADO Y ROBUSTO para Google TTS.
    
    SOLUCIONA los 4 problemas que causaban rechazo silencioso:
    - SIN prosody anidado (causaba rechazo en algunos casos)
    - SIN tags experimentales <emphasis> 
    - <break> y <say-as> son los más confiables
    - Escape XML correcto de & < > " '
    
    Aplica:
    - Expansión de abreviaturas chilenas (UF→"U F", IPC→"I P C", etc.)
    - Pronunciación correcta de números/montos con <say-as>
    - Pausas naturales en puntuación (<break>)
    """
    if not texto:
        return texto
    
    # 1. ESCAPE XML estricto (CRÍTICO — un solo & sin escapar rompe todo el SSML)
    t = (texto
         .replace('&', '&amp;')
         .replace('<', '&lt;')
         .replace('>', '&gt;')
         .replace('"', '&quot;')
         .replace("'", '&apos;'))
    
    # 2. EXPANSIONES DE ABREVIATURAS CHILENAS (en texto, ANTES de tags)
    abreviaturas = [
        (r'\bUF\b', 'U efe'),
        (r'\bCLP\b', 'pesos chilenos'),
        (r'\bUSD\b', 'dólares'),
        (r'\bEUR\b', 'euros'),
        (r'\bUTM\b', 'U te eme'),
        (r'\bIPC\b', 'I pe ce'),
        (r'\bTPM\b', 'te pe eme'),
        (r'\bAFP\b', 'a efe pe'),
        (r'\bAPV\b', 'a pe ve'),
        (r'\bTMC\b', 'te eme ce'),
        (r'\bCMF\b', 'ce eme efe'),
        (r'\bINE\b', 'I ene e'),
        (r'\bBCCh\b', 'Banco Central'),
        (r'\bSII\b', 'ese i i'),
        (r'\bIVA\b', 'iva'),
        (r'\bPYME[Ss]?\b', 'pymes'),
        (r'\bRUT\b', 'rut'),
    ]
    for patt, repl in abreviaturas:
        t = re.sub(patt, repl, t)
    
    # 3. NÚMEROS Y MONTOS — usar <say-as> para pronunciación correcta
    def _format_monto(m):
        num = m.group(1).replace('.', '').replace(',', '')
        if num.isdigit():
            return f'<say-as interpret-as="cardinal">{num}</say-as> pesos'
        return m.group(0)
    t = re.sub(r'\$\s*([\d.,]+)', _format_monto, t)
    
    def _format_pct(m):
        num = m.group(1).replace(',', '.')
        return f'{num} por ciento'  # más simple, no usar say-as con decimales
    t = re.sub(r'(\d+[,\.]?\d*)\s*%', _format_pct, t)
    
    # 4. PAUSAS NATURALES con <break> (separadas con espacios para evitar adyacencia)
    t = t.replace('. ', '. <break time="500ms"/> ')
    t = t.replace(': ', ': <break time="350ms"/> ')
    t = t.replace('; ', '; <break time="350ms"/> ')
    t = t.replace(', ', ', <break time="200ms"/> ')
    t = t.replace('? ', '? <break time="500ms"/> ')
    t = t.replace('! ', '! <break time="500ms"/> ')
    
    # 5. WRAP EN <speak> SIMPLE — SIN prosody (evita conflicto con audioConfig)
    # CRITICO: NO duplicar rate/pitch aquí, se aplican via audioConfig
    ssml = f'<speak>{t}</speak>'
    return ssml


def _llamar_google_tts(texto: str) -> Optional[bytes]:
    """FASE 11: Llamada robusta a Google TTS con SSML opcional.
    
    Cuando USE_SSML=True:
    - Envía SSML en input.ssml
    - audioConfig SIN speakingRate/pitch duplicado (ya que <prosody> los manejaría
      pero ahora no usamos <prosody>, así que aplicamos rate/pitch via audioConfig)
    - SIN effectsProfileId (incompatible con SSML en algunos casos)
    
    Cuando USE_SSML=False (modo v91):
    - Texto plano + audioConfig completo con effectsProfileId
    """
    url = f"{GOOGLE_TTS_URL}?key={GOOGLE_TTS_KEY}"
    
    # FASE 11: Intentar SSML si está activado — payload SIMPLIFICADO
    if USE_SSML:
        try:
            ssml = _texto_a_ssml(texto)
            # Logging del SSML para diagnóstico (primeros 200 chars)
            print(f"🔊 [TTS-SSML] Generando SSML: {ssml[:200]}...")
            payload_ssml = {
                "input": {"ssml": ssml},
                "voice": {"languageCode": VOICE_LANGUAGE, "name": VOICE_NAME},
                "audioConfig": {
                    "audioEncoding":    "MP3",
                    "speakingRate":     SPEAKING_RATE,  # rate aplicado aquí (sin prosody en SSML)
                    "pitch":            PITCH,           # pitch aplicado aquí
                    "volumeGainDb":     2.0,             # ligero boost
                    # NO usar effectsProfileId con SSML — causa rechazos en Neural2
                }
            }
            r = requests.post(url, json=payload_ssml, timeout=15)
            if r.status_code == 200:
                audio = base64.b64decode(r.json().get("audioContent", ""))
                if audio:
                    print(f"✅ [TTS-SSML] Google {VOICE_NAME} OK ({len(audio):,} bytes)")
                    logger.info(f"✅ [TTS-SSML] Google {VOICE_NAME} OK ({len(audio):,} bytes)")
                    return audio
            # SSML rechazado → loggear DETALLE COMPLETO para diagnóstico
            err_text = r.text[:500] if r.text else 'sin detalle'
            print(f"⚠️ [TTS-SSML] Google rechazó SSML ({r.status_code}): {err_text}")
            logger.warning(f"Google TTS SSML rechazado HTTP {r.status_code}: {err_text}")
            print(f"⚠️ [TTS-SSML] Cayendo a TEXTO PLANO automáticamente")
        except Exception as e:
            print(f"⚠️ [TTS-SSML] excepción: {e}, usando texto plano")
            logger.warning(f"Google TTS SSML excepción: {e}")
    
    # MODO TEXTO PLANO (default si SSML=false, o fallback si SSML falló)
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
                print(f"✅ [TTS] Google {VOICE_NAME} OK texto-plano ({len(audio):,} bytes)")
                logger.info(f"✅ [TTS] Google {VOICE_NAME} OK texto-plano ({len(audio):,} bytes)")
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

    # FASE 12: Banner mejorado por cada llamada — identifica EXACTAMENTE qué voz se usa
    print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🎤 [TTS REQUEST] versión={TTS_VERSION_TAG}")
    print(f"🎤   GOOGLE_TTS_KEY: {'CONFIGURADA' if GOOGLE_TTS_KEY else 'NO_CONFIGURADA'}")
    print(f"🎤   VOICE_NAME    : {VOICE_NAME}")
    print(f"🎤   USE_SSML      : {USE_SSML}")
    print(f"🎤   Texto (primeros 80 chars): '{texto_limpio[:80]}'")
    logger.info(f"🎤 [TTS REQUEST] voz={VOICE_NAME} ssml={USE_SSML} key={'SI' if GOOGLE_TTS_KEY else 'NO'}")

    if USE_CACHE:
        f_cache = CACHE_DIR / f"{_cache_key(texto_limpio)}.mp3"
        if f_cache.exists():
            print(f"🎵 [TTS] Audio desde caché ({f_cache.stat().st_size:,} bytes)")
            print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            return f_cache.read_bytes()

    if GOOGLE_TTS_KEY:
        loop  = asyncio.get_event_loop()
        audio = await loop.run_in_executor(None, _llamar_google_tts, texto_limpio)
        if audio:
            if USE_CACHE:
                f_cache.write_bytes(audio)
            print(f"🎤 [TTS RESULT] ✅ GOOGLE TTS → {len(audio):,} bytes generados")
            print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            return audio
        # Google TTS falló → fallback a edge-tts Catalina
        print(f"🎤 [TTS RESULT] ⚠️ GOOGLE FALLÓ → cayendo a edge-tts Catalina (sonará más robótica)")
        logger.warning(f"⚠️ Google TTS falló — usando edge-tts Catalina como fallback")
        result = await _edge_tts_fallback(texto_limpio)
        print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return result
    else:
        print(f"⏭️ [TTS] Sin GOOGLE_TTS_KEY → usando edge-tts Catalina")
        logger.warning("⚠️ GOOGLE_TTS_KEY no configurada — usando Catalina como fallback")
        result = await _edge_tts_fallback(texto_limpio)
        print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return result


async def limpiar_cache_tts(dias: int = 7):
    import time
    ahora, borrados = time.time(), 0
    for f in CACHE_DIR.glob("*.mp3"):
        if ahora - f.stat().st_mtime > dias * 86400:
            f.unlink(); borrados += 1
    logger.info(f"🗑️ Caché TTS: {borrados} eliminados.")
