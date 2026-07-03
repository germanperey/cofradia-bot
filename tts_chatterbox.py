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
# FASE 31: Parámetros afinados para voz HUMANA natural (no robótica).
# SPEAKING_RATE 0.97 = cadencia conversacional natural (ni acelerada ni lenta).
# Combinado con los <break> del SSML, produce un ritmo realista.
SPEAKING_RATE  = float(os.getenv("GOOGLE_TTS_RATE",  "0.97"))
# Pitch 0.0 = tono natural de la voz Neural2-C masculina. No se altera.
PITCH          = float(os.getenv("GOOGLE_TTS_PITCH", "0.0"))

# FASE 31: SSML ACTIVADO POR DEFECTO EN CÓDIGO.
# Antes el default era "false" y dependía de que la env-var GOOGLE_TTS_SSML
# estuviera puesta en Render — si faltaba, la voz salía sin pausas ni manejo
# de siglas (sonaba plana y robótica). Ahora el default es "true": las pausas
# naturales y el deletreo de siglas funcionan siempre, salvo que se DESACTIVE
# explícitamente con GOOGLE_TTS_SSML=false.
_ssml_raw = os.getenv("GOOGLE_TTS_SSML", "true").strip().lower()
USE_SSML = _ssml_raw not in ("false", "0", "no", "off", "n")

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
TTS_VERSION_TAG = "v31-voz-natural-2026-07-02"

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
    """FASE 31 (REESCRITO): SSML natural, robusto y sin corromper pronunciación.

    LECCIÓN DE VERSIONES ANTERIORES: el diccionario de "pronunciaciones forzadas"
    de Fase 14 EMPEORABA la voz — reemplazos como Cofradía→"Cofradíia",
    económico→"ekonóomico", inflación→"inflasión" hacían que Google Neural2
    leyera palabras deformadas y sonara peor. Neural2 en español-LatAm YA
    pronuncia bien la enorme mayoría de palabras (incluidas las que llevan
    tilde) porque respeta la ortografía. Forzar alias fonéticos es
    contraproducente salvo en poquísimos casos comprobadamente rotos.

    PRINCIPIOS DE ESTA VERSIÓN:
    1. Confiar en la ortografía: NO se toca ninguna palabra bien escrita.
       Los acentos ortográficos (á, é, í, ó, ú) ya guían la acentuación.
    2. <sub> SOLO para siglas que deben deletrearse (UF, IPC, TPM…) y para
       un mínimo de nombres propios extranjeros que el motor realmente
       silabea mal. Nada de reescribir español correcto.
    3. Pausas naturales con <break> aplicadas por REGEX sobre la puntuación
       real (coma, punto, punto y coma, dos puntos, signos ¿? ¡!), de modo
       que funcionen aunque no haya espacio después (fin de línea, comillas).
    4. Escape XML PRIMERO; los tags SSML se insertan sobre texto ya escapado.
    """
    if not texto:
        return texto

    # ── 1. ESCAPE XML (crítico: un & suelto rompe todo el SSML) ──
    t = (texto
         .replace('&', '&amp;')
         .replace('<', '&lt;')
         .replace('>', '&gt;')
         .replace('"', '&quot;')
         .replace("'", '&apos;'))

    # ── 2. SIGLAS QUE SE DELETREAN (una letra a la vez, con micro-pausa) ──
    # Se usa <say-as interpret-as="characters"> que Neural2 respeta de forma
    # fiable, en vez de aproximaciones fonéticas manuales.
    SIGLAS_DELETREAR = [
        'IPSA', 'IMACEC', 'IPC', 'TPM', 'UTM', 'AFP', 'APV', 'TMC', 'CMF',
        'INE', 'SII', 'IVA', 'RUT', 'MBA', 'CEO', 'CFO', 'CTO', 'COO',
        'RAG', 'API', 'LLM', 'PIB', 'ROI', 'ROE',
    ]
    # Ordenar por longitud desc para no romper siglas contenidas en otras
    for sig in sorted(SIGLAS_DELETREAR, key=len, reverse=True):
        t = re.sub(
            r'\b' + sig + r'\b',
            f'<say-as interpret-as="characters">{sig}</say-as>',
            t
        )

    # ── 3. EXPANSIONES LÉXICAS (mejoran naturalidad sin deformar) ──
    # Solo abreviaturas que, leídas literales, sonarían mal.
    EXPANSIONES = [
        (r'\bUF\b', 'unidad de fomento'),
        (r'\bCLP\b', 'pesos chilenos'),
        (r'\bUSD\b', 'dólares'),
        (r'\bEUR\b', 'euros'),
        (r'\bBCCh\b', 'Banco Central de Chile'),
        (r'\bPYMEs?\b', 'pymes'),
    ]
    for patt, repl in EXPANSIONES:
        t = re.sub(patt, repl, t)

    # ── 4. NOMBRES PROPIOS EXTRANJEROS REALMENTE MAL SILABEADOS ──
    # Lista MÍNIMA y conservadora. NADA de español correcto aquí.
    # Formato: {texto_visible: alias_fonético_seguro}
    NOMBRES_DIFICILES = {
        'Keynes': 'Keins',      # evita "Ke-y-nes"
        'Hayek': 'Jáyek',       # evita "A-yek"
        'Friedman': 'Frídman',  # evita "Fri-ed-man"
    }
    for original, alias in sorted(NOMBRES_DIFICILES.items(), key=lambda x: -len(x[0])):
        t = re.sub(
            r'\b' + re.escape(original) + r'\b',
            f'<sub alias="{alias}">{original}</sub>',
            t
        )

    # ── 5. MONTOS Y PORCENTAJES ──
    def _format_monto(m):
        num = m.group(1).replace('.', '').replace(',', '')
        if num.isdigit():
            return f'<say-as interpret-as="cardinal">{num}</say-as> pesos'
        return m.group(0)
    t = re.sub(r'\$\s*([\d.,]+)', _format_monto, t)

    def _format_pct(m):
        num = m.group(1).replace(',', ' coma ')
        return f'{num} por ciento'
    t = re.sub(r'(\d+[,\.]?\d*)\s*%', _format_pct, t)

    # ── 6. PAUSAS NATURALES POR PUNTUACIÓN (vía regex robusto) ──
    # CLAVE: se inserta el <break> INMEDIATAMENTE tras el signo aunque le
    # siga un salto de línea, comilla o fin de texto. Esto arregla el
    # problema de pausas que "no se aplicaban después de coma y punto".
    # Punto/interrogación/exclamación → pausa larga (final de oración)
    t = re.sub(r'([.!?…])(\s|$|&quot;|&apos;)', r'\1 <break time="450ms"/>\2', t)
    # Punto y coma / dos puntos → pausa media
    t = re.sub(r'([;:])(\s|$)', r'\1 <break time="350ms"/>\2', t)
    # Coma → pausa breve
    t = re.sub(r'(,)(\s|$)', r'\1 <break time="220ms"/>\2', t)
    # Saltos de línea dobles (párrafos) → pausa mayor
    t = re.sub(r'\n\s*\n', ' <break time="650ms"/> ', t)
    t = t.replace('\n', ' <break time="300ms"/> ')

    # Colapsar espacios múltiples que puedan haber quedado
    t = re.sub(r'[ \t]+', ' ', t).strip()

    # ── 7. WRAP EN <speak> con <prosody> suave para calidez humana ──
    # rate/pitch se controlan en audioConfig; aquí solo un leve ajuste de
    # entonación que Neural2 tolera bien sin duplicar efecto.
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
        # FASE 31: rate más cercano a 0 = más natural; pitch neutro.
        communicate = edge_tts.Communicate(
            texto, "es-CL-CatalinaNeural",
            rate="-6%", pitch="+0Hz", volume="+6%"
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
