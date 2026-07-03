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

# FASE 31.1: MOTOR PRINCIPAL = CATALINA (es-CL, femenina, acento chileno).
# Requisito de Germán: la voz del bot debe ser la de Catalina, con entonación
# natural. edge-tts (voz neuronal de Microsoft es-CL-CatalinaNeural) es
# gratuita, no requiere API key y suena genuinamente chilena. Google Neural2
# queda como RESPALDO si edge-tts fallara.
# Para volver a Google sin tocar código: poner TTS_MOTOR=google en Render.
TTS_MOTOR = os.getenv("TTS_MOTOR", "catalina").strip().lower()

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
TTS_VERSION_TAG = "v31.6-catalina-pausa-titulos-2026-07-03"

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


def _texto_para_edge(texto: str) -> str:
    """FASE 31.1: Prepara el texto para que CATALINA lo lea con máxima
    naturalidad. edge-tts no acepta SSML personalizado desde la librería,
    así que la naturalidad se logra vía TEXTO: expansiones léxicas,
    deletreo de siglas con espacios y puntuación que induce pausas.
    NUNCA se deforman palabras en español correcto (lección de Fase 14)."""
    t = texto
    # ═══ FASE 31.6: PAUSA entre TÍTULO / SUBTÍTULO / PÁRRAFO.
    # Antes Catalina leía "Contenido del libro El libro se divide..." de
    # corrido (título pegado al párrafo). Ahora: toda línea corta que no
    # termine en signo de puntuación se considera encabezado y recibe un
    # punto → el motor de pausas hace el silencio de fin de oración.
    # Además, encabezados markdown (**, ##) también se cierran con punto.
    _lineas_t = []
    for _ln in t.split('\n'):
        _ln_s = _ln.strip()
        if _ln_s:
            # quitar marcadores markdown de encabezado para no leerlos
            _ln_limpio = _ln_s.lstrip('#').strip()
            _sin_md = _ln_limpio.replace('**', '').replace('__', '')
            # ¿es encabezado? línea corta (<10 palabras) sin puntuación final
            _es_titulo = (len(_sin_md.split()) <= 9
                          and not _sin_md.endswith(('.', ':', '!', '?', ',', ';')))
            if _es_titulo:
                _lineas_t.append(_sin_md + '.')
            else:
                _lineas_t.append(_ln_s)
        else:
            _lineas_t.append('')
    t = '\n'.join(_lineas_t)
    # Siglas → deletreo natural ("IPC" → "i pe cé" suena raro; "I P C" con
    # espacios hace que la voz neuronal las diga letra a letra correctamente)
    SIGLAS = ['IPSA', 'IMACEC', 'IPC', 'TPM', 'UTM', 'AFP', 'APV', 'TMC',
              'CMF', 'INE', 'SII', 'RUT', 'MBA', 'CEO', 'CFO', 'CTO',
              'RAG', 'API', 'LLM', 'PIB', 'ROI', 'ROE']
    for sig in sorted(SIGLAS, key=len, reverse=True):
        t = re.sub(r'\b' + sig + r'\b', ' '.join(sig), t)
    # Expansiones léxicas
    for patt, repl in [
        (r'\bUF\b', 'unidad de fomento'), (r'\bCLP\b', 'pesos chilenos'),
        (r'\bUSD\b', 'dólares'), (r'\bEUR\b', 'euros'),
        (r'\bBCCh\b', 'Banco Central de Chile'), (r'\bPYMEs?\b', 'pymes'),
        (r'\bIVA\b', 'iva'),
    ]:
        t = re.sub(patt, repl, t)
    # Nombres extranjeros que la voz silabea mal (lista mínima)
    for orig, alias in [('Keynes', 'Keins'), ('Hayek', 'Jáyek'),
                        ('Friedman', 'Frídman')]:
        t = re.sub(r'\b' + orig + r'\b', alias, t)
    # ═══ FASE 31.3 (1) ACENTOS: nombres con tilde que el motor entona mal.
    # "Germán" en posición vocativa suena "Géerman"; el respelling "Jermán"
    # fuerza el fonema /x/ y la sílaba tónica correcta (-mán). Solo AUDIO,
    # el texto escrito no se toca. Lista corta y comprobada (lección F14).
    for orig, alias in [('Germán', 'Jermán'), ('German', 'Jermán'),
                        ('Ángel', 'Ánjel'), ('Sebastián', 'Sebastián')]:
        t = re.sub(r'\b' + orig + r'\b', alias, t)
    # ═══ FASE 31.3 (2) ANGLICISMOS: pronunciación inglesa natural vía
    # respelling fonético español (edge-tts no soporta <lang> desde la lib).
    # FASE 31.5: 'sh'→'ch' (la sh inglesa se leía como 's'); +francés,
    # portugués y alemán para nombres/términos frecuentes. Sin tildes que
    # alarguen de más. EN=inglés · FR=francés · PT=portugués · DE=alemán.
    _ANGLICISMOS = [
        # ── Inglés (tecnología/negocios) ──
        (r'\bdashboards\b', 'dáchbords'), (r'\bdashboard\b', 'dáchbord'),
        (r'\branking\b', 'ránkin'), (r'\brankings\b', 'ránkins'),
        (r'\bemail\b', 'imeil'), (r'\be-mail\b', 'imeil'),
        (r'\bonline\b', 'onlain'), (r'\bsoftware\b', 'sóftwer'),
        (r'\bhardware\b', 'járdwer'), (r'\bmarketing\b', 'márketin'),
        (r'\bnetworking\b', 'nétwerkin'), (r'\bstartups\b', 'stártaps'),
        (r'\bstartup\b', 'stártap'), (r'\bfeedback\b', 'fídbak'),
        (r'\bstreaming\b', 'strímin'), (r'\bcloud\b', 'claud'),
        (r'\blinks\b', 'lincs'), (r'\blink\b', 'linc'),
        (r'\bWhatsApp\b', 'guátsap'), (r'\bExcel\b', ' excél'),
        (r'\bGoogle Drive\b', 'gúgol draiv'), (r'\bGoogle\b', 'gúgol'),
        (r'\bblockchain\b', 'blókchein'), (r'\bcoaching\b', 'cóuchin'),
        (r'\bheadhunter\b', 'jédjanter'), (r'\bpartners?\b', 'pártner'),
        (r'\bmeeting\b', 'mítin'), (r'\bmeetings\b', 'mítins'),
        (r'\bpassword\b', 'pásword'), (r'\bmanager\b', 'mánayer'),
        (r'\bbusiness\b', 'bísnes'), (r'\bhashtag\b', 'jáctag'),
        (r'\bshocks\b', 'chocs'), (r'\bshock\b', 'choc'),
        (r'\bpodcast\b', 'pódcast'), (r'\bwebinar\b', 'güébinar'),
        (r'\bcowork\b', 'cówork'), (r'\bshowroom\b', 'chórum'),
        # ── Francés ──
        (r'\brendez-vous\b', 'randevú'), (r'\bboutique\b', 'butík'),
        (r'\bchef\b', 'chef'), (r'\bbureau\b', 'buró'),
        # ── Portugués ──
        (r'\bobrigado\b', 'obrigádu'), (r'\bsaudade\b', 'saudádye'),
        # ── Alemán ──
        (r'\bGesundheit\b', 'gezúndjait'), (r'\bKindergarten\b', 'kíndergarten'),
    ]
    for patt_a, repl_a in _ANGLICISMOS:
        t = re.sub(patt_a, repl_a, t, flags=re.IGNORECASE)
    # ═══ FASE 31.9-TTS: PRONUNCIACIÓN INGLESA/FRANCESA NATURAL ═══
    # Problema reportado: Catalina lee palabras inglesas con fonética
    # española ("Love Me Tender" → "lobe me tendér", efecto "Jane de
    # Tarzán"). Solución en 3 capas, SOLO para el audio:
    #   1) Diccionario de 321 palabras derivado del CMU Pronouncing
    #      Dictionary (Carnegie Mellon, estándar académico de pronunciación
    #      inglesa) convertido a respelling español con la sílaba tónica
    #      marcada. 2) Mini-diccionario francés curado. 3) Detector
    #      heurístico + reglas grafema→fonema para palabras inglesas fuera
    #      del diccionario. REGLA DE ORO: una palabra española JAMÁS se
    #      toca (exige señal inequívoca de que NO es español).
    _PRON_EN = {"actor": "ákter", "actress": "áktres", "afternoon": "afternún", "air": "er", "album": "álbem", "albums": "álbems", "alone": "elóun", "always": "ólueis", "amazing": "eméisin", "american": "emériken", "anchor": "ánker", "answer": "ánser", "april": "éiprel", "aren't": "árent", "army": "ármi", "august": "óguest", "baby": "béibi", "baseball": "béisból", "basketball": "básketbol", "bass": "bas", "beautiful": "bíutefel", "birthday": "bérsdei", "blue": "blu", "boat": "bóut", "book": "buk", "books": "buks", "boxing": "báksin", "boy": "boi", "break": "bréik", "breakfast": "brékfest", "british": "brítich", "business": "bísnis", "can't": "kant", "captain": "kápten", "car": "kar", "channel": "chánel", "children": "chíldren", "christmas": "krísmes", "city": "síti", "code": "kóud", "coffee": "kófi", "college": "káliy", "company": "kémpeni", "computer": "kempíuter", "concert": "kánsert", "content": "kántent", "could": "kud", "country": "kéntri", "crew": "kru", "cruel": "krúel", "dance": "dans", "dancing": "dánsin", "data": "dáte", "day": "dei", "december": "disémber", "deep": "dip", "device": "diváis", "didn't": "dídent", "different": "díferent", "dinner": "díner", "director": "dirékter", "doesn't": "désent", "don't": "dóunt", "drums": "drams", "earth": "ars", "eight": "eit", "english": "ínlich", "evening": "ívnin", "false": "fols", "fame": "féim", "family": "fámeli", "famous": "féimes", "february": "fébrueri", "fire": "faíer", "first": "farst", "five": "fáiv", "fleet": "flit", "flight": "fláit", "follow": "fálou", "follower": "fálouer", "following": "fálouin", "food": "fud", "football": "fútbol", "force": "fors", "forever": "feréver", "four": "for", "free": "fri", "friday": "fráidei", "friend": "frend", "friends": "frends", "full": "ful", "game": "guéim", "games": "guéims", "girl": "garl", "good": "gud", "goodbye": "gudbái", "government": "gévernment", "great": "gréit", "growth": "gróus", "guitar": "guitár", "halloween": "jaleúin", "happy": "jápi", "harbor": "járber", "health": "jels", "heart": "jart", "heartbreak": "jártbreik", "heavy": "jévi", "hello": "jelóu", "high": "jai", "history": "jísteri", "hits": "jits", "home": "jóum", "hotel": "joutél", "hound": "jáund", "house": "jáus", "how": "jau", "important": "impórtent", "impossible": "impásebel", "internet": "ínternet", "isn't": "ísent", "jailhouse": "yéiljaus", "january": "yániueri", "job": "yab", "july": "yulái", "june": "yun", "king": "kin", "kingdom": "kíndem", "leader": "líder", "leadership": "líderchip", "learn": "larn", "legend": "léyend", "life": "láif", "light": "láit", "like": "láik", "likes": "láiks", "live": "liv", "living": "lívin", "long": "lon", "love": "lav", "low": "lou", "lunch": "lanch", "management": "mániyment", "manager": "mániyer", "market": "márkit", "may": "mei", "medicine": "médesen", "minds": "máinds", "mobile": "móubel", "monday": "méndei", "money": "méni", "moon": "mun", "morning": "mórnin", "movie": "múvi", "movies": "múvis", "much": "mach", "music": "míusik", "name": "néim", "navy": "néivi", "network": "nétuerk", "never": "néver", "new": "nu", "night": "náit", "nine": "náin", "november": "nouvémber", "ocean": "óuchen", "october": "aktóuber", "office": "ófes", "officer": "ófiser", "old": "óuld", "one": "uen", "page": "péiy", "party": "párti", "people": "pípel", "phone": "fóun", "piano": "piáne", "plane": "pléin", "play": "pléi", "player": "pleíer", "playing": "pléin", "please": "plis", "possible": "pásebel", "post": "póust", "posts": "póusts", "power": "paúer", "president": "président", "price": "práis", "project": "práyekt", "question": "kúechen", "race": "réis", "rain": "réin", "read": "red", "record": "rékerd", "records": "rékerds", "report": "ripórt", "research": "ríserch", "review": "rivíu", "right": "ráit", "road": "róud", "rock": "rak", "roll": "róul", "running": "rénin", "sailor": "séiler", "saturday": "sátidei", "school": "skul", "science": "saíens", "screen": "skrin", "second": "sékend", "september": "septémber", "server": "sérver", "seven": "séven", "share": "cher", "ship": "chip", "ships": "chips", "shoes": "chus", "short": "chort", "should": "chud", "show": "chóu", "shows": "chóus", "singing": "sínin", "single": "sínguel", "six": "siks", "sky": "skái", "slow": "slóu", "small": "smol", "snow": "snóu", "song": "son", "songs": "sons", "sorry": "sári", "sound": "sáund", "spanish": "spánich", "special": "spéchel", "spring": "spérin", "stage": "stéiy", "state": "stéit", "states": "stéits", "stock": "stak", "story": "stóri", "street": "strit", "strong": "stron", "student": "stúdent", "suede": "suéid", "summer": "sémer", "sun": "san", "sunday": "séndi", "suspicious": "sespíches", "system": "sístem", "teacher": "tícher", "team": "tim", "tender": "ténder", "tennis": "ténis", "thank": "zank", "thanks": "zanks", "thanksgiving": "zanksguívin", "that": "dat", "the": "da", "theater": "zíeiter", "their": "der", "them": "dem", "there": "der", "these": "dis", "they": "dei", "third": "zard", "this": "dis", "those": "dóus", "three": "sri", "thursday": "zérsdei", "time": "táim", "today": "tedéi", "together": "tegéder", "tomorrow": "temárou", "tonight": "tenáit", "tour": "tur", "train": "tréin", "travel": "trável", "true": "tru", "tuesday": "túsdei", "two": "tu", "united": "iunáitid", "university": "iunevérseti", "value": "váliu", "very": "véri", "video": "vídiou", "videos": "vídious", "voice": "vóis", "walking": "úokin", "was": "uas", "wasn't": "úesent", "water": "úoter", "way": "uei", "web": "ueb", "website": "úebsait", "wednesday": "úensdei", "weekend": "úikind", "welcome": "úelkem", "were": "uer", "what": "uet", "when": "uin", "where": "uer", "which": "úich", "who": "ju", "why": "uai", "will": "uil", "wind": "úind", "winner": "úiner", "winter": "úinter", "woman": "úumen", "women": "úimen", "won't": "uóunt", "wonderful": "úenderfel", "word": "úerd", "words": "úerds", "work": "úerk", "world": "úerld", "would": "ud", "write": "ráit", "wrong": "ron", "yesterday": "íesterdei", "you": "iu", "young": "ien", "your": "iur", "yours": "íurs"}
    _PRON_FR = {
        "croissant": "cruasán", "baguette": "baguét", "champagne": "champán",
        "déjà": "deyá", "vu": "vú", "cliché": "cliché", "élite": "elít",
        "gourmet": "gurmét", "buffet": "bufét", "ballet": "balét",
        "cabernet": "cabernét", "sauvignon": "soviñón", "monsieur": "mesié",
        "madame": "madám", "merci": "mersí", "bonjour": "bonyúr",
        "voilà": "gualá", "toilette": "tualét", "amateur": "amatér",
        "chauffeur": "chofér", "entrepreneur": "antreprenér",
        "tour": "tur", "force": "fors", "avant": "aván", "garde": "gard",
        "beaucoup": "bocú", "papier": "papié", "atelier": "atelié",
        "soirée": "suaré", "première": "premiér", "menu": "menú",
    }
    def _fonetizar_palabra_31_9(m):
        w = m.group(0)
        wl = w.lower()
        # capa 1 y 2: diccionarios (CMU inglés + francés curado)
        r = _PRON_EN.get(wl) or _PRON_FR.get(wl)
        if r is None:
            # capa 3: heurística SOLO con señal inequívoca de palabra inglesa
            if not re.search(r"th|sh|ck|gh|oo|ee|wh|ough|[qwk]|'", wl):
                return w                       # sin señal → se deja intacta
            if re.search(r"[áéíóúñü]", wl):
                return w                       # ya es español / ya procesada
            r = wl
            for pa, re_ in [(r"ough", "of"), (r"ght", "t"), (r"th", "z"),
                            (r"sh", "ch"), (r"ck", "k"), (r"wh", "u"),
                            (r"oo", "u"), (r"ee", "i"), (r"ea", "i"),
                            (r"^h(?=[aeiou])", "j"), (r"w", "u"),
                            (r"ing\b", "in"), (r"y\b", "i")]:
                r = re.sub(pa, re_, r)
            if r == wl:
                return w
        # preservar mayúscula inicial (nombres propios, inicios de frase)
        return (r[0].upper() + r[1:]) if w[0].isupper() else r
    t = re.sub(r"\b[A-Za-z][A-Za-z']{2,}\b", _fonetizar_palabra_31_9, t)
    # Montos y porcentajes → lectura natural
    t = re.sub(r'\$\s*([\d.,]+)',
               lambda m: m.group(1).replace('.', ' mil ') + ' pesos'
               if m.group(1).count('.') == 1 and m.group(1).replace('.', '').isdigit()
               else m.group(1) + ' pesos', t)
    t = re.sub(r'(\d+),(\d+)\s*%', r'\1 coma \2 por ciento', t)
    t = re.sub(r'(\d+)\s*%', r'\1 por ciento', t)
    # ═══ FASE 31.5 (2) SÍMBOLOS con pronunciación inglesa REALISTA.
    # edge-tts ya NO acepta SSML <lang> (Microsoft lo bloqueó), así que la
    # única vía es el respelling fonético. Clave aprendida: la 'sh' inglesa
    # NO existe en español (Catalina la lee como 's' → "sláas"). Se aproxima
    # con 'ch' (la ch española /tʃ/ es lo más cercano a /ʃ/), y se EVITAN
    # las tildes que alargan la vocal. "/" (slash) → "slach".
    t = re.sub(r'(\d+)\s*/\s*(\d+)', r'\1 \2', t)          # 24/7 → "24 7"
    t = re.sub(r'\by/o\b', 'y o', t, flags=re.IGNORECASE)     # y/o → "y o"
    t = re.sub(r'/(?=[A-Za-zÁ-Úá-ú])', ' slach ', t)            # /economia → "slach economia"
    t = re.sub(r'\s*/\s*', ' slach ', t)                       # "/" suelto
    t = t.replace('@', ' arroba ')
    t = re.sub(r'#(?=\w)', ' jactag ', t).replace('#', ' jactag ')  # hashtag ≈ "jáctag"
    t = t.replace('&', ' y ').replace('+', ' más ').replace('=', ' igual a ')
    t = re.sub(r'\bvs\.?\b', 'vérsus', t, flags=re.IGNORECASE)
    # Dominios y correos: el punto se DICE ("cofradia.cl" → "cofradia punto cl")
    # y así no lo captura la regla de pausa de fin de oración.
    t = re.sub(r'(?<=[a-záéíóúñ0-9])\.(?=(?:cl|com|net|org|ai|io|es|app|dev)\b)',
               ' punto ', t, flags=re.IGNORECASE)
    t = t.replace('→', ', ').replace('←', ', ').replace('≈', ' aproximadamente ')
    t = re.sub(r'_(?=\w)', ' ', t).replace('_', ' ')            # buscar_profesional → "buscar profesional"
    # Separadores visuales → pausa hablada
    t = t.replace(' · ', ', ').replace('·', ',').replace('—', ', ').replace(' - ', ', ')
    # Asegurar espacio tras signos (induce la pausa del motor)
    t = re.sub(r'([.!?;:,])(?=\S)', r'\1 ', t)
    # ═══ FASE 31.3 (3) PAUSAS HUMANAS:
    # · PUNTO / ! / ? → salto de línea = frontera de oración → pausa LARGA
    #   (la respiración del hablante). · PUNTO Y COMA → coma (pausa SUAVE).
    # · La COMA se deja tal cual: en la voz neuronal ya es la pausa corta
    #   y relajada. Resultado: coma < punto, como habla una persona.
    t = t.replace('; ', ', ')
    t = re.sub(r'([.!?…])\s+', r'\1\n', t)
    # ═══ FASE 31.5 RESPIRACIÓN NATURAL (basada en fonética del habla)
    # La investigación (breath groups, Fuchs/Trouvain) muestra que NO se
    # respira "cada N palabras": se respira en FRONTERAS SINTÁCTICAS, en
    # grupos de aliento de ~2-4 segundos. A ~3 sílabas/seg del habla, un
    # grupo cómodo es ~7-12 sílabas. Por eso:
    #   1) SOLO se considera insertar una micro-pausa ante CONECTORES de
    #      cláusula (donde la gramática permite respirar sin cortar la idea).
    #   2) Se usa un presupuesto de SÍLABAS (≈tiempo), no de palabras, y solo
    #      se dispara si ya pasaron ≥9 sílabas Y quedan ≥6 por delante — así
    #      nunca corta el impulso de una frase corta ni deja una coma huérfana.
    _CONECTORES_CLAUSULA = {'y', 'pero', 'aunque', 'porque', 'pues', 'mientras',
                            'cuando', 'donde', 'que', 'para', 'porqué', 'sino',
                            'aún', 'además', 'también', 'entonces', 'luego',
                            'sin embargo', 'no obstante', 'es decir'}
    def _silabas_aprox(palabra):
        # Conteo de sílabas por grupos vocálicos (aprox. suficiente para timing)
        import re as _re2
        v = _re2.findall(r'[aeiouáéíóúü]+', palabra.lower())
        return max(1, len(v))
    _lineas_out = []
    for _linea in t.split('\n'):
        _toks = _linea.split(' ')
        # sílabas restantes acumuladas desde cada posición (para mirar adelante)
        _sil = [_silabas_aprox(w) for w in _toks]
        _out, _budget = [], 0
        for _idx, _tk in enumerate(_toks):
            _tkl = _tk.lower().strip('.,;:¿?¡!')
            _ya_punt = _out and _out[-1].endswith((',', '.', ':', ';', '?', '!'))
            _sil_delante = sum(_sil[_idx:])
            if (_budget >= 9 and _tkl in _CONECTORES_CLAUSULA
                    and _sil_delante >= 6 and not _ya_punt):
                _out[-1] = _out[-1] + ','   # micro-pausa: respira aquí
                _budget = 0
            _out.append(_tk)
            _budget = 0 if _tk.endswith((',', '.', ':', ';', '?', '!')) else _budget + _sil[_idx]
        _lineas_out.append(' '.join(_out))
    t = '\n'.join(_lineas_out)
    t = re.sub(r'[ \t]{2,}', ' ', t).strip()
    return t


async def _edge_tts_fallback(texto: str) -> Optional[bytes]:
    try:
        import edge_tts
        # FASE 31.1: prosodia CONVERSACIONAL de Catalina — rate levemente
        # pausado (-5%) para dicción clara sin sonar lenta; pitch +2Hz da
        # calidez; el texto llega pre-procesado por _texto_para_edge.
        texto = _texto_para_edge(texto)
        communicate = edge_tts.Communicate(
            texto, "es-CL-CatalinaNeural",
            rate="-5%", pitch="+2Hz", volume="+10%"
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

    # ═══════════════════════════════════════════════════════════════════
    # FASE 31.1 — ORDEN DE MOTORES INVERTIDO POR PEDIDO DE GERMÁN:
    #   PRINCIPAL: edge-tts CATALINA (es-CL, femenina, acento chileno)
    #   RESPALDO : Google Neural2 (solo si Catalina falla y hay key)
    # TTS_MOTOR=google en Render restaura el orden anterior sin tocar código.
    # ═══════════════════════════════════════════════════════════════════
    if TTS_MOTOR != "google":
        audio = await _edge_tts_fallback(texto_limpio)
        if audio:
            if USE_CACHE:
                f_cache.write_bytes(audio)
            print(f"🎤 [TTS RESULT] ✅ CATALINA (es-CL) → {len(audio):,} bytes generados")
            print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            return audio
        print(f"🎤 [TTS RESULT] ⚠️ CATALINA FALLÓ → intentando Google TTS de respaldo")
        logger.warning("⚠️ edge-tts Catalina falló — intentando Google TTS como respaldo")
        if GOOGLE_TTS_KEY:
            loop = asyncio.get_event_loop()
            audio = await loop.run_in_executor(None, _llamar_google_tts, texto_limpio)
            if audio:
                if USE_CACHE:
                    f_cache.write_bytes(audio)
                print(f"🎤 [TTS RESULT] ✅ GOOGLE (respaldo) → {len(audio):,} bytes")
                print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                return audio
        print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return None

    # TTS_MOTOR=google → comportamiento anterior (Google primario)
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
        print(f"🎤 [TTS RESULT] ⚠️ GOOGLE FALLÓ → cayendo a edge-tts Catalina")
        logger.warning(f"⚠️ Google TTS falló — usando edge-tts Catalina como fallback")
        result = await _edge_tts_fallback(texto_limpio)
        print(f"🎤━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return result
    else:
        print(f"⏭️ [TTS] Sin GOOGLE_TTS_KEY → usando edge-tts Catalina")
        logger.warning("⚠️ GOOGLE_TTS_KEY no configurada — usando Catalina")
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
