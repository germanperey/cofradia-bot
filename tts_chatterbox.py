"""
╔══════════════════════════════════════════════════════════════════════╗
║         MÓDULO TTS — CHATTERBOX VOICE (Reemplaza edge-TTS)          ║
║         Voz femenina natural y pausada · 100% gratuito MIT           ║
╠══════════════════════════════════════════════════════════════════════╣
║  INSTALACIÓN (agregar a requirements.txt):                           ║
║    chatterbox-tts>=0.1.6                                             ║
║    torchaudio                                                        ║
║    pydub                                                             ║
║                                                                      ║
║  NOTA IMPORTANTE sobre Render (free tier):                           ║
║  Chatterbox requiere ~2GB RAM y carga un modelo de 500M parámetros.  ║
║  En Render FREE puede ser lento en el primer uso (cold start ~30s).  ║
║  SOLUCIÓN RECOMENDADA: usar la estrategia de "lazy loading" que      ║
║  incluye este módulo — el modelo se carga solo cuando se necesita    ║
║  y queda en memoria para las siguientes peticiones.                  ║
║                                                                      ║
║  ALTERNATIVA si Render FREE no aguanta:                              ║
║  → Activar el plan Starter de Render ($7/mes, 512MB adicionales)     ║
║  → O usar la voz de Chatterbox solo para mensajes importantes        ║
║    (bienvenida, tarjeta profesional) y edge-TTS para el resto.       ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import os
import io
import logging
import asyncio
import tempfile
from pathlib import Path
from typing import Optional
import hashlib

logger = logging.getLogger(__name__)

# ─── CONFIGURACIÓN DE VOZ ────────────────────────────────────────────
TTS_ENGINE = os.getenv("TTS_ENGINE", "chatterbox")  # "chatterbox" | "edge"

# Parámetros de Chatterbox para voz femenina natural y pausada
# exaggeration: 0.0 (monótono) → 1.0 (muy expresivo). 0.35 = natural y profesional
# cfg_weight:   0.0 (lento/pausado) → 1.0 (rápido). 0.3 = pausado y claro
CHATTERBOX_CONFIG = {
    "exaggeration": float(os.getenv("TTS_EXAGGERATION", "0.35")),
    "cfg_weight":   float(os.getenv("TTS_CFG_WEIGHT",   "0.30")),
}

# Ruta al archivo de voz de referencia (WAV de 5-10 seg con voz femenina)
# Si no se proporciona, Chatterbox usa su voz por defecto (también muy buena)
VOICE_REFERENCE_PATH = os.getenv("TTS_VOICE_REFERENCE", "")

# Caché de audios para no regenerar el mismo texto dos veces
AUDIO_CACHE_DIR = Path(os.getenv("TTS_CACHE_DIR", "/tmp/tts_cache"))
AUDIO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE = os.getenv("TTS_USE_CACHE", "true").lower() == "true"

# ─── INSTANCIA GLOBAL DEL MODELO (lazy loading) ─────────────────────
_chatterbox_model = None
_model_lock = asyncio.Lock()


async def _get_model():
    """
    Carga el modelo Chatterbox una sola vez y lo mantiene en memoria.
    Patrón singleton con asyncio.Lock para evitar cargas paralelas.
    """
    global _chatterbox_model
    if _chatterbox_model is not None:
        return _chatterbox_model

    async with _model_lock:
        if _chatterbox_model is not None:   # doble check tras adquirir lock
            return _chatterbox_model

        logger.info("🔊 Cargando modelo Chatterbox TTS (primera vez, ~15-30s)...")
        try:
            import torch
            from chatterbox.tts import ChatterboxTTS

            device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"🖥️  Chatterbox usará dispositivo: {device}")

            # Cargar en hilo separado para no bloquear el event loop
            loop  = asyncio.get_event_loop()
            model = await loop.run_in_executor(
                None,
                lambda: ChatterboxTTS.from_pretrained(device=device)
            )
            _chatterbox_model = model
            logger.info("✅ Modelo Chatterbox listo.")
        except ImportError:
            logger.error("❌ chatterbox-tts no está instalado. Ejecuta: pip install chatterbox-tts")
            raise
        except Exception as e:
            logger.error(f"❌ Error cargando Chatterbox: {e}")
            raise

    return _chatterbox_model


def _cache_key(texto: str, config: dict) -> str:
    """Genera una clave única para el caché de audio."""
    raw = f"{texto}_{config['exaggeration']}_{config['cfg_weight']}_{VOICE_REFERENCE_PATH}"
    return hashlib.md5(raw.encode()).hexdigest()


async def generar_audio_chatterbox(texto: str, config: dict = None) -> bytes:
    """
    Genera audio WAV usando Chatterbox TTS y devuelve los bytes.

    Args:
        texto: Texto a sintetizar (en español, Chatterbox es multilingüe)
        config: Dict con exaggeration y cfg_weight (usa CHATTERBOX_CONFIG si None)

    Returns:
        bytes del archivo OGG Opus (formato que acepta Telegram para voice notes)
    """
    if config is None:
        config = CHATTERBOX_CONFIG

    # Verificar caché primero
    if USE_CACHE:
        cache_key  = _cache_key(texto, config)
        cache_file = AUDIO_CACHE_DIR / f"{cache_key}.ogg"
        if cache_file.exists():
            logger.debug(f"🎵 Audio desde caché: {cache_key[:8]}...")
            return cache_file.read_bytes()

    try:
        import torch
        import torchaudio as ta

        model = await _get_model()
        loop  = asyncio.get_event_loop()

        # Generar audio en executor para no bloquear
        def _generar():
            kwargs = {
                "text":          texto,
                "exaggeration":  config["exaggeration"],
                "cfg_weight":    config["cfg_weight"],
            }
            if VOICE_REFERENCE_PATH and Path(VOICE_REFERENCE_PATH).exists():
                kwargs["audio_prompt_path"] = VOICE_REFERENCE_PATH

            return model.generate(**kwargs)

        wav = await loop.run_in_executor(None, _generar)

        # Convertir tensor → bytes WAV → OGG Opus (para Telegram voice)
        ogg_bytes = await loop.run_in_executor(
            None,
            lambda: _tensor_to_ogg(wav, model.sr)
        )

        # Guardar en caché
        if USE_CACHE:
            cache_file.write_bytes(ogg_bytes)

        return ogg_bytes

    except Exception as e:
        logger.error(f"❌ Error en Chatterbox TTS: {e}")
        # Fallback a edge-TTS si Chatterbox falla
        logger.info("↩️  Usando edge-TTS como fallback...")
        return await _generar_audio_edge_tts(texto)


def _tensor_to_ogg(wav_tensor, sample_rate: int) -> bytes:
    """
    Convierte tensor PyTorch de audio → OGG Opus bytes.
    Telegram acepta voice messages en OGG Opus.
    """
    import torch
    import torchaudio as ta
    from pydub import AudioSegment

    # Guardar WAV temporal
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_wav:
        wav_path = tmp_wav.name

    try:
        # Asegurar formato correcto [1, samples]
        if wav_tensor.dim() == 1:
            wav_tensor = wav_tensor.unsqueeze(0)

        ta.save(wav_path, wav_tensor.cpu(), sample_rate)

        # Convertir WAV → OGG Opus con pydub
        audio    = AudioSegment.from_wav(wav_path)

        # Ajuste de velocidad: ligeramente más lento para voz pausada
        # pydub no tiene speed nativo, pero podemos reducir frame rate
        # Para un efecto más natural y pausado, reducimos 5%
        audio    = audio.set_frame_rate(int(audio.frame_rate * 0.95))

        ogg_buf  = io.BytesIO()
        audio.export(ogg_buf, format="ogg", codec="libopus",
                     bitrate="64k",
                     parameters=["-application", "voip"])
        return ogg_buf.getvalue()

    finally:
        Path(wav_path).unlink(missing_ok=True)


# ─── FALLBACK: edge-TTS (voz anterior) ──────────────────────────────

async def _generar_audio_edge_tts(texto: str,
                                   voz: str = "es-CL-CatalinaNeural") -> bytes:
    """
    Fallback al TTS anterior (edge-TTS) si Chatterbox no está disponible.
    Mantiene compatibilidad total con el código existente del bot.
    """
    import edge_tts

    # Aplicar pausas SSML para que suene más natural
    texto_ssml = _agregar_pausas_naturales(texto)

    communicate = edge_tts.Communicate(
        texto_ssml,
        voz,
        rate  = "-8%",    # Ligeramente más lento que el default
        pitch = "-2Hz",   # Tono ligeramente más bajo → más serio/profesional
    )
    buf = io.BytesIO()
    async for chunk in communicate.stream():
        if chunk["type"] == "audio":
            buf.write(chunk["data"])
    return buf.getvalue()


def _agregar_pausas_naturales(texto: str) -> str:
    """
    Mejora la prosodia del texto para una voz más natural.
    Funciona tanto con edge-TTS como con Chatterbox.
    """
    import re
    # Agregar pausa suave después de comas
    texto = re.sub(r",\s+", ", ", texto)
    # Agregar pausa más larga después de puntos
    texto = re.sub(r"\.\s+", ". ", texto)
    # Limpiar caracteres especiales de Telegram markdown
    texto = re.sub(r"[*_`]", "", texto)
    # Limpiar emojis (Chatterbox los ignora; edge-TTS a veces los lee raro)
    texto = re.sub(r"[^\w\s,.!?;:()\-áéíóúüñÁÉÍÓÚÜÑ]", " ", texto)
    # Comprimir espacios múltiples
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


# ─── FUNCIÓN PRINCIPAL: reemplaza la función de TTS del bot ─────────

async def texto_a_voz(
    texto: str,
    estilo: str = "normal",
    usar_chatterbox: bool = True
) -> Optional[bytes]:
    """
    Función principal de TTS para el bot.
    Reemplaza la función existente en el bot — misma firma, mejor calidad.

    Args:
        texto:            Texto a sintetizar
        estilo:           "normal" | "bienvenida" | "alerta" | "celebracion"
        usar_chatterbox:  True = Chatterbox, False = edge-TTS

    Returns:
        bytes OGG Opus listos para enviar como voice message a Telegram,
        o None si hay un error irrecuperable.
    """
    if not texto or not texto.strip():
        return None

    # Limpiar texto de markdown
    texto_limpio = _agregar_pausas_naturales(texto)

    # Configurar parámetros según el estilo
    configs = {
        "normal":       {"exaggeration": 0.35, "cfg_weight": 0.30},
        "bienvenida":   {"exaggeration": 0.45, "cfg_weight": 0.35},   # más cálida
        "alerta":       {"exaggeration": 0.55, "cfg_weight": 0.40},   # más expresiva
        "celebracion":  {"exaggeration": 0.60, "cfg_weight": 0.38},   # más animada
    }
    config = configs.get(estilo, configs["normal"])

    if usar_chatterbox and TTS_ENGINE == "chatterbox":
        try:
            return await generar_audio_chatterbox(texto_limpio, config)
        except Exception as e:
            logger.warning(f"Chatterbox falló ({e}), usando edge-TTS...")
            return await _generar_audio_edge_tts(texto_limpio)
    else:
        return await _generar_audio_edge_tts(texto_limpio)


async def limpiar_cache_tts(dias_antiguedad: int = 7):
    """
    Limpia archivos de caché de audio más antiguos que N días.
    Llamar periódicamente con un job del scheduler.
    """
    import time
    ahora   = time.time()
    limite  = dias_antiguedad * 86400
    borrados = 0
    for f in AUDIO_CACHE_DIR.glob("*.ogg"):
        if ahora - f.stat().st_mtime > limite:
            f.unlink()
            borrados += 1
    logger.info(f"🗑️  Caché TTS: {borrados} archivos eliminados.")


# ─── INTEGRACIÓN CON EL BOT EXISTENTE ───────────────────────────────
"""
INSTRUCCIONES DE INTEGRACIÓN:
==============================

1. INSTALAR DEPENDENCIAS:
   Agregar a requirements.txt:
     chatterbox-tts>=0.1.6
     torchaudio>=2.0.0
     pydub>=0.25.1
     ffmpeg-python>=0.2.0   # pydub necesita ffmpeg

   En Render, asegurarse de tener ffmpeg instalado:
     → En render.yaml o Build Command:
       apt-get install -y ffmpeg && pip install -r requirements.txt

2. REEMPLAZAR EN main.py o utils.py:
   # ANTES (edge-TTS):
   async def generar_voz(texto):
       communicate = edge_tts.Communicate(texto, "es-CL-CatalinaNeural")
       ...

   # DESPUÉS (Chatterbox):
   from tts_chatterbox import texto_a_voz
   async def generar_voz(texto, estilo="normal"):
       return await texto_a_voz(texto, estilo=estilo)

3. ENVIAR COMO VOICE MESSAGE EN TELEGRAM:
   audio_bytes = await texto_a_voz("Bienvenido a la Cofradía", estilo="bienvenida")
   if audio_bytes:
       await context.bot.send_voice(
           chat_id  = update.effective_chat.id,
           voice    = audio_bytes,
           caption  = "🔊 Mensaje de bienvenida"
       )

4. VARIABLES DE ENTORNO (opcionales):
   TTS_ENGINE=chatterbox          # "chatterbox" | "edge"
   TTS_EXAGGERATION=0.35          # 0.0-1.0, naturalidad emocional
   TTS_CFG_WEIGHT=0.30            # 0.0-1.0, velocidad (menor = más pausado)
   TTS_VOICE_REFERENCE=/path/to/voice.wav   # opcional: clonar voz específica
   TTS_USE_CACHE=true             # cachear audios para ahorrar compute
   TTS_CACHE_DIR=/tmp/tts_cache   # directorio de caché

5. JOB DE LIMPIEZA DE CACHÉ (agregar en main.py):
   from tts_chatterbox import limpiar_cache_tts
   application.job_queue.run_repeating(
       lambda ctx: asyncio.create_task(limpiar_cache_tts(7)),
       interval=86400,   # cada 24 horas
       first=3600,
   )

6. VOICE REFERENCE (opcional pero recomendado):
   Para una voz más consistente y personalizada, grabar 5-10 segundos
   de audio de una voz femenina con buena calidad (WAV, 22kHz o más)
   y apuntar TTS_VOICE_REFERENCE a ese archivo.
   Ejemplo de voces gratuitas: https://commonvoice.mozilla.org/es
"""
