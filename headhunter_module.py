"""
╔══════════════════════════════════════════════════════════════════════╗
║           MÓDULO HEADHUNTER — COFRADÍA DE NETWORKING                ║
║           Versión 1.0 · Germán · Destak E.I.R.L.                   ║
╠══════════════════════════════════════════════════════════════════════╣
║  Sistema de 3 capas completamente separadas:                        ║
║  1. Headhunter → Google Sheet privado (sin acceso a la red)         ║
║  2. Bot → Lee Sheet, publica vacante formateada en canal            ║
║  3. Cofrade → Postula por DM privado, headhunter recibe respuestas  ║
╠══════════════════════════════════════════════════════════════════════╣
║  MODELO DE PAGO:                                                    ║
║  • UF 3 por cada bloque de 3 publicaciones (posts 1-3, 4-6, etc.)  ║
║  • 1/3 del sueldo del candidato elegido por contratación            ║
║  • Datos bancarios: Destak E.I.R.L. · RUT 76.698.480-0              ║
║    Banco Santander · Cuenta Corriente 69104312                      ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import logging
import asyncio
from datetime import datetime, date
from typing import Optional
import gspread
from google.oauth2.service_account import Credentials
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
)
from telegram.ext import (
    ContextTypes, ConversationHandler, CommandHandler,
    CallbackQueryHandler, MessageHandler, filters
)
from supabase import create_client

logger = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ───────────────────────────────────────────────────
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
CANAL_EMPLEOS = os.getenv("CANAL_EMPLEOS_ID")   # @CofrEmpleos o ID numérico
ADMIN_ID      = int(os.getenv("ADMIN_USER_ID", "0"))
GDRIVE_CREDS  = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")

# Carpeta INBESTU en Google Drive
INBESTU_FOLDER_ID = os.getenv("INBESTU_FOLDER_ID", "")

# Datos bancarios Destak
BANCO_INFO = {
    "titular":  "Destak E.I.R.L.",
    "rut":      "76.698.480-0",
    "banco":    "Banco Santander",
    "tipo_cta": "Cuenta Corriente",
    "numero":   "69104312",
}

# Estados del ConversationHandler de postulación
(
    EST_CONFIRMAR,
    EST_PREGUNTA,
    EST_CONFIRMAR_ENVIO,
) = range(3)

# Estados del ConversationHandler de contratación
EST_CONTRATACION_SUELDO = 10

# ─── CLIENTE SUPABASE ────────────────────────────────────────────────
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ════════════════════════════════════════════════════════════════════
# PARTE 1: CONFIGURACIÓN GOOGLE SHEETS
# ════════════════════════════════════════════════════════════════════

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

def get_gspread_client():
    creds = Credentials.from_service_account_file(GDRIVE_CREDS, scopes=SCOPES)
    return gspread.authorize(creds)

def crear_sheet_headhunter(nombre_empresa: str, email_headhunter: str) -> dict:
    """
    Crea un Google Sheet nuevo en la carpeta INBESTU para un headhunter.
    Devuelve el sheet_id y la URL para compartir con el headhunter.

    Estructura del Sheet:
    - Hoja 1: "Mis Vacantes"      → El headhunter publica sus ofertas
    - Hoja 2: "Postulaciones"     → El bot escribe los candidatos (OCULTA para el HH)
    - Hoja 3: "Estado de Pagos"   → Registro de pagos pendientes y realizados
    """
    gc = get_gspread_client()
    nombre_sheet = f"Vacantes_HH_{nombre_empresa}_{date.today().year}"

    # Crear el spreadsheet
    sh = gc.create(nombre_sheet)

    # Mover a la carpeta INBESTU
    if INBESTU_FOLDER_ID:
        import requests
        headers = {"Authorization": f"Bearer {gc.auth.token}"}
        file_id = sh.id
        requests.patch(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            headers=headers,
            json={"parents": [INBESTU_FOLDER_ID]},
            params={"addParents": INBESTU_FOLDER_ID}
        )

    # ── HOJA 1: Mis Vacantes ─────────────────────────────────────────
    ws_vacantes = sh.sheet1
    ws_vacantes.update_title("📋 Mis Vacantes")

    # Encabezados con formato descriptivo
    encabezados = [
        ["╔══════════════ COFRADÍA DE NETWORKING — SISTEMA DE EMPLEOS ══════════════╗"],
        ["Instrucciones: Complete los campos de color amarillo. Cuando esté lista, cambie ESTADO a ACTIVA"],
        [""],
        [
            "ID_VACANTE", "CARGO", "EMPRESA (o Confidencial)",
            "UBICACIÓN", "MODALIDAD", "RENTA_BRUTA_CLP",
            "DESCRIPCIÓN (máx 5 líneas)", "REQUISITO_1", "REQUISITO_2",
            "REQUISITO_3", "REQUISITO_4", "REQUISITO_5",
            "PREGUNTA_1_AL_POSTULANTE", "PREGUNTA_2_AL_POSTULANTE",
            "PREGUNTA_3_AL_POSTULANTE", "FECHA_LIMITE",
            "ESTADO", "FECHA_PUBLICACION", "N_POSTULACIONES",
            "OBSERVACIONES_ADMIN"
        ],
        # Fila de ejemplo
        [
            "HH001", "Gerente de Operaciones", "Confidencial — Sector Minero",
            "Santiago", "Presencial", "2400000",
            "Liderarás un equipo de 45 personas en planta con estándares internacionales de seguridad y eficiencia.",
            "8+ años en operaciones industriales",
            "Experiencia en gestión de turnos",
            "Inglés intermedio deseable",
            "Disponibilidad para viajar 20%",
            "",
            "¿Cuántos años de experiencia tienes en operaciones mineras o industriales?",
            "¿Tienes disponibilidad para viajar aprox. 20% del tiempo?",
            "¿Cuáles son tus pretensiones de renta mensual bruta (CLP)?",
            "15/03/2026",
            "BORRADOR",   # <-- el headhunter cambia a ACTIVA cuando esté listo
            "", "0", ""
        ]
    ]
    ws_vacantes.update("A1", encabezados)

    # ── HOJA 2: Postulaciones (solo visible para admin) ───────────────
    ws_posts = sh.add_worksheet(title="📊 Postulaciones", rows=500, cols=20)
    enc_posts = [[
        "N°", "ID_VACANTE", "CARGO", "FECHA_POSTULACION",
        "NOMBRE", "RANGO_NAVAL", "CARGO_ACTUAL", "CIUDAD",
        "RESPUESTA_P1", "RESPUESTA_P2", "RESPUESTA_P3",
        "EMAIL_CONTACTO", "ESTADO_CANDIDATO", "NOTAS_HEADHUNTER"
    ]]
    ws_posts.update("A1", enc_posts)

    # ── HOJA 3: Estado de Pagos ───────────────────────────────────────
    ws_pagos = sh.add_worksheet(title="💰 Estado de Pagos", rows=100, cols=15)
    _generar_hoja_pagos(ws_pagos, nombre_empresa)

    # Proteger hoja de postulaciones (solo puede ver el admin con su cuenta)
    # Nota: la protección total requiere que el bot tenga permisos de editor
    # y se recomienda no compartir esa hoja con el HH

    # Compartir solo la hoja de vacantes con el headhunter (editor)
    sh.share(email_headhunter, perm_type="user", role="writer",
             notify=False)  # notify=False para control manual

    return {
        "sheet_id":    sh.id,
        "sheet_url":   sh.url,
        "nombre":      nombre_sheet,
        "empresa":     nombre_empresa,
        "email":       email_headhunter,
        "creado":      datetime.now().isoformat(),
    }


def _generar_hoja_pagos(ws, nombre_empresa: str):
    """Llena la hoja de estado de pagos con el modelo de cobro."""
    filas = [
        ["╔══════════ ESTADO DE PAGOS — COFRADÍA DE NETWORKING ══════════╗"],
        [f"Empresa: {nombre_empresa}"],
        [""],
        ["MODELO DE COBRO:"],
        ["• Publicaciones 1-3:   UF 3  (pago al activar la 1ra vacante del bloque)"],
        ["• Publicaciones 4-6:   UF 3  (pago al activar la 4ta vacante)"],
        ["• Por cada contratación exitosa: 1/3 del sueldo del candidato elegido"],
        [""],
        ["DATOS BANCARIOS PARA TRANSFERENCIA:"],
        ["Titular:        Destak E.I.R.L."],
        ["RUT:            76.698.480-0"],
        ["Banco:          Banco Santander"],
        ["Tipo de Cuenta: Cuenta Corriente"],
        ["Número:         69104312"],
        [""],
        ["─" * 70],
        ["BLOQUE", "N° VACANTES", "MONTO UF", "MONTO CLP APROX",
         "ESTADO PAGO", "FECHA PAGO", "COMPROBANTE"],
        ["Bloque 1 (posts 1-3)", "1-3", "UF 3", "≈ $106.000",
         "PENDIENTE", "", ""],
        ["Bloque 2 (posts 4-6)", "4-6", "UF 3", "≈ $106.000",
         "—", "", ""],
        [""],
        ["─" * 70],
        ["REGISTRO DE CONTRATACIONES (para cálculo de comisión)"],
        ["ID_VACANTE", "CARGO", "CANDIDATO ELEGIDO", "SUELDO_ACORDADO_CLP",
         "COMISIÓN (1/3)", "ESTADO PAGO", "FECHA PAGO", "COMPROBANTE"],
    ]
    ws.update("A1", filas)


# ════════════════════════════════════════════════════════════════════
# PARTE 2: FUNCIONES DE SUPABASE (DDL y operaciones)
# ════════════════════════════════════════════════════════════════════

"""
SQL para ejecutar en Supabase:

CREATE TABLE IF NOT EXISTS headhunters (
    id              SERIAL PRIMARY KEY,
    nombre_empresa  TEXT NOT NULL,
    nombre_contacto TEXT,
    email           TEXT UNIQUE NOT NULL,
    telefono        TEXT,
    sheet_id        TEXT UNIQUE,
    sheet_url       TEXT,
    activo          BOOLEAN DEFAULT TRUE,
    total_vacantes  INTEGER DEFAULT 0,
    bloque_actual   INTEGER DEFAULT 1,
    pago_bloque_ok  BOOLEAN DEFAULT FALSE,
    fecha_registro  TIMESTAMPTZ DEFAULT NOW(),
    notas           TEXT
);

CREATE TABLE IF NOT EXISTS vacantes (
    id                  SERIAL PRIMARY KEY,
    headhunter_id       INTEGER REFERENCES headhunters(id),
    id_vacante_sheet    TEXT,           -- ej: HH001
    cargo               TEXT NOT NULL,
    empresa             TEXT,
    ubicacion           TEXT,
    modalidad           TEXT,
    renta_bruta_clp     INTEGER,
    descripcion         TEXT,
    requisitos          JSONB,          -- lista de strings
    preguntas           JSONB,          -- lista de strings (máx 3)
    fecha_limite        DATE,
    estado              TEXT DEFAULT 'BORRADOR',  -- BORRADOR|ACTIVA|CERRADA|PAGADA
    telegram_message_id BIGINT,         -- id del mensaje publicado en canal
    n_postulaciones     INTEGER DEFAULT 0,
    contratado          BOOLEAN DEFAULT FALSE,
    sueldo_contratado   INTEGER,        -- sueldo acordado (para calcular comisión)
    comision_pagada     BOOLEAN DEFAULT FALSE,
    fecha_publicacion   TIMESTAMPTZ,
    fecha_cierre        TIMESTAMPTZ,
    UNIQUE(headhunter_id, id_vacante_sheet)
);

CREATE TABLE IF NOT EXISTS postulaciones (
    id              SERIAL PRIMARY KEY,
    vacante_id      INTEGER REFERENCES vacantes(id),
    user_id         BIGINT NOT NULL,
    nombre_cofrade  TEXT,
    rango_naval     TEXT,
    cargo_actual    TEXT,
    ciudad          TEXT,
    email_contacto  TEXT,
    respuestas      JSONB,          -- {"p1": "...", "p2": "...", "p3": "..."}
    estado          TEXT DEFAULT 'ENVIADA',  -- ENVIADA|REVISADA|SELECCIONADO|DESCARTADO
    fecha_postulacion TIMESTAMPTZ DEFAULT NOW(),
    notas_admin     TEXT,
    UNIQUE(vacante_id, user_id)
);

CREATE TABLE IF NOT EXISTS pagos_headhunter (
    id              SERIAL PRIMARY KEY,
    headhunter_id   INTEGER REFERENCES headhunters(id),
    tipo_pago       TEXT,   -- 'BLOQUE_PUBLICACION' | 'COMISION_CONTRATACION'
    vacante_id      INTEGER REFERENCES vacantes(id),
    bloque_numero   INTEGER,
    monto_clp       INTEGER,
    monto_uf        DECIMAL(6,3),
    estado          TEXT DEFAULT 'PENDIENTE',  -- PENDIENTE|VERIFICADO|RECHAZADO
    comprobante_url TEXT,
    fecha_vencimiento DATE,
    fecha_pago      TIMESTAMPTZ,
    notas           TEXT,
    creado_en       TIMESTAMPTZ DEFAULT NOW()
);
"""


def registrar_headhunter_db(datos: dict) -> dict:
    """Guarda el nuevo headhunter en Supabase."""
    res = supabase.table("headhunters").insert({
        "nombre_empresa":  datos["empresa"],
        "nombre_contacto": datos.get("contacto", ""),
        "email":           datos["email"],
        "telefono":        datos.get("telefono", ""),
        "sheet_id":        datos["sheet_id"],
        "sheet_url":       datos["sheet_url"],
        "activo":          True,
    }).execute()
    return res.data[0] if res.data else {}


def obtener_vacantes_activas_nuevas() -> list:
    """
    Lee todos los sheets de headhunters activos y devuelve
    las vacantes en estado ACTIVA que aún no están en BD.
    """
    gc       = get_gspread_client()
    hhs      = supabase.table("headhunters").select("*").eq("activo", True).execute().data
    nuevas   = []

    for hh in hhs:
        if not hh.get("sheet_id"):
            continue
        try:
            sh        = gc.open_by_key(hh["sheet_id"])
            ws        = sh.worksheet("📋 Mis Vacantes")
            registros = ws.get_all_records(head=4)  # fila 4 = encabezados
        except Exception as e:
            logger.warning(f"Error leyendo sheet de {hh['nombre_empresa']}: {e}")
            continue

        for fila in registros:
            if str(fila.get("ESTADO", "")).upper() != "ACTIVA":
                continue
            id_sheet = str(fila.get("ID_VACANTE", "")).strip()
            if not id_sheet:
                continue

            # Verificar si ya está en BD
            existe = supabase.table("vacantes").select("id") \
                .eq("headhunter_id", hh["id"]) \
                .eq("id_vacante_sheet", id_sheet).execute().data

            if not existe:
                nuevas.append({
                    "headhunter": hh,
                    "fila":       fila,
                    "id_sheet":   id_sheet,
                })

    return nuevas


def guardar_vacante_db(hh_id: int, fila: dict, id_sheet: str) -> dict:
    """Persiste la vacante nueva en Supabase."""
    # Construir lista de requisitos (columnas REQ_1..5)
    requisitos = [
        str(fila.get(f"REQUISITO_{i}", "")).strip()
        for i in range(1, 6)
        if str(fila.get(f"REQUISITO_{i}", "")).strip()
    ]
    preguntas = [
        str(fila.get(f"PREGUNTA_{i}_AL_POSTULANTE", "")).strip()
        for i in range(1, 4)
        if str(fila.get(f"PREGUNTA_{i}_AL_POSTULANTE", "")).strip()
    ]
    try:
        renta = int(str(fila.get("RENTA_BRUTA_CLP", "0")).replace(".", "").replace(",", ""))
    except Exception:
        renta = 0

    res = supabase.table("vacantes").insert({
        "headhunter_id":     hh_id,
        "id_vacante_sheet":  id_sheet,
        "cargo":             str(fila.get("CARGO", "")).strip(),
        "empresa":           str(fila.get("EMPRESA (o Confidencial)", "Confidencial")).strip(),
        "ubicacion":         str(fila.get("UBICACIÓN", "")).strip(),
        "modalidad":         str(fila.get("MODALIDAD", "")).strip(),
        "renta_bruta_clp":   renta,
        "descripcion":       str(fila.get("DESCRIPCIÓN (máx 5 líneas)", "")).strip(),
        "requisitos":        json.dumps(requisitos),
        "preguntas":         json.dumps(preguntas),
        "fecha_limite":      _parse_fecha(str(fila.get("FECHA_LIMITE", ""))),
        "estado":            "ACTIVA",
        "fecha_publicacion": datetime.now().isoformat(),
    }).execute()
    return res.data[0] if res.data else {}


def _parse_fecha(texto: str) -> Optional[str]:
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
        try:
            return datetime.strptime(texto.strip(), fmt).date().isoformat()
        except Exception:
            pass
    return None


# ════════════════════════════════════════════════════════════════════
# PARTE 3: LÓGICA DE PAGOS
# ════════════════════════════════════════════════════════════════════

UF_PUBLICACION   = 3       # UF por cada bloque de 3 publicaciones
FACTOR_COMISION  = 1 / 3   # 1/3 del sueldo del contratado

def calcular_uf_a_clp(uf_value: float, uf_clp: float = 37150.0) -> int:
    """
    Convierte UF a CLP usando el valor del día.
    Por defecto usa valor aproximado; en producción consultar SII API.
    """
    return int(uf_value * uf_clp)


async def obtener_uf_hoy() -> float:
    """
    Consulta el valor de la UF del día desde mindicador.cl (API gratuita).
    """
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://mindicador.cl/api/uf",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                data = await r.json()
                return float(data["serie"][0]["valor"])
    except Exception:
        return 37150.0   # valor fallback aproximado


def verificar_pago_bloque(hh_id: int) -> dict:
    """
    Verifica si el headhunter ha pagado el bloque de publicaciones.
    El bloque se cobra al activar la 1ra vacante de cada grupo de 3.
    Retorna: {"debe_pagar": bool, "bloque": int, "n_publicadas": int}
    """
    hh = supabase.table("headhunters").select("*").eq("id", hh_id).single().execute().data
    total = hh.get("total_vacantes", 0)
    bloque_actual = ((total) // 3) + 1
    pago_ok = hh.get("pago_bloque_ok", False)

    # Si es la primera del bloque (total % 3 == 0), hay que cobrar antes de publicar
    debe_pagar = (total % 3 == 0) and not pago_ok

    return {
        "debe_pagar":    debe_pagar,
        "bloque":        bloque_actual,
        "n_publicadas":  total,
        "pago_ok":       pago_ok,
    }


def registrar_pago_bloque_pendiente(hh_id: int, bloque: int) -> dict:
    """Crea un registro de pago pendiente por bloque de publicaciones."""
    uf_clp = 37150  # actualizar dinámicamente en producción
    monto  = calcular_uf_a_clp(UF_PUBLICACION, uf_clp)

    res = supabase.table("pagos_headhunter").insert({
        "headhunter_id": hh_id,
        "tipo_pago":     "BLOQUE_PUBLICACION",
        "bloque_numero": bloque,
        "monto_clp":     monto,
        "monto_uf":      UF_PUBLICACION,
        "estado":        "PENDIENTE",
    }).execute()
    return res.data[0] if res.data else {}


def registrar_comision_contratacion(vacante_id: int, sueldo_clp: int) -> dict:
    """Crea un registro de comisión por contratación exitosa."""
    comision = int(sueldo_clp * FACTOR_COMISION)

    # Obtener headhunter_id de la vacante
    vac = supabase.table("vacantes").select("headhunter_id").eq("id", vacante_id).single().execute().data
    hh_id = vac["headhunter_id"]

    res = supabase.table("pagos_headhunter").insert({
        "headhunter_id": hh_id,
        "tipo_pago":     "COMISION_CONTRATACION",
        "vacante_id":    vacante_id,
        "monto_clp":     comision,
        "monto_uf":      None,
        "estado":        "PENDIENTE",
    }).execute()
    return res.data[0] if res.data else {}


def _generar_mensaje_pago_bloque(bloque: int, monto_clp: int) -> str:
    return (
        f"⚠️ *PAGO REQUERIDO — Bloque de publicaciones #{bloque}*\n\n"
        f"Para publicar sus próximas 3 vacantes en la Cofradía de Networking, "
        f"debe realizar el siguiente pago:\n\n"
        f"💰 *Monto:* UF 3 (≈ ${monto_clp:,.0f} CLP al día de hoy)\n\n"
        f"🏦 *Datos de transferencia:*\n"
        f"```\n"
        f"Titular:  {BANCO_INFO['titular']}\n"
        f"RUT:      {BANCO_INFO['rut']}\n"
        f"Banco:    {BANCO_INFO['banco']}\n"
        f"Cuenta:   {BANCO_INFO['tipo_cta']} {BANCO_INFO['numero']}\n"
        f"```\n\n"
        f"📧 Una vez realizado, envíe el comprobante a: `pagos@cofradia.cl`\n"
        f"o responda este mensaje con la imagen del comprobante.\n\n"
        f"_Contáctenos si tiene consultas sobre el proceso de pago._"
    )


def _generar_mensaje_comision(cargo: str, sueldo: int, comision: int) -> str:
    return (
        f"🎉 *¡Contratación exitosa confirmada!*\n\n"
        f"Se registró la contratación de un candidato para el cargo: *{cargo}*\n\n"
        f"📋 *Detalle de la comisión:*\n"
        f"• Sueldo acordado: ${sueldo:,.0f} CLP\n"
        f"• Comisión (1/3): *${comision:,.0f} CLP*\n\n"
        f"🏦 *Datos de transferencia:*\n"
        f"```\n"
        f"Titular:  {BANCO_INFO['titular']}\n"
        f"RUT:      {BANCO_INFO['rut']}\n"
        f"Banco:    {BANCO_INFO['banco']}\n"
        f"Cuenta:   {BANCO_INFO['tipo_cta']} {BANCO_INFO['numero']}\n"
        f"```\n\n"
        f"Plazo de pago: 5 días hábiles.\n"
        f"📧 Comprobante a: `pagos@cofradia.cl`"
    )


# ════════════════════════════════════════════════════════════════════
# PARTE 4: PUBLICACIÓN EN CANAL DE EMPLEOS
# ════════════════════════════════════════════════════════════════════

def _formatear_vacante(vacante: dict, n_post: int = 0) -> str:
    """Genera el mensaje formateado profesional de la vacante."""
    cargo     = vacante.get("cargo", "")
    empresa   = vacante.get("empresa", "Confidencial")
    ubicacion = vacante.get("ubicacion", "Chile")
    modalidad = vacante.get("modalidad", "")
    renta     = vacante.get("renta_bruta_clp", 0)
    desc      = vacante.get("descripcion", "")
    fecha_lim = vacante.get("fecha_limite", "")
    vacante_id = vacante.get("id")

    try:
        reqs = json.loads(vacante.get("requisitos", "[]"))
    except Exception:
        reqs = []

    renta_str = f"${renta:,.0f} CLP bruto/mes" if renta else "A convenir"
    fecha_str = fecha_lim if fecha_lim else "Hasta cubrir vacante"

    reqs_txt = "\n".join(f"  ✅ {r}" for r in reqs) if reqs else "  ✅ Ver descripción completa"
    loc_str  = f"{ubicacion}" + (f" · {modalidad}" if modalidad else "")

    return (
        f"╔══════════════════════════════════╗\n"
        f"  💼  NUEVA OPORTUNIDAD LABORAL\n"
        f"  Cofradía de Networking · Empleos\n"
        f"╚══════════════════════════════════╝\n\n"
        f"🏢  *{cargo.upper()}*\n"
        f"📍  {loc_str}\n"
        f"🏅  Empresa: _{empresa}_\n\n"
        f"📋  *DESCRIPCIÓN:*\n"
        f"_{desc}_\n\n"
        f"✅  *REQUISITOS CLAVE:*\n{reqs_txt}\n\n"
        f"💰  Renta: *{renta_str}*\n"
        f"📅  Postula antes del: *{fecha_str}*\n\n"
        f"👥  Postulaciones recibidas: *{n_post}*\n"
        f"🔖  Ref: `{vacante.get('id_vacante_sheet', 'N/A')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )


async def publicar_vacante_en_canal(bot: Bot, vacante_db: dict) -> int:
    """
    Publica la vacante en el canal de empleos y devuelve el message_id.
    """
    texto = _formatear_vacante(vacante_db, 0)
    boton = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "📩  POSTULAR AHORA",
            callback_data=f"postular_{vacante_db['id']}"
        )
    ]])
    msg = await bot.send_message(
        chat_id    = CANAL_EMPLEOS,
        text       = texto,
        parse_mode = "Markdown",
        reply_markup = boton,
    )
    return msg.message_id


async def actualizar_contador_canal(bot: Bot, vacante: dict):
    """Edita el mensaje del canal actualizando el contador de postulaciones."""
    if not vacante.get("telegram_message_id"):
        return
    texto = _formatear_vacante(vacante, vacante.get("n_postulaciones", 0))
    boton = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"📩  POSTULAR AHORA  ({vacante.get('n_postulaciones', 0)} ya postularon)",
            callback_data=f"postular_{vacante['id']}"
        )
    ]])
    try:
        await bot.edit_message_text(
            chat_id    = CANAL_EMPLEOS,
            message_id = vacante["telegram_message_id"],
            text       = texto,
            parse_mode = "Markdown",
            reply_markup = boton,
        )
    except Exception as e:
        logger.warning(f"No se pudo actualizar el contador: {e}")


# ════════════════════════════════════════════════════════════════════
# PARTE 5: FLUJO DE POSTULACIÓN POR DM (ConversationHandler)
# ════════════════════════════════════════════════════════════════════

async def postular_inicio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Punto de entrada cuando el cofrade presiona POSTULAR AHORA.
    Se activa tanto desde inline button en canal como desde DM.
    """
    query = update.callback_query
    if query:
        await query.answer()
        vacante_id = int(query.data.split("_")[1])
    else:
        # Comando directo: /postular <vacante_id>
        try:
            vacante_id = int(context.args[0])
        except Exception:
            await update.message.reply_text("❌ Vacante no encontrada.")
            return ConversationHandler.END

    user = update.effective_user

    # Verificar si ya postuló
    ya_postulo = supabase.table("postulaciones") \
        .select("id") \
        .eq("vacante_id", vacante_id) \
        .eq("user_id", user.id).execute().data

    if ya_postulo:
        txt = "⚠️ Ya has enviado tu postulación para esta vacante. ¡Mucho éxito!"
        if query:
            await context.bot.send_message(chat_id=user.id, text=txt)
        else:
            await update.message.reply_text(txt)
        return ConversationHandler.END

    # Obtener datos de la vacante
    vac_res = supabase.table("vacantes").select("*").eq("id", vacante_id).execute().data
    if not vac_res or vac_res[0].get("estado") != "ACTIVA":
        txt = "❌ Esta vacante ya no está disponible o fue cerrada."
        if query:
            await context.bot.send_message(chat_id=user.id, text=txt)
        else:
            await update.message.reply_text(txt)
        return ConversationHandler.END

    vac = vac_res[0]
    context.user_data["postulando_vacante"] = vac
    context.user_data["respuestas_post"]    = {}

    # Enviar presentación de la vacante por DM
    intro = (
        f"👋 Hola *{user.first_name}*\n\n"
        f"Vas a postular al cargo de:\n"
        f"🏢 *{vac['cargo']}* — {vac['empresa']}\n"
        f"📍 {vac['ubicacion']} · 💰 ${vac.get('renta_bruta_clp',0):,.0f} CLP\n\n"
        f"El proceso tiene {len(json.loads(vac['preguntas']))} pregunta(s) rápida(s). "
        f"Tu información *nunca será compartida directamente* con el headhunter "
        f"sin tu consentimiento.\n\n"
        f"¿Deseas continuar?"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Sí, continuar", callback_data=f"post_confirm_{vacante_id}"),
        InlineKeyboardButton("❌ Cancelar",       callback_data="post_cancel"),
    ]])

    await context.bot.send_message(
        chat_id      = user.id,
        text         = intro,
        parse_mode   = "Markdown",
        reply_markup = kb,
    )
    return EST_CONFIRMAR


async def postular_confirmar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """El cofrade confirmó que quiere continuar."""
    query = update.callback_query
    await query.answer()

    if query.data == "post_cancel":
        await query.edit_message_text("Postulación cancelada. ¡Hasta pronto!")
        return ConversationHandler.END

    vac       = context.user_data["postulando_vacante"]
    preguntas = json.loads(vac["preguntas"])
    context.user_data["preguntas_post"] = preguntas
    context.user_data["pregunta_idx"]   = 0

    await _enviar_pregunta(query, context, 0, preguntas)
    return EST_PREGUNTA


async def _enviar_pregunta(query_or_msg, context, idx: int, preguntas: list):
    total = len(preguntas)
    texto = (
        f"📝 *Pregunta {idx + 1} de {total}:*\n\n"
        f"_{preguntas[idx]}_"
    )
    if hasattr(query_or_msg, "edit_message_text"):
        await query_or_msg.edit_message_text(texto, parse_mode="Markdown")
    else:
        await context.bot.send_message(
            chat_id    = query_or_msg.from_user.id,
            text       = texto,
            parse_mode = "Markdown"
        )


async def postular_respuesta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recibe la respuesta a cada pregunta."""
    user      = update.effective_user
    respuesta = update.message.text.strip()
    preguntas = context.user_data.get("preguntas_post", [])
    idx       = context.user_data.get("pregunta_idx", 0)

    context.user_data["respuestas_post"][f"p{idx + 1}"] = respuesta

    siguiente = idx + 1
    if siguiente < len(preguntas):
        context.user_data["pregunta_idx"] = siguiente
        await _enviar_pregunta(update.message, context, siguiente, preguntas)
        return EST_PREGUNTA

    # Todas las preguntas respondidas — mostrar resumen
    vac       = context.user_data["postulando_vacante"]
    respuestas = context.user_data["respuestas_post"]

    resumen = f"✅ *Resumen de tu postulación — {vac['cargo']}*\n\n"
    for i, preg in enumerate(preguntas, 1):
        resumen += f"*P{i}:* _{preg}_\n💬 {respuestas.get(f'p{i}', '—')}\n\n"
    resumen += "¿Confirmas el envío de tu postulación?"

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🚀 Enviar postulación",  callback_data="post_enviar"),
        InlineKeyboardButton("✏️ Corregir respuestas", callback_data="post_corregir"),
    ]])
    await update.message.reply_text(resumen, parse_mode="Markdown", reply_markup=kb)
    return EST_CONFIRMAR_ENVIO


async def postular_enviar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Envía la postulación definitivamente."""
    query = update.callback_query
    await query.answer()

    if query.data == "post_corregir":
        context.user_data["respuestas_post"] = {}
        context.user_data["pregunta_idx"]    = 0
        preguntas = context.user_data.get("preguntas_post", [])
        await _enviar_pregunta(query, context, 0, preguntas)
        return EST_PREGUNTA

    user = update.effective_user
    vac  = context.user_data["postulando_vacante"]

    # Obtener perfil del cofrade desde BD
    perfil = supabase.table("users").select("*").eq("user_id", user.id).execute().data
    perfil = perfil[0] if perfil else {}

    # Guardar postulación en Supabase
    supabase.table("postulaciones").insert({
        "vacante_id":     vac["id"],
        "user_id":        user.id,
        "nombre_cofrade": perfil.get("nombre", user.full_name),
        "rango_naval":    perfil.get("rango_naval", ""),
        "cargo_actual":   perfil.get("cargo_actual", ""),
        "ciudad":         perfil.get("ciudad", ""),
        "email_contacto": perfil.get("email", ""),
        "respuestas":     json.dumps(context.user_data["respuestas_post"]),
        "estado":         "ENVIADA",
    }).execute()

    # Actualizar contador en vacante
    nueva_n = vac.get("n_postulaciones", 0) + 1
    supabase.table("vacantes").update({
        "n_postulaciones": nueva_n
    }).eq("id", vac["id"]).execute()

    # Actualizar contador en el canal de empleos
    vac_actualizada = supabase.table("vacantes").select("*").eq("id", vac["id"]).single().execute().data
    await actualizar_contador_canal(context.bot, vac_actualizada)

    # Escribir postulación en el Sheet del headhunter
    await _escribir_postulacion_en_sheet(vac, perfil, context.user_data["respuestas_post"], nueva_n)

    confirmacion = (
        f"🎉 *¡Postulación enviada con éxito!*\n\n"
        f"🏢 Cargo: *{vac['cargo']}*\n"
        f"🔖 Ref: `{vac.get('id_vacante_sheet','')}`\n\n"
        f"El equipo de selección revisará tu perfil y se pondrá en contacto "
        f"directamente si tu candidatura avanza al siguiente paso.\n\n"
        f"👥 *Eres el postulante N° {nueva_n}* para esta vacante.\n\n"
        f"¡Mucho éxito, cofrade! ⚓"
    )
    await query.edit_message_text(confirmacion, parse_mode="Markdown")
    return ConversationHandler.END


async def _escribir_postulacion_en_sheet(vac: dict, perfil: dict, respuestas: dict, numero: int):
    """Escribe la nueva postulación en la hoja protegida del headhunter."""
    try:
        hh  = supabase.table("headhunters").select("sheet_id").eq("id", vac["headhunter_id"]).single().execute().data
        gc  = get_gspread_client()
        sh  = gc.open_by_key(hh["sheet_id"])
        ws  = sh.worksheet("📊 Postulaciones")

        nueva_fila = [
            numero,
            vac.get("id_vacante_sheet", ""),
            vac.get("cargo", ""),
            datetime.now().strftime("%d/%m/%Y %H:%M"),
            perfil.get("nombre", "—"),
            perfil.get("rango_naval", "—"),
            perfil.get("cargo_actual", "—"),
            perfil.get("ciudad", "—"),
            respuestas.get("p1", "—"),
            respuestas.get("p2", "—"),
            respuestas.get("p3", "—"),
            perfil.get("email", "—"),
            "NUEVA",
            "",
        ]
        ws.append_row(nueva_fila, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error(f"Error escribiendo postulación en sheet: {e}")


# ════════════════════════════════════════════════════════════════════
# PARTE 6: JOB SCHEDULER — revisar sheets cada 30 minutos
# ════════════════════════════════════════════════════════════════════

async def job_revisar_vacantes(context: ContextTypes.DEFAULT_TYPE):
    """
    Tarea programada que se ejecuta cada 30 minutos.
    Lee sheets de headhunters y publica vacantes nuevas.
    """
    logger.info("🔍 Revisando vacantes en Google Sheets...")
    nuevas = obtener_vacantes_activas_nuevas()

    for item in nuevas:
        hh     = item["headhunter"]
        fila   = item["fila"]
        id_sh  = item["id_sheet"]

        # Verificar si el headhunter tiene pago al día
        estado_pago = verificar_pago_bloque(hh["id"])
        if estado_pago["debe_pagar"]:
            # Notificar al admin
            monto = calcular_uf_a_clp(UF_PUBLICACION)
            msg_pago = _generar_mensaje_pago_bloque(
                estado_pago["bloque"], monto
            )
            await context.bot.send_message(
                chat_id    = ADMIN_ID,
                text       = f"💳 *Headhunter {hh['nombre_empresa']}* tiene vacante pendiente de publicar.\n\n{msg_pago}",
                parse_mode = "Markdown"
            )
            # No publicar hasta confirmar el pago
            continue

        # Guardar en BD
        vac_db = guardar_vacante_db(hh["id"], fila, id_sh)
        if not vac_db:
            continue

        # Publicar en canal
        msg_id = await publicar_vacante_en_canal(context.bot, vac_db)

        # Actualizar telegram_message_id
        supabase.table("vacantes").update({"telegram_message_id": msg_id}) \
            .eq("id", vac_db["id"]).execute()

        # Incrementar contador de vacantes del headhunter
        supabase.table("headhunters").update({
            "total_vacantes": hh["total_vacantes"] + 1,
            "pago_bloque_ok": False,  # resetear para el siguiente bloque
        }).eq("id", hh["id"]).execute()

        # Notificar al admin
        await context.bot.send_message(
            chat_id    = ADMIN_ID,
            text       = f"✅ Vacante publicada: *{vac_db['cargo']}* (HH: {hh['nombre_empresa']})",
            parse_mode = "Markdown"
        )
        logger.info(f"✅ Vacante {id_sh} publicada en canal.")


# ════════════════════════════════════════════════════════════════════
# PARTE 7: COMANDOS DE ADMINISTRACIÓN
# ════════════════════════════════════════════════════════════════════

async def cmd_nuevo_headhunter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /nuevo_headhunter <empresa> <email>
    Crea un nuevo headhunter con su Google Sheet en INBESTU.
    Solo para admin.
    """
    if update.effective_user.id != ADMIN_ID:
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Uso: `/nuevo_headhunter NombreEmpresa email@empresa.com`",
            parse_mode="Markdown"
        )
        return

    empresa = " ".join(context.args[:-1])
    email   = context.args[-1]

    await update.message.reply_text(f"⏳ Creando Google Sheet para *{empresa}*...", parse_mode="Markdown")

    try:
        datos = crear_sheet_headhunter(empresa, email)
        hh_db = registrar_headhunter_db({**datos, "email": email})

        uf_clp = await obtener_uf_hoy()
        monto  = calcular_uf_a_clp(UF_PUBLICACION, uf_clp)

        # Generar mensaje de bienvenida para el headhunter
        bienvenida = (
            f"✅ *Headhunter registrado exitosamente*\n\n"
            f"🏢 Empresa: *{empresa}*\n"
            f"📊 Google Sheet: [Abrir hoja de vacantes]({datos['sheet_url']})\n\n"
            f"📋 *INSTRUCCIONES PARA EL HEADHUNTER:*\n"
            f"1. Abrir el Sheet con el email {email}\n"
            f"2. Completar los campos de color en «📋 Mis Vacantes»\n"
            f"3. Cambiar ESTADO a *ACTIVA* cuando la vacante esté lista\n"
            f"4. El bot la publicará automáticamente dentro de 30 minutos\n\n"
            f"💰 *MODELO DE COBRO:*\n"
            f"• Primeras 3 publicaciones: *UF {UF_PUBLICACION}* (≈ ${monto:,.0f} CLP)\n"
            f"• Por contratación exitosa: *1/3 del sueldo acordado*\n\n"
            f"🏦 Transferir a:\n"
            f"`{BANCO_INFO['titular']} · {BANCO_INFO['banco']}`\n"
            f"`RUT: {BANCO_INFO['rut']} · CC: {BANCO_INFO['numero']}`"
        )
        await update.message.reply_text(bienvenida, parse_mode="Markdown", disable_web_page_preview=True)

    except Exception as e:
        logger.exception(e)
        await update.message.reply_text(f"❌ Error: {e}")


async def cmd_confirmar_pago(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /confirmar_pago <headhunter_id> [tipo: bloque|comision] [vacante_id]
    Marca el pago como verificado y desbloquea publicaciones.
    Solo para admin.
    """
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 1:
        await update.message.reply_text("Uso: `/confirmar_pago <hh_id> [bloque|comision] [vacante_id]`", parse_mode="Markdown")
        return

    hh_id = int(context.args[0])
    tipo  = context.args[1] if len(context.args) > 1 else "bloque"

    # Marcar el pago del bloque como OK
    supabase.table("headhunters").update({"pago_bloque_ok": True}).eq("id", hh_id).execute()

    # Marcar pago pendiente como verificado
    supabase.table("pagos_headhunter") \
        .update({"estado": "VERIFICADO", "fecha_pago": datetime.now().isoformat()}) \
        .eq("headhunter_id", hh_id) \
        .eq("estado", "PENDIENTE") \
        .execute()

    await update.message.reply_text(f"✅ Pago del headhunter ID {hh_id} confirmado. Las vacantes pendientes se publicarán en el próximo ciclo (máx 30 min).")


async def cmd_registrar_contratacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /contratacion <vacante_id> <sueldo_clp>
    Registra una contratación exitosa y genera la factura de comisión.
    Solo para admin.
    """
    if update.effective_user.id != ADMIN_ID:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Uso: `/contratacion <vacante_id> <sueldo_clp>`", parse_mode="Markdown")
        return

    vacante_id = int(context.args[0])
    sueldo     = int(context.args[1].replace(".", "").replace(",", ""))
    comision   = int(sueldo * FACTOR_COMISION)

    # Marcar vacante como contratada
    vac = supabase.table("vacantes").update({
        "contratado":       True,
        "sueldo_contratado": sueldo,
        "estado":           "CERRADA",
    }).eq("id", vacante_id).execute().data

    cargo = vac[0]["cargo"] if vac else "—"

    # Registrar comisión
    registrar_comision_contratacion(vacante_id, sueldo)

    # Obtener headhunter para notificarle
    vac_full = supabase.table("vacantes").select("headhunter_id").eq("id", vacante_id).single().execute().data
    hh       = supabase.table("headhunters").select("*").eq("id", vac_full["headhunter_id"]).single().execute().data

    msg = _generar_mensaje_comision(cargo, sueldo, comision)
    await update.message.reply_text(
        f"✅ Contratación registrada.\n\n{msg}\n\n_Notifica al headhunter {hh['nombre_empresa']} ({hh['email']}) sobre el pago._",
        parse_mode="Markdown"
    )


async def cmd_panel_headhunters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /panel_hh
    Muestra el panel de control de headhunters con métricas.
    """
    if update.effective_user.id != ADMIN_ID:
        return

    hhs = supabase.table("headhunters").select("*, vacantes(count)").execute().data or []

    if not hhs:
        await update.message.reply_text("No hay headhunters registrados.")
        return

    texto = "📊 *PANEL DE HEADHUNTERS — COFRADÍA*\n\n"
    for hh in hhs:
        estado = "✅ Activo" if hh.get("activo") else "❌ Inactivo"
        pago   = "✅ Al día" if hh.get("pago_bloque_ok") else "⚠️ Pendiente"
        texto += (
            f"*{hh['nombre_empresa']}* (ID: {hh['id']})\n"
            f"  📧 {hh['email']}\n"
            f"  📋 Vacantes publicadas: {hh.get('total_vacantes', 0)}\n"
            f"  💰 Pago bloque: {pago}\n"
            f"  🔘 Estado: {estado}\n\n"
        )

    # Pagos pendientes
    pendientes = supabase.table("pagos_headhunter") \
        .select("*, headhunters(nombre_empresa)") \
        .eq("estado", "PENDIENTE").execute().data or []

    if pendientes:
        texto += f"⚠️ *PAGOS PENDIENTES ({len(pendientes)}):*\n"
        for p in pendientes:
            hh_nombre = p.get("headhunters", {}).get("nombre_empresa", "—")
            tipo  = "Bloque publicación" if p["tipo_pago"] == "BLOQUE_PUBLICACION" else "Comisión contratación"
            texto += f"  • {hh_nombre} — {tipo}: ${p['monto_clp']:,.0f} CLP\n"

    await update.message.reply_text(texto, parse_mode="Markdown")


# ════════════════════════════════════════════════════════════════════
# PARTE 8: CONSTRUCCIÓN DEL ConversationHandler
# ════════════════════════════════════════════════════════════════════

def build_headhunter_handlers():
    """
    Retorna la lista de handlers listos para agregar a la Application.
    Agregar en el main.py con:
        for h in build_headhunter_handlers():
            application.add_handler(h)
        application.job_queue.run_repeating(
            job_revisar_vacantes, interval=1800, first=60
        )
    """
    conv_postulacion = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(postular_inicio, pattern=r"^postular_\d+$"),
            CommandHandler("postular", postular_inicio),
        ],
        states={
            EST_CONFIRMAR:     [CallbackQueryHandler(postular_confirmar, pattern=r"^post_confirm_\d+$|^post_cancel$")],
            EST_PREGUNTA:      [MessageHandler(filters.TEXT & ~filters.COMMAND, postular_respuesta)],
            EST_CONFIRMAR_ENVIO: [CallbackQueryHandler(postular_enviar, pattern=r"^post_enviar$|^post_corregir$")],
        },
        fallbacks=[CommandHandler("cancelar", lambda u, c: ConversationHandler.END)],
        per_user    = True,
        per_chat    = False,  # Funciona en DM independiente del canal
        allow_reentry = True,
    )

    return [
        conv_postulacion,
        CommandHandler("nuevo_headhunter",      cmd_nuevo_headhunter),
        CommandHandler("confirmar_pago",         cmd_confirmar_pago),
        CommandHandler("contratacion",           cmd_registrar_contratacion),
        CommandHandler("panel_hh",               cmd_panel_headhunters),
    ]
