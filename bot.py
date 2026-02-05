#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot Cofradía Premium - Versión Completa Corregida
Desarrollado para @Cofradia_de_Networking
"""

import os
import re
import io
import json
import logging
import sqlite3
import secrets
import string
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, time
from collections import Counter
from io import BytesIO

import requests
import PIL.Image
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, MessageHandler, CommandHandler, 
    filters, ContextTypes, CallbackQueryHandler
)

import google.generativeai as genai
from google.oauth2.service_account import Credentials

# ==================== CONFIGURACIÓN DE LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN GLOBAL ====================

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
TOKEN_BOT = os.environ.get('TOKEN_BOT')
OWNER_ID = int(os.environ.get('OWNER_TELEGRAM_ID', '0'))
COFRADIA_GROUP_ID = int(os.environ.get('COFRADIA_GROUP_ID', '0'))
BOT_USERNAME = "Cofradia_Premium_Bot"
DIAS_PRUEBA_GRATIS = 90

# Configuración de Gemini con modelo correcto
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    try:
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        logger.info("✅ Usando modelo gemini-1.5-flash-latest")
    except:
        try:
            model = genai.GenerativeModel('gemini-pro')
            logger.info("✅ Usando modelo gemini-pro")
        except:
            model = None
            logger.warning("⚠️ No se pudo inicializar modelo Gemini")
else:
    model = None
    logger.warning("⚠️ GEMINI_API_KEY no configurada")

DATOS_BANCARIOS = """
💳 **DATOS PARA TRANSFERENCIA**

🏦 **Titular:** Destak E.I.R.L.
🔢 **RUT:** 76.698.480-0
🏪 **Banco:** Banco Santander
💼 **Cuenta Corriente:** 69104312

📸 Envía el comprobante como imagen después de transferir.
"""

# ==================== CONFIGURACIÓN DE TOPICS/TEMAS DEL GRUPO ====================
# Mapeo de topic_id a nombre del tema (actualizar según los topics reales del grupo)
# Para obtener los IDs, revisa los mensajes guardados en la BD o usa el message_thread_id
TOPICS_COFRADIA = {
    # topic_id: ("Nombre del Tema", "Emoji")
    # Estos son ejemplos, debes actualizarlos con los IDs reales de tu grupo
    None: ("General", "💬"),  # Mensajes sin topic (chat general)
    # Agregar aquí los topics reales del grupo Cofradía:
    # 123: ("Ofertas Laborales", "💼"),
    # 124: ("Networking", "🤝"),
    # 125: ("Emprendimiento", "🚀"),
    # 126: ("Tecnología", "💻"),
    # 127: ("Eventos", "📅"),
    # 128: ("Recursos", "📚"),
    # 129: ("Presentaciones", "👋"),
    # 130: ("Cumpleaños y Efemérides", "🎂"),
}

def obtener_nombre_topic(topic_id):
    """Obtiene el nombre legible de un topic desde la BD"""
    if topic_id is None:
        topic_id = 0
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT nombre, emoji FROM topics_grupo WHERE topic_id = ?", (topic_id,))
    resultado = c.fetchone()
    conn.close()
    
    if resultado:
        return (resultado[0], resultado[1])
    return (f"Tema #{topic_id}", "📌")


def registrar_topic(topic_id, nombre_sugerido=None):
    """Registra un nuevo topic detectado automáticamente"""
    if topic_id is None:
        return
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Verificar si ya existe
    c.execute("SELECT topic_id FROM topics_grupo WHERE topic_id = ?", (topic_id,))
    if c.fetchone():
        # Incrementar contador de mensajes
        c.execute("UPDATE topics_grupo SET mensajes_count = mensajes_count + 1 WHERE topic_id = ?", (topic_id,))
    else:
        # Registrar nuevo topic
        nombre = nombre_sugerido or f"Tema #{topic_id}"
        c.execute("""INSERT INTO topics_grupo (topic_id, nombre, emoji, fecha_detectado, mensajes_count) 
                     VALUES (?, ?, '📌', ?, 1)""",
                  (topic_id, nombre, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        logger.info(f"📌 Nuevo topic detectado: {topic_id} - {nombre}")
    
    conn.commit()
    conn.close()


def obtener_todos_topics():
    """Obtiene todos los topics registrados"""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""SELECT topic_id, nombre, emoji, mensajes_count 
                 FROM topics_grupo 
                 WHERE activo = 1 
                 ORDER BY mensajes_count DESC""")
    topics = c.fetchall()
    conn.close()
    return topics


def actualizar_topic(topic_id, nombre=None, emoji=None):
    """Actualiza el nombre o emoji de un topic"""
    conn = get_db_connection()
    c = conn.cursor()
    
    if nombre:
        c.execute("UPDATE topics_grupo SET nombre = ? WHERE topic_id = ?", (nombre, topic_id))
    if emoji:
        c.execute("UPDATE topics_grupo SET emoji = ? WHERE topic_id = ?", (emoji, topic_id))
    
    conn.commit()
    conn.close()

# Estilos de gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)
plt.rcParams['font.size'] = 11

# ==================== FUNCIONES UTILITARIAS ====================

def formato_clp(monto):
    return f"${monto:,}".replace(",", ".")

def es_chat_privado(update: Update) -> bool:
    return update.effective_chat.type == 'private'

def es_chat_grupo(update: Update) -> bool:
    return update.effective_chat.type in ['group', 'supergroup']

def truncar_texto(texto: str, max_length: int = 100) -> str:
    if len(texto) <= max_length:
        return texto
    return texto[:max_length-3] + "..."

async def enviar_mensaje_largo(update_or_context, texto: str, chat_id=None, parse_mode='Markdown'):
    max_length = 4000
    if len(texto) <= max_length:
        if chat_id:
            await update_or_context.bot.send_message(chat_id=chat_id, text=texto, parse_mode=parse_mode)
        else:
            await update_or_context.message.reply_text(texto, parse_mode=parse_mode)
    else:
        partes = [texto[i:i+max_length] for i in range(0, len(texto), max_length)]
        for parte in partes:
            if chat_id:
                await update_or_context.bot.send_message(chat_id=chat_id, text=parte, parse_mode=parse_mode)
            else:
                await update_or_context.message.reply_text(parte, parse_mode=parse_mode)

# ==================== KEEP-ALIVE SERVER ====================

class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Bot Cofradia Premium - Activo')
    
    def log_message(self, format, *args):
        pass

def run_keepalive_server():
    port = int(os.environ.get('PORT', 10000))
    server = HTTPServer(('0.0.0.0', port), KeepAliveHandler)
    logger.info(f"🌐 Keep-alive server en puerto {port}")
    server.serve_forever()

# ==================== BASE DE DATOS ====================

def get_db_connection():
    return sqlite3.connect('mensajes.db', check_same_thread=False)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS mensajes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER, username TEXT, first_name TEXT,
                  message TEXT, topic_id INTEGER, fecha TEXT,
                  embedding TEXT, categoria TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS resumenes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  fecha TEXT, tipo TEXT, resumen TEXT, mensajes_count INTEGER)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS suscripciones
                 (user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT,
                  es_admin INTEGER DEFAULT 0, fecha_registro TEXT,
                  fecha_expiracion TEXT, estado TEXT DEFAULT 'activo',
                  mensajes_engagement INTEGER DEFAULT 0,
                  ultimo_mensaje_engagement TEXT,
                  servicios_usados TEXT DEFAULT '[]')''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS codigos_activacion
                 (codigo TEXT PRIMARY KEY, dias_validez INTEGER, precio INTEGER,
                  fecha_creacion TEXT, fecha_expiracion TEXT,
                  usado INTEGER DEFAULT 0, usado_por INTEGER, fecha_uso TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS pagos_pendientes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
                  first_name TEXT, dias_plan INTEGER, precio INTEGER,
                  comprobante_file_id TEXT, fecha_envio TEXT,
                  estado TEXT DEFAULT 'pendiente', datos_ocr TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS precios_planes
                 (dias INTEGER PRIMARY KEY, precio INTEGER, nombre_plan TEXT)''')
    
    # Nueva tabla para topics/temas del grupo
    c.execute('''CREATE TABLE IF NOT EXISTS topics_grupo
                 (topic_id INTEGER PRIMARY KEY,
                  nombre TEXT,
                  emoji TEXT DEFAULT '📌',
                  descripcion TEXT,
                  fecha_detectado TEXT,
                  mensajes_count INTEGER DEFAULT 0,
                  activo INTEGER DEFAULT 1)''')
    
    c.execute("SELECT COUNT(*) FROM precios_planes")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO precios_planes VALUES (30, 2000, 'Mensual')")
        c.execute("INSERT INTO precios_planes VALUES (180, 10500, 'Semestral')")
        c.execute("INSERT INTO precios_planes VALUES (365, 20000, 'Anual')")
    
    # Insertar topic General (None/0) si no existe
    c.execute("SELECT COUNT(*) FROM topics_grupo WHERE topic_id = 0")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO topics_grupo (topic_id, nombre, emoji, descripcion, fecha_detectado) VALUES (0, 'General', '💬', 'Chat general del grupo', ?)",
                  (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
    
    conn.commit()
    conn.close()
    logger.info("✅ Base de datos inicializada")
# ==================== FUNCIONES DE SUSCRIPCIÓN ====================

def registrar_usuario_suscripcion(user_id, first_name, username, es_admin=False, dias_gratis=90):
    conn = get_db_connection()
    c = conn.cursor()
    fecha_registro = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fecha_expiracion = (datetime.now() + timedelta(days=dias_gratis)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT OR REPLACE INTO suscripciones 
                 (user_id, first_name, username, es_admin, fecha_registro, 
                  fecha_expiracion, estado, mensajes_engagement, 
                  ultimo_mensaje_engagement, servicios_usados) 
                 VALUES (?, ?, ?, ?, ?, ?, 'activo', 0, ?, '[]')""",
              (user_id, first_name, username, 1 if es_admin else 0, 
               fecha_registro, fecha_expiracion, fecha_registro))
    conn.commit()
    conn.close()

def verificar_suscripcion_activa(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT fecha_expiracion, estado FROM suscripciones WHERE user_id = ?", (user_id,))
    resultado = c.fetchone()
    conn.close()
    if not resultado:
        return False
    fecha_exp, estado = resultado
    if estado != 'activo':
        return False
    try:
        fecha_expiracion = datetime.strptime(fecha_exp, "%Y-%m-%d %H:%M:%S")
        return datetime.now() < fecha_expiracion
    except:
        return False

def obtener_dias_restantes(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT fecha_expiracion FROM suscripciones WHERE user_id = ?", (user_id,))
    resultado = c.fetchone()
    conn.close()
    if not resultado:
        return 0
    try:
        fecha_exp = datetime.strptime(resultado[0], "%Y-%m-%d %H:%M:%S")
        dias = (fecha_exp - datetime.now()).days
        return max(0, dias)
    except:
        return 0

def generar_codigo_activacion(dias, precio):
    caracteres = string.ascii_uppercase + string.digits
    codigo = ''.join(secrets.choice(caracteres) for _ in range(12))
    codigo = f"COF-{codigo[:4]}-{codigo[4:8]}-{codigo[8:]}"
    conn = get_db_connection()
    c = conn.cursor()
    fecha_creacion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fecha_expiracion = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO codigos_activacion VALUES (?, ?, ?, ?, ?, 0, NULL, NULL)",
              (codigo, dias, precio, fecha_creacion, fecha_expiracion))
    conn.commit()
    conn.close()
    return codigo

def validar_y_usar_codigo(user_id, codigo):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT dias_validez, fecha_expiracion, usado FROM codigos_activacion WHERE codigo = ?", (codigo,))
    resultado = c.fetchone()
    if not resultado:
        conn.close()
        return False, "❌ Código inválido."
    dias_validez, fecha_exp_codigo, usado = resultado
    if usado:
        conn.close()
        return False, "❌ Este código ya fue utilizado."
    try:
        fecha_exp = datetime.strptime(fecha_exp_codigo, "%Y-%m-%d %H:%M:%S")
        if datetime.now() > fecha_exp:
            conn.close()
            return False, "❌ Código expirado."
    except:
        pass
    c.execute("UPDATE codigos_activacion SET usado = 1, usado_por = ?, fecha_uso = ? WHERE codigo = ?",
              (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), codigo))
    c.execute("SELECT fecha_expiracion FROM suscripciones WHERE user_id = ?", (user_id,))
    resultado_user = c.fetchone()
    if resultado_user:
        try:
            fecha_exp_actual = datetime.strptime(resultado_user[0], "%Y-%m-%d %H:%M:%S")
            if fecha_exp_actual < datetime.now():
                nueva_fecha = datetime.now() + timedelta(days=dias_validez)
            else:
                nueva_fecha = fecha_exp_actual + timedelta(days=dias_validez)
        except:
            nueva_fecha = datetime.now() + timedelta(days=dias_validez)
        c.execute("UPDATE suscripciones SET fecha_expiracion = ?, estado = 'activo' WHERE user_id = ?",
                  (nueva_fecha.strftime("%Y-%m-%d %H:%M:%S"), user_id))
    conn.commit()
    conn.close()
    return True, f"✅ ¡Código activado! Tu suscripción se extendió por **{dias_validez} días**."

def registrar_servicio_usado(user_id, servicio):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT servicios_usados FROM suscripciones WHERE user_id = ?", (user_id,))
    resultado = c.fetchone()
    if resultado:
        try:
            servicios = json.loads(resultado[0])
        except:
            servicios = []
        if servicio not in servicios:
            servicios.append(servicio)
            c.execute("UPDATE suscripciones SET servicios_usados = ? WHERE user_id = ?",
                      (json.dumps(servicios), user_id))
            conn.commit()
    conn.close()

def obtener_precios():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT dias, precio, nombre_plan FROM precios_planes ORDER BY dias")
    precios = c.fetchall()
    conn.close()
    return precios

def actualizar_precio(dias, nuevo_precio):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE precios_planes SET precio = ? WHERE dias = ?", (nuevo_precio, dias))
    conn.commit()
    conn.close()

# ==================== FUNCIONES DE IA ====================

def categorizar_mensaje(mensaje):
    if not model:
        return 'Otros'
    try:
        prompt = f"""Clasifica en UNA categoría:
Categorías: Networking, Negocios, Tecnología, Marketing, Eventos, Emprendimiento, Consultas, Recursos, Empleos, Social, Otros
Mensaje: "{mensaje[:300]}"
Responde SOLO la categoría."""
        response = model.generate_content(prompt)
        categoria = response.text.strip()
        categorias_validas = ['Networking', 'Negocios', 'Tecnología', 'Marketing', 'Eventos', 
                             'Emprendimiento', 'Consultas', 'Recursos', 'Empleos', 'Social', 'Otros']
        for cat in categorias_validas:
            if cat.lower() in categoria.lower():
                return cat
        return 'Otros'
    except:
        return 'Otros'

def generar_embedding(texto):
    try:
        result = genai.embed_content(model="models/embedding-001", content=texto, task_type="retrieval_document")
        return json.dumps(result['embedding'])
    except:
        return None

def guardar_mensaje(user_id, username, first_name, message, topic_id=None):
    conn = get_db_connection()
    c = conn.cursor()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    embedding = generar_embedding(message)
    categoria = categorizar_mensaje(message)
    c.execute("""INSERT INTO mensajes 
                 (user_id, username, first_name, message, topic_id, fecha, embedding, categoria) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (user_id, username, first_name, message, topic_id, fecha, embedding, categoria))
    conn.commit()
    conn.close()

def buscar_semantica(query, topic_id=None, limit=5):
    try:
        query_result = genai.embed_content(model="models/embedding-001", content=query, task_type="retrieval_query")
        query_embedding = query_result['embedding']
        conn = get_db_connection()
        c = conn.cursor()
        if topic_id:
            c.execute("SELECT first_name, message, fecha, embedding FROM mensajes WHERE embedding IS NOT NULL AND topic_id = ?", (topic_id,))
        else:
            c.execute("SELECT first_name, message, fecha, embedding FROM mensajes WHERE embedding IS NOT NULL")
        resultados = c.fetchall()
        conn.close()
        similitudes = []
        for nombre, mensaje, fecha, emb_str in resultados:
            if emb_str:
                try:
                    emb = json.loads(emb_str)
                    similitud = sum(a * b for a, b in zip(query_embedding, emb))
                    similitudes.append((similitud, nombre, mensaje, fecha))
                except:
                    continue
        similitudes.sort(reverse=True)
        return [(n, m, f) for _, n, m, f in similitudes[:limit]]
    except:
        return []

def buscar_en_historial(query, topic_id=None, limit=10):
    conn = get_db_connection()
    c = conn.cursor()
    query_lower = f'%{query.lower()}%'
    if topic_id:
        c.execute("""SELECT first_name, message, fecha FROM mensajes 
                     WHERE LOWER(message) LIKE ? AND topic_id = ?
                     ORDER BY fecha DESC LIMIT ?""", (query_lower, topic_id, limit))
    else:
        c.execute("""SELECT first_name, message, fecha FROM mensajes 
                     WHERE LOWER(message) LIKE ?
                     ORDER BY fecha DESC LIMIT ?""", (query_lower, limit))
    resultados = c.fetchall()
    conn.close()
    return resultados
# ==================== GOOGLE DRIVE ====================

def buscar_archivo_excel_drive():
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDS')
        if not creds_json:
            return None
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        access_token = creds.get_access_token().access_token
        headers = {'Authorization': f'Bearer {access_token}'}
        search_url = "https://www.googleapis.com/drive/v3/files"
        
        params_carpeta = {'q': "name='INBESTU' and mimeType='application/vnd.google-apps.folder'", 'fields': 'files(id, name)'}
        response_carpeta = requests.get(search_url, headers=headers, params=params_carpeta, timeout=30)
        if response_carpeta.status_code != 200:
            return None
        carpetas = response_carpeta.json().get('files', [])
        if not carpetas:
            return None
        carpeta_id = carpetas[0]['id']
        
        params_archivos = {'q': f"name contains 'BD Grupo Laboral' and '{carpeta_id}' in parents and trashed=false", 'fields': 'files(id, name)', 'orderBy': 'name desc'}
        response_archivos = requests.get(search_url, headers=headers, params=params_archivos, timeout=30)
        if response_archivos.status_code != 200:
            return None
        archivos = response_archivos.json().get('files', [])
        if not archivos:
            return None
        
        file_id = archivos[0]['id']
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        response_download = requests.get(download_url, headers=headers, timeout=60)
        if response_download.status_code == 200:
            return io.BytesIO(response_download.content)
        return None
    except Exception as e:
        logger.error(f"Error Drive: {e}")
        return None

def buscar_profesionales(query):
    try:
        archivo = buscar_archivo_excel_drive()
        if not archivo:
            return "❌ No se pudo acceder a la base de datos de profesionales."
        df = pd.read_excel(archivo, engine='openpyxl')
        df.columns = df.columns.str.strip().str.lower()
        profesionales = []
        for idx, row in df.iterrows():
            nombre = str(row.get('nombre completo', row.get('nombre', 'N/A'))).strip()
            profesion = str(row.get('profesión', row.get('profesion', row.get('área', 'N/A')))).strip()
            email = str(row.get('email', row.get('correo', 'N/A'))).strip()
            telefono = str(row.get('teléfono', row.get('telefono', 'N/A'))).strip()
            if nombre == 'N/A' or nombre == 'nan' or not nombre:
                continue
            profesionales.append({'nombre': nombre, 'profesion': profesion, 'email': email, 'telefono': telefono})
        
        if not profesionales:
            return "❌ No se encontraron profesionales."
        
        query_lower = query.lower()
        encontrados = [p for p in profesionales if query_lower in p['nombre'].lower() or query_lower in p['profesion'].lower()]
        
        if not encontrados:
            return f"❌ No se encontraron profesionales para: {query}"
        
        resultado = "**👥 PROFESIONALES ENCONTRADOS**\n\n"
        for i, prof in enumerate(encontrados[:10], 1):
            resultado += f"**{i}. {prof['nombre']}**\n🎯 {prof['profesion']}\n📧 {prof['email']}\n📱 {prof['telefono']}\n\n"
        return resultado
    except Exception as e:
        return f"❌ Error: {str(e)}"

async def buscar_empleos_web(cargo=None, ubicacion=None, renta=None):
    if not model:
        return "❌ Servicio de IA no disponible."
    try:
        partes = []
        if cargo: partes.append(f"cargo: {cargo}")
        if ubicacion: partes.append(f"ubicación: {ubicacion}")
        if renta: partes.append(f"renta: {renta}")
        consulta = ", ".join(partes) if partes else "empleos generales"
        
        prompt = f"""Busca ofertas de empleo en Chile para: {consulta}
Proporciona 5-8 ofertas con: título, empresa, ubicación, salario, descripción breve.
Formatea profesionalmente en español."""
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"❌ Error: {str(e)}"

# ==================== ESTADÍSTICAS ====================

def obtener_estadisticas_graficos(dias=7):
    conn = get_db_connection()
    c = conn.cursor()
    fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    
    c.execute("SELECT DATE(fecha), COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY DATE(fecha) ORDER BY DATE(fecha)", (fecha_inicio,))
    por_dia = c.fetchall()
    c.execute("SELECT first_name, COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 10", (fecha_inicio,))
    usuarios_activos = c.fetchall()
    c.execute("SELECT categoria, COUNT(*) FROM mensajes WHERE fecha >= ? AND categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC", (fecha_inicio,))
    por_categoria = c.fetchall()
    c.execute("SELECT CAST(strftime('%H', fecha) AS INTEGER), COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY strftime('%H', fecha)", (fecha_inicio,))
    por_hora = c.fetchall()
    conn.close()
    return {'por_dia': por_dia, 'usuarios_activos': usuarios_activos, 'por_categoria': por_categoria, 'por_hora': por_hora}

def generar_grafico_visual(stats):
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('📊 ANÁLISIS VISUAL - COFRADÍA', fontsize=16, fontweight='bold')
    
    if stats['por_dia']:
        dias = [d[0][-5:] for d in stats['por_dia']]
        valores = [d[1] for d in stats['por_dia']]
        ax1.fill_between(range(len(dias)), valores, alpha=0.3, color='#2E86AB')
        ax1.plot(range(len(dias)), valores, marker='o', linewidth=2, color='#2E86AB')
        ax1.set_title('📈 Mensajes por Día')
        ax1.set_xticks(range(len(dias)))
        ax1.set_xticklabels(dias, rotation=45)
    
    if stats['usuarios_activos']:
        usuarios = [u[0][:12] for u in stats['usuarios_activos'][:8]]
        mensajes = [u[1] for u in stats['usuarios_activos'][:8]]
        ax2.barh(usuarios, mensajes, color=plt.cm.viridis(range(len(usuarios))))
        ax2.set_title('👥 Usuarios Más Activos')
        ax2.invert_yaxis()
    
    if stats['por_categoria']:
        categorias = [c[0] for c in stats['por_categoria']]
        valores_cat = [c[1] for c in stats['por_categoria']]
        ax3.pie(valores_cat, labels=categorias, autopct='%1.1f%%', startangle=90)
        ax3.set_title('🏷️ Categorías')
    
    if stats['por_hora']:
        horas = list(range(24))
        valores_hora = [0] * 24
        for hora, count in stats['por_hora']:
            if 0 <= hora < 24:
                valores_hora[hora] = count
        ax4.bar(horas, valores_hora, color='#f5576c')
        ax4.set_title('🕐 Actividad por Hora')
        ax4.set_xlabel('Hora')
    
    plt.tight_layout()
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    plt.close()
    return buffer

def generar_resumen_usuarios(dias=1):
    """Genera resumen ejecutivo diferenciado por Topics/Temas del grupo"""
    conn = get_db_connection()
    c = conn.cursor()
    fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    
    # Obtener mensajes con su topic_id
    c.execute("""SELECT first_name, message, categoria, topic_id, fecha 
                 FROM mensajes WHERE fecha >= ? 
                 ORDER BY topic_id, fecha""", (fecha_inicio,))
    mensajes = c.fetchall()
    
    if not mensajes:
        conn.close()
        return None
    
    # Agrupar mensajes por Topic/Tema
    por_topic = {}
    for nombre, msg, cat, topic_id, fecha in mensajes:
        if topic_id not in por_topic:
            por_topic[topic_id] = {
                'mensajes': [],
                'categorias': {},
                'participantes': set()
            }
        por_topic[topic_id]['mensajes'].append({
            'autor': nombre,
            'mensaje': msg,
            'categoria': cat or 'Otros',
            'fecha': fecha
        })
        por_topic[topic_id]['participantes'].add(nombre)
        
        # Contar por categoría dentro del topic
        cat = cat or 'Otros'
        if cat not in por_topic[topic_id]['categorias']:
            por_topic[topic_id]['categorias'][cat] = 0
        por_topic[topic_id]['categorias'][cat] += 1
    
    # Construir contexto para la IA organizado por topics
    if model:
        try:
            contexto_topics = ""
            resumen_stats = ""
            
            for topic_id, data in por_topic.items():
                nombre_topic, emoji = obtener_nombre_topic(topic_id)
                num_msgs = len(data['mensajes'])
                num_participantes = len(data['participantes'])
                
                # Estadísticas del topic
                resumen_stats += f"\n{emoji} **{nombre_topic}**: {num_msgs} msgs, {num_participantes} participantes"
                
                # Contenido del topic (máximo 10 mensajes por topic para el contexto)
                contexto_topics += f"\n\n{'='*40}\n{emoji} TEMA: {nombre_topic.upper()}\n{'='*40}\n"
                
                for msg_data in data['mensajes'][:10]:
                    contexto_topics += f"- {msg_data['autor']}: {msg_data['mensaje'][:200]}\n"
                
                # Categorías principales del topic
                if data['categorias']:
                    cats_ordenadas = sorted(data['categorias'].items(), key=lambda x: x[1], reverse=True)[:3]
                    contexto_topics += f"Temas principales: {', '.join([c[0] for c in cats_ordenadas])}\n"
            
            periodo = "DIARIO" if dias == 1 else ("SEMANAL" if dias == 7 else f"ÚLTIMOS {dias} DÍAS")
            
            prompt = f"""Eres el asistente de Cofradía de Networking, una comunidad profesional chilena.

Genera un RESUMEN EJECUTIVO de la actividad del grupo, organizado por cada TEMA/SUBGRUPO.

DATOS DEL PERÍODO:
- Fecha: {datetime.now().strftime('%d/%m/%Y')}
- Total mensajes: {len(mensajes)}
- Topics activos: {len(por_topic)}

ESTADÍSTICAS POR TEMA:{resumen_stats}

CONTENIDO POR TEMA:
{contexto_topics[:8000]}

FORMATO REQUERIDO:

📊 **RESUMEN {periodo} - COFRADÍA DE NETWORKING**
📅 {datetime.now().strftime('%d/%m/%Y')}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Para CADA tema/subgrupo activo, genera una sección con:

[EMOJI] **NOMBRE DEL TEMA**
📝 Resumen ejecutivo (2-3 oraciones)
🔑 Puntos clave: (2-3 bullets)
👥 Participantes destacados

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 **INSIGHTS GENERALES**
• (3-4 observaciones transversales)

🎯 **OPORTUNIDADES DETECTADAS**
• (2-3 oportunidades de networking o negocio)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Total: {len(mensajes)} mensajes | {len(por_topic)} temas activos

INSTRUCCIONES:
1. Sé conciso y ejecutivo
2. Destaca información accionable
3. Menciona nombres de participantes cuando sea relevante
4. Identifica oportunidades de conexión entre miembros
5. Máximo 500 palabras total
6. Usa español profesional chileno"""

            response = model.generate_content(prompt)
            conn.close()
            return response.text
            
        except Exception as e:
            logger.error(f"Error generando resumen con IA: {e}")
    
    # Resumen básico sin IA
    conn.close()
    resumen_basico = f"📊 **RESUMEN {'DIARIO' if dias == 1 else 'SEMANAL'}** - {datetime.now().strftime('%d/%m/%Y')}\n\n"
    resumen_basico += f"📝 Total mensajes: {len(mensajes)}\n"
    resumen_basico += f"📁 Temas activos: {len(por_topic)}\n\n"
    
    for topic_id, data in por_topic.items():
        nombre_topic, emoji = obtener_nombre_topic(topic_id)
        resumen_basico += f"{emoji} **{nombre_topic}**: {len(data['mensajes'])} msgs\n"
    
    return resumen_basico


def generar_resumen_admins(dias=1):
    """Genera resumen ampliado para administradores con métricas adicionales"""
    resumen_base = generar_resumen_usuarios(dias)
    
    if not resumen_base:
        return None
    
    conn = get_db_connection()
    c = conn.cursor()
    fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    
    # Estadísticas adicionales para admins
    c.execute("SELECT COUNT(*) FROM mensajes WHERE fecha >= ?", (fecha_inicio,))
    total_msgs = c.fetchone()[0]
    
    c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes WHERE fecha >= ?", (fecha_inicio,))
    usuarios_activos = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM suscripciones WHERE estado = 'activo'")
    suscriptores = c.fetchone()[0]
    
    # Top usuarios del período
    c.execute("""SELECT first_name, COUNT(*) as total 
                 FROM mensajes WHERE fecha >= ? 
                 GROUP BY user_id ORDER BY total DESC LIMIT 5""", (fecha_inicio,))
    top_usuarios = c.fetchall()
    
    # Usuarios nuevos del período
    c.execute("""SELECT COUNT(*) FROM suscripciones 
                 WHERE fecha_registro >= ?""", (fecha_inicio,))
    nuevos = c.fetchone()[0]
    
    # Próximos vencimientos (7 días)
    fecha_limite = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""SELECT COUNT(*) FROM suscripciones 
                 WHERE estado = 'activo' AND fecha_expiracion <= ?""", (fecha_limite,))
    por_vencer = c.fetchone()[0]
    
    conn.close()
    
    # Sección exclusiva admin
    seccion_admin = f"""

{'='*50}
👑 **SECCIÓN EXCLUSIVA ADMIN**
{'='*50}

📊 **MÉTRICAS DEL PERÍODO:**
• Total mensajes: {total_msgs}
• Usuarios activos: {usuarios_activos}
• Suscriptores totales: {suscriptores}
• Nuevos registros: {nuevos}
• Por vencer (7 días): {por_vencer}

🏆 **TOP 5 PARTICIPANTES:**
"""
    
    for i, (nombre, total) in enumerate(top_usuarios, 1):
        medalla = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][i-1]
        seccion_admin += f"{medalla} {nombre}: {total} msgs\n"
    
    if por_vencer > 0:
        seccion_admin += f"\n⚠️ **ATENCIÓN:** {por_vencer} usuario(s) por vencer esta semana"
    
    return resumen_base + seccion_admin
# ==================== DECORADOR ====================

def requiere_suscripcion(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        if not verificar_suscripcion_activa(user_id):
            dias = obtener_dias_restantes(user_id)
            if dias > 0:
                await update.message.reply_text(f"⏰ Tu suscripción vence en **{dias} días**.\n\nUsa /renovar", parse_mode='Markdown')
            else:
                await update.message.reply_text("❌ **Tu suscripción ha expirado.**\n\nRenueva con /renovar", parse_mode='Markdown')
            return
        return await func(update, context, *args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper

# ==================== COMANDOS BÁSICOS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not es_chat_privado(update):
        await update.message.reply_text("❌ Debes ingresar el comando /start en @Cofradia_Premium_Bot")
        return
    if verificar_suscripcion_activa(user.id):
        dias = obtener_dias_restantes(user.id)
        await update.message.reply_text(f"👋 **¡Hola de nuevo, {user.first_name}!**\n\n✅ Suscripción activa ({dias} días)\n\n📋 Usa /ayuda", parse_mode='Markdown')
        return
    mensaje = f"""
🎉 **¡Bienvenido/a {user.first_name} al Bot Cofradía Premium!**

━━━━━━━━━━━━━━━━━━━━
📌 **¿CÓMO EMPEZAR?**
━━━━━━━━━━━━━━━━━━━━

**PASO 1️⃣** → Ve al grupo Cofradía
**PASO 2️⃣** → Escribe: /registrarse
**PASO 3️⃣** → ¡Listo! Ahora puedo asistirte

━━━━━━━━━━━━━━━━━━━━
🛠️ **¿QUÉ PUEDO HACER?**
━━━━━━━━━━━━━━━━━━━━

🔍 Buscar información → /buscar o /buscar_ia
👥 Encontrar profesionales → /buscar_profesional
💼 Buscar empleos → /empleo
📊 Ver estadísticas → /graficos
📝 Resúmenes diarios → /resumen
🤖 Preguntarme → @Cofradia_Premium_Bot + pregunta

Escribe /ayuda para ver todos los comandos.
🚀 **¡Regístrate en el grupo para comenzar!**
"""
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_chat_privado(update):
        await update.message.reply_text("❌ Debes ingresar el comando /ayuda en @Cofradia_Premium_Bot")
        return
    texto = """
🤖 **BOT COFRADÍA - GUÍA**

🔍 **BÚSQUEDA**
/buscar palabra – Búsqueda exacta
/buscar_ia [frase] - Búsqueda IA

💼 **EMPLEOS/PROFESIONALES**
/empleo cargo:[X], ubicación:[X], renta:[X]
/buscar_profesional área

📊 **ANÁLISIS**
/graficos - Gráficos
/estadisticas – KPIs
/categorias - Distribución
/mi_perfil - Tu perfil

👥 **RR.HH.**
/ingresos N°mes_año
/top_usuarios - Ranking
/dotacion – Total integrantes
/crecimiento_mes
/crecimiento_anual

📝 **RESÚMENES**
/resumen - Del día
/resumen_semanal - 7 días
/resumen_mes - Mensual
/resumen_usuario @nombre

💳 **SUSCRIPCIÓN**
/registrarse - Activar cuenta

💬 IA: @Cofradia_Premium_Bot + pregunta
"""
    await update.message.reply_text(texto, parse_mode='Markdown')

async def registrarse_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if es_chat_privado(update):
        await update.message.reply_text("❌ Debes ingresar el comando /registrarse en @Cofradia_de_Networking")
        return
    if verificar_suscripcion_activa(user.id):
        dias = obtener_dias_restantes(user.id)
        await update.message.reply_text(f"✅ ¡{user.first_name} ya estás registrado! ({dias} días restantes)", parse_mode='Markdown')
        return
    try:
        chat_member = await context.bot.get_chat_member(update.effective_chat.id, user.id)
        es_admin = chat_member.status in ['creator', 'administrator']
    except:
        es_admin = False
    registrar_usuario_suscripcion(user.id, user.first_name, user.username or "sin_username", es_admin)
    await update.message.reply_text(f"""
✅ **¡@{user.username or user.first_name} estás registrado!**

🚀 Ya puedes usar tu bot asistente.
📱 Inicia un chat privado conmigo en @Cofradia_Premium_Bot
💡 Envíame el mensaje inicial: /start
""", parse_mode='Markdown')
    try:
        await context.bot.send_message(chat_id=user.id, text=f"🎉 **¡Bienvenido/a {user.first_name}!**\n\nTu cuenta está activa.\nUsa /ayuda para ver comandos.", parse_mode='Markdown')
    except:
        pass

async def renovar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_chat_privado(update):
        await update.message.reply_text("❌ Debes ingresar el comando /renovar en @Cofradia_Premium_Bot")
        return
    precios = obtener_precios()
    keyboard = [[InlineKeyboardButton(f"💎 {nombre} ({dias}d) - {formato_clp(precio)}", callback_data=f"plan_{dias}")] for dias, precio, nombre in precios]
    mensaje = """
🤖 **GUÍA DE RENOVACIÓN**

/renovar - Renovar plan
/activar [código] - Usar código
/mi_cuenta - Ver estado

**Secuencia:**
1️⃣ Elige plan → 2️⃣ Paga → 3️⃣ Envía comprobante → 4️⃣ Recibe código → 5️⃣ Actívalo

💳 **SELECCIONA TU PLAN:**
"""
    for dias, precio, nombre in precios:
        mensaje += f"\n💎 **{nombre}** ({dias}d) - {formato_clp(precio)}"
    await update.message.reply_text(mensaje, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def activar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_chat_privado(update):
        await update.message.reply_text("❌ Debes ingresar el comando /activar en @Cofradia_Premium_Bot")
        return
    if not context.args:
        await update.message.reply_text("❌ **Uso:** /activar [código]\n\nEjemplo: `/activar COF-ABCD-1234-EFGH`", parse_mode='Markdown')
        return
    exito, mensaje = validar_y_usar_codigo(update.message.from_user.id, context.args[0].upper())
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def mi_cuenta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT fecha_registro, fecha_expiracion, estado, es_admin, servicios_usados FROM suscripciones WHERE user_id = ?", (user.id,))
    resultado = c.fetchone()
    conn.close()
    if not resultado:
        await update.message.reply_text("❌ No estás registrado. Usa /registrarse en el grupo.")
        return
    fecha_reg, fecha_exp, estado, es_admin, servicios_str = resultado
    try:
        fecha_exp_dt = datetime.strptime(fecha_exp, "%Y-%m-%d %H:%M:%S")
        dias_restantes = (fecha_exp_dt - datetime.now()).days
    except:
        dias_restantes = 0
    try:
        servicios = json.loads(servicios_str)
    except:
        servicios = []
    estado_activo = estado == 'activo' and dias_restantes > 0
    mensaje = f"""
👤 **MI CUENTA**

{'✅' if estado_activo else '❌'} **Estado:** {'Activo' if estado_activo else 'Expirado'}
{'👑 Admin' if es_admin else ''}

⏳ **Días restantes:** {max(0, dias_restantes)}
📅 **Vence:** {fecha_exp_dt.strftime('%d/%m/%Y') if dias_restantes > 0 else 'Expirado'}

**Servicios usados:** {', '.join(servicios) if servicios else 'Ninguno'}
"""
    await update.message.reply_text(mensaje, parse_mode='Markdown')
# ==================== CALLBACKS ====================

async def callback_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    dias = int(query.data.split('_')[1])
    precios = obtener_precios()
    precio = next((p[1] for p in precios if p[0] == dias), 0)
    nombre_plan = next((p[2] for p in precios if p[0] == dias), "Plan")
    mensaje = f"✅ **Plan:** {nombre_plan}\n💰 **Precio:** {formato_clp(precio)}\n⏳ **Duración:** {dias} días\n\n{DATOS_BANCARIOS}\n\n📸 Envía el comprobante como **imagen**."
    await query.edit_message_text(mensaje, parse_mode='Markdown')
    context.user_data['plan_seleccionado'] = dias
    context.user_data['precio'] = precio

async def callback_generar_codigo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        return
    dias = int(query.data.split('_')[1])
    precios = obtener_precios()
    precio = next((p[1] for p in precios if p[0] == dias), 0)
    codigo = generar_codigo_activacion(dias, precio)
    await query.edit_message_text(f"✅ **CÓDIGO GENERADO**\n\n`{codigo}`\n\n📋 {dias} días\n💰 {formato_clp(precio)}", parse_mode='Markdown')

async def callback_aprobar_rechazar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        await query.answer("❌ Solo el administrador", show_alert=True)
        return
    parts = query.data.split('_')
    accion, pago_id = parts[0], int(parts[1])
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT user_id, first_name, dias_plan, precio FROM pagos_pendientes WHERE id = ?", (pago_id,))
    resultado = c.fetchone()
    if not resultado:
        await query.edit_message_caption("❌ Pago no encontrado.")
        conn.close()
        return
    user_id, nombre, dias, precio = resultado
    if accion == 'aprobar':
        codigo = generar_codigo_activacion(dias, precio)
        c.execute("UPDATE pagos_pendientes SET estado = 'aprobado' WHERE id = ?", (pago_id,))
        conn.commit()
        try:
            await context.bot.send_message(chat_id=user_id, text=f"✅ **¡PAGO APROBADO!**\n\n🎉 Código: `{codigo}`\n\nActívalo: /activar {codigo}", parse_mode='Markdown')
            await query.edit_message_caption(f"{query.message.caption}\n\n✅ APROBADO\nCódigo: `{codigo}`", parse_mode='Markdown')
        except Exception as e:
            await query.edit_message_caption(f"✅ Aprobado. Código: {codigo}")
    else:
        c.execute("UPDATE pagos_pendientes SET estado = 'rechazado' WHERE id = ?", (pago_id,))
        conn.commit()
        try:
            await context.bot.send_message(chat_id=user_id, text="❌ Pago no verificado. Contacta al administrador.")
            await query.edit_message_caption(f"{query.message.caption}\n\n❌ RECHAZADO", parse_mode='Markdown')
        except:
            pass
    conn.close()

async def recibir_comprobante(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if not es_chat_privado(update):
        return
    if 'plan_seleccionado' not in context.user_data:
        await update.message.reply_text("❌ Primero selecciona un plan con /renovar")
        return
    dias = context.user_data['plan_seleccionado']
    precio = context.user_data['precio']
    msg = await update.message.reply_text("🔍 Analizando comprobante...")
    photo = update.message.photo[-1]
    file = await context.bot.get_file(photo.file_id)
    
    datos_ocr = {}
    try:
        image_bytes = requests.get(file.file_path, timeout=30).content
        if model:
            vision_model = genai.GenerativeModel('gemini-1.5-flash-latest')
            image = PIL.Image.open(BytesIO(image_bytes))
            prompt_ocr = f"Analiza comprobante. Monto esperado: ${precio:,}. Cuenta: 69104312. JSON: {{\"monto_correcto\": true/false, \"legible\": true/false}}"
            response = vision_model.generate_content([prompt_ocr, image])
            response_text = re.sub(r'```json\s*|\s*```', '', response.text.strip())
            try:
                datos_ocr = json.loads(response_text)
            except:
                datos_ocr = {"legible": True}
    except Exception as e:
        datos_ocr = {"error": str(e)[:50]}
    
    await msg.delete()
    await update.message.reply_text("✅ **Comprobante recibido**\n\n⏳ En revisión por el administrador.", parse_mode='Markdown')
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("INSERT INTO pagos_pendientes (user_id, first_name, dias_plan, precio, comprobante_file_id, fecha_envio, estado, datos_ocr) VALUES (?, ?, ?, ?, ?, ?, 'pendiente', ?)",
              (user.id, user.first_name, dias, precio, photo.file_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), json.dumps(datos_ocr)))
    pago_id = c.lastrowid
    conn.commit()
    conn.close()
    
    nombre_plan = dict([(p[0], p[2]) for p in obtener_precios()]).get(dias, "Plan")
    keyboard = [[InlineKeyboardButton("✅ Aprobar", callback_data=f"aprobar_{pago_id}")], [InlineKeyboardButton("❌ Rechazar", callback_data=f"rechazar_{pago_id}")]]
    
    ocr_info = f"\n\n🔍 OCR: {datos_ocr}" if datos_ocr else ""
    try:
        await context.bot.send_photo(chat_id=OWNER_ID, photo=photo.file_id, 
            caption=f"💳 **PAGO #{pago_id}**\n\n👤 {user.first_name} (@{user.username or 'N/A'})\n💎 {nombre_plan} ({dias}d)\n💰 {formato_clp(precio)}{ocr_info}", 
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error notificando admin: {e}")
    
    del context.user_data['plan_seleccionado']
    del context.user_data['precio']

# ==================== COMANDOS CON SUSCRIPCIÓN ====================

@requiere_suscripcion
async def buscar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'búsqueda')
    if not context.args:
        await update.message.reply_text("❌ **Uso:** /buscar [palabra]", parse_mode='Markdown')
        return
    query = ' '.join(context.args)
    topic_id = update.message.message_thread_id if update.message.is_topic_message else None
    msg = await update.message.reply_text(f"🔍 Búsqueda: {query}")
    resultados = buscar_en_historial(query, topic_id, limit=5)
    await msg.delete()
    if not resultados:
        await update.message.reply_text(f"❌ No encontré: *{query}*", parse_mode='Markdown')
        return
    respuesta = f"🔍 **Búsqueda:** {query}\n\n"
    for nombre, mensaje, fecha in resultados:
        respuesta += f"👤 **{nombre}** ({fecha[:10]}):\n{truncar_texto(mensaje, 150)}\n\n"
    await enviar_mensaje_largo(update, respuesta)

@requiere_suscripcion
async def buscar_ia_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'búsqueda_ia')
    if not context.args:
        await update.message.reply_text("❌ **Uso:** /buscar_ia [frase]", parse_mode='Markdown')
        return
    query = ' '.join(context.args)
    topic_id = update.message.message_thread_id if update.message.is_topic_message else None
    msg = await update.message.reply_text("🧠 Buscando con IA...")
    resultados = buscar_semantica(query, topic_id, limit=5)
    await msg.delete()
    if not resultados:
        await update.message.reply_text("❌ Sin resultados")
        return
    respuesta = f"🧠 **Búsqueda IA:** {query}\n\n"
    for nombre, mensaje, fecha in resultados:
        respuesta += f"👤 **{nombre}** ({fecha[:10]}):\n{truncar_texto(mensaje, 150)}\n\n"
    await enviar_mensaje_largo(update, respuesta)

@requiere_suscripcion
async def empleo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'empleos')
    if not context.args:
        await update.message.reply_text("❌ **Uso:** /empleo cargo:[X], ubicación:[X], renta:[X]", parse_mode='Markdown')
        return
    texto = ' '.join(context.args)
    cargo = ubicacion = renta = None
    if 'cargo:' in texto.lower():
        match = re.search(r'cargo:\s*([^,]+)', texto, re.IGNORECASE)
        if match: cargo = match.group(1).strip()
    if 'ubicaci' in texto.lower():
        match = re.search(r'ubicaci[oó]n:\s*([^,]+)', texto, re.IGNORECASE)
        if match: ubicacion = match.group(1).strip()
    if 'renta:' in texto.lower():
        match = re.search(r'renta:\s*([^,]+)', texto, re.IGNORECASE)
        if match: renta = match.group(1).strip()
    msg = await update.message.reply_text("🔍 Buscando empleos...")
    resultados = await buscar_empleos_web(cargo, ubicacion, renta)
    await msg.delete()
    await enviar_mensaje_largo(update, resultados)

@requiere_suscripcion
async def buscar_profesional_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'buscar_profesional')
    if not context.args:
        await update.message.reply_text("❌ **Uso:** /buscar_profesional [área]", parse_mode='Markdown')
        return
    query = ' '.join(context.args)
    msg = await update.message.reply_text("🔍 Buscando profesionales...")
    resultados = buscar_profesionales(query)
    await msg.delete()
    await enviar_mensaje_largo(update, resultados)

@requiere_suscripcion
async def graficos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'gráficos')
    msg = await update.message.reply_text("📊 Generando...")
    try:
        stats = obtener_estadisticas_graficos(dias=7)
        imagen_buffer = generar_grafico_visual(stats)
        await msg.delete()
        await update.message.reply_photo(photo=imagen_buffer, caption="📊 **Análisis Visual**", parse_mode='Markdown')
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:50]}")

@requiere_suscripcion
async def estadisticas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM mensajes")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes")
    usuarios = c.fetchone()[0]
    hoy = datetime.now().strftime("%Y-%m-%d")
    c.execute("SELECT COUNT(*) FROM mensajes WHERE DATE(fecha) = ?", (hoy,))
    hoy_count = c.fetchone()[0]
    conn.close()
    await update.message.reply_text(f"📊 **ESTADÍSTICAS**\n\n📝 Total: {total:,}\n👥 Usuarios: {usuarios}\n🕐 Hoy: {hoy_count}", parse_mode='Markdown')

@requiere_suscripcion
async def categorias_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT categoria, COUNT(*) FROM mensajes WHERE categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC")
    categorias = c.fetchall()
    conn.close()
    if not categorias:
        await update.message.reply_text("❌ No hay datos")
        return
    total = sum([c[1] for c in categorias])
    respuesta = "🏷️ **CATEGORÍAS**\n\n"
    for cat, count in categorias:
        porcentaje = (count / total) * 100
        barra = '█' * int(porcentaje / 5)
        respuesta += f"**{cat}:** {barra} {count} ({porcentaje:.1f}%)\n"
    await update.message.reply_text(respuesta, parse_mode='Markdown')
# ==================== MÁS COMANDOS ====================

@requiere_suscripcion
async def top_usuarios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT first_name, COUNT(*) as total, COUNT(DISTINCT DATE(fecha)) as dias FROM mensajes GROUP BY user_id ORDER BY total DESC LIMIT 15")
    top_users = c.fetchall()
    conn.close()
    if not top_users:
        await update.message.reply_text("📭 Sin datos")
        return
    respuesta = "🏆 **TOP USUARIOS**\n\n"
    medallas = ["🥇", "🥈", "🥉"]
    for i, (nombre, total, dias) in enumerate(top_users, 1):
        emoji = medallas[i-1] if i <= 3 else f"**{i}.**"
        respuesta += f"{emoji} **{nombre}**: {total} msgs ({total/max(dias,1):.1f}/día)\n"
    await update.message.reply_text(respuesta, parse_mode='Markdown')

@requiere_suscripcion
async def mi_perfil_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM mensajes WHERE user_id = ?", (user.id,))
    resultado = c.fetchone()
    if not resultado or resultado[0] == 0:
        conn.close()
        await update.message.reply_text("📭 Sin actividad registrada")
        return
    total, primera, ultima = resultado
    c.execute("SELECT categoria, COUNT(*) FROM mensajes WHERE user_id = ? AND categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC LIMIT 3", (user.id,))
    categorias = c.fetchall()
    c.execute("SELECT COUNT(*) + 1 FROM (SELECT user_id, COUNT(*) as t FROM mensajes GROUP BY user_id HAVING t > ?)", (total,))
    posicion = c.fetchone()[0]
    conn.close()
    primera_fecha = datetime.strptime(primera, "%Y-%m-%d %H:%M:%S")
    dias_activo = (datetime.now() - primera_fecha).days + 1
    respuesta = f"""
👤 **TU PERFIL - {user.first_name}**

📊 Mensajes: **{total}**
🏆 Ranking: **#{posicion}**
📅 Miembro desde: {primera_fecha.strftime('%d/%m/%Y')}
📈 Promedio: **{total/max(dias_activo,1):.1f}** msgs/día

🏷️ **TUS CATEGORÍAS:**
"""
    for cat, count in categorias[:3]:
        respuesta += f"• {cat}: {count} msgs\n"
    await update.message.reply_text(respuesta, parse_mode='Markdown')

@requiere_suscripcion
async def resumen_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("📝 Generando...")
    resumen = generar_resumen_usuarios(dias=1)
    await msg.delete()
    if not resumen:
        await update.message.reply_text("❌ No hay mensajes hoy")
        return
    await enviar_mensaje_largo(update, resumen)

@requiere_suscripcion
async def resumen_semanal_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("📝 Generando...")
    resumen = generar_resumen_usuarios(dias=7)
    await msg.delete()
    if not resumen:
        await update.message.reply_text("❌ No hay mensajes")
        return
    await enviar_mensaje_largo(update, resumen)

@requiere_suscripcion
async def resumen_mes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("📊 Generando mensual...")
    resumen = generar_resumen_usuarios(dias=30)
    await msg.delete()
    if not resumen:
        await update.message.reply_text("📭 Sin datos suficientes")
        return
    await enviar_mensaje_largo(update, resumen)

@requiere_suscripcion
async def resumen_usuario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Uso: /resumen_usuario @nombre")
        return
    username = context.args[0].replace('@', '').lower()
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT user_id, first_name, username, COUNT(*), MIN(fecha), MAX(fecha) FROM mensajes WHERE LOWER(username) LIKE ? OR LOWER(first_name) LIKE ? GROUP BY user_id", (f'%{username}%', f'%{username}%'))
    resultado = c.fetchone()
    if not resultado:
        conn.close()
        await update.message.reply_text(f"❌ Usuario no encontrado: {username}")
        return
    user_id, nombre, username_real, total, primera, ultima = resultado
    c.execute("SELECT categoria, COUNT(*) FROM mensajes WHERE user_id = ? AND categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC LIMIT 3", (user_id,))
    categorias_top = c.fetchall()
    conn.close()
    primera_fecha = datetime.strptime(primera, "%Y-%m-%d %H:%M:%S")
    respuesta = f"👤 **PERFIL DE {nombre.upper()}**\n\n📊 Mensajes: **{total}**\n📅 Desde: {primera_fecha.strftime('%d/%m/%Y')}\n\n🏷️ **CATEGORÍAS:**\n"
    for cat, count in categorias_top:
        respuesta += f"• {cat}: {count}\n"
    await update.message.reply_text(respuesta, parse_mode='Markdown')

@requiere_suscripcion
async def dotacion_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM suscripciones")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM suscripciones WHERE estado = 'activo' AND fecha_expiracion > datetime('now')")
    activos = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes")
    participantes = c.fetchone()[0]
    conn.close()
    await update.message.reply_text(f"👥 **DOTACIÓN**\n\n📊 Registrados: **{total}**\n✅ Activos: **{activos}**\n💬 Participantes: **{participantes}**", parse_mode='Markdown')

@requiere_suscripcion
async def ingresos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        mes, año = datetime.now().month, datetime.now().year
    else:
        try:
            partes = context.args[0].split('_')
            mes = int(partes[0])
            año = int(partes[1]) if len(partes) > 1 else datetime.now().year
        except:
            await update.message.reply_text("❌ Uso: /ingresos mes_año (ej: /ingresos 3_2024)")
            return
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT first_name, username, fecha_registro FROM suscripciones WHERE strftime('%m', fecha_registro) = ? AND strftime('%Y', fecha_registro) = ? ORDER BY fecha_registro", (f"{mes:02d}", str(año)))
    ingresos = c.fetchall()
    conn.close()
    meses = ['','Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
    mensaje = f"👥 **INGRESOS - {meses[mes]} {año}**\n\n"
    if not ingresos:
        mensaje += "Sin ingresos en este período."
    else:
        for nombre, username, fecha in ingresos:
            mensaje += f"👤 **{nombre}** (@{username or 'N/A'})\n"
        mensaje += f"\n📊 **Total:** {len(ingresos)}"
    await update.message.reply_text(mensaje, parse_mode='Markdown')

@requiere_suscripcion
async def crecimiento_mes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    c = conn.cursor()
    datos = []
    for i in range(11, -1, -1):
        fecha = datetime.now() - timedelta(days=i*30)
        c.execute("SELECT COUNT(*) FROM suscripciones WHERE strftime('%m', fecha_registro) = ? AND strftime('%Y', fecha_registro) = ?", (f"{fecha.month:02d}", str(fecha.year)))
        datos.append((fecha.strftime('%b'), c.fetchone()[0]))
    conn.close()
    mensaje = "📈 **CRECIMIENTO MENSUAL**\n\n"
    max_val = max([d[1] for d in datos]) if datos else 1
    for mes, count in datos:
        barra = '█' * int((count / max_val) * 10) if max_val > 0 else ''
        mensaje += f"`{mes}` {barra} **{count}**\n"
    await update.message.reply_text(mensaje, parse_mode='Markdown')

@requiere_suscripcion
async def crecimiento_anual_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    c = conn.cursor()
    año_actual = datetime.now().year
    datos = []
    for año in range(año_actual - 2, año_actual + 1):
        c.execute("SELECT COUNT(*) FROM suscripciones WHERE strftime('%Y', fecha_registro) = ?", (str(año),))
        datos.append((año, c.fetchone()[0]))
    conn.close()
    mensaje = "📈 **CRECIMIENTO ANUAL**\n\n"
    max_val = max([d[1] for d in datos]) if datos else 1
    for año, count in datos:
        barra = '█' * int((count / max_val) * 15) if max_val > 0 else ''
        mensaje += f"`{año}` {barra} **{count}**\n"
    await update.message.reply_text(mensaje, parse_mode='Markdown')

# ==================== COMANDOS ADMIN ====================

async def cobros_admin_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return
    mensaje = """
🤖 **CÓDIGOS ADMIN**

💰 **COBROS Y CÓDIGOS:**
/generar_codigo – Crea Códigos
/precios – Ver precios
/set_precio – Modificar precios
/pagos_pendientes – Ver pagos

📅 **VENCIMIENTOS:**
/vencimientos – Próximos vencimientos
/vencimientos_mes – Por mes (1 al 12)

📋 **TOPICS/TEMAS:**
/ver_topics – Ver todos los topics
/set_topic [id] [nombre] – Renombrar topic
/set_topic_emoji [id] [emoji] – Cambiar emoji
"""
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def generar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return
    precios = obtener_precios()
    keyboard = [[InlineKeyboardButton(f"{nombre} ({dias}d) - {formato_clp(precio)}", callback_data=f"gencodigo_{dias}")] for dias, precio, nombre in precios]
    await update.message.reply_text("👑 **GENERAR CÓDIGO**\n\nSelecciona:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def precios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return
    precios = obtener_precios()
    mensaje = "💰 **PRECIOS**\n\n"
    for dias, precio, nombre in precios:
        mensaje += f"• {nombre} ({dias}d): {formato_clp(precio)}\n"
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def set_precio_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return
    if len(context.args) != 2:
        await update.message.reply_text("❌ Uso: /set_precio [dias] [precio]")
        return
    try:
        dias, precio = int(context.args[0]), int(context.args[1])
        actualizar_precio(dias, precio)
        await update.message.reply_text(f"✅ Actualizado: {dias}d = {formato_clp(precio)}")
    except:
        await update.message.reply_text("❌ Error")

async def pagos_pendientes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id, first_name, dias_plan, precio, estado FROM pagos_pendientes ORDER BY fecha_envio DESC LIMIT 20")
    pagos = c.fetchall()
    conn.close()
    if not pagos:
        await update.message.reply_text("✅ No hay pagos")
        return
    mensaje = "💳 **PAGOS**\n\n"
    for pago_id, nombre, dias, precio, estado in pagos:
        emoji = "⏳" if estado == 'pendiente' else ("✅" if estado == 'aprobado' else "❌")
        mensaje += f"{emoji} #{pago_id} {nombre} - {dias}d - {estado}\n"
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def vencimientos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return
    conn = get_db_connection()
    c = conn.cursor()
    fecha_limite = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("SELECT first_name, username, fecha_expiracion FROM suscripciones WHERE estado = 'activo' AND fecha_expiracion <= ? ORDER BY fecha_expiracion LIMIT 20", (fecha_limite,))
    vencimientos = c.fetchall()
    conn.close()
    if not vencimientos:
        await update.message.reply_text("✅ Sin vencimientos próximos")
        return
    mensaje = "⏰ **VENCIMIENTOS**\n\n"
    for nombre, username, fecha_exp in vencimientos:
        try:
            fecha = datetime.strptime(fecha_exp, "%Y-%m-%d %H:%M:%S")
            dias = (fecha - datetime.now()).days
            emoji = "🔴" if dias <= 3 else "🟡" if dias <= 7 else "🟢"
            mensaje += f"{emoji} **{nombre}** - {dias} días\n"
        except:
            continue
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def vencimientos_mes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        return
    if not context.args:
        await update.message.reply_text("❌ Uso: /vencimientos_mes [mes]")
        return
    try:
        mes = int(context.args[0])
    except:
        await update.message.reply_text("❌ Mes inválido")
        return
    conn = get_db_connection()
    c = conn.cursor()
    año = datetime.now().year
    c.execute("SELECT first_name, fecha_expiracion FROM suscripciones WHERE strftime('%m', fecha_expiracion) = ? AND strftime('%Y', fecha_expiracion) = ?", (f"{mes:02d}", str(año)))
    vencimientos = c.fetchall()
    conn.close()
    meses = ['','Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
    mensaje = f"📅 **VENCIMIENTOS {meses[mes]} {año}**\n\n"
    if not vencimientos:
        mensaje += "Sin vencimientos"
    else:
        for nombre, fecha in vencimientos:
            mensaje += f"📌 {nombre}\n"
        mensaje += f"\n📊 Total: {len(vencimientos)}"
    await update.message.reply_text(mensaje, parse_mode='Markdown')

# ==================== COMANDOS DE GESTIÓN DE TOPICS (ADMIN) ====================

async def ver_topics_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /ver_topics - Ver todos los topics detectados (solo admin)"""
    if update.message.from_user.id != OWNER_ID:
        return
    
    topics = obtener_todos_topics()
    
    if not topics:
        await update.message.reply_text("📭 No hay topics registrados aún.\n\nLos topics se detectan automáticamente cuando hay actividad en el grupo.")
        return
    
    mensaje = "📋 **TOPICS/TEMAS DEL GRUPO**\n\n"
    mensaje += "```\n"
    mensaje += f"{'ID':<8} {'Nombre':<20} {'Msgs':<8} {'Emoji'}\n"
    mensaje += "-" * 45 + "\n"
    
    for topic_id, nombre, emoji, msgs_count in topics:
        nombre_corto = nombre[:18] + ".." if len(nombre) > 20 else nombre
        mensaje += f"{topic_id:<8} {nombre_corto:<20} {msgs_count:<8} {emoji}\n"
    
    mensaje += "```\n"
    mensaje += f"\n📊 **Total:** {len(topics)} topics\n\n"
    mensaje += "💡 **Para renombrar:** `/set_topic [id] [nombre]`\n"
    mensaje += "💡 **Para cambiar emoji:** `/set_topic_emoji [id] [emoji]`"
    
    await update.message.reply_text(mensaje, parse_mode='Markdown')


async def set_topic_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /set_topic - Renombrar un topic (solo admin)"""
    if update.message.from_user.id != OWNER_ID:
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ **Uso:** /set_topic [topic_id] [nuevo nombre]\n\n"
            "**Ejemplo:** `/set_topic 123 Ofertas Laborales`",
            parse_mode='Markdown'
        )
        return
    
    try:
        topic_id = int(context.args[0])
        nuevo_nombre = ' '.join(context.args[1:])
        
        actualizar_topic(topic_id, nombre=nuevo_nombre)
        
        await update.message.reply_text(
            f"✅ Topic #{topic_id} renombrado a: **{nuevo_nombre}**",
            parse_mode='Markdown'
        )
    except ValueError:
        await update.message.reply_text("❌ El ID del topic debe ser un número")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def set_topic_emoji_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /set_topic_emoji - Cambiar emoji de un topic (solo admin)"""
    if update.message.from_user.id != OWNER_ID:
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ **Uso:** /set_topic_emoji [topic_id] [emoji]\n\n"
            "**Ejemplo:** `/set_topic_emoji 123 💼`",
            parse_mode='Markdown'
        )
        return
    
    try:
        topic_id = int(context.args[0])
        nuevo_emoji = context.args[1]
        
        actualizar_topic(topic_id, emoji=nuevo_emoji)
        
        await update.message.reply_text(
            f"✅ Emoji del topic #{topic_id} cambiado a: {nuevo_emoji}",
            parse_mode='Markdown'
        )
    except ValueError:
        await update.message.reply_text("❌ El ID del topic debe ser un número")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")


# ==================== HANDLER MENCIONES ====================

async def responder_mencion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    mensaje = update.message.text
    user_id = update.effective_user.id
    try:
        bot_username = context.bot.username.lower()
    except:
        bot_username = BOT_USERNAME.lower()
    
    menciones_validas = [f"@{bot_username}", "@cofradia_premium_bot", "@bot"]
    tiene_mencion = any(m.lower() in mensaje.lower() for m in menciones_validas)
    if not tiene_mencion:
        return
    
    if not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("❌ Necesitas suscripción activa.\nUsa /registrarse en el grupo.")
        return
    
    pregunta = re.sub(r'@\w+', '', mensaje).strip()
    if not pregunta:
        await update.message.reply_text(f"💡 Mencióname con tu pregunta:\n@{bot_username} ¿Qué es networking?", parse_mode='Markdown')
        return
    
    if not model:
        await update.message.reply_text("❌ Servicio IA no disponible.")
        return
    
    msg = await update.message.reply_text("🤔 Procesando...")
    try:
        topic_id = update.message.message_thread_id if update.message.is_topic_message else None
        resultados = buscar_semantica(pregunta, topic_id, limit=3)
        contexto = ""
        if resultados:
            contexto = "\n\nCONTEXTO:\n"
            for nombre, msg_txt, fecha in resultados:
                contexto += f"- {nombre}: {msg_txt[:100]}...\n"
        prompt = f"Asistente de Cofradía. Responde amigable y profesional.\nPREGUNTA: {pregunta}\n{contexto}\nMáximo 3 párrafos. Español."
        response = model.generate_content(prompt)
        await msg.delete()
        await enviar_mensaje_largo(update, response.text)
        registrar_servicio_usado(user_id, 'ia_mencion')
    except Exception as e:
        logger.error(f"Error mencion: {e}")
        await msg.delete()
        await update.message.reply_text("❌ Error. Intenta de nuevo.")

async def guardar_mensaje_grupo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Guarda mensajes del grupo y registra topics automáticamente"""
    if not update.message or not update.message.text:
        return
    if es_chat_privado(update):
        return
    
    user = update.message.from_user
    topic_id = update.message.message_thread_id if update.message.is_topic_message else None
    
    # Registrar el topic si es nuevo
    if topic_id:
        registrar_topic(topic_id)
    
    # Guardar el mensaje
    guardar_mensaje(user.id, user.username or "sin_username", user.first_name or "Anónimo", update.message.text, topic_id)

# ==================== JOBS PROGRAMADOS ====================

async def resumen_automatico(context: ContextTypes.DEFAULT_TYPE):
    """Envía resúmenes diarios automáticos a las 20:00 - diferenciado para usuarios y admins"""
    logger.info("⏰ Ejecutando resumen automático diario...")
    try:
        # Generar ambos tipos de resumen
        resumen_usuarios = generar_resumen_usuarios(dias=1)
        resumen_admins = generar_resumen_admins(dias=1)
        
        if not resumen_usuarios:
            logger.info("No hay mensajes para resumir hoy")
            return
        
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, first_name, es_admin FROM suscripciones WHERE estado = 'activo'")
        usuarios = c.fetchall()
        conn.close()
        
        enviados_usuarios = 0
        enviados_admins = 0
        
        for user_id, nombre, es_admin in usuarios:
            # Verificar que la suscripción siga activa
            if not verificar_suscripcion_activa(user_id):
                continue
            
            try:
                if es_admin and resumen_admins:
                    # Enviar resumen completo con sección admin
                    mensaje = f"👑 **RESUMEN DIARIO - ADMIN**\n\n{resumen_admins}"
                    enviados_admins += 1
                else:
                    # Enviar resumen estándar para usuarios
                    mensaje = f"📧 **RESUMEN DIARIO - COFRADÍA**\n\n{resumen_usuarios}"
                    enviados_usuarios += 1
                
                # Dividir mensaje si es muy largo
                if len(mensaje) > 4000:
                    partes = [mensaje[i:i+4000] for i in range(0, len(mensaje), 4000)]
                    for parte in partes:
                        await context.bot.send_message(
                            chat_id=user_id, 
                            text=parte, 
                            parse_mode='Markdown'
                        )
                else:
                    await context.bot.send_message(
                        chat_id=user_id, 
                        text=mensaje, 
                        parse_mode='Markdown'
                    )
                    
            except Exception as e:
                logger.warning(f"No se pudo enviar resumen a {nombre} ({user_id}): {e}")
        
        logger.info(f"✅ Resúmenes enviados: {enviados_usuarios} usuarios, {enviados_admins} admins")
        
    except Exception as e:
        logger.error(f"Error en resumen automático: {e}")

async def enviar_recordatorios(context: ContextTypes.DEFAULT_TYPE):
    logger.info("⏰ Recordatorios...")
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, first_name, fecha_expiracion FROM suscripciones WHERE estado = 'activo'")
        usuarios = c.fetchall()
        conn.close()
        for user_id, nombre, fecha_exp_str in usuarios:
            try:
                fecha_exp = datetime.strptime(fecha_exp_str, "%Y-%m-%d %H:%M:%S")
                dias = (fecha_exp - datetime.now()).days
                mensaje = None
                if dias == 5:
                    mensaje = f"🔔 **Hola {nombre}!**\n\nTu suscripción vence en **5 días**.\n\n💳 /renovar"
                elif dias == 3:
                    mensaje = f"⭐ **{nombre}**, quedan **3 días**!\n\n/renovar"
                elif dias == 1:
                    mensaje = f"⚠️ **{nombre}**, ¡MAÑANA vence tu acceso!\n\n⏰ /renovar ahora"
                if mensaje:
                    await context.bot.send_message(chat_id=user_id, text=mensaje, parse_mode='Markdown')
            except:
                pass
    except Exception as e:
        logger.error(f"Error recordatorios: {e}")

async def verificar_cumpleanos(context: ContextTypes.DEFAULT_TYPE):
    logger.info("🎂 Verificando cumpleaños...")
    if not COFRADIA_GROUP_ID:
        return
    try:
        # Aquí iría la lógica de cumpleaños desde Drive
        pass
    except Exception as e:
        logger.error(f"Error cumpleaños: {e}")

async def enviar_mensajes_engagement(context: ContextTypes.DEFAULT_TYPE):
    logger.info("💬 Engagement...")
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, first_name, mensajes_engagement, ultimo_mensaje_engagement FROM suscripciones WHERE estado = 'activo' AND mensajes_engagement < 12")
        usuarios = c.fetchall()
        mensajes = [
            "👋 **Hola {nombre}!** Prueba /buscar_ia 🧠",
            "💼 **{nombre}**, usa /empleo 🚀",
            "📊 **{nombre}**, usa /graficos 📈",
        ]
        for user_id, nombre, num_msg, ultimo_msg_str in usuarios:
            if ultimo_msg_str:
                try:
                    ultimo = datetime.strptime(ultimo_msg_str, "%Y-%m-%d %H:%M:%S")
                    if (datetime.now() - ultimo).days < 7:
                        continue
                except:
                    pass
            try:
                await context.bot.send_message(chat_id=user_id, text=mensajes[num_msg % len(mensajes)].format(nombre=nombre), parse_mode='Markdown')
                c.execute("UPDATE suscripciones SET mensajes_engagement = ?, ultimo_mensaje_engagement = ? WHERE user_id = ?", (num_msg + 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
                conn.commit()
            except:
                pass
        conn.close()
    except Exception as e:
        logger.error(f"Error engagement: {e}")

# ==================== MAIN ====================

def main():
    logger.info("🚀 Iniciando Bot Cofradía Premium...")
    init_db()
    
    keepalive_thread = threading.Thread(target=run_keepalive_server, daemon=True)
    keepalive_thread.start()
    
    if not TOKEN_BOT:
        logger.error("❌ TOKEN_BOT no configurado")
        return
    
    application = Application.builder().token(TOKEN_BOT).build()
    
    async def set_commands(app):
        commands = [
            BotCommand("start", "Iniciar bot"),
            BotCommand("ayuda", "Ver comandos"),
            BotCommand("registrarse", "Activar cuenta"),
            BotCommand("buscar", "Buscar texto"),
            BotCommand("buscar_ia", "Buscar con IA"),
            BotCommand("buscar_profesional", "Buscar expertos"),
            BotCommand("empleo", "Buscar empleos"),
            BotCommand("graficos", "Ver gráficos"),
            BotCommand("estadisticas", "Ver números"),
            BotCommand("categorias", "Distribución"),
            BotCommand("top_usuarios", "Ranking"),
            BotCommand("mi_perfil", "Tu perfil"),
            BotCommand("resumen", "Resumen del día"),
            BotCommand("resumen_semanal", "7 días"),
            BotCommand("resumen_mes", "Mensual"),
            BotCommand("dotacion", "Total integrantes"),
            BotCommand("mi_cuenta", "Mi suscripción"),
            BotCommand("renovar", "Renovar plan"),
        ]
        await app.bot.set_my_commands(commands)
    
    application.post_init = set_commands
    
    # Jobs
    job_queue = application.job_queue
    job_queue.run_daily(resumen_automatico, time=time(hour=20, minute=0), name='resumen_diario')
    job_queue.run_daily(enviar_recordatorios, time=time(hour=10, minute=0), name='recordatorios')
    job_queue.run_daily(verificar_cumpleanos, time=time(hour=8, minute=0), name='cumpleanos')
    job_queue.run_daily(enviar_mensajes_engagement, time=time(hour=15, minute=0), name='engagement')
    
    # Handlers básicos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ayuda", ayuda))
    application.add_handler(CommandHandler("registrarse", registrarse_comando))
    application.add_handler(CommandHandler("renovar", renovar_comando))
    application.add_handler(CommandHandler("activar", activar_codigo_comando))
    application.add_handler(CommandHandler("mi_cuenta", mi_cuenta_comando))
    
    # Handlers búsqueda
    application.add_handler(CommandHandler("buscar", buscar_comando))
    application.add_handler(CommandHandler("buscar_ia", buscar_ia_comando))
    application.add_handler(CommandHandler("empleo", empleo_comando))
    application.add_handler(CommandHandler("buscar_profesional", buscar_profesional_comando))
    
    # Handlers estadísticas
    application.add_handler(CommandHandler("graficos", graficos_comando))
    application.add_handler(CommandHandler("estadisticas", estadisticas_comando))
    application.add_handler(CommandHandler("categorias", categorias_comando))
    application.add_handler(CommandHandler("top_usuarios", top_usuarios_comando))
    application.add_handler(CommandHandler("mi_perfil", mi_perfil_comando))
    
    # Handlers resúmenes
    application.add_handler(CommandHandler("resumen", resumen_comando))
    application.add_handler(CommandHandler("resumen_semanal", resumen_semanal_comando))
    application.add_handler(CommandHandler("resumen_mes", resumen_mes_comando))
    application.add_handler(CommandHandler("resumen_usuario", resumen_usuario_comando))
    
    # Handlers RRHH
    application.add_handler(CommandHandler("dotacion", dotacion_comando))
    application.add_handler(CommandHandler("ingresos", ingresos_comando))
    application.add_handler(CommandHandler("crecimiento_mes", crecimiento_mes_comando))
    application.add_handler(CommandHandler("crecimiento_anual", crecimiento_anual_comando))
    
    # Handlers admin
    application.add_handler(CommandHandler("cobros_admin", cobros_admin_comando))
    application.add_handler(CommandHandler("generar_codigo", generar_codigo_comando))
    application.add_handler(CommandHandler("precios", precios_comando))
    application.add_handler(CommandHandler("set_precio", set_precio_comando))
    application.add_handler(CommandHandler("pagos_pendientes", pagos_pendientes_comando))
    application.add_handler(CommandHandler("vencimientos", vencimientos_comando))
    application.add_handler(CommandHandler("vencimientos_mes", vencimientos_mes_comando))
    
    # Handlers admin - Topics
    application.add_handler(CommandHandler("ver_topics", ver_topics_comando))
    application.add_handler(CommandHandler("set_topic", set_topic_comando))
    application.add_handler(CommandHandler("set_topic_emoji", set_topic_emoji_comando))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(callback_plan, pattern='^plan_'))
    application.add_handler(CallbackQueryHandler(callback_generar_codigo, pattern='^gencodigo_'))
    application.add_handler(CallbackQueryHandler(callback_aprobar_rechazar, pattern='^(aprobar|rechazar)_'))
    
    # Mensajes
    application.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, recibir_comprobante))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'@'), responder_mencion))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS & ~filters.COMMAND, guardar_mensaje_grupo))
    
    logger.info("✅ Bot Cofradía Premium iniciado!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
