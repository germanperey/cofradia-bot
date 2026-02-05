#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot Cofradía Premium - Versión con Supabase PostgreSQL
Desarrollado para @Cofradia_de_Networking
"""

import os
import re
import io
import json
import logging
import secrets
import string
import threading
import base64
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, time
from collections import Counter
from io import BytesIO

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, MenuButtonCommands
from telegram.ext import (
    Application, MessageHandler, CommandHandler, 
    filters, ContextTypes, CallbackQueryHandler
)

# ==================== CONFIGURACIÓN DE LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN GLOBAL ====================

GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
TOKEN_BOT = os.environ.get('TOKEN_BOT')
OWNER_ID = int(os.environ.get('OWNER_TELEGRAM_ID', '0'))
COFRADIA_GROUP_ID = int(os.environ.get('COFRADIA_GROUP_ID', '0'))
DATABASE_URL = os.environ.get('DATABASE_URL')  # URL de Supabase PostgreSQL
BOT_USERNAME = "Cofradia_Premium_Bot"
DIAS_PRUEBA_GRATIS = 90

# ==================== CONFIGURACIÓN DE GROQ AI ====================
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

# ==================== CONFIGURACIÓN DE GEMINI (OCR) ====================
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

# ==================== CONFIGURACIÓN DE JSEARCH (EMPLEOS REALES) ====================
RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY')
JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"

# Variables globales para indicar si las IAs están disponibles
ia_disponible = False
gemini_disponible = False
jsearch_disponible = False
db_disponible = False

# ==================== INICIALIZACIÓN DE SERVICIOS ====================

# Probar conexión con Groq
if GROQ_API_KEY:
    try:
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        test_payload = {
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": "Hola"}],
            "max_tokens": 10
        }
        response = requests.post(GROQ_API_URL, headers=headers, json=test_payload, timeout=10)
        if response.status_code == 200:
            ia_disponible = True
            logger.info(f"✅ Groq AI inicializado correctamente (modelo: {GROQ_MODEL})")
        else:
            logger.error(f"❌ Error conectando con Groq: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Error inicializando Groq: {str(e)[:100]}")
else:
    logger.warning("⚠️ GROQ_API_KEY no configurada")

# Verificar Gemini
if GEMINI_API_KEY:
    gemini_disponible = True
    logger.info("✅ Gemini API Key configurada (OCR disponible)")
else:
    logger.warning("⚠️ GEMINI_API_KEY no configurada - OCR no disponible")

# Verificar JSearch (RapidAPI)
if RAPIDAPI_KEY:
    jsearch_disponible = True
    logger.info("✅ RapidAPI Key configurada (JSearch empleos reales)")
else:
    logger.warning("⚠️ RAPIDAPI_KEY no configurada - empleos reales no disponibles")

# Verificar Database URL
if DATABASE_URL:
    logger.info("✅ DATABASE_URL configurada (Supabase)")
else:
    logger.warning("⚠️ DATABASE_URL no configurada - usando SQLite local")


# ==================== CONEXIÓN A BASE DE DATOS ====================

def get_db_connection():
    """Obtiene conexión a la base de datos (Supabase PostgreSQL o SQLite fallback)"""
    if DATABASE_URL:
        try:
            conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
            return conn
        except Exception as e:
            logger.error(f"Error conectando a Supabase: {e}")
            return None
    else:
        # Fallback a SQLite (solo para desarrollo local)
        import sqlite3
        conn = sqlite3.connect('mensajes.db', check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn


def init_db():
    """Inicializa las tablas en la base de datos"""
    conn = get_db_connection()
    if not conn:
        logger.error("❌ No se pudo conectar a la base de datos")
        return False
    
    try:
        c = conn.cursor()
        
        if DATABASE_URL:
            # PostgreSQL (Supabase)
            c.execute('''CREATE TABLE IF NOT EXISTS mensajes (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                username TEXT,
                first_name TEXT,
                message TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                topic_id BIGINT,
                categoria TEXT
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS suscripciones (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT,
                username TEXT,
                es_admin INTEGER DEFAULT 0,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_expiracion TIMESTAMP,
                estado TEXT DEFAULT 'activo',
                mensajes_engagement INTEGER DEFAULT 0,
                ultimo_mensaje_engagement TIMESTAMP,
                servicios_usados TEXT DEFAULT '[]'
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS codigos_activacion (
                codigo TEXT PRIMARY KEY,
                dias INTEGER,
                usado INTEGER DEFAULT 0,
                user_id_usado BIGINT,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_uso TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS pagos_pendientes (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                first_name TEXT,
                dias_plan INTEGER,
                precio INTEGER,
                comprobante_file_id TEXT,
                fecha_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                estado TEXT DEFAULT 'pendiente',
                datos_ocr TEXT
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS precios_planes (
                dias INTEGER PRIMARY KEY,
                precio INTEGER,
                nombre TEXT
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS topics_grupo (
                topic_id BIGINT PRIMARY KEY,
                nombre TEXT,
                emoji TEXT DEFAULT '📌',
                fecha_detectado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # Insertar precios por defecto si no existen
            c.execute('''INSERT INTO precios_planes (dias, precio, nombre) VALUES 
                (30, 2000, 'Mensual'),
                (180, 10500, 'Semestral'),
                (365, 20000, 'Anual')
                ON CONFLICT (dias) DO NOTHING''')
            
            logger.info("✅ Base de datos PostgreSQL (Supabase) inicializada")
        else:
            # SQLite (fallback local)
            c.execute('''CREATE TABLE IF NOT EXISTS mensajes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                first_name TEXT,
                message TEXT,
                fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
                topic_id INTEGER,
                categoria TEXT
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS suscripciones (
                user_id INTEGER PRIMARY KEY,
                first_name TEXT,
                username TEXT,
                es_admin INTEGER DEFAULT 0,
                fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
                fecha_expiracion DATETIME,
                estado TEXT DEFAULT 'activo',
                mensajes_engagement INTEGER DEFAULT 0,
                ultimo_mensaje_engagement DATETIME,
                servicios_usados TEXT DEFAULT '[]'
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS codigos_activacion (
                codigo TEXT PRIMARY KEY,
                dias INTEGER,
                usado INTEGER DEFAULT 0,
                user_id_usado INTEGER,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                fecha_uso DATETIME
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS pagos_pendientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                first_name TEXT,
                dias_plan INTEGER,
                precio INTEGER,
                comprobante_file_id TEXT,
                fecha_envio DATETIME DEFAULT CURRENT_TIMESTAMP,
                estado TEXT DEFAULT 'pendiente',
                datos_ocr TEXT
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS precios_planes (
                dias INTEGER PRIMARY KEY,
                precio INTEGER,
                nombre TEXT
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS topics_grupo (
                topic_id INTEGER PRIMARY KEY,
                nombre TEXT,
                emoji TEXT DEFAULT '📌',
                fecha_detectado DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute("INSERT OR IGNORE INTO precios_planes VALUES (30, 2000, 'Mensual')")
            c.execute("INSERT OR IGNORE INTO precios_planes VALUES (180, 10500, 'Semestral')")
            c.execute("INSERT OR IGNORE INTO precios_planes VALUES (365, 20000, 'Anual')")
            
            logger.info("✅ Base de datos SQLite inicializada (modo local)")
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error inicializando base de datos: {e}")
        if conn:
            conn.close()
        return False


# ==================== FUNCIONES DE GROQ AI ====================

def llamar_groq(prompt: str, max_tokens: int = 1024, temperature: float = 0.7, reintentos: int = 3) -> str:
    """Llama a la API de Groq con reintentos automáticos"""
    if not GROQ_API_KEY:
        logger.warning("⚠️ Intento de llamar Groq sin API Key")
        return None
    
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {
                "role": "system",
                "content": """Eres el asistente de IA de Cofradía de Networking, una comunidad profesional chilena de alto nivel.

Tu personalidad:
- Profesional, amigable y cercano
- Experto en networking, negocios, emprendimiento y desarrollo profesional
- Conoces el mercado laboral chileno
- Respondes siempre en español, de forma clara y útil
- Eres conciso pero completo en tus respuestas
- Agregas valor real con cada interacción"""
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": max_tokens,
        "temperature": temperature
    }
    
    for intento in range(reintentos):
        try:
            response = requests.post(GROQ_API_URL, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                respuesta = data['choices'][0]['message']['content']
                if respuesta and len(respuesta.strip()) > 0:
                    return respuesta.strip()
                    
            elif response.status_code == 429:
                logger.warning(f"Rate limit Groq, esperando... (intento {intento + 1})")
                import time
                time.sleep(2 * (intento + 1))
                
            elif response.status_code >= 500:
                logger.warning(f"Error servidor Groq {response.status_code} (intento {intento + 1})")
                import time
                time.sleep(1)
                
            else:
                logger.error(f"Error Groq API: {response.status_code} - {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout Groq (intento {intento + 1})")
            continue
        except Exception as e:
            logger.error(f"Error inesperado Groq: {str(e)[:100]}")
            return None
    
    return None


# ==================== FUNCIONES DE GEMINI OCR ====================

def analizar_imagen_ocr(image_bytes: bytes, precio_esperado: int) -> dict:
    """Analiza una imagen de comprobante usando Gemini Vision API"""
    if not GEMINI_API_KEY or not gemini_disponible:
        return {
            "analizado": False,
            "motivo": "Servicio OCR no disponible",
            "requiere_revision_manual": True
        }
    
    try:
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
        
        prompt = f"""Analiza esta imagen de un comprobante de transferencia bancaria chilena.

DATOS ESPERADOS:
- Cuenta destino debe contener: 69104312 (Banco Santander)
- Titular: Destak E.I.R.L. o RUT 76.698.480-0
- Monto esperado: aproximadamente ${precio_esperado:,} CLP

EXTRAE Y VERIFICA:
1. ¿Es un comprobante de transferencia válido? (SI/NO)
2. ¿El monto visible coincide aproximadamente con ${precio_esperado:,}? (SI/NO/NO_VISIBLE)
3. ¿La cuenta destino coincide con 69104312? (SI/NO/NO_VISIBLE)
4. Monto detectado (solo número)
5. Fecha de la transferencia si es visible

RESPONDE EN FORMATO JSON:
{{"es_comprobante": true/false, "monto_coincide": true/false/null, "cuenta_coincide": true/false/null, "monto_detectado": "número o null", "fecha_detectada": "fecha o null", "observaciones": "texto breve"}}"""

        url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
        
        payload = {
            "contents": [{
                "parts": [
                    {"inline_data": {"mime_type": "image/jpeg", "data": image_base64}},
                    {"text": prompt}
                ]
            }],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 500}
        }
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            texto_respuesta = data['candidates'][0]['content']['parts'][0]['text']
            
            try:
                json_match = re.search(r'\{[^{}]*\}', texto_respuesta, re.DOTALL)
                if json_match:
                    resultado = json.loads(json_match.group())
                    resultado["analizado"] = True
                    resultado["precio_esperado"] = precio_esperado
                    return resultado
            except:
                pass
            
            return {
                "analizado": True,
                "es_comprobante": "comprobante" in texto_respuesta.lower(),
                "observaciones": texto_respuesta[:200],
                "precio_esperado": precio_esperado
            }
        else:
            return {"analizado": False, "error": f"Error API: {response.status_code}", "requiere_revision_manual": True}
            
    except Exception as e:
        logger.error(f"Error en OCR Gemini: {str(e)[:100]}")
        return {"analizado": False, "error": str(e)[:100], "requiere_revision_manual": True}


# ==================== FUNCIONES DE SUSCRIPCIÓN ====================

def verificar_suscripcion_activa(user_id):
    """Verifica si un usuario tiene suscripción activa"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT fecha_expiracion, estado FROM suscripciones WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT fecha_expiracion, estado FROM suscripciones WHERE user_id = ?", (user_id,))
        
        resultado = c.fetchone()
        conn.close()
        
        if not resultado:
            return False
        
        if DATABASE_URL:
            fecha_exp = resultado['fecha_expiracion']
            estado = resultado['estado']
        else:
            fecha_exp = resultado['fecha_expiracion']
            estado = resultado['estado']
        
        if estado != 'activo':
            return False
        
        if isinstance(fecha_exp, str):
            fecha_exp = datetime.strptime(fecha_exp, "%Y-%m-%d %H:%M:%S")
        
        return fecha_exp > datetime.now()
        
    except Exception as e:
        logger.error(f"Error verificando suscripción: {e}")
        if conn:
            conn.close()
        return False


def obtener_dias_restantes(user_id):
    """Obtiene los días restantes de suscripción"""
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT fecha_expiracion FROM suscripciones WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT fecha_expiracion FROM suscripciones WHERE user_id = ?", (user_id,))
        
        resultado = c.fetchone()
        conn.close()
        
        if not resultado:
            return 0
        
        fecha_exp = resultado['fecha_expiracion'] if DATABASE_URL else resultado['fecha_expiracion']
        
        if isinstance(fecha_exp, str):
            fecha_exp = datetime.strptime(fecha_exp, "%Y-%m-%d %H:%M:%S")
        
        dias = (fecha_exp - datetime.now()).days
        return max(0, dias)
        
    except Exception as e:
        logger.error(f"Error obteniendo días restantes: {e}")
        if conn:
            conn.close()
        return 0


def registrar_usuario_suscripcion(user_id, first_name, username, es_admin=False, dias_gratis=DIAS_PRUEBA_GRATIS):
    """Registra un nuevo usuario con período de prueba gratuito"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        c = conn.cursor()
        
        # Verificar si el usuario ya existe
        if DATABASE_URL:
            c.execute("SELECT user_id FROM suscripciones WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT user_id FROM suscripciones WHERE user_id = ?", (user_id,))
        
        existente = c.fetchone()
        fecha_registro = datetime.now()
        
        if existente:
            # Usuario ya existe - solo actualizar nombre/username
            if DATABASE_URL:
                c.execute("""UPDATE suscripciones 
                             SET first_name = %s, username = %s, es_admin = %s
                             WHERE user_id = %s""",
                          (first_name, username, 1 if es_admin else 0, user_id))
            else:
                c.execute("""UPDATE suscripciones 
                             SET first_name = ?, username = ?, es_admin = ?
                             WHERE user_id = ?""",
                          (first_name, username, 1 if es_admin else 0, user_id))
            logger.info(f"Usuario existente actualizado: {first_name} (ID: {user_id})")
        else:
            # Nuevo usuario - dar período de prueba GRATIS
            fecha_expiracion = fecha_registro + timedelta(days=dias_gratis)
            
            if DATABASE_URL:
                c.execute("""INSERT INTO suscripciones 
                             (user_id, first_name, username, es_admin, fecha_registro, 
                              fecha_expiracion, estado, mensajes_engagement, servicios_usados) 
                             VALUES (%s, %s, %s, %s, %s, %s, 'activo', 0, '[]')""",
                          (user_id, first_name, username, 1 if es_admin else 0, 
                           fecha_registro, fecha_expiracion))
            else:
                c.execute("""INSERT INTO suscripciones 
                             (user_id, first_name, username, es_admin, fecha_registro, 
                              fecha_expiracion, estado, mensajes_engagement, servicios_usados) 
                             VALUES (?, ?, ?, ?, ?, ?, 'activo', 0, '[]')""",
                          (user_id, first_name, username, 1 if es_admin else 0, 
                           fecha_registro.strftime("%Y-%m-%d %H:%M:%S"), 
                           fecha_expiracion.strftime("%Y-%m-%d %H:%M:%S")))
            logger.info(f"Nuevo usuario registrado: {first_name} (ID: {user_id}) - {dias_gratis} días gratis")
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error registrando usuario: {e}")
        if conn:
            conn.close()
        return False


def extender_suscripcion(user_id, dias):
    """Extiende la suscripción de un usuario"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("SELECT fecha_expiracion FROM suscripciones WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT fecha_expiracion FROM suscripciones WHERE user_id = ?", (user_id,))
        
        resultado = c.fetchone()
        
        if resultado:
            fecha_actual = resultado['fecha_expiracion'] if DATABASE_URL else resultado['fecha_expiracion']
            if isinstance(fecha_actual, str):
                fecha_actual = datetime.strptime(fecha_actual, "%Y-%m-%d %H:%M:%S")
            
            if fecha_actual < datetime.now():
                fecha_actual = datetime.now()
            
            nueva_fecha = fecha_actual + timedelta(days=dias)
            
            if DATABASE_URL:
                c.execute("UPDATE suscripciones SET fecha_expiracion = %s, estado = 'activo' WHERE user_id = %s",
                          (nueva_fecha, user_id))
            else:
                c.execute("UPDATE suscripciones SET fecha_expiracion = ?, estado = 'activo' WHERE user_id = ?",
                          (nueva_fecha.strftime("%Y-%m-%d %H:%M:%S"), user_id))
            
            conn.commit()
            conn.close()
            return True
        
        conn.close()
        return False
        
    except Exception as e:
        logger.error(f"Error extendiendo suscripción: {e}")
        if conn:
            conn.close()
        return False


# ==================== FUNCIONES DE MENSAJES ====================

def guardar_mensaje(user_id, username, first_name, message, topic_id=None):
    """Guarda un mensaje en la base de datos"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        c = conn.cursor()
        categoria = categorizar_mensaje(message)
        
        if DATABASE_URL:
            c.execute("""INSERT INTO mensajes (user_id, username, first_name, message, topic_id, categoria)
                         VALUES (%s, %s, %s, %s, %s, %s)""",
                      (user_id, username, first_name, message[:4000], topic_id, categoria))
        else:
            c.execute("""INSERT INTO mensajes (user_id, username, first_name, message, topic_id, categoria)
                         VALUES (?, ?, ?, ?, ?, ?)""",
                      (user_id, username, first_name, message[:4000], topic_id, categoria))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error guardando mensaje: {e}")
        if conn:
            conn.close()


def categorizar_mensaje(texto):
    """Categoriza un mensaje según su contenido"""
    texto_lower = texto.lower()
    
    categorias = {
        'Empleo': ['trabajo', 'empleo', 'vacante', 'busco', 'oferta laboral', 'cv', 'currículum', 'postular'],
        'Networking': ['contacto', 'networking', 'conocer', 'conectar', 'alianza', 'colaboración'],
        'Consulta': ['ayuda', 'pregunta', 'duda', 'consulta', 'cómo', 'qué es', 'alguien sabe'],
        'Emprendimiento': ['emprendimiento', 'negocio', 'startup', 'empresa', 'proyecto', 'inversión'],
        'Evento': ['evento', 'webinar', 'charla', 'meetup', 'conferencia', 'taller'],
        'Saludo': ['hola', 'buenos días', 'buenas tardes', 'saludos', 'bienvenido']
    }
    
    for categoria, palabras in categorias.items():
        if any(palabra in texto_lower for palabra in palabras):
            return categoria
    
    return 'General'


# ==================== FUNCIONES DE PRECIOS Y CÓDIGOS ====================

def obtener_precios():
    """Obtiene los precios de los planes"""
    conn = get_db_connection()
    if not conn:
        return [(30, 2000, 'Mensual'), (180, 10500, 'Semestral'), (365, 20000, 'Anual')]
    
    try:
        c = conn.cursor()
        c.execute("SELECT dias, precio, nombre FROM precios_planes ORDER BY dias")
        precios = c.fetchall()
        conn.close()
        
        if DATABASE_URL:
            return [(p['dias'], p['precio'], p['nombre']) for p in precios]
        else:
            return [(p['dias'], p['precio'], p['nombre']) for p in precios]
    except Exception as e:
        logger.error(f"Error obteniendo precios: {e}")
        if conn:
            conn.close()
        return [(30, 2000, 'Mensual'), (180, 10500, 'Semestral'), (365, 20000, 'Anual')]


def formato_clp(numero):
    """Formatea un número como pesos chilenos"""
    return f"${numero:,}".replace(",", ".")


def generar_codigo_activacion(dias):
    """Genera un código de activación único"""
    codigo = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(12))
    
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("INSERT INTO codigos_activacion (codigo, dias) VALUES (%s, %s)", (codigo, dias))
        else:
            c.execute("INSERT INTO codigos_activacion (codigo, dias) VALUES (?, ?)", (codigo, dias))
        conn.commit()
        conn.close()
        return codigo
    except Exception as e:
        logger.error(f"Error generando código: {e}")
        if conn:
            conn.close()
        return None


def usar_codigo_activacion(codigo, user_id):
    """Usa un código de activación"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT dias, usado FROM codigos_activacion WHERE codigo = %s", (codigo,))
        else:
            c.execute("SELECT dias, usado FROM codigos_activacion WHERE codigo = ?", (codigo,))
        
        resultado = c.fetchone()
        
        if not resultado:
            conn.close()
            return None
        
        dias = resultado['dias'] if DATABASE_URL else resultado['dias']
        usado = resultado['usado'] if DATABASE_URL else resultado['usado']
        
        if usado:
            conn.close()
            return -1
        
        if DATABASE_URL:
            c.execute("""UPDATE codigos_activacion 
                         SET usado = 1, user_id_usado = %s, fecha_uso = %s 
                         WHERE codigo = %s""",
                      (user_id, datetime.now(), codigo))
        else:
            c.execute("""UPDATE codigos_activacion 
                         SET usado = 1, user_id_usado = ?, fecha_uso = ? 
                         WHERE codigo = ?""",
                      (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), codigo))
        
        conn.commit()
        conn.close()
        
        extender_suscripcion(user_id, dias)
        return dias
        
    except Exception as e:
        logger.error(f"Error usando código: {e}")
        if conn:
            conn.close()
        return None


# ==================== FUNCIONES DE BÚSQUEDA ====================

def buscar_en_historial(query, topic_id=None, limit=10):
    """Busca en el historial de mensajes"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        c = conn.cursor()
        query_like = f'%{query.lower()}%'
        
        if DATABASE_URL:
            if topic_id:
                c.execute("""SELECT first_name, message, fecha FROM mensajes 
                             WHERE LOWER(message) LIKE %s AND topic_id = %s
                             ORDER BY fecha DESC LIMIT %s""", (query_like, topic_id, limit))
            else:
                c.execute("""SELECT first_name, message, fecha FROM mensajes 
                             WHERE LOWER(message) LIKE %s
                             ORDER BY fecha DESC LIMIT %s""", (query_like, limit))
        else:
            if topic_id:
                c.execute("""SELECT first_name, message, fecha FROM mensajes 
                             WHERE LOWER(message) LIKE ? AND topic_id = ?
                             ORDER BY fecha DESC LIMIT ?""", (query_like, topic_id, limit))
            else:
                c.execute("""SELECT first_name, message, fecha FROM mensajes 
                             WHERE LOWER(message) LIKE ?
                             ORDER BY fecha DESC LIMIT ?""", (query_like, limit))
        
        resultados = c.fetchall()
        conn.close()
        
        if DATABASE_URL:
            return [(r['first_name'], r['message'], r['fecha']) for r in resultados]
        else:
            return [(r['first_name'], r['message'], r['fecha']) for r in resultados]
            
    except Exception as e:
        logger.error(f"Error buscando en historial: {e}")
        if conn:
            conn.close()
        return []


# ==================== BÚSQUEDA DE EMPLEOS REALES CON JSEARCH ====================

def buscar_empleos_jsearch(query: str, ubicacion: str = "Chile", num_pages: int = 1) -> list:
    """Busca empleos REALES usando JSearch API (Google for Jobs)"""
    if not RAPIDAPI_KEY or not jsearch_disponible:
        return None
    
    try:
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }
        
        # Construir query de búsqueda
        search_query = f"{query} in {ubicacion}"
        
        params = {
            "query": search_query,
            "page": "1",
            "num_pages": str(num_pages),
            "date_posted": "month"  # Empleos del último mes
        }
        
        response = requests.get(JSEARCH_URL, headers=headers, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            empleos = data.get('data', [])
            logger.info(f"✅ JSearch encontró {len(empleos)} empleos para: {query}")
            return empleos
        else:
            logger.error(f"❌ Error JSearch API: {response.status_code} - {response.text[:100]}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error en JSearch: {str(e)[:100]}")
        return None


async def buscar_empleos_web(cargo=None, ubicacion=None, renta=None):
    """Busca empleos - primero intenta JSearch (reales), luego fallback a IA"""
    
    busqueda_texto = cargo or "empleo"
    ubicacion_busqueda = ubicacion or "Chile"
    
    # Intentar buscar empleos REALES con JSearch
    if jsearch_disponible:
        empleos = buscar_empleos_jsearch(busqueda_texto, ubicacion_busqueda)
        
        if empleos and len(empleos) > 0:
            # Formatear empleos reales
            fecha_actual = datetime.now().strftime("%d/%m/%Y")
            resultado = f"🔎 **EMPLEOS REALES ENCONTRADOS**\n"
            resultado += f"📋 Búsqueda: _{busqueda_texto}_\n"
            resultado += f"📍 Ubicación: _{ubicacion_busqueda}_\n"
            resultado += f"📅 Fecha: {fecha_actual}\n"
            resultado += f"📊 Resultados: {len(empleos[:8])} ofertas\n"
            resultado += "━" * 30 + "\n\n"
            
            for i, empleo in enumerate(empleos[:8], 1):
                titulo = empleo.get('job_title', 'Sin título')
                empresa = empleo.get('employer_name', 'Empresa no especificada')
                ubicacion_job = empleo.get('job_city', empleo.get('job_country', 'No especificada'))
                
                # Sueldo
                min_salary = empleo.get('job_min_salary')
                max_salary = empleo.get('job_max_salary')
                salary_period = empleo.get('job_salary_period', '')
                
                if min_salary and max_salary:
                    sueldo = f"${int(min_salary):,} - ${int(max_salary):,}".replace(",", ".")
                    if salary_period:
                        sueldo += f" ({salary_period})"
                elif min_salary:
                    sueldo = f"Desde ${int(min_salary):,}".replace(",", ".")
                else:
                    sueldo = "No especificado"
                
                # Tipo de empleo
                tipo = empleo.get('job_employment_type', 'No especificado')
                if tipo == 'FULLTIME':
                    tipo = 'Tiempo completo'
                elif tipo == 'PARTTIME':
                    tipo = 'Medio tiempo'
                elif tipo == 'CONTRACTOR':
                    tipo = 'Contrato'
                
                # Link de postulación
                link = empleo.get('job_apply_link', '')
                
                # Fecha de publicación
                posted = empleo.get('job_posted_at_datetime_utc', '')
                if posted:
                    try:
                        fecha_pub = datetime.fromisoformat(posted.replace('Z', '+00:00'))
                        dias_atras = (datetime.now(fecha_pub.tzinfo) - fecha_pub).days
                        if dias_atras == 0:
                            fecha_str = "Hoy"
                        elif dias_atras == 1:
                            fecha_str = "Ayer"
                        else:
                            fecha_str = f"Hace {dias_atras} días"
                    except:
                        fecha_str = ""
                else:
                    fecha_str = ""
                
                resultado += f"**{i}. {titulo}**\n"
                resultado += f"🏢 {empresa}\n"
                resultado += f"📍 {ubicacion_job}\n"
                resultado += f"💰 {sueldo}\n"
                resultado += f"📋 {tipo}"
                if fecha_str:
                    resultado += f" • {fecha_str}"
                resultado += "\n"
                
                if link:
                    resultado += f"🔗 [**POSTULAR AQUÍ**]({link})\n"
                
                resultado += "\n"
            
            resultado += "━" * 30 + "\n"
            resultado += "✅ _Estos son empleos REALES de LinkedIn, Indeed, Glassdoor y otros portales._\n"
            resultado += "👆 _Haz clic en 'POSTULAR AQUÍ' para ir directo a la oferta._"
            
            return resultado
    
    # FALLBACK: Si JSearch no está disponible o no encontró resultados
    # Crear links de búsqueda para portales reales
    busqueda_encoded = urllib.parse.quote(busqueda_texto)
    busqueda_laborum = busqueda_texto.replace(" ", "-").lower()
    
    links_portales = f"""
🔗 **BUSCA EN ESTOS PORTALES:**

• [🔵 LinkedIn Jobs](https://www.linkedin.com/jobs/search/?keywords={busqueda_encoded}&location=Chile)
• [🟠 Trabajando.com](https://www.trabajando.cl/empleos?q={busqueda_encoded})
• [🟢 Laborum](https://www.laborum.cl/empleos-busqueda-{busqueda_laborum}.html)
• [🔴 Indeed Chile](https://cl.indeed.com/jobs?q={busqueda_encoded}&l=Chile)
• [🟣 Computrabajo](https://www.computrabajo.cl/empleos?q={busqueda_encoded})
"""

    if not ia_disponible:
        return f"🔍 **BÚSQUEDA DE EMPLEO**\n📋 Criterios: _{busqueda_texto}_\n{links_portales}\n\n💡 Haz clic en los links para ver ofertas reales."
    
    try:
        consulta = f"cargo: {cargo}" if cargo else "empleos generales"
        if ubicacion:
            consulta += f", ubicación: {ubicacion}"
        
        prompt = f"""Genera 5 ejemplos de ofertas laborales REALISTAS para Chile.

BÚSQUEDA: {consulta}

REGLAS:
1. Sueldos MENSUALES LÍQUIDOS en pesos chilenos
2. Empresas REALES chilenas
3. Si el cargo no existe exactamente, muestra CARGOS SIMILARES

FORMATO:
💼 **[CARGO]**
🏢 Empresa: [Nombre]
📍 Ubicación: [Ciudad], Chile
💰 Sueldo: $X.XXX.XXX - $X.XXX.XXX mensuales
📋 Modalidad: [Presencial/Híbrido/Remoto]

---

Solo las 5 ofertas, sin introducciones."""

        respuesta = llamar_groq(prompt, max_tokens=1200, temperature=0.7)
        
        if respuesta:
            resultado = f"🔎 **SUGERENCIAS DE EMPLEO (IA)**\n"
            resultado += f"📋 Búsqueda: _{consulta}_\n"
            resultado += "━" * 30 + "\n\n"
            resultado += respuesta
            resultado += "\n\n" + "━" * 30
            resultado += "\n⚠️ _Estas son sugerencias de IA. Para ofertas reales:_\n"
            resultado += links_portales
            return resultado
        else:
            return f"🔍 **BÚSQUEDA DE EMPLEO**\n{links_portales}\n💡 Usa los links para buscar directamente."
            
    except Exception as e:
        logger.error(f"Error en buscar_empleos_web: {e}")
        return f"❌ Error al buscar.\n{links_portales}"


# ==================== KEEP-ALIVE PARA RENDER ====================

class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        status = "✅ Activo" if ia_disponible else "⚠️ Sin IA"
        db_status = "✅ Supabase" if DATABASE_URL else "⚠️ SQLite"
        self.wfile.write(f"Bot Cofradía Premium - {status} - DB: {db_status}".encode())
    
    def log_message(self, format, *args):
        pass

def run_keepalive_server():
    port = int(os.environ.get('PORT', 10000))
    server = HTTPServer(('0.0.0.0', port), KeepAliveHandler)
    logger.info(f"🌐 Servidor keep-alive en puerto {port}")
    server.serve_forever()

def auto_ping():
    """Auto-ping para mantener el servicio activo"""
    import time as t
    url = os.environ.get('RENDER_EXTERNAL_URL')
    while True:
        t.sleep(300)
        if url:
            try:
                requests.get(url, timeout=10)
            except:
                pass


# ==================== DECORADORES ====================

def requiere_suscripcion(func):
    """Decorador que verifica suscripción activa"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not verificar_suscripcion_activa(user_id):
            await update.message.reply_text(
                "❌ **Necesitas una suscripción activa**\n\n"
                "📝 Usa /registrarse en @Cofradia_de_Networking\n"
                "💳 O renueva con /renovar",
                parse_mode='Markdown'
            )
            return
        return await func(update, context)
    return wrapper


def es_chat_privado(update: Update) -> bool:
    """Verifica si es un chat privado"""
    return update.effective_chat.type == 'private'


# ==================== FUNCIONES AUXILIARES ====================

async def enviar_mensaje_largo(update: Update, texto: str, parse_mode='Markdown'):
    """Envía mensajes largos dividiéndolos si es necesario"""
    if len(texto) <= 4000:
        await update.message.reply_text(texto, parse_mode=parse_mode)
    else:
        partes = [texto[i:i+4000] for i in range(0, len(texto), 4000)]
        for parte in partes:
            await update.message.reply_text(parte, parse_mode=parse_mode)


def registrar_servicio_usado(user_id, servicio):
    """Registra el uso de un servicio por el usuario"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT servicios_usados FROM suscripciones WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT servicios_usados FROM suscripciones WHERE user_id = ?", (user_id,))
        
        resultado = c.fetchone()
        if resultado:
            servicios_str = resultado['servicios_usados'] if DATABASE_URL else resultado['servicios_usados']
            try:
                servicios = json.loads(servicios_str) if servicios_str else []
            except:
                servicios = []
            
            servicios.append({"servicio": servicio, "fecha": datetime.now().isoformat()})
            servicios = servicios[-100:]  # Mantener últimos 100
            
            if DATABASE_URL:
                c.execute("UPDATE suscripciones SET servicios_usados = %s WHERE user_id = %s",
                          (json.dumps(servicios), user_id))
            else:
                c.execute("UPDATE suscripciones SET servicios_usados = ? WHERE user_id = ?",
                          (json.dumps(servicios), user_id))
            conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error registrando servicio: {e}")
        if conn:
            conn.close()


# ==================== COMANDOS BÁSICOS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start - Bienvenida"""
    user = update.message.from_user
    
    mensaje = f"""
🎉 **¡Bienvenido/a {user.first_name} al Bot Cofradía Premium!**

━━━━━━━━━━━━━━━━━━━━
📌 **¿CÓMO EMPEZAR?**
━━━━━━━━━━━━━━━━━━━━

**PASO 1️⃣** → Ve al grupo @Cofradia_de_Networking
**PASO 2️⃣** → Escribe: /registrarse (¡Sólo si no lo has hecho!)
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
    """Comando /ayuda - Lista de comandos"""
    texto = """
📚 **COMANDOS DISPONIBLES**

━━━ **BÁSICOS** ━━━
/start - Iniciar bot
/ayuda - Ver esta ayuda
/registrarse - Activar cuenta (usar en @Cofradia_de_Networking)
/mi_cuenta - Ver tu suscripción
/renovar - Renovar plan
/activar [código] - Usar código de activación

━━━ **BÚSQUEDA** ━━━
/buscar [texto] - Buscar en historial
/buscar_ia [consulta] - Búsqueda con IA
/buscar_profesional [área] - Buscar expertos
/empleo cargo:[X], ubicación:[Y] - Buscar empleos

━━━ **ESTADÍSTICAS** ━━━
/graficos - Ver gráficos de actividad
/estadisticas - Estadísticas generales
/categorias - Ver categorías de mensajes
/top_usuarios - Ranking de participación
/mi_perfil - Tu perfil de actividad

━━━ **RESÚMENES** ━━━
/resumen - Resumen del día
/resumen_semanal - Resumen de 7 días
/resumen_mes - Resumen mensual
/resumen_usuario @nombre - Perfil de usuario

━━━ **RRHH** ━━━
/dotacion - Total de integrantes
/ingresos [mes_año] - Nuevos ingresos
/crecimiento_mes - Crecimiento mensual
/crecimiento_anual - Crecimiento anual

💡 **TIP:** Mencióname en el grupo @Cofradia_de_Networking con tu pregunta:
`@Cofradia_Premium_Bot ¿tu pregunta?`
"""
    await update.message.reply_text(texto, parse_mode='Markdown')


async def registrarse_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /registrarse - Registrar usuario"""
    user = update.message.from_user
    
    if es_chat_privado(update):
        await update.message.reply_text(
            "❌ Debes usar /registrarse en el grupo @Cofradia_de_Networking",
            parse_mode='Markdown'
        )
        return
    
    # Verificar si ya está registrado con cuenta activa
    if verificar_suscripcion_activa(user.id):
        await update.message.reply_text(
            f"✅ ¡{user.first_name} ya estás registrado con una cuenta activa!",
            parse_mode='Markdown'
        )
        return
    
    # Verificar si es admin del grupo
    try:
        chat_member = await context.bot.get_chat_member(update.effective_chat.id, user.id)
        es_admin = chat_member.status in ['creator', 'administrator']
    except:
        es_admin = False
    
    # Registrar usuario
    if registrar_usuario_suscripcion(user.id, user.first_name, user.username or "sin_username", es_admin):
        await update.message.reply_text(f"""
✅ **¡@{user.username or user.first_name} estás registrado!**

🚀 Ya puedes usar tu bot asistente.
📱 Inicia un chat privado conmigo: @Cofradia_Premium_Bot
💡 Escríbeme: /start
""", parse_mode='Markdown')
        
        # Enviar mensaje privado
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=f"🎉 **¡Bienvenido/a {user.first_name}!**\n\n"
                     f"Tu cuenta está activa.\n"
                     f"Usa /ayuda para ver los comandos disponibles.\n"
                     f"Usa /mi_cuenta para ver el estado de tu suscripción.",
                parse_mode='Markdown'
            )
        except:
            pass
    else:
        await update.message.reply_text("❌ Hubo un error al registrarte. Intenta de nuevo.")


async def mi_cuenta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mi_cuenta - Ver estado de suscripción"""
    user = update.message.from_user
    
    if verificar_suscripcion_activa(user.id):
        dias = obtener_dias_restantes(user.id)
        
        if dias > 30:
            emoji = "🟢"
            estado = "Excelente"
        elif dias > 7:
            emoji = "🟡"
            estado = "Por vencer pronto"
        else:
            emoji = "🔴"
            estado = "¡Renueva pronto!"
        
        await update.message.reply_text(f"""
👤 **MI CUENTA**

{emoji} **Estado:** Activa - {estado}
📅 **Días restantes:** {dias} días

💡 Para renovar usa /renovar
""", parse_mode='Markdown')
    else:
        await update.message.reply_text("""
👤 **MI CUENTA**

🔴 **Estado:** Sin suscripción activa

📝 Usa /registrarse en @Cofradia_de_Networking
💳 O renueva con /renovar
""", parse_mode='Markdown')


async def renovar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /renovar - Renovar suscripción"""
    if not es_chat_privado(update):
        await update.message.reply_text("❌ Usa /renovar en el chat privado @Cofradia_Premium_Bot")
        return
    
    precios = obtener_precios()
    keyboard = [
        [InlineKeyboardButton(f"💎 {nombre} ({dias}d) - {formato_clp(precio)}", callback_data=f"plan_{dias}")]
        for dias, precio, nombre in precios
    ]
    
    await update.message.reply_text("""
💳 **RENOVAR SUSCRIPCIÓN**

Selecciona tu plan:
""", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


async def activar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /activar - Activar código"""
    user = update.message.from_user
    
    if not context.args:
        await update.message.reply_text("❌ Uso: /activar TU_CODIGO")
        return
    
    codigo = context.args[0].upper()
    resultado = usar_codigo_activacion(codigo, user.id)
    
    if resultado is None:
        await update.message.reply_text("❌ Código inválido o no existe.")
    elif resultado == -1:
        await update.message.reply_text("❌ Este código ya fue usado.")
    else:
        dias = obtener_dias_restantes(user.id)
        await update.message.reply_text(f"""
✅ **¡CÓDIGO ACTIVADO!**

🎁 Se agregaron **{resultado} días** a tu cuenta.
📅 Días totales restantes: **{dias}**

¡Disfruta tu suscripción!
""", parse_mode='Markdown')


# ==================== COMANDOS DE BÚSQUEDA ====================

@requiere_suscripcion
async def buscar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /buscar - Buscar en historial"""
    if not context.args:
        await update.message.reply_text("❌ Uso: /buscar [término]")
        return
    
    query = ' '.join(context.args)
    topic_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
    
    resultados = buscar_en_historial(query, topic_id, limit=10)
    
    if not resultados:
        await update.message.reply_text(f"❌ No se encontraron resultados para: _{query}_", parse_mode='Markdown')
        return
    
    mensaje = f"🔍 **RESULTADOS PARA:** _{query}_\n\n"
    
    for nombre, texto, fecha in resultados[:10]:
        texto_corto = texto[:150] + "..." if len(texto) > 150 else texto
        mensaje += f"👤 **{nombre}**\n{texto_corto}\n\n"
    
    await enviar_mensaje_largo(update, mensaje)
    registrar_servicio_usado(update.effective_user.id, 'buscar')


@requiere_suscripcion
async def graficos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /graficos - Muestra gráficos de actividad del grupo"""
    msg = await update.message.reply_text("📊 Generando gráficos...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        dias = 7  # Últimos 7 días
        
        # Obtener estadísticas
        if DATABASE_URL:
            # PostgreSQL
            c.execute("""SELECT DATE(fecha), COUNT(*) FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '%s days'
                        GROUP BY DATE(fecha) ORDER BY DATE(fecha)""", (dias,))
            por_dia = c.fetchall()
            
            c.execute("""SELECT first_name, COUNT(*) FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '%s days'
                        GROUP BY first_name ORDER BY COUNT(*) DESC LIMIT 10""", (dias,))
            usuarios_activos = c.fetchall()
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '%s days' AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC""", (dias,))
            por_categoria = c.fetchall()
        else:
            # SQLite
            fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
            c.execute("""SELECT DATE(fecha), COUNT(*) FROM mensajes 
                        WHERE fecha >= ? GROUP BY DATE(fecha) ORDER BY DATE(fecha)""", (fecha_inicio,))
            por_dia = c.fetchall()
            
            c.execute("""SELECT first_name, COUNT(*) FROM mensajes 
                        WHERE fecha >= ? GROUP BY first_name ORDER BY COUNT(*) DESC LIMIT 10""", (fecha_inicio,))
            usuarios_activos = c.fetchall()
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE fecha >= ? AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC""", (fecha_inicio,))
            por_categoria = c.fetchall()
        
        conn.close()
        
        # Convertir resultados
        if DATABASE_URL:
            por_dia = [(str(r['date']), r['count']) for r in por_dia] if por_dia else []
            usuarios_activos = [(r['first_name'], r['count']) for r in usuarios_activos] if usuarios_activos else []
            por_categoria = [(r['categoria'], r['count']) for r in por_categoria] if por_categoria else []
        else:
            por_dia = [(r[0], r[1]) for r in por_dia] if por_dia else []
            usuarios_activos = [(r[0], r[1]) for r in usuarios_activos] if usuarios_activos else []
            por_categoria = [(r[0], r[1]) for r in por_categoria] if por_categoria else []
        
        if not por_dia and not usuarios_activos:
            await msg.edit_text("📊 No hay suficientes datos para generar gráficos.\n\nEl grupo necesita más actividad.")
            return
        
        # Crear gráfico
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('📊 ESTADÍSTICAS COFRADÍA - Últimos 7 días', fontsize=14, fontweight='bold')
        
        # Gráfico 1: Mensajes por día
        ax1 = axes[0, 0]
        if por_dia:
            fechas = [d[0][-5:] if len(d[0]) > 5 else d[0] for d in por_dia]
            valores = [d[1] for d in por_dia]
            ax1.bar(fechas, valores, color='#3498db', alpha=0.8)
            ax1.set_title('📈 Mensajes por Día')
            ax1.set_xlabel('Fecha')
            ax1.set_ylabel('Mensajes')
            ax1.tick_params(axis='x', rotation=45)
        else:
            ax1.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
            ax1.set_title('📈 Mensajes por Día')
        
        # Gráfico 2: Usuarios más activos
        ax2 = axes[0, 1]
        if usuarios_activos:
            nombres = [u[0][:10] for u in usuarios_activos[:8]]
            mensajes = [u[1] for u in usuarios_activos[:8]]
            colors = plt.cm.viridis([i/len(nombres) for i in range(len(nombres))])
            ax2.barh(nombres, mensajes, color=colors)
            ax2.set_title('👥 Usuarios Más Activos')
            ax2.set_xlabel('Mensajes')
        else:
            ax2.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
            ax2.set_title('👥 Usuarios Más Activos')
        
        # Gráfico 3: Categorías
        ax3 = axes[1, 0]
        if por_categoria:
            categorias = [c[0] for c in por_categoria[:6]]
            cantidades = [c[1] for c in por_categoria[:6]]
            ax3.pie(cantidades, labels=categorias, autopct='%1.1f%%', startangle=90)
            ax3.set_title('🏷️ Categorías de Mensajes')
        else:
            ax3.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
            ax3.set_title('🏷️ Categorías de Mensajes')
        
        # Gráfico 4: Resumen
        ax4 = axes[1, 1]
        ax4.axis('off')
        total_mensajes = sum([d[1] for d in por_dia]) if por_dia else 0
        total_usuarios = len(usuarios_activos)
        promedio = total_mensajes / dias if dias > 0 else 0
        
        resumen_texto = f"""
        📊 RESUMEN
        
        📝 Total mensajes: {total_mensajes}
        👥 Usuarios activos: {total_usuarios}
        📈 Promedio diario: {promedio:.1f}
        📅 Período: {dias} días
        """
        ax4.text(0.1, 0.5, resumen_texto, fontsize=12, verticalalignment='center',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
        
        plt.tight_layout()
        
        # Guardar y enviar
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        await msg.delete()
        await update.message.reply_photo(
            photo=buf,
            caption="📊 **Estadísticas de los últimos 7 días**\n\nUsa /estadisticas para ver más detalles.",
            parse_mode='Markdown'
        )
        
        registrar_servicio_usado(update.effective_user.id, 'graficos')
        
    except Exception as e:
        logger.error(f"Error en graficos_comando: {e}")
        await msg.edit_text(f"❌ Error generando gráficos: {str(e)[:100]}")


@requiere_suscripcion
async def buscar_ia_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /buscar_ia - Búsqueda inteligente en el historial del grupo con IA"""
    if not context.args:
        await update.message.reply_text(
            "❌ **Uso:** /buscar_ia [tu consulta]\n\n"
            "**Ejemplo:** `/buscar_ia aniversario`\n\n"
            "Este comando busca en el historial del grupo y usa IA para analizar los resultados.",
            parse_mode='Markdown'
        )
        return
    
    consulta = ' '.join(context.args)
    msg = await update.message.reply_text("🔍 Buscando en el historial del grupo...")
    
    # Buscar en el historial del grupo
    topic_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
    resultados = buscar_en_historial(consulta, topic_id, limit=15)
    
    if not resultados:
        await msg.edit_text(
            f"❌ No se encontraron mensajes relacionados con: **{consulta}**\n\n"
            f"💡 Intenta con otras palabras clave.",
            parse_mode='Markdown'
        )
        return
    
    # Si no hay IA disponible, mostrar resultados sin análisis
    if not ia_disponible:
        await msg.delete()
        mensaje = f"🔍 **RESULTADOS PARA:** _{consulta}_\n"
        mensaje += f"📊 Encontrados: {len(resultados)} mensajes\n\n"
        
        for nombre, texto, fecha in resultados[:10]:
            fecha_str = fecha.strftime("%d/%m/%Y") if hasattr(fecha, 'strftime') else str(fecha)[:10]
            texto_corto = texto[:150] + "..." if len(texto) > 150 else texto
            mensaje += f"👤 **{nombre}** ({fecha_str})\n{texto_corto}\n\n"
        
        await enviar_mensaje_largo(update, mensaje)
        registrar_servicio_usado(update.effective_user.id, 'buscar_ia')
        return
    
    # Preparar contexto con los mensajes encontrados
    await msg.edit_text("🧠 Analizando resultados con IA...")
    
    contexto_mensajes = ""
    for i, (nombre, texto, fecha) in enumerate(resultados, 1):
        fecha_str = fecha.strftime("%d/%m/%Y %H:%M") if hasattr(fecha, 'strftime') else str(fecha)[:16]
        contexto_mensajes += f"{i}. {nombre} ({fecha_str}): {texto[:300]}\n\n"
    
    prompt = f"""Eres el asistente de Cofradía de Networking. El usuario busca información sobre: "{consulta}"

MENSAJES ENCONTRADOS EN EL HISTORIAL DEL GRUPO:
{contexto_mensajes}

INSTRUCCIONES:
1. Analiza los mensajes encontrados y extrae la información relevante sobre "{consulta}"
2. Resume los puntos más importantes mencionados por los miembros
3. Si hay fechas, eventos o datos específicos, destácalos
4. Menciona quiénes aportaron información relevante
5. Si los mensajes no son relevantes para la búsqueda, indícalo honestamente

Responde de forma organizada y útil. NO inventes información que no esté en los mensajes."""

    respuesta = llamar_groq(prompt, max_tokens=1000, temperature=0.3)
    
    await msg.delete()
    
    if respuesta:
        mensaje_final = f"🔍 **BÚSQUEDA:** _{consulta}_\n"
        mensaje_final += f"📊 **Mensajes analizados:** {len(resultados)}\n"
        mensaje_final += "━" * 25 + "\n\n"
        mensaje_final += respuesta
        
        await enviar_mensaje_largo(update, mensaje_final)
        registrar_servicio_usado(update.effective_user.id, 'buscar_ia')
    else:
        # Fallback: mostrar resultados sin IA
        mensaje = f"🔍 **RESULTADOS PARA:** _{consulta}_\n\n"
        for nombre, texto, fecha in resultados[:8]:
            texto_corto = texto[:150] + "..." if len(texto) > 150 else texto
            mensaje += f"👤 **{nombre}**\n{texto_corto}\n\n"
        
        await enviar_mensaje_largo(update, mensaje)


@requiere_suscripcion
async def empleo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /empleo - Buscar empleos"""
    texto = ' '.join(context.args) if context.args else ""
    
    # Parsear parámetros flexibles
    cargo = None
    ubicacion = None
    renta = None
    
    if texto:
        # Formato: /empleo cargo:X, ubicación:Y, renta:Z
        if ':' in texto:
            partes = texto.split(',')
            for parte in partes:
                parte = parte.strip()
                if parte.lower().startswith('cargo:'):
                    cargo = parte[6:].strip()
                elif parte.lower().startswith('ubicación:') or parte.lower().startswith('ubicacion:'):
                    ubicacion = parte.split(':', 1)[1].strip()
                elif parte.lower().startswith('renta:'):
                    renta = parte[6:].strip()
        else:
            # Formato simple: /empleo Gerente Finanzas
            cargo = texto
    
    msg = await update.message.reply_text("🔍 Buscando ofertas de empleo...")
    
    resultado = await buscar_empleos_web(cargo, ubicacion, renta)
    
    await msg.delete()
    await enviar_mensaje_largo(update, resultado)
    registrar_servicio_usado(update.effective_user.id, 'empleo')


# ==================== CALLBACKS ====================

async def callback_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback cuando seleccionan un plan"""
    query = update.callback_query
    await query.answer()
    
    dias = int(query.data.split('_')[1])
    precios = obtener_precios()
    precio = next((p[1] for p in precios if p[0] == dias), 0)
    nombre = next((p[2] for p in precios if p[0] == dias), "Plan")
    
    context.user_data['plan_seleccionado'] = dias
    context.user_data['precio'] = precio
    
    await query.edit_message_text(f"""
💳 **PLAN SELECCIONADO: {nombre}**

💰 **Precio:** {formato_clp(precio)}
📅 **Duración:** {dias} días

━━━━━━━━━━━━━━━━━━━━
📱 **DATOS PARA TRANSFERENCIA:**
━━━━━━━━━━━━━━━━━━━━

🏦 **Banco:** Santander
👤 **Nombre:** Destak E.I.R.L.
🔢 **RUT:** 76.698.480-0
💳 **Cuenta:** 69104312
📧 **Email:** contacto@destak.cl

━━━━━━━━━━━━━━━━━━━━

📸 **Envía el comprobante de transferencia** como FOTO a este chat.

⏳ Una vez verificado, recibirás tu código de activación.
""", parse_mode='Markdown')


async def callback_generar_codigo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para generar código (admin)"""
    query = update.callback_query
    
    if query.from_user.id != OWNER_ID:
        await query.answer("❌ No autorizado", show_alert=True)
        return
    
    await query.answer()
    
    dias = int(query.data.split('_')[1])
    codigo = generar_codigo_activacion(dias)
    
    if codigo:
        precios = obtener_precios()
        nombre = next((p[2] for p in precios if p[0] == dias), "Plan")
        
        await query.edit_message_text(f"""
✅ **CÓDIGO GENERADO**

🎫 **Código:** `{codigo}`
📅 **Plan:** {nombre} ({dias} días)

📋 El usuario debe usar:
`/activar {codigo}`
""", parse_mode='Markdown')
    else:
        await query.edit_message_text("❌ Error generando código")


async def callback_aprobar_rechazar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para aprobar/rechazar pagos"""
    query = update.callback_query
    
    if query.from_user.id != OWNER_ID:
        await query.answer("❌ No autorizado", show_alert=True)
        return
    
    await query.answer()
    
    data = query.data.split('_')
    accion = data[0]
    pago_id = int(data[1])
    
    conn = get_db_connection()
    if not conn:
        await query.edit_message_caption("❌ Error de conexión a base de datos")
        return
    
    try:
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("SELECT user_id, dias_plan FROM pagos_pendientes WHERE id = %s", (pago_id,))
        else:
            c.execute("SELECT user_id, dias_plan FROM pagos_pendientes WHERE id = ?", (pago_id,))
        
        pago = c.fetchone()
        
        if not pago:
            await query.edit_message_caption("❌ Pago no encontrado")
            conn.close()
            return
        
        user_id = pago['user_id'] if DATABASE_URL else pago['user_id']
        dias = pago['dias_plan'] if DATABASE_URL else pago['dias_plan']
        
        if accion == 'aprobar':
            codigo = generar_codigo_activacion(dias)
            
            if DATABASE_URL:
                c.execute("UPDATE pagos_pendientes SET estado = 'aprobado' WHERE id = %s", (pago_id,))
            else:
                c.execute("UPDATE pagos_pendientes SET estado = 'aprobado' WHERE id = ?", (pago_id,))
            
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"""
✅ **¡PAGO APROBADO!**

🎫 Tu código de activación es:
`{codigo}`

📋 Usa el comando:
`/activar {codigo}`

¡Gracias por tu preferencia!
""",
                    parse_mode='Markdown'
                )
            except:
                pass
            
            await query.edit_message_caption(f"✅ APROBADO - Código enviado: {codigo}")
            
        else:
            if DATABASE_URL:
                c.execute("UPDATE pagos_pendientes SET estado = 'rechazado' WHERE id = %s", (pago_id,))
            else:
                c.execute("UPDATE pagos_pendientes SET estado = 'rechazado' WHERE id = ?", (pago_id,))
            
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="❌ Tu comprobante fue rechazado.\n\nPor favor, verifica los datos y envía un nuevo comprobante.",
                    parse_mode='Markdown'
                )
            except:
                pass
            
            await query.edit_message_caption("❌ RECHAZADO - Usuario notificado")
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error en callback aprobar/rechazar: {e}")
        await query.edit_message_caption(f"❌ Error: {str(e)[:100]}")
        if conn:
            conn.close()


# ==================== RECIBIR COMPROBANTE ====================

async def recibir_comprobante(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recibe y procesa comprobantes de pago"""
    user = update.message.from_user
    
    if not es_chat_privado(update):
        return
    
    if 'plan_seleccionado' not in context.user_data:
        await update.message.reply_text("❌ Primero selecciona un plan con /renovar")
        return
    
    dias = context.user_data['plan_seleccionado']
    precio = context.user_data['precio']
    
    msg = await update.message.reply_text("🔍 Procesando comprobante...")
    
    photo = update.message.photo[-1]
    file = await context.bot.get_file(photo.file_id)
    
    # Análisis OCR
    datos_ocr = {"analizado": False, "precio_esperado": precio}
    
    if gemini_disponible:
        try:
            file_bytes = await file.download_as_bytearray()
            datos_ocr = analizar_imagen_ocr(bytes(file_bytes), precio)
        except Exception as e:
            logger.error(f"Error en OCR: {e}")
    
    await msg.delete()
    await update.message.reply_text("""
✅ **Comprobante recibido**

⏳ En revisión por el administrador.
📩 Recibirás tu código una vez aprobado.
""", parse_mode='Markdown')
    
    # Guardar en BD
    conn = get_db_connection()
    if conn:
        try:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("""INSERT INTO pagos_pendientes 
                             (user_id, first_name, dias_plan, precio, comprobante_file_id, estado, datos_ocr)
                             VALUES (%s, %s, %s, %s, %s, 'pendiente', %s)
                             RETURNING id""",
                          (user.id, user.first_name, dias, precio, photo.file_id, json.dumps(datos_ocr)))
                pago_id = c.fetchone()['id']
            else:
                c.execute("""INSERT INTO pagos_pendientes 
                             (user_id, first_name, dias_plan, precio, comprobante_file_id, estado, datos_ocr)
                             VALUES (?, ?, ?, ?, ?, 'pendiente', ?)""",
                          (user.id, user.first_name, dias, precio, photo.file_id, json.dumps(datos_ocr)))
                pago_id = c.lastrowid
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error guardando pago: {e}")
            pago_id = 0
            if conn:
                conn.close()
    else:
        pago_id = 0
    
    # Notificar admin
    precios = obtener_precios()
    nombre_plan = next((p[2] for p in precios if p[0] == dias), "Plan")
    
    keyboard = [
        [InlineKeyboardButton("✅ Aprobar", callback_data=f"aprobar_{pago_id}")],
        [InlineKeyboardButton("❌ Rechazar", callback_data=f"rechazar_{pago_id}")]
    ]
    
    ocr_info = ""
    if datos_ocr.get("analizado"):
        ocr_info = "\n\n🔍 **Análisis OCR:**"
        if datos_ocr.get("es_comprobante") is not None:
            ocr_info += f"\n• Comprobante: {'✅' if datos_ocr.get('es_comprobante') else '❌'}"
        if datos_ocr.get("monto_detectado"):
            ocr_info += f"\n• Monto: ${datos_ocr.get('monto_detectado')}"
        if datos_ocr.get("cuenta_coincide") is not None:
            ocr_info += f"\n• Cuenta: {'✅' if datos_ocr.get('cuenta_coincide') else '❌'}"
    
    try:
        await context.bot.send_photo(
            chat_id=OWNER_ID,
            photo=photo.file_id,
            caption=f"""💳 **PAGO #{pago_id}**

👤 {user.first_name} (@{user.username or 'N/A'})
🆔 ID: `{user.id}`
💎 {nombre_plan} ({dias} días)
💰 {formato_clp(precio)}{ocr_info}""",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error notificando admin: {e}")
    
    del context.user_data['plan_seleccionado']
    del context.user_data['precio']


# ==================== HANDLER MENCIONES ====================

async def responder_mencion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Responde cuando mencionan al bot"""
    if not update.message or not update.message.text:
        return
    
    mensaje = update.message.text
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    try:
        bot_username = context.bot.username.lower()
    except:
        bot_username = BOT_USERNAME.lower()
    
    menciones_validas = [f"@{bot_username}", "@cofradia_premium_bot", "@cofradiapremiumbot"]
    
    if not any(m.lower() in mensaje.lower() for m in menciones_validas):
        return
    
    if not verificar_suscripcion_activa(user_id):
        await update.message.reply_text(
            "❌ Necesitas suscripción activa.\n📝 Usa /registrarse en @Cofradia_de_Networking"
        )
        return
    
    pregunta = re.sub(r'@\w+', '', mensaje).strip()
    
    if not pregunta:
        await update.message.reply_text(
            f"💡 Mencióname con tu pregunta:\n`@{bot_username} ¿tu pregunta?`",
            parse_mode='Markdown'
        )
        return
    
    if not ia_disponible:
        await update.message.reply_text("❌ IA no disponible. Intenta más tarde.")
        return
    
    msg = await update.message.reply_text("🧠 Procesando...")
    
    try:
        prompt = f"""Usuario {user_name} pregunta: "{pregunta}"

Responde de forma útil, concisa y profesional.
Máximo 3 párrafos."""

        respuesta = llamar_groq(prompt, max_tokens=800, temperature=0.7)
        
        await msg.delete()
        
        if respuesta:
            await enviar_mensaje_largo(update, respuesta)
            registrar_servicio_usado(user_id, 'ia_mencion')
        else:
            await update.message.reply_text("❌ No pude generar respuesta. Intenta de nuevo.")
            
    except Exception as e:
        logger.error(f"Error en mención: {e}")
        try:
            await msg.delete()
        except:
            pass
        await update.message.reply_text("❌ Error procesando tu pregunta.")


async def guardar_mensaje_grupo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Guarda mensajes del grupo"""
    if not update.message or not update.message.text:
        return
    if es_chat_privado(update):
        return
    
    user = update.message.from_user
    topic_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
    
    guardar_mensaje(
        user.id,
        user.username or "sin_username",
        user.first_name or "Anónimo",
        update.message.text,
        topic_id
    )


# ==================== COMANDOS ADMIN ====================

async def cobros_admin_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /cobros_admin - Panel admin"""
    if update.message.from_user.id != OWNER_ID:
        return
    
    await update.message.reply_text("""
👑 **PANEL ADMIN**

💰 **COBROS:**
/generar_codigo - Crear código
/precios - Ver precios
/pagos_pendientes - Ver pagos

📅 **VENCIMIENTOS:**
/vencimientos - Próximos
/vencimientos_mes [1-12] - Por mes
""", parse_mode='Markdown')


async def generar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /generar_codigo - Admin"""
    if update.message.from_user.id != OWNER_ID:
        return
    
    precios = obtener_precios()
    keyboard = [
        [InlineKeyboardButton(f"{nombre} ({dias}d) - {formato_clp(precio)}", callback_data=f"gencodigo_{dias}")]
        for dias, precio, nombre in precios
    ]
    
    await update.message.reply_text(
        "👑 **GENERAR CÓDIGO**\n\nSelecciona el plan:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


# ==================== MAIN ====================

def main():
    """Función principal"""
    logger.info("🚀 Iniciando Bot Cofradía Premium...")
    logger.info(f"📊 Groq IA: {'✅' if ia_disponible else '❌'}")
    logger.info(f"📷 Gemini OCR: {'✅' if gemini_disponible else '❌'}")
    logger.info(f"💼 JSearch (empleos reales): {'✅' if jsearch_disponible else '❌'}")
    logger.info(f"🗄️ Base de datos: {'Supabase' if DATABASE_URL else 'SQLite local'}")
    
    # Inicializar BD
    if not init_db():
        logger.error("❌ No se pudo inicializar la base de datos")
        return
    
    # Keep-alive para Render
    keepalive_thread = threading.Thread(target=run_keepalive_server, daemon=True)
    keepalive_thread.start()
    
    if os.environ.get('RENDER_EXTERNAL_URL'):
        ping_thread = threading.Thread(target=auto_ping, daemon=True)
        ping_thread.start()
        logger.info("🏓 Auto-ping activado")
    
    if not TOKEN_BOT:
        logger.error("❌ TOKEN_BOT no configurado")
        return
    
    # Crear aplicación
    application = Application.builder().token(TOKEN_BOT).build()
    
    # Configurar comandos
    async def setup_commands(app):
        commands = [
            BotCommand("start", "Iniciar bot"),
            BotCommand("ayuda", "Ver comandos"),
            BotCommand("registrarse", "Activar cuenta"),
            BotCommand("mi_cuenta", "Ver suscripción"),
            BotCommand("buscar", "Buscar en historial"),
            BotCommand("buscar_ia", "Búsqueda con IA"),
            BotCommand("graficos", "Ver gráficos"),
            BotCommand("empleo", "Buscar empleos"),
            BotCommand("renovar", "Renovar plan"),
            BotCommand("activar", "Usar código"),
        ]
        try:
            await app.bot.set_my_commands(commands)
            
            if COFRADIA_GROUP_ID:
                from telegram import BotCommandScopeChat
                comandos_grupo = [
                    BotCommand("registrarse", "Activar cuenta"),
                    BotCommand("buscar", "Buscar"),
                    BotCommand("buscar_ia", "Búsqueda IA"),
                    BotCommand("graficos", "Ver gráficos"),
                    BotCommand("empleo", "Buscar empleos"),
                    BotCommand("ayuda", "Ver comandos"),
                ]
                try:
                    await app.bot.set_my_commands(comandos_grupo, scope=BotCommandScopeChat(chat_id=COFRADIA_GROUP_ID))
                    await app.bot.set_chat_menu_button(chat_id=COFRADIA_GROUP_ID, menu_button=MenuButtonCommands())
                except Exception as e:
                    logger.warning(f"No se pudo configurar menú en grupo: {e}")
            
            logger.info("✅ Comandos configurados")
        except Exception as e:
            logger.warning(f"Error configurando comandos: {e}")
    
    application.post_init = setup_commands
    
    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ayuda", ayuda))
    application.add_handler(CommandHandler("registrarse", registrarse_comando))
    application.add_handler(CommandHandler("mi_cuenta", mi_cuenta_comando))
    application.add_handler(CommandHandler("renovar", renovar_comando))
    application.add_handler(CommandHandler("activar", activar_codigo_comando))
    application.add_handler(CommandHandler("buscar", buscar_comando))
    application.add_handler(CommandHandler("buscar_ia", buscar_ia_comando))
    application.add_handler(CommandHandler("graficos", graficos_comando))
    application.add_handler(CommandHandler("empleo", empleo_comando))
    application.add_handler(CommandHandler("cobros_admin", cobros_admin_comando))
    application.add_handler(CommandHandler("generar_codigo", generar_codigo_comando))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(callback_plan, pattern='^plan_'))
    application.add_handler(CallbackQueryHandler(callback_generar_codigo, pattern='^gencodigo_'))
    application.add_handler(CallbackQueryHandler(callback_aprobar_rechazar, pattern='^(aprobar|rechazar)_'))
    
    # Mensajes
    application.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, recibir_comprobante))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'@'), responder_mencion))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS & ~filters.COMMAND, guardar_mensaje_grupo))
    
    logger.info("✅ Bot iniciado!")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == '__main__':
    main()
