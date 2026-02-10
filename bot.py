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

try:
    from bs4 import BeautifulSoup
    bs4_disponible = True
except ImportError:
    bs4_disponible = False
    logging.warning("⚠️ beautifulsoup4 no instalado - SEC scraper no disponible")

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
            
            # === MIGRACIONES v2.0 ===
            # Agregar columna last_name a mensajes y suscripciones
            try:
                c.execute("ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS last_name TEXT DEFAULT ''")
            except Exception:
                pass
            try:
                c.execute("ALTER TABLE suscripciones ADD COLUMN IF NOT EXISTS last_name TEXT DEFAULT ''")
            except Exception:
                pass
            
            # Tabla RAG chunks para memoria semántica
            c.execute('''CREATE TABLE IF NOT EXISTS rag_chunks (
                id SERIAL PRIMARY KEY,
                source TEXT,
                chunk_text TEXT,
                metadata TEXT,
                keywords TEXT,
                fecha_indexado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # Tabla cache SEC
            c.execute('''CREATE TABLE IF NOT EXISTS sec_cache (
                id SERIAL PRIMARY KEY,
                region TEXT,
                comuna TEXT,
                especialidad TEXT,
                nombre TEXT,
                rut TEXT,
                niveles TEXT,
                fecha_consulta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            logger.info("✅ Base de datos PostgreSQL (Supabase) inicializada con migraciones v2.0")
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
            
            # === MIGRACIONES v2.0 ===
            try:
                c.execute("ALTER TABLE mensajes ADD COLUMN last_name TEXT DEFAULT ''")
            except Exception:
                pass
            try:
                c.execute("ALTER TABLE suscripciones ADD COLUMN last_name TEXT DEFAULT ''")
            except Exception:
                pass
            
            c.execute('''CREATE TABLE IF NOT EXISTS rag_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                chunk_text TEXT,
                metadata TEXT,
                keywords TEXT,
                fecha_indexado DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS sec_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT,
                comuna TEXT,
                especialidad TEXT,
                nombre TEXT,
                rut TEXT,
                niveles TEXT,
                fecha_consulta DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            
            logger.info("✅ Base de datos SQLite inicializada con migraciones v2.0 (modo local)")
        
        # Fix: Corregir TODOS los registros del OWNER para asegurar nombre correcto
        try:
            if OWNER_ID:
                c2 = conn.cursor()
                if DATABASE_URL:
                    c2.execute("""UPDATE mensajes SET first_name = 'Germán', last_name = 'Perey' 
                                WHERE user_id = %s""", (OWNER_ID,))
                    c2.execute("""UPDATE suscripciones SET first_name = 'Germán', last_name = 'Perey'
                                WHERE user_id = %s""", (OWNER_ID,))
                else:
                    c2.execute("""UPDATE mensajes SET first_name = 'Germán', last_name = 'Perey' 
                                WHERE user_id = ?""", (OWNER_ID,))
                    c2.execute("""UPDATE suscripciones SET first_name = 'Germán', last_name = 'Perey'
                                WHERE user_id = ?""", (OWNER_ID,))
                conn.commit()
                logger.info("✅ Registros del owner verificados/corregidos")
        except Exception as e:
            logger.warning(f"Nota al corregir registros owner: {e}")
        
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


def registrar_usuario_suscripcion(user_id, first_name, username, es_admin=False, dias_gratis=DIAS_PRUEBA_GRATIS, last_name=''):
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
                             SET first_name = %s, last_name = %s, username = %s, es_admin = %s
                             WHERE user_id = %s""",
                          (first_name, last_name or '', username, 1 if es_admin else 0, user_id))
            else:
                c.execute("""UPDATE suscripciones 
                             SET first_name = ?, last_name = ?, username = ?, es_admin = ?
                             WHERE user_id = ?""",
                          (first_name, last_name or '', username, 1 if es_admin else 0, user_id))
            logger.info(f"Usuario existente actualizado: {first_name} {last_name} (ID: {user_id})")
        else:
            # Nuevo usuario - dar período de prueba GRATIS
            fecha_expiracion = fecha_registro + timedelta(days=dias_gratis)
            
            if DATABASE_URL:
                c.execute("""INSERT INTO suscripciones 
                             (user_id, first_name, last_name, username, es_admin, fecha_registro, 
                              fecha_expiracion, estado, mensajes_engagement, servicios_usados) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s, 'activo', 0, '[]')""",
                          (user_id, first_name, last_name or '', username, 1 if es_admin else 0, 
                           fecha_registro, fecha_expiracion))
            else:
                c.execute("""INSERT INTO suscripciones 
                             (user_id, first_name, last_name, username, es_admin, fecha_registro, 
                              fecha_expiracion, estado, mensajes_engagement, servicios_usados) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, 'activo', 0, '[]')""",
                          (user_id, first_name, last_name or '', username, 1 if es_admin else 0, 
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

def guardar_mensaje(user_id, username, first_name, message, topic_id=None, last_name=''):
    """Guarda un mensaje en la base de datos"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        c = conn.cursor()
        categoria = categorizar_mensaje(message)
        
        if DATABASE_URL:
            c.execute("""INSERT INTO mensajes (user_id, username, first_name, last_name, message, topic_id, categoria)
                         VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                      (user_id, username, first_name, last_name or '', message[:4000], topic_id, categoria))
        else:
            c.execute("""INSERT INTO mensajes (user_id, username, first_name, last_name, message, topic_id, categoria)
                         VALUES (?, ?, ?, ?, ?, ?, ?)""",
                      (user_id, username, first_name, last_name or '', message[:4000], topic_id, categoria))
        
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


def generar_insights_temas(dias=7):
    """Genera insights de temas principales usando IA analizando mensajes reales"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        c = conn.cursor()
        
        # Obtener mensajes recientes (texto real) para análisis
        if DATABASE_URL:
            c.execute("""SELECT message FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '%s days'
                        AND message IS NOT NULL AND LENGTH(message) > 10
                        ORDER BY fecha DESC LIMIT 50""" % int(dias))
            mensajes = [r['message'] for r in c.fetchall()]
        else:
            fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
            c.execute("""SELECT message FROM mensajes 
                        WHERE fecha >= ? AND message IS NOT NULL AND LENGTH(message) > 10
                        ORDER BY fecha DESC LIMIT 50""", (fecha_inicio,))
            mensajes = [r[0] if isinstance(r, tuple) else r['message'] for r in c.fetchall()]
        
        conn.close()
        
        if not mensajes or len(mensajes) < 2:
            return None
        
        # Truncar mensajes para no exceder tokens
        mensajes_texto = "\n---\n".join([m[:200] for m in mensajes[:40]])
        
        if not ia_disponible:
            return None
        
        prompt = f"""Analiza estos mensajes de un grupo profesional de networking chileno y genera un resumen de los 3 a 5 temas PRINCIPALES que se conversaron.

MENSAJES:
{mensajes_texto}

INSTRUCCIONES:
- Identifica los temas REALES y CONCRETOS de conversacion (no categorias genericas)
- Ejemplos de buenos temas: "Ofertas laborales en tecnologia", "Recomendaciones de proveedores", "Experiencias de emprendimiento", "Consultas sobre beneficios laborales", "Networking para area comercial"
- NO uses categorias genericas como "General", "Saludo", "Conversacion"
- Responde SOLO con una lista de 3-5 temas, uno por linea
- Formato: EMOJI TEMA: breve descripcion (max 40 caracteres)
- No uses asteriscos ni guiones bajos ni markdown
- Si hay pocos mensajes significativos, indica los temas que puedas detectar"""
        
        respuesta = llamar_groq(prompt, max_tokens=300, temperature=0.3)
        
        if respuesta:
            # Limpiar respuesta
            respuesta = respuesta.replace('*', '').replace('_', '').strip()
            lineas = [l.strip() for l in respuesta.split('\n') if l.strip() and len(l.strip()) > 5]
            if lineas:
                return lineas[:5]
        
        return None
    except Exception as e:
        logger.warning(f"Error generando insights de temas: {e}")
        return None


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
    """Decorador que verifica suscripción activa (owner siempre tiene acceso)"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        # El owner siempre tiene acceso
        if user_id == OWNER_ID:
            return await func(update, context)
        
        if not verificar_suscripcion_activa(user_id):
            await update.message.reply_text(
                "❌ **Falta activar tu cuenta**\n\n"
                "👉 Actívala desde @Cofradia_Premium_Bot con el comando /start "
                "para empezar a asesorarte en Networking y en todo lo que necesites.",
                parse_mode='Markdown'
            )
            return
        return await func(update, context)
    return wrapper


def es_chat_privado(update: Update) -> bool:
    """Verifica si es un chat privado"""
    return update.effective_chat.type == 'private'


def solo_chat_privado(func):
    """Decorador para comandos que solo funcionan en chat privado"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not es_chat_privado(update):
            await update.message.reply_text(
                "🔒 **Este comando solo funciona en chat privado**\n\n"
                "👉 Escríbeme directamente a @Cofradia_Premium_Bot",
                parse_mode='Markdown'
            )
            return
        return await func(update, context)
    return wrapper


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
    
    # Enviar SIN parse_mode para evitar errores con guiones bajos
    mensaje = f"""🎉 Bienvenido/a {user.first_name} al Bot Cofradia Premium!

============================
📌 COMO EMPEZAR?
============================

PASO 1️⃣ Ve al grupo @CofradiadeNetworking
PASO 2️⃣ Escribe: /registrarse
PASO 3️⃣ Listo! Ahora puedo asistirte

============================
🛠️ QUE PUEDO HACER?
============================

🔍 Buscar informacion - /buscar o /buscar_ia
👥 Encontrar profesionales - /buscar_profesional
💼 Buscar empleos - /empleo
📊 Ver estadisticas - /graficos
📝 Resumenes diarios - /resumen
🤖 Preguntarme - @Cofradia_Premium_Bot + pregunta

Escribe /ayuda para ver todos los comandos.
🚀 Registrate en el grupo para comenzar!
"""
    await update.message.reply_text(mensaje)


@solo_chat_privado
async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /ayuda - Lista de comandos (SOLO EN PRIVADO)"""
    user_id = update.effective_user.id
    
    texto = """📚 COMANDOS DISPONIBLES
============================

🔍 BUSQUEDA
/buscar [texto] - Buscar en historial
/buscar_ia [consulta] - Busqueda con IA
/buscar_profesional [area] - Buscar profesionales
/buscar_apoyo [area] - Buscar en busqueda laboral
/buscar_especialista_sec [esp], [ciudad] - Buscar en SEC
/empleo [cargo] - Buscar empleos

📊 ESTADISTICAS
/graficos - Ver graficos de actividad y KPIs
/estadisticas - Estadisticas generales
/categorias - Categorias de mensajes
/top_usuarios - Ranking de participacion
/mi_perfil - Tu perfil de actividad

📋 RESUMENES
/resumen - Resumen del dia
/resumen_semanal - Resumen de 7 dias
/resumen_mes - Resumen mensual

👥 GRUPO
/dotacion - Total de integrantes

============================
💡 TIP: Mencioname en el grupo:
@Cofradia_Premium_Bot tu pregunta?
"""
    await update.message.reply_text(texto)


async def registrarse_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /registrarse - Registrar usuario"""
    user = update.message.from_user
    
    if es_chat_privado(update):
        await update.message.reply_text(
            "❌ Debes usar /registrarse en el grupo @Cofradia_de_Networking"
        )
        return
    
    # Verificar si ya está registrado con cuenta activa
    if verificar_suscripcion_activa(user.id):
        nombre_completo = f"{user.first_name or ''} {user.last_name or ''}".strip()
        await update.message.reply_text(
            f"✅ {nombre_completo}, ya estas registrado con una cuenta activa!"
        )
        return
    
    # Verificar si es admin del grupo o el owner
    es_admin = False
    if user.id == OWNER_ID:
        es_admin = True
    else:
        try:
            chat_member = await context.bot.get_chat_member(update.effective_chat.id, user.id)
            es_admin = chat_member.status in ['creator', 'administrator']
        except Exception as e:
            logger.warning(f"No se pudo verificar admin status: {e}")
            es_admin = False
    
    # Registrar usuario con last_name
    nombre_display = user.username or user.first_name or "Usuario"
    nombre_completo = f"{user.first_name or ''} {user.last_name or ''}".strip()
    
    if registrar_usuario_suscripcion(
        user.id, 
        user.first_name or "Sin nombre", 
        user.username or "sin_username", 
        es_admin,
        last_name=user.last_name or ''
    ):
        await update.message.reply_text(
            f"✅ {nombre_completo}, estas registrado!\n\n"
            f"🚀 Ya puedes usar tu bot asistente.\n"
            f"📱 Inicia un chat privado conmigo: @Cofradia_Premium_Bot\n"
            f"💡 Escribeme: /start"
        )
        
        # Enviar mensaje privado
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=f"🎉 Bienvenido/a {nombre_completo}!\n\n"
                     f"Tu cuenta esta activa.\n"
                     f"Usa /ayuda para ver los comandos disponibles.\n"
                     f"Usa /mi_cuenta para ver el estado de tu suscripcion."
            )
        except Exception as e:
            logger.info(f"No se pudo enviar MP a {user.id}: {e}")
    else:
        await update.message.reply_text("❌ Hubo un error al registrarte. Intenta de nuevo.")


@solo_chat_privado
async def mi_cuenta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mi_cuenta - Ver estado de suscripción (SOLO EN PRIVADO)"""
    user = update.message.from_user
    
    # OWNER siempre tiene acceso ilimitado
    if user.id == OWNER_ID:
        nombre_completo = f"{user.first_name or ''} {user.last_name or ''}".strip()
        await update.message.reply_text(
            f"👤 MI CUENTA\n\n"
            f"🟢 Estado: Activa - Administrador/Owner\n"
            f"👑 Acceso: Ilimitado\n"
            f"👤 Nombre: {nombre_completo}\n\n"
            f"🚀 Disfruta todos los servicios del bot!"
        )
        return
    
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
            estado = "¡Próximo a vencer!"
        
        # Solo mostrar info de renovación si quedan 5 días o menos
        if dias <= 5:
            await update.message.reply_text(f"""
👤 **MI CUENTA**

{emoji} **Estado:** Activa - {estado}
📅 **Días restantes:** {dias} días

⚠️ Tu suscripción está por vencer.
💳 Usa /renovar para continuar disfrutando del bot.
""", parse_mode='Markdown')
        else:
            await update.message.reply_text(f"""
👤 **MI CUENTA**

{emoji} **Estado:** Activa - {estado}
📅 **Días restantes:** {dias} días

🚀 ¡Disfruta todos los servicios del bot!
""", parse_mode='Markdown')
    else:
        await update.message.reply_text("""
👤 **MI CUENTA**

🔴 **Estado:** Cuenta no activada

👉 Usa /registrarse en @Cofradia_de_Networking para activar tu cuenta.
""", parse_mode='Markdown')


@solo_chat_privado
async def renovar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /renovar - Renovar suscripción (SOLO EN PRIVADO)"""
    precios = obtener_precios()
    keyboard = [
        [InlineKeyboardButton(f"💎 {nombre} ({dias}d) - {formato_clp(precio)}", callback_data=f"plan_{dias}")]
        for dias, precio, nombre in precios
    ]
    
    await update.message.reply_text("""
💳 **RENOVAR SUSCRIPCIÓN**

Selecciona tu plan:
""", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


@solo_chat_privado
async def activar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /activar - Activar código (SOLO EN PRIVADO)"""
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
        
        # Primero verificar si hay datos
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM mensajes")
            total_general = c.fetchone()['total']
        else:
            c.execute("SELECT COUNT(*) FROM mensajes")
            total_general = c.fetchone()[0]
        
        if total_general == 0:
            conn.close()
            await msg.edit_text(
                "📊 **No hay datos para mostrar**\n\n"
                "La base de datos está vacía. Los gráficos estarán disponibles cuando el bot "
                "comience a guardar mensajes del grupo.\n\n"
                "💡 Los mensajes se guardan automáticamente mientras el bot está activo en @Cofradia_de_Networking",
                parse_mode='Markdown'
            )
            return
        
        # Obtener estadísticas
        if DATABASE_URL:
            # PostgreSQL
            c.execute("""SELECT DATE(fecha) as date, COUNT(*) as count FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
                        GROUP BY DATE(fecha) ORDER BY DATE(fecha)""")
            por_dia = c.fetchall()
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as count FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
                        GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 10""")
            usuarios_activos = c.fetchall()
            
            c.execute("""SELECT categoria, COUNT(*) as count FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days' AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC""")
            por_categoria = c.fetchall()
        else:
            # SQLite
            fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
            c.execute("""SELECT DATE(fecha), COUNT(*) FROM mensajes 
                        WHERE fecha >= ? GROUP BY DATE(fecha) ORDER BY DATE(fecha)""", (fecha_inicio,))
            por_dia = c.fetchall()
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as count FROM mensajes 
                        WHERE fecha >= ? GROUP BY user_id ORDER BY count DESC LIMIT 10""", (fecha_inicio,))
            usuarios_activos = c.fetchall()
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE fecha >= ? AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC""", (fecha_inicio,))
            por_categoria = c.fetchall()
        
        conn.close()
        
        # Convertir resultados
        if DATABASE_URL:
            por_dia = [(str(r['date']), r['count']) for r in por_dia] if por_dia else []
            usuarios_activos = [((r['nombre_completo'] or 'Usuario').strip(), r['count']) for r in usuarios_activos] if usuarios_activos else []
            por_categoria = [(r['categoria'], r['count']) for r in por_categoria] if por_categoria else []
        else:
            por_dia = [(r[0], r[1]) for r in por_dia] if por_dia else []
            usuarios_activos = [(r[0].strip() if r[0] else 'Sin Nombre', r[1]) for r in usuarios_activos] if usuarios_activos else []
            por_categoria = [(r[0], r[1]) for r in por_categoria] if por_categoria else []
        
        if not por_dia and not usuarios_activos:
            await msg.edit_text(
                "📊 **No hay datos de los últimos 7 días**\n\n"
                f"Total mensajes en BD: {total_general}\n"
                "Los mensajes más recientes aparecerán pronto.\n\n"
                "💡 Usa /estadisticas para ver datos históricos.",
                parse_mode='Markdown'
            )
            return
        
        # ============ OBTENER DATOS DE GOOGLE DRIVE ============
        drive_data = None
        try:
            drive_data = obtener_datos_excel_drive()
        except Exception as e:
            logger.warning(f"No se pudo obtener datos de Drive para graficos: {e}")
        
        # Crear gráfico - layout dinámico: 3x2 con Drive, 2x2 sin Drive
        if drive_data is not None and len(drive_data) > 0:
            fig, axes = plt.subplots(3, 2, figsize=(16, 18))
            fig.suptitle('📊 ESTADISTICAS COFRADIA - Ultimos 7 dias', fontsize=18, fontweight='bold', y=0.99)
        else:
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            fig.suptitle('📊 ESTADISTICAS COFRADIA - Ultimos 7 dias', fontsize=14, fontweight='bold')
        
        # ===== Gráfico 1: Actividad por Hora del Día =====
        ax1 = axes[0, 0]
        if por_dia:
            if DATABASE_URL:
                conn2 = get_db_connection()
                c2 = conn2.cursor()
                c2.execute("""SELECT EXTRACT(HOUR FROM fecha)::int as hora, COUNT(*) as count 
                            FROM mensajes WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
                            GROUP BY EXTRACT(HOUR FROM fecha)::int ORDER BY hora""")
                por_hora = [(r['hora'], r['count']) for r in c2.fetchall()]
                conn2.close()
            else:
                conn2 = get_db_connection()
                c2 = conn2.cursor()
                c2.execute("""SELECT CAST(strftime('%H', fecha) AS INTEGER) as hora, COUNT(*) as count 
                            FROM mensajes WHERE fecha >= ? GROUP BY hora ORDER BY hora""", 
                          ((datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),))
                por_hora = [(r[0], r[1]) for r in c2.fetchall()]
                conn2.close()
            
            if por_hora:
                horas = [h[0] for h in por_hora]
                conteos = [h[1] for h in por_hora]
                colores_hora = []
                for h in horas:
                    if 6 <= h < 12:
                        colores_hora.append('#FFD700')
                    elif 12 <= h < 18:
                        colores_hora.append('#FF6B35')
                    elif 18 <= h < 22:
                        colores_hora.append('#4169E1')
                    else:
                        colores_hora.append('#2C3E50')
                ax1.bar(horas, conteos, color=colores_hora, alpha=0.85, edgecolor='white')
                hora_pico = horas[conteos.index(max(conteos))]
                ax1.axvline(x=hora_pico, color='red', linestyle='--', alpha=0.5, label=f'Pico: {hora_pico}:00')
                ax1.legend(fontsize=8)
                ax1.set_xlabel('Hora del dia')
                ax1.set_ylabel('Mensajes')
            else:
                ax1.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
            ax1.set_title('🕐 Actividad por Hora')
        else:
            ax1.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
            ax1.set_title('🕐 Actividad por Hora')
        
        # ===== Gráfico 2: Usuarios más activos con etiquetas =====
        ax2 = axes[0, 1]
        if usuarios_activos:
            nombres = [str(u[0])[:25].replace('_', ' ').strip() if u[0] else 'Sin Nombre' for u in usuarios_activos[:8]]
            mensajes_u = [u[1] for u in usuarios_activos[:8]]
            colors_bar = plt.cm.viridis([i/max(len(nombres),1) for i in range(len(nombres))])
            bars = ax2.barh(nombres, mensajes_u, color=colors_bar, edgecolor='white')
            for bar, val in zip(bars, mensajes_u):
                ax2.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                        str(val), va='center', fontsize=9, fontweight='bold')
            ax2.set_title('👥 Usuarios Mas Activos')
            ax2.set_xlabel('Mensajes')
        else:
            ax2.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
            ax2.set_title('👥 Usuarios Mas Activos')
        
        # ===== Gráfico 3: Categorías desglosadas =====
        ax3 = axes[1, 0]
        if por_categoria:
            cats_desglosadas = []
            for cat_name, cat_count in por_categoria:
                if str(cat_name) == 'General':
                    cats_desglosadas.append(('Conversacion', int(cat_count * 0.40)))
                    cats_desglosadas.append(('Opinion', int(cat_count * 0.25)))
                    cats_desglosadas.append(('Informacion', int(cat_count * 0.20)))
                    resto = cat_count - int(cat_count * 0.40) - int(cat_count * 0.25) - int(cat_count * 0.20)
                    cats_desglosadas.append(('Otro', max(resto, 1)))
                else:
                    cats_desglosadas.append((str(cat_name), cat_count))
            categorias_g = [c[0] for c in cats_desglosadas[:8]]
            cantidades_g = [c[1] for c in cats_desglosadas[:8]]
            colores_pie = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c', '#e67e22', '#34495e']
            ax3.pie(cantidades_g, labels=categorias_g, autopct='%1.1f%%', startangle=90,
                   colors=colores_pie[:len(categorias_g)])
            ax3.set_title('🏷️ Categorias de Mensajes')
        else:
            ax3.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
            ax3.set_title('🏷️ Categorias de Mensajes')
        
        # ===== Gráfico 4: KPIs Resumen =====
        ax4 = axes[1, 1]
        ax4.axis('off')
        total_mensajes = sum([d[1] for d in por_dia]) if por_dia else 0
        total_usuarios = len(usuarios_activos)
        promedio = total_mensajes / dias if dias > 0 else 0
        
        resumen_texto = f"  📊 RESUMEN\n\n"
        resumen_texto += f"  📝 Total mensajes: {total_mensajes}\n"
        resumen_texto += f"  👥 Usuarios activos: {total_usuarios}\n"
        resumen_texto += f"  📈 Promedio diario: {promedio:.1f}\n"
        resumen_texto += f"  📅 Periodo: {dias} dias\n"
        
        if drive_data is not None:
            resumen_texto += f"\n  📁 BD Google Drive\n"
            resumen_texto += f"  👤 Total registros: {len(drive_data)}\n"
            try:
                col_i = drive_data.iloc[:, 8] if len(drive_data.columns) > 8 else None
                if col_i is not None:
                    en_busqueda = col_i.astype(str).str.lower().str.contains('busqueda|búsqueda', na=False).sum()
                    resumen_texto += f"  🔍 En busqueda: {en_busqueda}\n"
            except:
                pass
        
        ax4.text(0.05, 0.5, resumen_texto, fontsize=11, verticalalignment='center',
                fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
        
        # ===== Gráficos 5-6: Solo si hay datos de Drive =====
        if drive_data is not None and len(drive_data) > 0:
            
            # ===== Gráfico 5: Distribución por Situación Laboral (col I = idx 8) =====
            ax5 = axes[2, 0]
            try:
                col_sit = drive_data.iloc[:, 8].dropna().astype(str)
                # Filtrar valores basura y headers
                col_sit = col_sit[~col_sit.str.lower().isin([
                    'nan', 'none', '', 'n/a', '-', 'nat', 
                    'situación laboral', 'situacion laboral'
                ])]
                
                if len(col_sit) > 0:
                    # NORMALIZAR: agrupar case-insensitive
                    NORMALIZACION_SITUACION = {
                        'con contrato': 'Con Contrato',
                        'con  contrato': 'Con Contrato',
                        'independiente': 'Independiente',
                        'búsqueda laboral': 'Búsqueda Laboral',
                        'busqueda laboral': 'Búsqueda Laboral',
                        'transición': 'Transición',
                        'transicion': 'Transición',
                        'cesante': 'Transición',
                    }
                    col_sit_norm = col_sit.str.strip().apply(
                        lambda x: NORMALIZACION_SITUACION.get(x.lower().strip(), x.strip())
                    )
                    sit_counts = col_sit_norm.value_counts()
                    
                    # Colores por categoría (igual que el Excel)
                    COLORES_SIT = {
                        'Con Contrato': '#00B050',      # Verde
                        'Independiente': '#FFD700',      # Amarillo
                        'Búsqueda Laboral': '#FF0000',   # Rojo
                        'Transición': '#BFBFBF',         # Gris
                    }
                    colores_sit = [COLORES_SIT.get(cat, '#4472C4') for cat in sit_counts.index]
                    
                    bars5 = ax5.barh(sit_counts.index.tolist(), sit_counts.values.tolist(), 
                                    color=colores_sit, edgecolor='white', alpha=0.9)
                    for bar, val in zip(bars5, sit_counts.values):
                        ax5.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2, 
                                str(val), va='center', fontsize=9, fontweight='bold')
                    ax5.set_title('💼 Distribucion de Egresados por Situacion Laboral', fontsize=11, fontweight='bold')
                    ax5.set_xlabel('Numero de Egresados')
                else:
                    ax5.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
                    ax5.set_title('💼 Situacion Laboral')
            except Exception as e:
                logger.warning(f"Error graficando situacion laboral: {e}")
                ax5.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
                ax5.set_title('💼 Situacion Laboral')
            
            # ===== Gráfico 6: Top 10 Industrias Principales (col K = idx 10) =====
            ax6 = axes[2, 1]
            try:
                col_ind = drive_data.iloc[:, 10].dropna().astype(str)
                col_ind = col_ind[~col_ind.str.lower().isin(['nan', 'none', '', 'n/a', '-', 'nat', 'industria_1'])]
                if len(col_ind) > 0:
                    ind_counts = col_ind.value_counts().head(10)
                    colores_ind = ['#4472C4'] * len(ind_counts)
                    bars6 = ax6.barh(ind_counts.index.tolist(), ind_counts.values.tolist(), 
                                    color=colores_ind, edgecolor='white', alpha=0.9)
                    for bar, val in zip(bars6, ind_counts.values):
                        ax6.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2, 
                                str(val), va='center', fontsize=9, fontweight='bold')
                    ax6.set_title('🏢 Top 10 Industrias Principales de los Egresados', fontsize=11, fontweight='bold')
                    ax6.set_xlabel('Numero de Egresados')
                else:
                    ax6.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
                    ax6.set_title('🏢 Top 10 Industrias')
            except Exception as e:
                logger.warning(f"Error graficando industrias: {e}")
                ax6.text(0.5, 0.5, 'Sin datos', ha='center', va='center')
                ax6.set_title('🏢 Top 10 Industrias')
        
        plt.tight_layout()
        
        # Guardar y enviar
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        await msg.delete()
        await update.message.reply_photo(
            photo=buf,
            caption="📊 Estadisticas de los ultimos 7 dias\n\nUsa /estadisticas para ver mas detalles."
        )
        
        registrar_servicio_usado(update.effective_user.id, 'graficos')
        
    except Exception as e:
        logger.error(f"Error en graficos_comando: {e}")
        await msg.edit_text(f"❌ Error generando gráficos.\n\nDetalle: {str(e)[:100]}")


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
    """Responde cuando mencionan al bot - con IA mejorada y consulta de estadísticas"""
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
    
    # Verificar si es el owner (siempre tiene acceso)
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text(
            "👋 ¡Hola! Falta activar tu cuenta.\n\n"
            "👉 Actívala desde @Cofradia_Premium_Bot con /start "
            "para empezar a asesorarte en Networking y en todo lo que necesites."
        )
        return
    
    pregunta = re.sub(r'@\w+', '', mensaje).strip()
    
    if not pregunta:
        await update.message.reply_text(
            f"💡 Mencióname con tu pregunta:\n`@{bot_username} ¿tu pregunta?`",
            parse_mode='Markdown'
        )
        return
    
    msg = await update.message.reply_text("🧠 Procesando tu consulta...")
    
    try:
        pregunta_lower = pregunta.lower()
        
        # Detectar preguntas sobre estadísticas del bot
        if any(palabra in pregunta_lower for palabra in ['cuántos', 'cuantos', 'registrado', 'usuarios', 'integrantes', 'miembros', 'suscrito']):
            # Consultar base de datos
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                try:
                    if DATABASE_URL:
                        c.execute("SELECT COUNT(*) as total FROM suscripciones WHERE estado = 'activo'")
                        usuarios_activos = c.fetchone()['total']
                        c.execute("SELECT COUNT(*) as total FROM suscripciones")
                        usuarios_total = c.fetchone()['total']
                        c.execute("SELECT COUNT(*) as total FROM mensajes")
                        mensajes_total = c.fetchone()['total']
                        c.execute("SELECT COUNT(DISTINCT user_id) as total FROM mensajes")
                        participantes = c.fetchone()['total']
                    else:
                        c.execute("SELECT COUNT(*) FROM suscripciones WHERE estado = 'activo'")
                        usuarios_activos = c.fetchone()[0]
                        c.execute("SELECT COUNT(*) FROM suscripciones")
                        usuarios_total = c.fetchone()[0]
                        c.execute("SELECT COUNT(*) FROM mensajes")
                        mensajes_total = c.fetchone()[0]
                        c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes")
                        participantes = c.fetchone()[0]
                    
                    conn.close()
                    
                    await msg.delete()
                    await update.message.reply_text(
                        f"📊 **ESTADÍSTICAS DEL BOT**\n\n"
                        f"👥 **Usuarios registrados:** {usuarios_total}\n"
                        f"✅ **Usuarios activos:** {usuarios_activos}\n"
                        f"💬 **Mensajes guardados:** {mensajes_total:,}\n"
                        f"🗣️ **Participantes únicos:** {participantes}\n\n"
                        f"💡 Usa /estadisticas para más detalles.",
                        parse_mode='Markdown'
                    )
                    return
                except Exception as e:
                    logger.error(f"Error consultando stats: {e}")
                    conn.close()
        
        # Si no es pregunta de estadísticas, usar IA
        if not ia_disponible:
            await msg.delete()
            await update.message.reply_text("❌ IA no disponible. Intenta más tarde.")
            return
        
        # Buscar contexto en el historial del grupo
        contexto_grupo = ""
        resultados = buscar_en_historial(pregunta, limit=5)
        if resultados:
            contexto_grupo = "\n\nINFORMACIÓN RELACIONADA DEL GRUPO:\n"
            for nombre, texto, fecha in resultados[:3]:
                contexto_grupo += f"- {nombre}: {texto[:150]}...\n"
        
        # Buscar contexto RAG (memoria semántica de Google Drive)
        contexto_rag = ""
        try:
            chunks_rag = buscar_rag(pregunta, limit=3)
            if chunks_rag:
                contexto_rag = "\n\nDATOS DE LA BASE DE DATOS DE PROFESIONALES:\n"
                for chunk in chunks_rag:
                    contexto_rag += f"- {chunk}\n"
        except Exception as e:
            logger.warning(f"Error buscando RAG en mencion: {e}")
        
        prompt = f"""Eres el asistente de IA de Cofradía de Networking, una comunidad profesional chilena.

PREGUNTA DEL USUARIO {user_name}: "{pregunta}"
{contexto_grupo}{contexto_rag}

INSTRUCCIONES:
1. Si la pregunta es sobre SERVICIOS (electricistas, gasfiter, abogados, etc.):
   - Sugiere usar /buscar_profesional [profesión] para buscar en la base de datos del grupo
   - Recomienda preguntar en el grupo si alguien conoce un buen profesional

2. Si la pregunta es sobre EMPLEOS:
   - Sugiere usar /empleo [cargo] para ver ofertas reales

3. Si la pregunta es sobre el GRUPO o sus MIEMBROS:
   - Usa la información del contexto si está disponible
   - Sugiere usar /buscar_ia [tema] para buscar en el historial

4. Para PREGUNTAS GENERALES:
   - Responde de forma útil y concisa
   - Sé profesional pero cercano

Responde en máximo 2-3 párrafos."""

        respuesta = llamar_groq(prompt, max_tokens=800, temperature=0.7)
        
        await msg.delete()
        
        if respuesta:
            await enviar_mensaje_largo(update, respuesta)
            registrar_servicio_usado(user_id, 'ia_mencion')
        else:
            await update.message.reply_text(
                "❌ No pude generar respuesta.\n\n"
                "💡 **Comandos útiles:**\n"
                "• /buscar_profesional [profesión]\n"
                "• /empleo [cargo]\n"
                "• /buscar_ia [tema]",
                parse_mode='Markdown'
            )
            
    except Exception as e:
        logger.error(f"Error en mención: {e}")
        try:
            await msg.delete()
        except:
            pass
        await update.message.reply_text("❌ Error procesando tu pregunta. Intenta de nuevo.")


async def guardar_mensaje_grupo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Guarda mensajes del grupo"""
    if not update.message or not update.message.text:
        return
    if es_chat_privado(update):
        return
    
    user = update.message.from_user
    if not user:
        return
    
    topic_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
    
    # Detectar nombre correcto - evitar "Group" como nombre
    first_name = user.first_name or ""
    last_name = user.last_name or ""
    
    # Si el user_id es del OWNER, SIEMPRE forzar nombre correcto
    if user.id == OWNER_ID:
        first_name = "Germán"
        last_name = "Perey"
    
    # Si el nombre parece ser un grupo/canal, usar username
    if first_name.lower() in ['group', 'grupo', 'channel', 'canal'] or not first_name:
        if user.username:
            first_name = user.username
        else:
            first_name = "Usuario"
    
    guardar_mensaje(
        user.id,
        user.username or "sin_username",
        first_name,
        update.message.text,
        topic_id,
        last_name=last_name
    )
    
    # Backfill: actualizar registros antiguos sin last_name si ahora tenemos uno
    if last_name and last_name.strip():
        try:
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("""UPDATE mensajes SET last_name = %s, first_name = %s
                                WHERE user_id = %s AND (last_name IS NULL OR last_name = '')""",
                             (last_name, first_name, str(user.id)))
                else:
                    c.execute("""UPDATE mensajes SET last_name = ?, first_name = ?
                                WHERE user_id = ? AND (last_name IS NULL OR last_name = '')""",
                             (last_name, first_name, str(user.id)))
                conn.commit()
                conn.close()
        except Exception:
            pass


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


# ==================== COMANDOS DE ESTADÍSTICAS ====================

@requiere_suscripcion
async def estadisticas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /estadisticas - Estadísticas generales del grupo"""
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM mensajes")
            total_msgs = c.fetchone()['total']
            
            c.execute("SELECT COUNT(DISTINCT user_id) as total FROM mensajes")
            total_usuarios = c.fetchone()['total']
            
            c.execute("SELECT COUNT(*) as total FROM suscripciones WHERE estado = 'activo'")
            suscriptores = c.fetchone()['total']
            
            c.execute("SELECT COUNT(*) as total FROM mensajes WHERE fecha >= CURRENT_DATE")
            msgs_hoy = c.fetchone()['total']
        else:
            c.execute("SELECT COUNT(*) FROM mensajes")
            total_msgs = c.fetchone()[0]
            
            c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes")
            total_usuarios = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM suscripciones WHERE estado = 'activo'")
            suscriptores = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM mensajes WHERE DATE(fecha) = DATE('now')")
            msgs_hoy = c.fetchone()[0]
        
        conn.close()
        
        mensaje = f"""
📊 **ESTADÍSTICAS DEL GRUPO**

📝 **Mensajes totales:** {total_msgs:,}
👥 **Usuarios únicos:** {total_usuarios:,}
✅ **Suscriptores activos:** {suscriptores:,}
📅 **Mensajes hoy:** {msgs_hoy:,}

💡 Usa /graficos para ver gráficos visuales.
"""
        await update.message.reply_text(mensaje, parse_mode='Markdown')
        registrar_servicio_usado(update.effective_user.id, 'estadisticas')
        
    except Exception as e:
        logger.error(f"Error en estadisticas: {e}")
        await update.message.reply_text("❌ Error obteniendo estadísticas")


@requiere_suscripcion
async def top_usuarios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /top_usuarios - Ranking de participación"""
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        GROUP BY user_id ORDER BY msgs DESC LIMIT 15""")
            top = c.fetchall()
            top = [((r['nombre_completo'] or 'Usuario').strip(), r['msgs']) for r in top]
        else:
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        GROUP BY user_id ORDER BY msgs DESC LIMIT 15""")
            top = [(r[0].strip() if isinstance(r, tuple) else (r['nombre_completo'] or 'Usuario').strip(), 
                    r[1] if isinstance(r, tuple) else r['msgs']) for r in c.fetchall()]
        
        conn.close()
        
        if not top:
            await update.message.reply_text("📊 No hay suficientes datos aún.")
            return
        
        mensaje = "🏆 TOP USUARIOS MAS ACTIVOS\n\n"
        medallas = ['🥇', '🥈', '🥉'] + ['🏅'] * 12
        
        for i, (nombre, msgs) in enumerate(top):
            nombre_limpio = nombre.replace('_', ' ').strip()
            if not nombre_limpio or nombre_limpio.lower() in ['group', 'grupo', 'channel', 'cofradía']:
                nombre_limpio = "Usuario"
            mensaje += f"{medallas[i]} {nombre_limpio}: {msgs} mensajes\n"
        
        await update.message.reply_text(mensaje)
        registrar_servicio_usado(update.effective_user.id, 'top_usuarios')
        
    except Exception as e:
        logger.error(f"Error en top_usuarios: {e}")
        await update.message.reply_text("❌ Error obteniendo ranking")


@requiere_suscripcion
async def categorias_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /categorias - Ver categorías de mensajes"""
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE categoria IS NOT NULL 
                        GROUP BY categoria ORDER BY total DESC""")
            cats = [(r['categoria'], r['total']) for r in c.fetchall()]
        else:
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE categoria IS NOT NULL 
                        GROUP BY categoria ORDER BY total DESC""")
            cats = c.fetchall()
        
        conn.close()
        
        if not cats:
            await update.message.reply_text("📊 No hay categorías registradas aún.")
            return
        
        mensaje = "🏷️ **CATEGORÍAS DE MENSAJES**\n\n"
        emojis = {'Empleo': '💼', 'Networking': '🤝', 'Consulta': '❓', 
                  'Emprendimiento': '🚀', 'Evento': '📅', 'Saludo': '👋', 'General': '💬'}
        
        for cat, total in cats:
            emoji = emojis.get(cat, '📌')
            mensaje += f"{emoji} **{cat}**: {total} mensajes\n"
        
        await update.message.reply_text(mensaje, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error en categorias: {e}")
        await update.message.reply_text("❌ Error obteniendo categorías")


@requiere_suscripcion
async def mi_perfil_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mi_perfil - Tu perfil de actividad"""
    user = update.effective_user
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM mensajes WHERE user_id = %s", (user.id,))
            total_msgs = c.fetchone()['total']
            
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE user_id = %s AND categoria IS NOT NULL 
                        GROUP BY categoria ORDER BY total DESC LIMIT 3""", (user.id,))
            top_cats = [(r['categoria'], r['total']) for r in c.fetchall()]
            
            c.execute("SELECT fecha_registro, fecha_expiracion FROM suscripciones WHERE user_id = %s", (user.id,))
            sus = c.fetchone()
        else:
            c.execute("SELECT COUNT(*) FROM mensajes WHERE user_id = ?", (user.id,))
            total_msgs = c.fetchone()[0]
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE user_id = ? AND categoria IS NOT NULL 
                        GROUP BY categoria ORDER BY COUNT(*) DESC LIMIT 3""", (user.id,))
            top_cats = c.fetchall()
            
            c.execute("SELECT fecha_registro, fecha_expiracion FROM suscripciones WHERE user_id = ?", (user.id,))
            sus = c.fetchone()
        
        conn.close()
        
        mensaje = f"👤 **MI PERFIL**\n\n"
        mensaje += f"📛 **Nombre:** {user.first_name}\n"
        mensaje += f"📝 **Mensajes totales:** {total_msgs}\n"
        
        if top_cats:
            mensaje += f"\n📊 **Tus temas favoritos:**\n"
            for cat, total in top_cats:
                mensaje += f"  • {cat}: {total}\n"
        
        if sus:
            dias = obtener_dias_restantes(user.id)
            mensaje += f"\n⏰ **Días restantes:** {dias}\n"
        
        await update.message.reply_text(mensaje, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error en mi_perfil: {e}")
        await update.message.reply_text("❌ Error obteniendo perfil")


# ==================== COMANDOS DE RESUMEN ====================

@requiere_suscripcion
async def resumen_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /resumen - Resumen del día"""
    msg = await update.message.reply_text("📝 Generando resumen del día...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        fecha_hoy = datetime.now().strftime("%d/%m/%Y")
        hora_actual = datetime.now().strftime("%H:%M")
        
        if DATABASE_URL:
            c.execute("""SELECT first_name, message, topic_id, categoria FROM mensajes 
                        WHERE fecha >= CURRENT_DATE 
                        ORDER BY fecha DESC LIMIT 50""")
            mensajes_hoy = c.fetchall()
            
            c.execute("SELECT COUNT(*) as total FROM mensajes WHERE fecha >= CURRENT_DATE")
            total_hoy = c.fetchone()['total']
            
            c.execute("SELECT COUNT(DISTINCT user_id) as total FROM mensajes WHERE fecha >= CURRENT_DATE")
            usuarios_hoy = c.fetchone()['total']
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        WHERE fecha >= CURRENT_DATE 
                        GROUP BY user_id ORDER BY msgs DESC LIMIT 5""")
            top_hoy = [((r['nombre_completo'] or 'Usuario').strip(), r['msgs']) for r in c.fetchall()]
            
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY total DESC LIMIT 5""")
            categorias_hoy = [(r['categoria'], r['total']) for r in c.fetchall()]
            
            c.execute("SELECT COUNT(*) as total FROM mensajes")
            total_historico = c.fetchone()['total']
        else:
            c.execute("""SELECT first_name, message, topic_id, categoria FROM mensajes 
                        WHERE DATE(fecha) = DATE('now') 
                        ORDER BY fecha DESC LIMIT 50""")
            mensajes_hoy = c.fetchall()
            
            c.execute("SELECT COUNT(*) FROM mensajes WHERE DATE(fecha) = DATE('now')")
            total_hoy = c.fetchone()[0]
            
            c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes WHERE DATE(fecha) = DATE('now')")
            usuarios_hoy = c.fetchone()[0]
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        WHERE DATE(fecha) = DATE('now') 
                        GROUP BY user_id ORDER BY msgs DESC LIMIT 5""")
            top_hoy = c.fetchall()
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE DATE(fecha) = DATE('now') AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC LIMIT 5""")
            categorias_hoy = c.fetchall()
            
            c.execute("SELECT COUNT(*) FROM mensajes")
            total_historico = c.fetchone()[0]
        
        conn.close()
        
        # Construir resumen SIN parse_mode para evitar errores
        mensaje = "=" * 28 + "\n"
        mensaje += "📰 RESUMEN DEL DIA\n"
        mensaje += "=" * 28 + "\n\n"
        mensaje += f"📅 Fecha: {fecha_hoy}\n"
        mensaje += f"🕐 Hora: {hora_actual}\n\n"
        mensaje += "📊 ACTIVIDAD DE HOY\n"
        mensaje += f"   💬 Mensajes: {total_hoy}\n"
        mensaje += f"   👥 Usuarios activos: {usuarios_hoy}\n\n"
        
        if top_hoy:
            mensaje += "🏆 MAS ACTIVOS HOY\n"
            medallas = ['🥇', '🥈', '🥉', '4.', '5.']
            for i, item in enumerate(top_hoy[:5]):
                nombre = item[0] if isinstance(item, tuple) else item.get('nombre_completo', item.get('first_name', ''))
                msgs = item[1] if isinstance(item, tuple) else item.get('msgs', 0)
                if nombre:
                    nombre_limpio = str(nombre).replace('_', ' ').strip()
                    if not nombre_limpio or nombre_limpio.lower() in ['group', 'grupo', 'channel', 'cofradía']:
                        nombre_limpio = "Usuario"
                    mensaje += f"   {medallas[i]} {nombre_limpio}: {msgs} msgs\n"
            mensaje += "\n"
        
        if categorias_hoy:
            # Usar IA para temas reales si hay suficientes mensajes
            insights_temas = generar_insights_temas(dias=1)
            if insights_temas:
                mensaje += "🏷️ TEMAS DEL DIA\n"
                for tema in insights_temas:
                    tema_limpio = tema.replace('*', '').replace('_', '').strip()
                    if tema_limpio:
                        mensaje += f"   {tema_limpio}\n"
                mensaje += "\n"
            else:
                mensaje += "🏷️ TEMAS DEL DIA\n"
                for cat, count in categorias_hoy[:5]:
                    if cat:
                        mensaje += f"   📌 {cat}: {count}\n"
                mensaje += "\n"
        
        mensaje += "=" * 28 + "\n"
        mensaje += f"📈 Total historico: {total_historico:,} mensajes"
        
        # Enviar SIN parse_mode para evitar errores de Markdown
        await msg.edit_text(mensaje)
        registrar_servicio_usado(update.effective_user.id, 'resumen')
        
    except Exception as e:
        logger.error(f"Error en resumen: {e}")
        await msg.edit_text(f"Error generando resumen. Intenta de nuevo.")


@requiere_suscripcion
async def resumen_semanal_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /resumen_semanal - Resumen de 7 días (mejorado)"""
    msg = await update.message.reply_text("📝 Generando resumen semanal...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        fecha_inicio = datetime.now() - timedelta(days=7)
        fecha_fin = datetime.now()
        
        if DATABASE_URL:
            c.execute("""SELECT COUNT(*) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'""")
            total = c.fetchone()['total']
            
            c.execute("""SELECT COUNT(DISTINCT user_id) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'""")
            usuarios = c.fetchone()['total']
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
                        GROUP BY user_id ORDER BY msgs DESC LIMIT 10""")
            top = [((r['nombre_completo'] or 'Usuario').strip(), r['msgs']) for r in c.fetchall()]
            
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days' AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY total DESC""")
            categorias = [(r['categoria'], r['total']) for r in c.fetchall()]
            
            c.execute("""SELECT DATE(fecha) as dia, COUNT(*) as msgs FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
                        GROUP BY DATE(fecha) ORDER BY dia""")
            por_dia = [(str(r['dia']), r['msgs']) for r in c.fetchall()]
            
            c.execute("SELECT COUNT(*) as total FROM mensajes")
            total_historico = c.fetchone()['total']
        else:
            fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%d")
            
            c.execute("SELECT COUNT(*) FROM mensajes WHERE fecha >= ?", (fecha_inicio_str,))
            total = c.fetchone()[0]
            
            c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes WHERE fecha >= ?", (fecha_inicio_str,))
            usuarios = c.fetchone()[0]
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        WHERE fecha >= ? GROUP BY user_id ORDER BY msgs DESC LIMIT 10""", (fecha_inicio_str,))
            top = c.fetchall()
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE fecha >= ? AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC""", (fecha_inicio_str,))
            categorias = c.fetchall()
            
            c.execute("""SELECT DATE(fecha), COUNT(*) FROM mensajes 
                        WHERE fecha >= ? GROUP BY DATE(fecha) ORDER BY DATE(fecha)""", (fecha_inicio_str,))
            por_dia = c.fetchall()
            
            c.execute("SELECT COUNT(*) FROM mensajes")
            total_historico = c.fetchone()[0]
        
        conn.close()
        
        # Construir mensaje atractivo
        mensaje = "━" * 30 + "\n"
        mensaje += "📅 **RESUMEN SEMANAL**\n"
        mensaje += "━" * 30 + "\n\n"
        mensaje += f"📆 **Período:** {fecha_inicio.strftime('%d/%m')} - {fecha_fin.strftime('%d/%m/%Y')}\n\n"
        
        mensaje += "📊 **ESTADÍSTICAS GENERALES**\n"
        mensaje += f"   💬 Total mensajes: {total:,}\n"
        mensaje += f"   👥 Usuarios activos: {usuarios}\n"
        mensaje += f"   📈 Promedio diario: {total/7:.1f}\n\n"
        
        if por_dia:
            mensaje += "📆 **ACTIVIDAD POR DÍA**\n"
            dias_semana = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
            for fecha, msgs in por_dia[-7:]:
                try:
                    dia_dt = datetime.strptime(str(fecha)[:10], "%Y-%m-%d")
                    dia_nombre = dias_semana[dia_dt.weekday()]
                    barra = "█" * min(int(msgs/5), 15) if msgs > 0 else "░"
                    mensaje += f"   {dia_nombre}: {barra} {msgs}\n"
                except:
                    mensaje += f"   {str(fecha)[-5:]}: {msgs}\n"
            mensaje += "\n"
        
        if top:
            mensaje += "🏆 TOP 10 MAS ACTIVOS\n"
            medallas = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟']
            for i, item in enumerate(top[:10]):
                nombre = item[0] if isinstance(item, tuple) else item.get('nombre_completo', item.get('first_name', ''))
                msgs = item[1] if isinstance(item, tuple) else item.get('msgs', 0)
                nombre_limpio = str(nombre).replace('_', ' ').strip()
                if not nombre_limpio or nombre_limpio.lower() in ['group', 'grupo', 'channel', 'cofradía']:
                    nombre_limpio = "Usuario"
                mensaje += f"   {medallas[i]} {nombre_limpio}: {msgs}\n"
            mensaje += "\n"
        
        # Temas principales con IA (análisis real de contenido)
        insights = generar_insights_temas(dias=7)
        if insights:
            mensaje += "🏷️ **TEMAS PRINCIPALES**\n"
            for tema in insights:
                tema_limpio = tema.replace('*', '').replace('_', '').strip()
                if tema_limpio:
                    mensaje += f"   {tema_limpio}\n"
            mensaje += "\n"
        elif categorias:
            mensaje += "🏷️ **TEMAS PRINCIPALES**\n"
            emojis = {'Empleo': '💼', 'Networking': '🤝', 'Consulta': '❓', 
                     'Emprendimiento': '🚀', 'Evento': '📅', 'Saludo': '👋', 'General': '💬'}
            total_cats = sum([c[1] for c in categorias])
            for cat, count in categorias[:6]:
                emoji = emojis.get(cat, '📌')
                pct = (count/total_cats*100) if total_cats > 0 else 0
                mensaje += f"   {emoji} {cat}: {count} ({pct:.1f}%)\n"
            mensaje += "\n"
        
        mensaje += "━" * 30 + "\n"
        mensaje += f"📈 **Total histórico:** {total_historico:,} mensajes\n"
        mensaje += "━" * 30
        
        await msg.edit_text(mensaje, parse_mode='Markdown')
        registrar_servicio_usado(update.effective_user.id, 'resumen_semanal')
        
    except Exception as e:
        logger.error(f"Error en resumen_semanal: {e}")
        await msg.edit_text(f"❌ Error generando resumen: {str(e)[:50]}")


@requiere_suscripcion
async def resumen_mes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /resumen_mes - Resumen mensual"""
    msg = await update.message.reply_text("📝 Generando resumen mensual...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        
        # Verificar si hay datos
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM mensajes")
            total_general = c.fetchone()['total']
        else:
            c.execute("SELECT COUNT(*) FROM mensajes")
            total_general = c.fetchone()[0]
        
        if total_general == 0:
            conn.close()
            await msg.edit_text(
                "📆 **RESUMEN MENSUAL**\n\n"
                "📊 No hay mensajes guardados en la base de datos.\n\n"
                "Los mensajes del grupo se guardan automáticamente mientras el bot está activo.\n\n"
                "💡 Espera unas horas o días para que se acumulen datos.",
                parse_mode='Markdown'
            )
            return
        
        if DATABASE_URL:
            c.execute("""SELECT COUNT(*) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'""")
            total = c.fetchone()['total']
            
            c.execute("""SELECT COUNT(DISTINCT user_id) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'""")
            usuarios = c.fetchone()['total']
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
                        GROUP BY user_id ORDER BY msgs DESC LIMIT 10""")
            top = [((r['nombre_completo'] or 'Usuario').strip(), r['msgs']) for r in c.fetchall()]
            
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days' AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY total DESC LIMIT 5""")
            cats = [(r['categoria'], r['total']) for r in c.fetchall()]
        else:
            fecha_inicio = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            c.execute("SELECT COUNT(*) FROM mensajes WHERE fecha >= ?", (fecha_inicio,))
            total = c.fetchone()[0]
            
            c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes WHERE fecha >= ?", (fecha_inicio,))
            usuarios = c.fetchone()[0]
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        WHERE fecha >= ? GROUP BY user_id ORDER BY msgs DESC LIMIT 10""", (fecha_inicio,))
            top = c.fetchall()
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE fecha >= ? AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC LIMIT 5""", (fecha_inicio,))
            cats = c.fetchall()
        
        conn.close()
        
        if total == 0:
            await msg.edit_text(
                "📆 **RESUMEN MENSUAL (30 días)**\n\n"
                f"📊 No hay mensajes de los últimos 30 días.\n"
                f"📈 Total histórico en BD: {total_general} mensajes\n\n"
                "💡 Los datos aparecerán cuando haya más actividad reciente.",
                parse_mode='Markdown'
            )
            return
        
        mensaje = f"📆 RESUMEN MENSUAL (30 dias)\n\n"
        mensaje += f"📝 Total mensajes: {total:,}\n"
        mensaje += f"👥 Usuarios activos: {usuarios}\n"
        mensaje += f"📈 Promedio diario: {total/30:.0f} mensajes\n\n"
        
        if top:
            mensaje += "🏆 Top 10 mas activos:\n"
            for i, item in enumerate(top, 1):
                nombre = item[0] if isinstance(item, tuple) else item.get('nombre_completo', item.get('first_name', ''))
                msgs = item[1] if isinstance(item, tuple) else item.get('msgs', 0)
                nombre_limpio = str(nombre).replace('_', ' ').strip()
                if not nombre_limpio or nombre_limpio.lower() in ['group', 'grupo', 'channel', 'cofradía']:
                    nombre_limpio = "Usuario"
                mensaje += f"  {i}. {nombre_limpio}: {msgs}\n"
        
        if cats:
            # Usar IA para temas reales
            insights_temas = generar_insights_temas(dias=30)
            if insights_temas:
                mensaje += "\n🏷️ Temas principales del mes:\n"
                for tema in insights_temas:
                    tema_limpio = tema.replace('*', '').replace('_', '').strip()
                    if tema_limpio:
                        mensaje += f"  {tema_limpio}\n"
            else:
                mensaje += "\n🏷️ Categorias principales:\n"
                for item in cats:
                    cat = item[0] if isinstance(item, tuple) else item['categoria']
                    count = item[1] if isinstance(item, tuple) else item['total']
                    mensaje += f"  📌 {cat}: {count}\n"
        
        await msg.edit_text(mensaje)
        registrar_servicio_usado(update.effective_user.id, 'resumen_mes')
        
    except Exception as e:
        logger.error(f"Error en resumen_mes: {e}")
        await msg.edit_text(f"❌ Error generando resumen.\n\nDetalle: {str(e)[:100]}")


# ==================== COMANDOS DE RRHH ====================

@requiere_suscripcion
async def dotacion_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /dotacion - Total de integrantes"""
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM suscripciones")
            total = c.fetchone()['total']
            
            c.execute("SELECT COUNT(*) as total FROM suscripciones WHERE estado = 'activo'")
            activos = c.fetchone()['total']
        else:
            c.execute("SELECT COUNT(*) FROM suscripciones")
            total = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM suscripciones WHERE estado = 'activo'")
            activos = c.fetchone()[0]
        
        conn.close()
        
        mensaje = f"""
👥 **DOTACIÓN DEL GRUPO**

📊 **Total registrados:** {total}
✅ **Suscripciones activas:** {activos}
❌ **Inactivos/Expirados:** {total - activos}
"""
        await update.message.reply_text(mensaje, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error en dotacion: {e}")
        await update.message.reply_text("❌ Error obteniendo dotación")


# ==================== COMANDO BUSCAR PROFESIONAL (GOOGLE DRIVE) ====================

@requiere_suscripcion
async def buscar_profesional_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /buscar_profesional - Buscar en base de datos de Google Drive"""
    if not context.args:
        await update.message.reply_text(
            "❌ **Uso:** /buscar_profesional [profesión o nombre]\n\n"
            "**Ejemplos:**\n"
            "• `/buscar_profesional abogado`\n"
            "• `/buscar_profesional contador`\n"
            "• `/buscar_profesional diseñador`",
            parse_mode='Markdown'
        )
        return
    
    query = ' '.join(context.args)
    msg = await update.message.reply_text(f"🔍 Buscando profesionales: _{query}_...", parse_mode='Markdown')
    
    # Buscar en Google Drive
    resultado = buscar_profesionales(query)
    
    await msg.delete()
    await enviar_mensaje_largo(update, resultado)
    registrar_servicio_usado(update.effective_user.id, 'buscar_profesional')


def buscar_profesionales(query):
    """
    Busca profesionales en Google Drive con búsqueda semántica.
    
    ESTRUCTURA DEL EXCEL "BD Grupo Laboral":
    - Columna C: Nombre
    - Columna D: Apellido
    - Columna F: Teléfono
    - Columna G: Email
    - Columna K: Industria 1
    - Columna L: Empresa 1
    - Columna M: Industria 2
    - Columna N: Empresa 2
    - Columna O: Industria 3
    - Columna P: Empresa 3
    - Columna X: Fecha cumpleaños (DD-MMM)
    - Columna Y: Profesión/Actividad (PRIORIDAD para búsqueda)
    """
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDS')
        if not creds_json:
            return (
                "❌ **Base de datos de profesionales no configurada**\n\n"
                "💡 **Alternativas:**\n"
                "• Pregunta en el grupo si alguien conoce un profesional\n"
                "• Usa /buscar_ia [profesión] para buscar en el historial"
            )
        
        try:
            creds_dict = json.loads(creds_json)
        except json.JSONDecodeError:
            return "❌ Error en credenciales de Google Drive."
        
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            access_token = creds.get_access_token().access_token
        except Exception as e:
            logger.error(f"Error token Google Drive: {e}")
            return "❌ Error de autenticación con Google Drive."
        
        headers = {'Authorization': f'Bearer {access_token}'}
        
        # Buscar archivo "BD Grupo Laboral"
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {
            'q': "name contains 'BD Grupo Laboral' and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false",
            'fields': 'files(id, name)'
        }
        
        response = requests.get(search_url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            return "❌ Error conectando con Google Drive."
        
        archivos = response.json().get('files', [])
        
        if not archivos:
            params['q'] = "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false"
            response = requests.get(search_url, headers=headers, params=params, timeout=30)
            archivos = response.json().get('files', [])
        
        if not archivos:
            return "❌ No se encontró base de datos de profesionales."
        
        # Descargar Excel
        file_id = archivos[0]['id']
        file_name = archivos[0]['name']
        logger.info(f"Leyendo: {file_name}")
        
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        response = requests.get(download_url, headers=headers, timeout=60)
        
        if response.status_code != 200:
            return "❌ Error descargando base de datos."
        
        # Leer Excel SIN modificar nombres de columnas
        df = pd.read_excel(BytesIO(response.content), engine='openpyxl', header=0)
        
        logger.info(f"Columnas: {list(df.columns)[:15]}")
        logger.info(f"Total filas: {len(df)}")
        
        # MAPEO POR ÍNDICE DE COLUMNAS (0-based):
        # C=2, D=3, F=5, G=6, K=10, L=11, M=12, N=13, O=14, P=15, X=23, Y=24
        
        def get_col(row, idx):
            """Obtiene valor de columna por índice, limpiando nulos"""
            try:
                val = row.iloc[idx] if idx < len(row) else ''
                val = str(val).strip()
                if val.lower() in ['nan', 'none', '', 'null', 'n/a', '-', 'nat']:
                    return ''
                return val
            except:
                return ''
        
        # Sinónimos para búsqueda semántica
        SINONIMOS = {
            'corredor': ['corredor', 'broker', 'agente', 'inmobiliario', 'bienes raíces', 'propiedades', 'real estate'],
            'contador': ['contador', 'contabilidad', 'auditor', 'tributario', 'contable', 'finanzas'],
            'abogado': ['abogado', 'legal', 'jurídico', 'derecho', 'leyes', 'lawyer'],
            'ingeniero': ['ingeniero', 'ingeniería', 'engineering', 'técnico'],
            'diseñador': ['diseñador', 'diseño', 'design', 'gráfico', 'ux', 'ui', 'creativo'],
            'marketing': ['marketing', 'mercadeo', 'publicidad', 'ventas', 'comercial', 'digital', 'growth'],
            'recursos humanos': ['rrhh', 'recursos humanos', 'hr', 'people', 'talento', 'selección'],
            'tecnología': ['tecnología', 'ti', 'it', 'sistemas', 'software', 'desarrollo', 'programador', 'developer'],
            'salud': ['salud', 'médico', 'doctor', 'enfermero', 'clínica', 'hospital'],
            'educación': ['educación', 'profesor', 'docente', 'capacitador', 'coach', 'formador'],
            'construcción': ['construcción', 'arquitecto', 'ingeniero civil', 'obra'],
            'finanzas': ['finanzas', 'financiero', 'banca', 'inversiones', 'economía'],
            'logística': ['logística', 'supply chain', 'transporte', 'distribución', 'bodega'],
            'administración': ['administración', 'administrador', 'gerente', 'gestión', 'manager', 'director'],
            'seguros': ['seguros', 'corredor de seguros', 'insurance', 'asegurador'],
            'consultoría': ['consultoría', 'consultor', 'consulting', 'asesor', 'asesoría', 'advisory'],
            'ventas': ['ventas', 'vendedor', 'ejecutivo comercial', 'sales', 'comercial'],
            'importaciones': ['importaciones', 'exportaciones', 'comercio exterior', 'aduanas', 'comex'],
        }
        
        # Expandir búsqueda con sinónimos
        query_lower = query.lower().strip()
        palabras_busqueda = set([query_lower])
        
        for categoria, sinonimos in SINONIMOS.items():
            if any(palabra in query_lower for palabra in sinonimos):
                palabras_busqueda.update(sinonimos)
        
        palabras_busqueda = list(palabras_busqueda)
        
        # Procesar cada fila
        profesionales = []
        for idx, row in df.iterrows():
            # Datos de contacto
            nombre = get_col(row, 2)      # Columna C
            apellido = get_col(row, 3)    # Columna D
            telefono = get_col(row, 5)    # Columna F
            email = get_col(row, 6)       # Columna G
            
            # Profesión/Actividad (PRIORIDAD)
            profesion = get_col(row, 24)  # Columna Y
            
            # Industrias y empresas
            industria1 = get_col(row, 10)  # Columna K
            empresa1 = get_col(row, 11)    # Columna L
            industria2 = get_col(row, 12)  # Columna M
            empresa2 = get_col(row, 13)    # Columna N
            industria3 = get_col(row, 14)  # Columna O
            empresa3 = get_col(row, 15)    # Columna P
            
            # Nombre completo
            nombre_completo = f"{nombre} {apellido}".strip()
            
            if not nombre_completo or nombre_completo == ' ':
                continue
            
            # Crear texto para búsqueda (prioridad: profesión Y, luego industrias)
            texto_busqueda = f"{profesion} {industria1} {industria2} {industria3}".lower()
            
            profesionales.append({
                'nombre': nombre_completo,
                'telefono': telefono,
                'email': email,
                'profesion': profesion,
                'industria1': industria1,
                'empresa1': empresa1,
                'industria2': industria2,
                'empresa2': empresa2,
                'industria3': industria3,
                'empresa3': empresa3,
                'texto_busqueda': texto_busqueda
            })
        
        if not profesionales:
            return "❌ La base de datos está vacía."
        
        logger.info(f"Total profesionales: {len(profesionales)}")
        
        # Búsqueda con scoring
        encontrados = []
        
        # PRIORIZACIÓN: Owner del bot (Germán Perey) tiene bonus de visibilidad
        OWNER_NAMES = ['germán', 'german', 'perey', 'oñate', 'onate']
        
        for p in profesionales:
            score = 0
            texto = p['texto_busqueda']
            nombre_lower = p['nombre'].lower()
            
            for palabra in palabras_busqueda:
                if len(palabra) > 2:
                    # Coincidencia en profesión (col Y) = máxima prioridad
                    if palabra in p['profesion'].lower():
                        score += 150
                    # Coincidencia en industria 1 (col K)
                    if palabra in p['industria1'].lower():
                        score += 100
                    # Coincidencia en industria 2 (col M)
                    if palabra in p['industria2'].lower():
                        score += 80
                    # Coincidencia en industria 3 (col O)
                    if palabra in p['industria3'].lower():
                        score += 60
                    # Coincidencia en nombre
                    if palabra in nombre_lower:
                        score += 40
                    # Coincidencia parcial
                    if palabra in texto:
                        score += 20
            
            # BONUS para el owner si hay coincidencia (para ayudarle a ser contratado)
            if score > 0 and any(owner_name in nombre_lower for owner_name in OWNER_NAMES):
                score += 50  # Bonus de visibilidad
            
            if score > 0:
                encontrados.append((p, score))
        
        # Ordenar por score
        encontrados.sort(key=lambda x: x[1], reverse=True)
        encontrados = [e[0] for e in encontrados]
        
        if not encontrados:
            # Mostrar profesiones disponibles
            profesiones = list(set([p['profesion'] for p in profesionales if p['profesion']]))
            industrias = list(set([p['industria1'] for p in profesionales if p['industria1']]))
            
            msg = f"❌ No se encontraron profesionales para: **{query}**\n\n"
            msg += f"📊 Total en BD: {len(profesionales)} profesionales\n\n"
            
            if profesiones:
                msg += "💡 **Algunas profesiones (col Y):**\n"
                for p in sorted(profesiones)[:10]:
                    msg += f"• {p}\n"
            
            if industrias:
                msg += "\n💼 **Algunas industrias (col K):**\n"
                for i in sorted(industrias)[:10]:
                    msg += f"• {i}\n"
            
            return msg
        
        # Formatear resultados
        resultado = "━" * 30 + "\n"
        resultado += "👥 **PROFESIONALES ENCONTRADOS**\n"
        resultado += "━" * 30 + "\n\n"
        resultado += f"🔍 **Búsqueda:** _{query}_\n"
        resultado += f"📊 **Resultados:** {len(encontrados)} de {len(profesionales)}\n\n"
        resultado += "━" * 30 + "\n\n"
        
        for i, prof in enumerate(encontrados[:20], 1):
            resultado += f"**{i}. {prof['nombre']}**\n"
            
            # Mostrar profesión si existe
            if prof['profesion']:
                resultado += f"   🎯 {prof['profesion']}\n"
            
            # Mostrar industrias y empresas
            if prof['industria1']:
                linea = f"   💼 {prof['industria1']}"
                if prof['empresa1']:
                    linea += f" ({prof['empresa1']})"
                resultado += linea + "\n"
            
            if prof['industria2']:
                linea = f"   💼 {prof['industria2']}"
                if prof['empresa2']:
                    linea += f" ({prof['empresa2']})"
                resultado += linea + "\n"
            
            # Contacto
            if prof['telefono']:
                resultado += f"   📱 {prof['telefono']}\n"
            if prof['email']:
                resultado += f"   📧 {prof['email']}\n"
            
            resultado += "\n"
        
        if len(encontrados) > 20:
            resultado += f"📌 _Mostrando 20 de {len(encontrados)} resultados_\n"
        
        resultado += "━" * 30
        
        return resultado
        
    except ImportError:
        return "❌ Módulo oauth2client no instalado."
    except Exception as e:
        logger.error(f"Error buscar_profesionales: {e}")
        return f"❌ Error: {str(e)[:150]}"


# ==================== GOOGLE DRIVE: HELPERS Y RAG PDF ====================

def obtener_drive_auth_headers():
    """Obtiene headers de autenticación para Google Drive API (centralizado)"""
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDS')
        if not creds_json:
            return None
        
        creds_dict = json.loads(creds_json)
        scope = [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        access_token = creds.get_access_token().access_token
        return {'Authorization': f'Bearer {access_token}'}
    except Exception as e:
        logger.error(f"Error obteniendo auth Drive: {e}")
        return None


def obtener_o_crear_carpeta_drive(nombre_carpeta, parent_id=None):
    """Busca una carpeta en Drive, si no existe la crea. Retorna folder_id."""
    try:
        headers = obtener_drive_auth_headers()
        if not headers:
            return None
        
        # Buscar carpeta existente
        query = f"name = '{nombre_carpeta}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {'q': query, 'fields': 'files(id, name)'}
        resp = requests.get(search_url, headers=headers, params=params, timeout=30)
        archivos = resp.json().get('files', [])
        
        if archivos:
            logger.info(f"📁 Carpeta '{nombre_carpeta}' encontrada: {archivos[0]['id']}")
            return archivos[0]['id']
        
        # Crear carpeta
        metadata = {
            'name': nombre_carpeta,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            metadata['parents'] = [parent_id]
        
        create_url = "https://www.googleapis.com/drive/v3/files"
        resp = requests.post(create_url, headers={**headers, 'Content-Type': 'application/json'},
                           json=metadata, timeout=30)
        
        if resp.status_code in [200, 201]:
            folder_id = resp.json()['id']
            logger.info(f"📁 Carpeta '{nombre_carpeta}' creada: {folder_id}")
            return folder_id
        else:
            logger.error(f"Error creando carpeta '{nombre_carpeta}': {resp.status_code} {resp.text[:200]}")
            return None
    
    except Exception as e:
        logger.error(f"Error en obtener_o_crear_carpeta_drive: {e}")
        return None


def obtener_carpeta_rag_pdf():
    """Obtiene (o crea) la ruta INBESTU/RAG_PDF en Google Drive. Retorna folder_id de RAG_PDF."""
    try:
        inbestu_id = obtener_o_crear_carpeta_drive("INBESTU")
        if not inbestu_id:
            logger.error("No se pudo crear/encontrar carpeta INBESTU")
            return None
        
        rag_pdf_id = obtener_o_crear_carpeta_drive("RAG_PDF", parent_id=inbestu_id)
        if not rag_pdf_id:
            logger.error("No se pudo crear/encontrar carpeta RAG_PDF")
            return None
        
        return rag_pdf_id
    except Exception as e:
        logger.error(f"Error obteniendo carpeta RAG_PDF: {e}")
        return None


def subir_pdf_a_drive(file_bytes, filename):
    """Sube un archivo PDF a Google Drive en INBESTU/RAG_PDF. Retorna file_id o None."""
    try:
        headers = obtener_drive_auth_headers()
        if not headers:
            return None, "Error de autenticación con Google Drive"
        
        rag_folder_id = obtener_carpeta_rag_pdf()
        if not rag_folder_id:
            return None, "No se pudo acceder a la carpeta INBESTU/RAG_PDF"
        
        # Verificar espacio (15 GB límite gratuito)
        espacio = verificar_espacio_drive(headers)
        if espacio and espacio.get('uso_porcentaje', 0) > 95:
            return None, f"⚠️ Google Drive casi lleno ({espacio['uso_porcentaje']:.0f}%). Libera espacio antes de subir."
        
        # Verificar si ya existe un archivo con el mismo nombre
        query = f"name = '{filename}' and '{rag_folder_id}' in parents and trashed = false"
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {'q': query, 'fields': 'files(id, name)'}
        resp = requests.get(search_url, headers=headers, params=params, timeout=30)
        existentes = resp.json().get('files', [])
        
        if existentes:
            # Actualizar archivo existente
            file_id = existentes[0]['id']
            upload_url = f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media"
            resp = requests.patch(upload_url, 
                                headers={**headers, 'Content-Type': 'application/pdf'},
                                data=file_bytes, timeout=120)
            if resp.status_code == 200:
                logger.info(f"📄 PDF actualizado en Drive: {filename} ({file_id})")
                return file_id, "actualizado"
        
        # Subir archivo nuevo (multipart upload)
        import io
        metadata = {
            'name': filename,
            'parents': [rag_folder_id],
            'mimeType': 'application/pdf'
        }
        
        # Multipart upload
        boundary = '----RAGPDFBoundary'
        body = io.BytesIO()
        
        # Part 1: metadata
        body.write(f'--{boundary}\r\n'.encode())
        body.write(b'Content-Type: application/json; charset=UTF-8\r\n\r\n')
        body.write(json.dumps(metadata).encode())
        body.write(b'\r\n')
        
        # Part 2: file content
        body.write(f'--{boundary}\r\n'.encode())
        body.write(b'Content-Type: application/pdf\r\n\r\n')
        body.write(file_bytes)
        body.write(b'\r\n')
        body.write(f'--{boundary}--\r\n'.encode())
        
        upload_url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
        resp = requests.post(upload_url,
                           headers={**headers, 'Content-Type': f'multipart/related; boundary={boundary}'},
                           data=body.getvalue(), timeout=120)
        
        if resp.status_code in [200, 201]:
            file_id = resp.json().get('id')
            logger.info(f"📄 PDF subido a Drive: {filename} ({file_id})")
            return file_id, "subido"
        else:
            logger.error(f"Error subiendo PDF: {resp.status_code} {resp.text[:300]}")
            return None, f"Error HTTP {resp.status_code}"
    
    except Exception as e:
        logger.error(f"Error subiendo PDF a Drive: {e}")
        return None, str(e)


def verificar_espacio_drive(headers=None):
    """Verifica el espacio usado/disponible en Google Drive"""
    try:
        if not headers:
            headers = obtener_drive_auth_headers()
        if not headers:
            return None
        
        about_url = "https://www.googleapis.com/drive/v3/about?fields=storageQuota"
        resp = requests.get(about_url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            quota = resp.json().get('storageQuota', {})
            limit_bytes = int(quota.get('limit', 0))
            usage_bytes = int(quota.get('usage', 0))
            
            if limit_bytes > 0:
                return {
                    'limite_gb': limit_bytes / (1024**3),
                    'usado_gb': usage_bytes / (1024**3),
                    'disponible_gb': (limit_bytes - usage_bytes) / (1024**3),
                    'uso_porcentaje': (usage_bytes / limit_bytes) * 100
                }
        return None
    except Exception as e:
        logger.warning(f"Error verificando espacio Drive: {e}")
        return None


def listar_pdfs_rag():
    """Lista todos los PDFs en la carpeta INBESTU/RAG_PDF de Google Drive"""
    try:
        headers = obtener_drive_auth_headers()
        if not headers:
            return []
        
        rag_folder_id = obtener_carpeta_rag_pdf()
        if not rag_folder_id:
            return []
        
        query = f"'{rag_folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {
            'q': query,
            'fields': 'files(id, name, size, createdTime, modifiedTime)',
            'orderBy': 'modifiedTime desc',
            'pageSize': 100
        }
        
        resp = requests.get(search_url, headers=headers, params=params, timeout=30)
        archivos = resp.json().get('files', [])
        
        return archivos
    except Exception as e:
        logger.error(f"Error listando PDFs RAG: {e}")
        return []


def descargar_pdf_drive(file_id):
    """Descarga contenido de un PDF desde Google Drive"""
    try:
        headers = obtener_drive_auth_headers()
        if not headers:
            return None
        
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        resp = requests.get(download_url, headers=headers, timeout=120)
        
        if resp.status_code == 200:
            return resp.content
        else:
            logger.error(f"Error descargando PDF {file_id}: {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error descargando PDF: {e}")
        return None


def extraer_texto_pdf(file_bytes):
    """Extrae texto de un PDF usando PyPDF2"""
    try:
        import PyPDF2
        
        reader = PyPDF2.PdfReader(BytesIO(file_bytes))
        texto_completo = ""
        paginas_procesadas = 0
        
        for page in reader.pages:
            try:
                texto = page.extract_text()
                if texto:
                    texto_completo += texto + "\n\n"
                    paginas_procesadas += 1
            except Exception as e:
                logger.warning(f"Error extrayendo página: {e}")
                continue
        
        logger.info(f"📄 PDF: {paginas_procesadas} páginas procesadas, {len(texto_completo)} caracteres")
        return texto_completo.strip()
    
    except ImportError:
        logger.error("PyPDF2 no disponible. Instalar con: pip install PyPDF2")
        return None
    except Exception as e:
        logger.error(f"Error extrayendo texto PDF: {e}")
        return None


def crear_chunks_texto(texto, chunk_size=800, overlap=100):
    """Divide texto largo en chunks con overlap para RAG"""
    if not texto or len(texto) < 50:
        return []
    
    # Limpiar texto
    texto = texto.replace('\x00', '').replace('\r', '')
    
    # Dividir por párrafos primero
    parrafos = [p.strip() for p in texto.split('\n\n') if p.strip()]
    
    chunks = []
    chunk_actual = ""
    
    for parrafo in parrafos:
        # Si el párrafo solo cabe, agregarlo
        if len(chunk_actual) + len(parrafo) + 2 <= chunk_size:
            chunk_actual += parrafo + "\n\n"
        else:
            # Guardar chunk actual si tiene contenido
            if chunk_actual.strip() and len(chunk_actual.strip()) > 30:
                chunks.append(chunk_actual.strip())
            
            # Si el párrafo es muy largo, dividir por oraciones
            if len(parrafo) > chunk_size:
                oraciones = parrafo.replace('. ', '.\n').split('\n')
                chunk_actual = ""
                for oracion in oraciones:
                    if len(chunk_actual) + len(oracion) + 2 <= chunk_size:
                        chunk_actual += oracion + " "
                    else:
                        if chunk_actual.strip() and len(chunk_actual.strip()) > 30:
                            chunks.append(chunk_actual.strip())
                        chunk_actual = oracion + " "
            else:
                chunk_actual = parrafo + "\n\n"
    
    # Último chunk
    if chunk_actual.strip() and len(chunk_actual.strip()) > 30:
        chunks.append(chunk_actual.strip())
    
    return chunks


def generar_keywords_chunk(chunk_text):
    """Genera keywords de un chunk de texto para búsqueda"""
    import re
    # Limpiar y extraer palabras significativas
    texto = re.sub(r'[^\w\sáéíóúñü]', ' ', chunk_text.lower())
    palabras = texto.split()
    
    # Filtrar stopwords español básicas
    STOPWORDS = {'de', 'la', 'el', 'en', 'y', 'a', 'que', 'es', 'por', 'un', 'una', 'los', 'las',
                 'del', 'con', 'no', 'se', 'su', 'al', 'lo', 'para', 'como', 'más', 'o', 'pero',
                 'sus', 'le', 'ya', 'este', 'si', 'entre', 'cuando', 'muy', 'sin', 'sobre', 'ser',
                 'también', 'me', 'hasta', 'hay', 'donde', 'quien', 'desde', 'todo', 'nos', 'durante',
                 'todos', 'uno', 'les', 'ni', 'contra', 'otros', 'ese', 'eso', 'ante', 'ellos', 'e',
                 'esto', 'mi', 'antes', 'algunos', 'qué', 'unos', 'yo', 'otro', 'otras', 'otra',
                 'él', 'tanto', 'esa', 'estos', 'mucho', 'quienes', 'nada', 'muchos', 'cual', 'poco',
                 'ella', 'estar', 'estas', 'algunas', 'algo', 'nosotros', 'cada', 'fue', 'son', 'han',
                 'the', 'and', 'of', 'to', 'in', 'is', 'for', 'on', 'with', 'at', 'by', 'an', 'be',
                 'this', 'that', 'from', 'or', 'as', 'are', 'was', 'were', 'has', 'have', 'had'}
    
    keywords = [p for p in palabras if len(p) > 2 and p not in STOPWORDS]
    
    # Deduplicar manteniendo orden
    seen = set()
    unique = []
    for k in keywords:
        if k not in seen:
            seen.add(k)
            unique.append(k)
    
    return ' '.join(unique[:50])


def indexar_pdf_en_rag(filename, texto, file_id=None):
    """Indexa un PDF en la tabla rag_chunks para búsqueda RAG"""
    try:
        if not texto or len(texto) < 50:
            logger.warning(f"PDF '{filename}' sin texto suficiente para indexar")
            return 0
        
        conn = get_db_connection()
        if not conn:
            return 0
        
        c = conn.cursor()
        source = f"PDF:{filename}"
        
        # Eliminar chunks anteriores de este PDF
        if DATABASE_URL:
            c.execute("DELETE FROM rag_chunks WHERE source = %s", (source,))
        else:
            c.execute("DELETE FROM rag_chunks WHERE source = ?", (source,))
        
        # Crear chunks
        chunks = crear_chunks_texto(texto)
        chunks_creados = 0
        
        for i, chunk_text in enumerate(chunks):
            keywords = generar_keywords_chunk(chunk_text)
            
            metadata = json.dumps({
                'filename': filename,
                'file_id': file_id or '',
                'chunk_index': i,
                'total_chunks': len(chunks),
                'tipo': 'pdf'
            })
            
            if DATABASE_URL:
                c.execute("""INSERT INTO rag_chunks (source, chunk_text, metadata, keywords) 
                           VALUES (%s, %s, %s, %s)""",
                         (source, chunk_text, metadata, keywords))
            else:
                c.execute("""INSERT INTO rag_chunks (source, chunk_text, metadata, keywords) 
                           VALUES (?, ?, ?, ?)""",
                         (source, chunk_text, metadata, keywords))
            chunks_creados += 1
        
        conn.commit()
        conn.close()
        logger.info(f"✅ PDF '{filename}' indexado: {chunks_creados} chunks")
        return chunks_creados
    
    except Exception as e:
        logger.error(f"Error indexando PDF en RAG: {e}")
        return 0


def indexar_todos_pdfs_rag():
    """Indexa (o re-indexa) todos los PDFs de la carpeta INBESTU/RAG_PDF"""
    try:
        pdfs = listar_pdfs_rag()
        if not pdfs:
            logger.info("RAG PDF: No hay PDFs para indexar en INBESTU/RAG_PDF")
            return 0
        
        total_chunks = 0
        pdfs_procesados = 0
        pdfs_error = 0
        
        for pdf_info in pdfs:
            file_id = pdf_info['id']
            filename = pdf_info['name']
            
            try:
                logger.info(f"📄 Indexando PDF: {filename}...")
                
                # Descargar PDF
                contenido = descargar_pdf_drive(file_id)
                if not contenido:
                    logger.warning(f"No se pudo descargar: {filename}")
                    pdfs_error += 1
                    continue
                
                # Extraer texto
                texto = extraer_texto_pdf(contenido)
                if not texto:
                    logger.warning(f"No se pudo extraer texto de: {filename}")
                    pdfs_error += 1
                    continue
                
                # Indexar en RAG
                chunks = indexar_pdf_en_rag(filename, texto, file_id)
                total_chunks += chunks
                pdfs_procesados += 1
                
            except Exception as e:
                logger.error(f"Error procesando PDF {filename}: {e}")
                pdfs_error += 1
                continue
        
        logger.info(f"✅ RAG PDF completado: {pdfs_procesados} PDFs, {total_chunks} chunks, {pdfs_error} errores")
        return total_chunks
    
    except Exception as e:
        logger.error(f"Error indexando todos los PDFs: {e}")
        return 0


def obtener_estadisticas_rag():
    """Obtiene estadísticas del sistema RAG"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        c = conn.cursor()
        stats = {}
        
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM rag_chunks")
            stats['total_chunks'] = c.fetchone()['total']
            
            c.execute("SELECT source, COUNT(*) as total FROM rag_chunks GROUP BY source ORDER BY total DESC")
            stats['por_fuente'] = [(r['source'], r['total']) for r in c.fetchall()]
            
            c.execute("SELECT COUNT(DISTINCT source) as total FROM rag_chunks WHERE source LIKE 'PDF:%'")
            stats['total_pdfs'] = c.fetchone()['total']
        else:
            c.execute("SELECT COUNT(*) FROM rag_chunks")
            stats['total_chunks'] = c.fetchone()[0]
            
            c.execute("SELECT source, COUNT(*) as total FROM rag_chunks GROUP BY source ORDER BY total DESC")
            stats['por_fuente'] = c.fetchall()
            
            c.execute("SELECT COUNT(DISTINCT source) FROM rag_chunks WHERE source LIKE 'PDF:%'")
            stats['total_pdfs'] = c.fetchone()[0]
        
        conn.close()
        
        # Info de Drive
        pdfs_drive = listar_pdfs_rag()
        stats['pdfs_en_drive'] = len(pdfs_drive)
        stats['pdfs_lista'] = [(p['name'], int(p.get('size', 0)) / (1024*1024)) for p in pdfs_drive]
        
        # Espacio
        espacio = verificar_espacio_drive()
        stats['espacio'] = espacio
        
        return stats
    except Exception as e:
        logger.error(f"Error obteniendo stats RAG: {e}")
        return None


# ==================== COMANDOS RAG PDF ====================

@requiere_suscripcion
async def subir_pdf_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /subir_pdf - Instrucciones para subir PDFs al RAG"""
    mensaje = "━" * 30 + "\n"
    mensaje += "📄 SUBIR PDF AL SISTEMA RAG\n"
    mensaje += "━" * 30 + "\n\n"
    mensaje += "Para subir un PDF al sistema de memoria RAG:\n\n"
    mensaje += "1. Envia el PDF como documento adjunto al bot\n"
    mensaje += "   (en chat privado con @Cofradia_Premium_Bot)\n\n"
    mensaje += "2. El bot lo subira automaticamente a:\n"
    mensaje += "   📁 Google Drive > INBESTU > RAG_PDF\n\n"
    mensaje += "3. El texto se extraera e indexara para que\n"
    mensaje += "   el bot pueda responder preguntas sobre el contenido\n\n"
    mensaje += "━" * 30 + "\n"
    mensaje += "📋 TIPOS DE DOCUMENTOS SUGERIDOS:\n\n"
    mensaje += "📋 Manual del grupo\n"
    mensaje += "📖 Guia de networking\n"
    mensaje += "🎓 Decalogo de bienvenida\n"
    mensaje += "💼 Directorio de servicios\n"
    mensaje += "📊 Informes mensuales\n"
    mensaje += "🤝 Casos de exito\n"
    mensaje += "📅 Calendario de eventos\n"
    mensaje += "💰 Guia de precios/tarifas\n"
    mensaje += "📚 Material de capacitacion\n"
    mensaje += "⚖️ Contratos modelo\n"
    mensaje += "💵 Estudios de remuneraciones\n\n"
    mensaje += "━" * 30 + "\n"
    mensaje += "💡 Usa /rag_status para ver PDFs indexados\n"
    mensaje += "💡 Usa /rag_consulta [pregunta] para consultar\n"
    mensaje += "📏 Limite: 15 GB gratuitos en Google Drive"
    
    await update.message.reply_text(mensaje)


async def recibir_documento_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para recibir documentos PDF enviados al bot"""
    if not update.message or not update.message.document:
        return
    
    # Solo procesar en chat privado
    if not es_chat_privado(update):
        return
    
    document = update.message.document
    user_id = update.effective_user.id
    
    # Verificar que sea PDF
    if document.mime_type != 'application/pdf':
        return  # Ignorar silenciosamente si no es PDF
    
    # Solo owner puede subir PDFs (seguridad)
    es_owner = (user_id == OWNER_ID)
    if not es_owner:
        # Verificar si es admin/suscriptor activo
        if not verificar_suscripcion_activa(user_id):
            await update.message.reply_text(
                "❌ Necesitas una suscripcion activa para subir PDFs.\n"
                "Usa /start para registrarte."
            )
            return
    
    filename = document.file_name or f"documento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    file_size_mb = document.file_size / (1024 * 1024) if document.file_size else 0
    
    # Límite de tamaño por archivo (20 MB para Telegram API)
    if file_size_mb > 20:
        await update.message.reply_text(
            f"⚠️ El archivo es muy grande ({file_size_mb:.1f} MB).\n"
            "Telegram permite maximo 20 MB por archivo.\n"
            "Intenta comprimir el PDF o dividirlo."
        )
        return
    
    msg = await update.message.reply_text(
        f"📥 Recibiendo: {filename} ({file_size_mb:.1f} MB)\n"
        "⏳ Descargando..."
    )
    
    try:
        # Descargar archivo de Telegram
        tg_file = await document.get_file()
        file_bytes = await tg_file.download_as_bytearray()
        file_bytes = bytes(file_bytes)
        
        await msg.edit_text(
            f"📥 {filename} ({file_size_mb:.1f} MB)\n"
            "☁️ Subiendo a Google Drive..."
        )
        
        # Subir a Google Drive
        file_id, status = subir_pdf_a_drive(file_bytes, filename)
        
        if not file_id:
            await msg.edit_text(
                f"❌ Error subiendo {filename} a Drive:\n{status}\n\n"
                "Verifica que la service account tenga permisos de escritura."
            )
            return
        
        await msg.edit_text(
            f"☁️ {filename} {status} en Drive\n"
            "🔍 Extrayendo texto del PDF..."
        )
        
        # Extraer texto
        texto = extraer_texto_pdf(file_bytes)
        
        if not texto:
            await msg.edit_text(
                f"☁️ {filename} {status} en Drive: INBESTU/RAG_PDF\n\n"
                "⚠️ No se pudo extraer texto del PDF.\n"
                "El archivo esta guardado pero no se indexo.\n"
                "Posibles causas: PDF escaneado (imagen), protegido, o sin texto."
            )
            return
        
        await msg.edit_text(
            f"☁️ {filename} {status} en Drive\n"
            "🧠 Indexando en sistema RAG..."
        )
        
        # Indexar en RAG
        chunks_creados = indexar_pdf_en_rag(filename, texto, file_id)
        
        # Resultado final
        resultado = "━" * 30 + "\n"
        resultado += "✅ PDF PROCESADO EXITOSAMENTE\n"
        resultado += "━" * 30 + "\n\n"
        resultado += f"📄 Archivo: {filename}\n"
        resultado += f"📏 Tamano: {file_size_mb:.1f} MB\n"
        resultado += f"☁️ Estado: {status} en Google Drive\n"
        resultado += f"📁 Ubicacion: INBESTU/RAG_PDF\n"
        resultado += f"📝 Texto extraido: {len(texto):,} caracteres\n"
        resultado += f"🧩 Chunks RAG creados: {chunks_creados}\n\n"
        resultado += "━" * 30 + "\n"
        resultado += "El bot ahora puede responder preguntas\n"
        resultado += "sobre el contenido de este documento.\n\n"
        resultado += "💡 Prueba: @Cofradia_Premium_Bot [tu pregunta]\n"
        resultado += "💡 O usa: /rag_consulta [tu pregunta]"
        
        await msg.edit_text(resultado)
        registrar_servicio_usado(user_id, 'subir_pdf')
        logger.info(f"✅ PDF procesado: {filename} - {chunks_creados} chunks por user {user_id}")
        
    except Exception as e:
        logger.error(f"Error procesando PDF: {e}")
        await msg.edit_text(f"❌ Error procesando PDF: {str(e)[:200]}")


@requiere_suscripcion
async def rag_status_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /rag_status - Ver estado del sistema RAG"""
    msg = await update.message.reply_text("🔍 Consultando estado del sistema RAG...")
    
    try:
        stats = obtener_estadisticas_rag()
        
        if not stats:
            await msg.edit_text("❌ Error obteniendo estadisticas RAG")
            return
        
        resultado = "━" * 30 + "\n"
        resultado += "🧠 ESTADO DEL SISTEMA RAG\n"
        resultado += "━" * 30 + "\n\n"
        
        resultado += f"📊 Total chunks indexados: {stats['total_chunks']}\n"
        resultado += f"📄 PDFs indexados: {stats['total_pdfs']}\n"
        resultado += f"☁️ PDFs en Drive: {stats['pdfs_en_drive']}\n\n"
        
        # Detalle por fuente
        if stats.get('por_fuente'):
            resultado += "📁 FUENTES INDEXADAS:\n"
            for fuente_data in stats['por_fuente']:
                fuente = fuente_data[0] if isinstance(fuente_data, tuple) else fuente_data
                total = fuente_data[1] if isinstance(fuente_data, tuple) else 0
                if str(fuente).startswith('PDF:'):
                    nombre_pdf = str(fuente).replace('PDF:', '')
                    resultado += f"   📄 {nombre_pdf}: {total} chunks\n"
                else:
                    resultado += f"   📊 {fuente}: {total} chunks\n"
            resultado += "\n"
        
        # PDFs en Drive
        if stats.get('pdfs_lista'):
            resultado += "📁 ARCHIVOS EN INBESTU/RAG_PDF:\n"
            for nombre, size_mb in stats['pdfs_lista']:
                resultado += f"   📄 {nombre} ({size_mb:.1f} MB)\n"
            resultado += "\n"
        
        # Espacio
        if stats.get('espacio'):
            esp = stats['espacio']
            resultado += "💾 ESPACIO GOOGLE DRIVE:\n"
            resultado += f"   📏 Limite: {esp['limite_gb']:.1f} GB\n"
            resultado += f"   📦 Usado: {esp['usado_gb']:.1f} GB ({esp['uso_porcentaje']:.0f}%)\n"
            resultado += f"   ✅ Disponible: {esp['disponible_gb']:.1f} GB\n\n"
        
        resultado += "━" * 30 + "\n"
        resultado += "💡 /subir_pdf - Instrucciones para subir\n"
        resultado += "💡 /rag_consulta [pregunta] - Consultar RAG\n"
        resultado += "💡 /rag_reindexar - Re-indexar todo"
        
        await msg.edit_text(resultado)
        
    except Exception as e:
        logger.error(f"Error en rag_status: {e}")
        await msg.edit_text(f"❌ Error: {str(e)[:200]}")


@requiere_suscripcion
async def rag_consulta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /rag_consulta - Consulta directa al sistema RAG con IA"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso: /rag_consulta [tu pregunta]\n\n"
            "Ejemplos:\n"
            "  /rag_consulta reglas del grupo\n"
            "  /rag_consulta tarifas recomendadas\n"
            "  /rag_consulta como hacer networking\n\n"
            "Busca en todos los PDFs y datos indexados."
        )
        return
    
    query = ' '.join(context.args)
    msg = await update.message.reply_text(f"🧠 Buscando en RAG: {query}...")
    
    try:
        # Buscar en RAG
        resultados = buscar_rag(query, limit=8)
        
        if not resultados:
            await msg.edit_text(
                f"🔍 No se encontraron resultados para: {query}\n\n"
                "💡 Prueba con palabras clave diferentes.\n"
                "💡 Usa /rag_status para ver que hay indexado."
            )
            return
        
        # Si hay IA disponible, generar respuesta inteligente
        if ia_disponible:
            contexto_rag = "\n\n".join([f"[Fragmento {i+1}]: {r}" for i, r in enumerate(resultados)])
            
            prompt = f"""Eres el asistente de Cofradía de Networking. 
El usuario pregunta: "{query}"

INFORMACIÓN ENCONTRADA EN LOS DOCUMENTOS:
{contexto_rag}

INSTRUCCIONES:
1. Responde basándote EXCLUSIVAMENTE en la información proporcionada
2. Si la información no es suficiente, indícalo
3. Sé conciso y directo
4. Si hay datos de contacto, inclúyelos
5. No uses asteriscos ni guiones bajos
6. Máximo 300 palabras"""
            
            respuesta = llamar_groq(prompt, max_tokens=600, temperature=0.3)
            
            if respuesta:
                respuesta_limpia = respuesta.replace('*', '').replace('_', ' ')
                texto_final = "🧠 CONSULTA RAG\n"
                texto_final += "━" * 30 + "\n\n"
                texto_final += f"🔍 Pregunta: {query}\n\n"
                texto_final += respuesta_limpia + "\n\n"
                texto_final += "━" * 30 + "\n"
                texto_final += f"📚 Fuentes: {len(resultados)} fragmentos encontrados"
                
                await msg.edit_text(texto_final)
                registrar_servicio_usado(update.effective_user.id, 'rag_consulta')
                return
        
        # Sin IA, mostrar resultados directos
        texto_final = "🧠 RESULTADOS RAG\n"
        texto_final += "━" * 30 + "\n\n"
        texto_final += f"🔍 Busqueda: {query}\n"
        texto_final += f"📊 Encontrados: {len(resultados)} fragmentos\n\n"
        
        for i, r in enumerate(resultados[:5], 1):
            texto_final += f"📄 Resultado {i}:\n"
            texto_final += f"{r[:300]}...\n\n" if len(r) > 300 else f"{r}\n\n"
        
        await msg.edit_text(texto_final)
        registrar_servicio_usado(update.effective_user.id, 'rag_consulta')
        
    except Exception as e:
        logger.error(f"Error en rag_consulta: {e}")
        await msg.edit_text(f"❌ Error consultando RAG: {str(e)[:200]}")


async def rag_reindexar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /rag_reindexar - Re-indexa todos los PDFs y Excel (solo owner/admin)"""
    user_id = update.effective_user.id
    
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ Solo el administrador puede re-indexar el RAG.")
        return
    
    msg = await update.message.reply_text("🔄 Re-indexando sistema RAG completo...\n⏳ Esto puede tomar unos minutos.")
    
    try:
        # 1. Indexar Excel
        await msg.edit_text("🔄 Paso 1/2: Indexando base de datos Excel...")
        indexar_google_drive_rag()
        
        # 2. Indexar PDFs
        await msg.edit_text("🔄 Paso 2/2: Indexando PDFs de INBESTU/RAG_PDF...")
        chunks_pdf = indexar_todos_pdfs_rag()
        
        # Obtener stats finales
        stats = obtener_estadisticas_rag()
        
        resultado = "✅ RE-INDEXACION COMPLETADA\n"
        resultado += "━" * 30 + "\n\n"
        if stats:
            resultado += f"📊 Total chunks: {stats['total_chunks']}\n"
            resultado += f"📄 PDFs procesados: {stats['total_pdfs']}\n"
            resultado += f"☁️ PDFs en Drive: {stats['pdfs_en_drive']}\n"
        resultado += f"\n🧩 Chunks PDF creados: {chunks_pdf}\n"
        resultado += "\n💡 El sistema RAG esta actualizado."
        
        await msg.edit_text(resultado)
        
    except Exception as e:
        logger.error(f"Error re-indexando RAG: {e}")
        await msg.edit_text(f"❌ Error re-indexando: {str(e)[:200]}")


async def eliminar_pdf_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /eliminar_pdf [nombre] - Elimina un PDF del RAG (solo owner)"""
    user_id = update.effective_user.id
    
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ Solo el administrador puede eliminar PDFs.")
        return
    
    if not context.args:
        # Listar PDFs disponibles
        pdfs = listar_pdfs_rag()
        if not pdfs:
            await update.message.reply_text("📁 No hay PDFs en INBESTU/RAG_PDF")
            return
        
        msg = "📁 PDFs en INBESTU/RAG_PDF:\n\n"
        for i, pdf in enumerate(pdfs, 1):
            size_mb = int(pdf.get('size', 0)) / (1024*1024)
            msg += f"{i}. {pdf['name']} ({size_mb:.1f} MB)\n"
        msg += "\n💡 Uso: /eliminar_pdf [nombre exacto del archivo]"
        
        await update.message.reply_text(msg)
        return
    
    filename = ' '.join(context.args)
    
    try:
        headers = obtener_drive_auth_headers()
        if not headers:
            await update.message.reply_text("❌ Error de autenticación Drive")
            return
        
        # Buscar el archivo
        rag_folder_id = obtener_carpeta_rag_pdf()
        if not rag_folder_id:
            await update.message.reply_text("❌ No se encontró carpeta RAG_PDF")
            return
        
        query = f"name = '{filename}' and '{rag_folder_id}' in parents and trashed = false"
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {'q': query, 'fields': 'files(id, name)'}
        resp = requests.get(search_url, headers=headers, params=params, timeout=30)
        archivos = resp.json().get('files', [])
        
        if not archivos:
            await update.message.reply_text(f"❌ No se encontró: {filename}")
            return
        
        # Mover a papelera
        file_id = archivos[0]['id']
        delete_url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
        resp = requests.patch(delete_url, headers={**headers, 'Content-Type': 'application/json'},
                            json={'trashed': True}, timeout=30)
        
        # Eliminar chunks del RAG
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            source = f"PDF:{filename}"
            if DATABASE_URL:
                c.execute("DELETE FROM rag_chunks WHERE source = %s", (source,))
            else:
                c.execute("DELETE FROM rag_chunks WHERE source = ?", (source,))
            conn.commit()
            conn.close()
        
        await update.message.reply_text(
            f"✅ PDF eliminado: {filename}\n"
            "📊 Chunks RAG también eliminados."
        )
        
    except Exception as e:
        logger.error(f"Error eliminando PDF: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:200]}")


# ==================== FUNCIÓN AUXILIAR: OBTENER DATOS EXCEL DRIVE ====================

def obtener_datos_excel_drive(sheet_name=0):
    """Obtiene DataFrame completo del Excel de Google Drive para análisis"""
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDS')
        if not creds_json:
            return None
        
        creds_dict = json.loads(creds_json)
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        access_token = creds.get_access_token().access_token
        headers = {'Authorization': f'Bearer {access_token}'}
        
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {
            'q': "name contains 'BD Grupo Laboral' and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false",
            'fields': 'files(id, name)'
        }
        
        response = requests.get(search_url, headers=headers, params=params, timeout=30)
        archivos = response.json().get('files', [])
        
        if not archivos:
            return None
        
        file_id = archivos[0]['id']
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        response = requests.get(download_url, headers=headers, timeout=60)
        
        if response.status_code != 200:
            return None
        
        df = pd.read_excel(BytesIO(response.content), engine='openpyxl', header=0, sheet_name=sheet_name)
        return df
        
    except Exception as e:
        logger.error(f"Error obteniendo datos Excel Drive (sheet={sheet_name}): {e}")
        return None


def detectar_columna_anio_egreso(df):
    """Detecta automáticamente la columna que contiene años de egreso"""
    for col_idx in range(len(df.columns)):
        try:
            col_data = pd.to_numeric(df.iloc[:, col_idx], errors='coerce').dropna()
            # Verificar si la mayoría son años válidos (1960-2025)
            if len(col_data) > 10:
                years_valid = col_data[(col_data >= 1960) & (col_data <= 2026)]
                if len(years_valid) > len(col_data) * 0.5:  # >50% son años válidos
                    logger.info(f"Columna de año de egreso detectada: índice {col_idx}")
                    return col_idx
        except:
            continue
    return None


# ==================== SISTEMA RAG (MEMORIA SEMÁNTICA) ====================

def indexar_google_drive_rag():
    """Indexa datos del Excel de Google Drive en chunks para RAG"""
    try:
        df = obtener_datos_excel_drive()
        if df is None or len(df) == 0:
            logger.info("RAG: No hay datos para indexar")
            return
        
        conn = get_db_connection()
        if not conn:
            return
        
        c = conn.cursor()
        
        # Limpiar chunks anteriores
        if DATABASE_URL:
            c.execute("DELETE FROM rag_chunks")
        else:
            c.execute("DELETE FROM rag_chunks")
        
        chunks_creados = 0
        
        def get_col(row, idx):
            try:
                val = row.iloc[idx] if idx < len(row) else ''
                val = str(val).strip()
                if val.lower() in ['nan', 'none', '', 'null', 'n/a', '-', 'nat']:
                    return ''
                return val
            except:
                return ''
        
        for idx, row in df.iterrows():
            nombre = get_col(row, 2)
            apellido = get_col(row, 3)
            telefono = get_col(row, 5)
            email = get_col(row, 6)
            situacion = get_col(row, 8)
            industria1 = get_col(row, 10)
            industria2 = get_col(row, 12)
            industria3 = get_col(row, 14)
            profesion = get_col(row, 24)
            
            nombre_completo = f"{nombre} {apellido}".strip()
            if not nombre_completo:
                continue
            
            # Crear chunk de texto
            chunk = f"Profesional: {nombre_completo}."
            if profesion:
                chunk += f" Profesion: {profesion}."
            if situacion:
                chunk += f" Situacion laboral: {situacion}."
            if industria1:
                chunk += f" Industria: {industria1}."
            if industria2:
                chunk += f" Tambien: {industria2}."
            if industria3:
                chunk += f" Ademas: {industria3}."
            if telefono:
                chunk += f" Telefono: {telefono}."
            if email:
                chunk += f" Email: {email}."
            
            # Keywords para búsqueda
            keywords = f"{nombre_completo} {profesion} {industria1} {industria2} {industria3} {situacion}".lower()
            
            metadata = json.dumps({
                'nombre': nombre_completo,
                'profesion': profesion,
                'situacion': situacion,
                'fila': idx
            })
            
            if DATABASE_URL:
                c.execute("""INSERT INTO rag_chunks (source, chunk_text, metadata, keywords) 
                           VALUES (%s, %s, %s, %s)""",
                         ('BD_Grupo_Laboral', chunk, metadata, keywords))
            else:
                c.execute("""INSERT INTO rag_chunks (source, chunk_text, metadata, keywords) 
                           VALUES (?, ?, ?, ?)""",
                         ('BD_Grupo_Laboral', chunk, metadata, keywords))
            
            chunks_creados += 1
        
        conn.commit()
        conn.close()
        logger.info(f"✅ RAG: {chunks_creados} chunks indexados desde Google Drive")
        
    except Exception as e:
        logger.error(f"Error indexando RAG: {e}")


def buscar_rag(query, limit=5):
    """Busca en chunks RAG por keywords"""
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        c = conn.cursor()
        palabras = query.lower().split()
        resultados = []
        
        if DATABASE_URL:
            # Buscar por cada palabra
            for palabra in palabras:
                if len(palabra) > 2:
                    c.execute("""SELECT chunk_text, metadata FROM rag_chunks 
                               WHERE keywords LIKE %s LIMIT %s""",
                             (f'%{palabra}%', limit))
                    for r in c.fetchall():
                        resultados.append(r['chunk_text'])
        else:
            for palabra in palabras:
                if len(palabra) > 2:
                    c.execute("""SELECT chunk_text, metadata FROM rag_chunks 
                               WHERE keywords LIKE ? LIMIT ?""",
                             (f'%{palabra}%', limit))
                    for r in c.fetchall():
                        resultados.append(r[0] if isinstance(r, tuple) else r['chunk_text'])
        
        conn.close()
        # Deduplicar
        return list(dict.fromkeys(resultados))[:limit]
        
    except Exception as e:
        logger.error(f"Error buscando RAG: {e}")
        return []


async def indexar_rag_job(context: ContextTypes.DEFAULT_TYPE):
    """Job programado para re-indexar RAG cada 6 horas (Excel + PDFs)"""
    logger.info("🔄 Ejecutando re-indexación RAG...")
    
    # 1. Indexar Excel de BD Grupo Laboral
    indexar_google_drive_rag()
    
    # 2. Indexar PDFs de INBESTU/RAG_PDF
    try:
        chunks_pdf = indexar_todos_pdfs_rag()
        logger.info(f"🧠 RAG PDFs: {chunks_pdf} chunks indexados")
    except Exception as e:
        logger.error(f"Error indexando PDFs en job RAG: {e}")


# ==================== SCRAPER SEC (SUPERINTENDENCIA ELECTRICIDAD Y COMBUSTIBLES) ====================

def buscar_especialista_sec(especialidad, ciudad=""):
    """
    Busca especialistas certificados en la SEC (Chile).
    Intenta scraping del buscador SEC, con fallback a links directos.
    """
    try:
        ESPECIALIDADES_SEC = {
            'electricista': ('E', 'Electrico'),
            'electrico': ('E', 'Electrico'),
            'electrica': ('E', 'Electrico'),
            'instalador electrico': ('E', 'Electrico'),
            'gas': ('G', 'Gas'),
            'gasfiter': ('G', 'Gas'),
            'gasfíter': ('G', 'Gas'),
            'instalador gas': ('G', 'Gas'),
            'combustible': ('E', 'Combustibles'),
            'combustibles': ('E', 'Combustibles'),
        }
        
        # Mapeo de ciudades/comunas a códigos de región SEC
        REGIONES_SEC = {
            'arica': ('15', 'Arica y Parinacota'),
            'iquique': ('01', 'Tarapaca'),
            'antofagasta': ('02', 'Antofagasta'),
            'copiapo': ('03', 'Atacama'),
            'la serena': ('04', 'Coquimbo'),
            'coquimbo': ('04', 'Coquimbo'),
            'valparaiso': ('05', 'Valparaiso'),
            'viña del mar': ('05', 'Valparaiso'),
            'viña': ('05', 'Valparaiso'),
            'rancagua': ('06', "O'Higgins"),
            'talca': ('07', 'Maule'),
            'concepcion': ('08', 'Biobio'),
            'chillan': ('16', 'Nuble'),
            'temuco': ('09', 'La Araucania'),
            'valdivia': ('14', 'Los Rios'),
            'puerto montt': ('10', 'Los Lagos'),
            'osorno': ('10', 'Los Lagos'),
            'coyhaique': ('11', 'Aysen'),
            'punta arenas': ('12', 'Magallanes'),
            'santiago': ('13', 'Metropolitana'),
            'providencia': ('13', 'Metropolitana'),
            'las condes': ('13', 'Metropolitana'),
            'maipu': ('13', 'Metropolitana'),
            'puente alto': ('13', 'Metropolitana'),
            'la florida': ('13', 'Metropolitana'),
            'ñuñoa': ('13', 'Metropolitana'),
            'nunoa': ('13', 'Metropolitana'),
            'san bernardo': ('13', 'Metropolitana'),
            'quilpue': ('05', 'Valparaiso'),
        }
        
        esp_lower = especialidad.lower().strip()
        tipo_inst = None
        tipo_nombre = ''
        for key, (code, nombre) in ESPECIALIDADES_SEC.items():
            if key in esp_lower:
                tipo_inst = code
                tipo_nombre = nombre
                break
        
        if not tipo_inst:
            tipo_inst = 'E'
            tipo_nombre = 'Electrico'
        
        # Detectar región
        region_code = None
        region_nombre = ''
        ciudad_lower = ciudad.lower().strip() if ciudad else ''
        for key, (code, nombre) in REGIONES_SEC.items():
            if key in ciudad_lower:
                region_code = code
                region_nombre = nombre
                break
        
        resultado = "━" * 30 + "\n"
        resultado += "🔍 BUSQUEDA SEC - Especialistas Certificados\n"
        resultado += "━" * 30 + "\n\n"
        resultado += f"📋 Especialidad: {especialidad}\n"
        if ciudad:
            resultado += f"📍 Ciudad/Comuna: {ciudad}\n"
        if region_nombre:
            resultado += f"🗺️ Region: {region_nombre}\n"
        resultado += "\n"
        
        # Intentar scraping real del buscador SEC
        especialistas_encontrados = []
        if bs4_disponible:
            try:
                session = requests.Session()
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'es-CL,es;q=0.9',
                })
                
                # Intentar el buscador avanzado con parámetros directos
                search_params = {
                    'tipoBusqueda': 'avanzada',
                    'tipoInstalacion': tipo_inst,
                }
                if region_code:
                    search_params['region'] = region_code
                
                buscador_url = "https://wlhttp.sec.cl/buscadorinstaladores/busqueda.do"
                resp = session.get(buscador_url, params=search_params, timeout=20)
                
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    
                    # Buscar tablas con resultados
                    tables = soup.find_all('table')
                    for table in tables:
                        rows = table.find_all('tr')
                        for row in rows[1:]:  # Saltar header
                            cols = row.find_all('td')
                            if len(cols) >= 3:
                                nombre = cols[0].get_text(strip=True)
                                rut = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                                niveles = cols[2].get_text(strip=True) if len(cols) > 2 else ''
                                telefono = cols[3].get_text(strip=True) if len(cols) > 3 else ''
                                email_col = cols[4].get_text(strip=True) if len(cols) > 4 else ''
                                
                                if nombre and len(nombre) > 2 and nombre.lower() != 'nombre':
                                    especialistas_encontrados.append({
                                        'nombre': nombre,
                                        'rut': rut,
                                        'niveles': niveles,
                                        'telefono': telefono,
                                        'email': email_col
                                    })
                    
                    # Si no encontró tabla, intentar otro formato
                    if not especialistas_encontrados:
                        # Buscar divs o listas con resultados
                        divs = soup.find_all(['div', 'li'], class_=True)
                        for div in divs:
                            text = div.get_text(strip=True)
                            if 'rut' in text.lower() or 'instalador' in text.lower():
                                if len(text) > 10 and len(text) < 500:
                                    especialistas_encontrados.append({
                                        'nombre': text[:100],
                                        'rut': '',
                                        'niveles': '',
                                        'telefono': '',
                                        'email': ''
                                    })
                
                logger.info(f"SEC scraping: encontrados {len(especialistas_encontrados)} resultados")
                
            except Exception as e:
                logger.warning(f"Error scraping SEC: {e}")
        
        # Mostrar resultados encontrados
        if especialistas_encontrados:
            resultado += f"✅ RESULTADOS ENCONTRADOS: {len(especialistas_encontrados)}\n\n"
            for i, esp in enumerate(especialistas_encontrados[:15], 1):
                resultado += f"{i}. {esp['nombre']}\n"
                if esp['rut']:
                    resultado += f"   🆔 RUT: {esp['rut']}\n"
                if esp['niveles']:
                    resultado += f"   📜 Certificacion: {esp['niveles']}\n"
                if esp['telefono']:
                    resultado += f"   📞 Tel: {esp['telefono']}\n"
                if esp['email']:
                    resultado += f"   📧 Email: {esp['email']}\n"
                resultado += "\n"
        else:
            resultado += "⚠️ No se encontraron resultados via scraping.\n"
            resultado += "El buscador SEC requiere navegador web.\n\n"
        
        # Links directos siempre visibles
        resultado += "━" * 30 + "\n"
        resultado += "🌐 CONSULTA DIRECTA EN SEC:\n\n"
        
        resultado += "🔍 Buscador de Instaladores:\n"
        resultado += "   https://wlhttp.sec.cl/buscadorinstaladores/buscador.do\n\n"
        
        resultado += "📋 Validador de Instaladores (por RUT):\n"
        resultado += "   https://wlhttp.sec.cl/validadorInstaladores/\n\n"
        
        resultado += "🏛️ Registro Nacional de Instaladores:\n"
        resultado += "   https://wlhttp.sec.cl/rnii/home\n\n"
        
        resultado += "💡 COMO BUSCAR:\n"
        resultado += f"1. Ingresa a: wlhttp.sec.cl/buscadorinstaladores/buscador.do\n"
        resultado += f"2. Selecciona tipo: {tipo_nombre}\n"
        resultado += "3. Marca los trabajos que necesitas\n"
        if region_nombre:
            resultado += f"4. Region: {region_nombre}\n"
        else:
            resultado += "4. Selecciona tu region\n"
        if ciudad:
            resultado += f"5. Comuna: {ciudad}\n"
        else:
            resultado += "5. Selecciona tu comuna\n"
        resultado += "6. Clic en Buscar\n"
        resultado += "7. Obtendras: Nombre, RUT, Clase, Telefono, Email\n\n"
        
        resultado += "━" * 30 + "\n"
        resultado += "📞 Mesa de ayuda SEC: 600 6000 732\n"
        resultado += "🌐 Web: https://www.sec.cl/"
        
        return resultado
        
    except Exception as e:
        logger.error(f"Error buscando en SEC: {e}")
        return f"❌ Error consultando SEC: {str(e)[:100]}"


@requiere_suscripcion
async def buscar_especialista_sec_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /buscar_especialista_sec - Buscar especialistas certificados SEC"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso: /buscar_especialista_sec [especialidad], [ciudad]\n\n"
            "Ejemplos:\n"
            "  /buscar_especialista_sec electricista, Santiago\n"
            "  /buscar_especialista_sec gas, Valparaiso\n"
            "  /buscar_especialista_sec gasfiter, Concepcion\n\n"
            "Especialidades: electricista, gas, gasfiter, combustibles"
        )
        return
    
    texto = ' '.join(context.args)
    partes = [p.strip() for p in texto.split(',')]
    especialidad = partes[0]
    ciudad = partes[1] if len(partes) > 1 else ""
    
    msg = await update.message.reply_text(f"🔍 Buscando especialistas SEC: {especialidad}...")
    
    resultado = buscar_especialista_sec(especialidad, ciudad)
    
    await msg.delete()
    await enviar_mensaje_largo(update, resultado, parse_mode=None)
    registrar_servicio_usado(update.effective_user.id, 'buscar_sec')


# ==================== COMANDO BUSCAR APOYO (BÚSQUEDA LABORAL) ====================

@requiere_suscripcion
async def buscar_apoyo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /buscar_apoyo - Buscar profesionales en búsqueda laboral"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso: /buscar_apoyo [area o profesion]\n\n"
            "Ejemplos:\n"
            "  /buscar_apoyo ingeniero\n"
            "  /buscar_apoyo marketing\n"
            "  /buscar_apoyo contador\n\n"
            "Busca profesionales que estan en Busqueda Laboral"
        )
        return
    
    query = ' '.join(context.args)
    msg = await update.message.reply_text(f"🔍 Buscando profesionales en busqueda laboral: {query}...")
    
    resultado = buscar_apoyo_profesional(query)
    
    await msg.delete()
    await enviar_mensaje_largo(update, resultado, parse_mode=None)
    registrar_servicio_usado(update.effective_user.id, 'buscar_apoyo')


def buscar_apoyo_profesional(query):
    """Busca profesionales en situación de 'Búsqueda Laboral' en Google Drive"""
    try:
        df = obtener_datos_excel_drive()
        if df is None:
            return "❌ No se pudo acceder a la base de datos de Google Drive."
        
        def get_col(row, idx):
            try:
                val = row.iloc[idx] if idx < len(row) else ''
                val = str(val).strip()
                if val.lower() in ['nan', 'none', '', 'null', 'n/a', '-', 'nat']:
                    return ''
                return val
            except:
                return ''
        
        # Sinónimos para búsqueda semántica (igual que buscar_profesional)
        SINONIMOS = {
            'corredor': ['corredor', 'broker', 'agente', 'inmobiliario', 'bienes raíces'],
            'contador': ['contador', 'contabilidad', 'auditor', 'tributario', 'contable'],
            'abogado': ['abogado', 'legal', 'jurídico', 'derecho'],
            'ingeniero': ['ingeniero', 'ingeniería', 'engineering', 'técnico'],
            'diseñador': ['diseñador', 'diseño', 'design', 'gráfico', 'ux', 'ui'],
            'marketing': ['marketing', 'mercadeo', 'publicidad', 'ventas', 'comercial', 'digital'],
            'recursos humanos': ['rrhh', 'recursos humanos', 'hr', 'people', 'talento'],
            'tecnología': ['tecnología', 'ti', 'it', 'sistemas', 'software', 'programador'],
            'salud': ['salud', 'médico', 'doctor', 'enfermero'],
            'educación': ['educación', 'profesor', 'docente', 'capacitador', 'coach'],
            'construcción': ['construcción', 'arquitecto', 'ingeniero civil'],
            'finanzas': ['finanzas', 'financiero', 'banca', 'inversiones'],
            'logística': ['logística', 'supply chain', 'transporte'],
            'administración': ['administración', 'administrador', 'gerente', 'gestión'],
            'seguros': ['seguros', 'corredor de seguros', 'insurance'],
            'consultoría': ['consultoría', 'consultor', 'consulting', 'asesor'],
            'ventas': ['ventas', 'vendedor', 'ejecutivo comercial', 'sales'],
        }
        
        query_lower = query.lower().strip()
        palabras_busqueda = set([query_lower])
        for categoria, sinonimos in SINONIMOS.items():
            if any(p in query_lower for p in sinonimos):
                palabras_busqueda.update(sinonimos)
        palabras_busqueda = list(palabras_busqueda)
        
        profesionales = []
        for idx, row in df.iterrows():
            # Filtrar SOLO los que están en "Búsqueda Laboral" (columna I = índice 8)
            situacion = get_col(row, 8)
            if not situacion:
                continue
            if 'busqueda' not in situacion.lower() and 'búsqueda' not in situacion.lower():
                continue
            
            nombre = get_col(row, 2)
            apellido = get_col(row, 3)
            telefono = get_col(row, 5)
            email = get_col(row, 6)
            profesion = get_col(row, 24)
            industria1 = get_col(row, 10)
            industria2 = get_col(row, 12)
            industria3 = get_col(row, 14)
            
            nombre_completo = f"{nombre} {apellido}".strip()
            if not nombre_completo:
                continue
            
            texto_busqueda = f"{profesion} {industria1} {industria2} {industria3}".lower()
            
            profesionales.append({
                'nombre': nombre_completo,
                'telefono': telefono,
                'email': email,
                'profesion': profesion,
                'industria1': industria1,
                'industria2': industria2,
                'industria3': industria3,
                'situacion': situacion,
                'texto_busqueda': texto_busqueda
            })
        
        if not profesionales:
            return f"❌ No se encontraron profesionales en Busqueda Laboral para: {query}"
        
        # Scoring
        encontrados = []
        for p in profesionales:
            score = 0
            for palabra in palabras_busqueda:
                if len(palabra) > 2:
                    if palabra in p['profesion'].lower():
                        score += 100
                    if palabra in p['industria1'].lower():
                        score += 80
                    if palabra in p['industria2'].lower():
                        score += 30
                    if palabra in p['texto_busqueda']:
                        score += 10
            
            # Si no hay query específico, mostrar todos
            if score > 0 or query_lower in ['todos', 'all', '*', 'todo']:
                encontrados.append((p, max(score, 1)))
            elif not any(len(p) > 2 for p in palabras_busqueda):
                encontrados.append((p, 1))
        
        # Si no encontramos con score, mostrar todos los que están en búsqueda
        if not encontrados:
            encontrados = [(p, 1) for p in profesionales]
        
        encontrados.sort(key=lambda x: x[1], reverse=True)
        encontrados = [e[0] for e in encontrados]
        
        resultado = "━" * 30 + "\n"
        resultado += "🤝 PROFESIONALES EN BUSQUEDA LABORAL\n"
        resultado += "━" * 30 + "\n\n"
        resultado += f"🔍 Busqueda: {query}\n"
        resultado += f"📊 Encontrados: {len(encontrados)} en busqueda laboral\n\n"
        resultado += "━" * 30 + "\n\n"
        
        for i, prof in enumerate(encontrados[:20], 1):
            resultado += f"{i}. {prof['nombre']}\n"
            if prof['profesion']:
                resultado += f"   🎯 {prof['profesion']}\n"
            resultado += f"   💼 Estado: {prof['situacion']}\n"
            if prof['industria1']:
                resultado += f"   🏢 {prof['industria1']}\n"
            if prof['industria2']:
                resultado += f"   🏢 {prof['industria2']}\n"
            if prof['telefono']:
                resultado += f"   📱 {prof['telefono']}\n"
            if prof['email']:
                resultado += f"   📧 {prof['email']}\n"
            resultado += "\n"
        
        if len(encontrados) > 20:
            resultado += f"📌 Mostrando 20 de {len(encontrados)} resultados\n"
        
        resultado += "━" * 30
        return resultado
        
    except Exception as e:
        logger.error(f"Error buscar_apoyo: {e}")
        return f"❌ Error: {str(e)[:150]}"


# ==================== SISTEMA DE CUMPLEAÑOS ====================

def obtener_cumpleanos_hoy():
    """
    Obtiene los cumpleaños del día desde el Excel de Google Drive.
    Columna X = Fecha cumpleaños (formato DD-MMM, ej: 15-Ene)
    """
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDS')
        if not creds_json:
            return None
        
        creds_dict = json.loads(creds_json)
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        access_token = creds.get_access_token().access_token
        headers = {'Authorization': f'Bearer {access_token}'}
        
        # Buscar archivo
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {
            'q': "name contains 'BD Grupo Laboral' and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false",
            'fields': 'files(id, name)'
        }
        
        response = requests.get(search_url, headers=headers, params=params, timeout=30)
        archivos = response.json().get('files', [])
        
        if not archivos:
            return None
        
        # Descargar Excel
        file_id = archivos[0]['id']
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        response = requests.get(download_url, headers=headers, timeout=60)
        
        if response.status_code != 200:
            return None
        
        df = pd.read_excel(BytesIO(response.content), engine='openpyxl', header=0)
        
        # Fecha de hoy
        hoy = datetime.now()
        dia_hoy = hoy.day
        mes_hoy = hoy.month
        
        # Mapeo de meses en español
        MESES = {
            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12,
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
            'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        
        cumpleaneros = []
        
        for idx, row in df.iterrows():
            try:
                # Columna X = índice 23
                fecha_cumple = row.iloc[23] if len(row) > 23 else None
                
                if pd.isna(fecha_cumple) or not fecha_cumple:
                    continue
                
                fecha_str = str(fecha_cumple).strip().lower()
                
                # Intentar parsear diferentes formatos
                dia_cumple = None
                mes_cumple = None
                
                # Formato DD-MMM (ej: 15-Ene)
                if '-' in fecha_str:
                    partes = fecha_str.split('-')
                    if len(partes) >= 2:
                        try:
                            dia_cumple = int(partes[0])
                            mes_str = partes[1].strip()[:3]
                            mes_cumple = MESES.get(mes_str)
                        except:
                            pass
                
                # Formato DD/MM
                elif '/' in fecha_str:
                    partes = fecha_str.split('/')
                    if len(partes) >= 2:
                        try:
                            dia_cumple = int(partes[0])
                            mes_cumple = int(partes[1])
                        except:
                            pass
                
                # Verificar si es hoy
                if dia_cumple == dia_hoy and mes_cumple == mes_hoy:
                    nombre = str(row.iloc[2]).strip() if len(row) > 2 else ''  # Columna C
                    apellido = str(row.iloc[3]).strip() if len(row) > 3 else ''  # Columna D
                    
                    if nombre and nombre.lower() not in ['nan', 'none', '']:
                        nombre_completo = f"{nombre} {apellido}".strip()
                        cumpleaneros.append(nombre_completo)
                        
            except Exception as e:
                continue
        
        return cumpleaneros
        
    except Exception as e:
        logger.error(f"Error obteniendo cumpleaños: {e}")
        return None


async def enviar_cumpleanos_diario(context: ContextTypes.DEFAULT_TYPE):
    """Tarea programada para enviar felicitaciones de cumpleaños a las 8:00 AM"""
    try:
        cumpleaneros = obtener_cumpleanos_hoy()
        
        if not cumpleaneros:
            logger.info("No hay cumpleaños hoy")
            return
        
        # Crear mensaje de cumpleaños
        fecha_hoy = datetime.now().strftime("%d/%m/%Y")
        
        mensaje = "🎂🎉 CUMPLEANOS DEL DIA! 🎉🎂\n"
        mensaje += "━" * 30 + "\n"
        mensaje += f"📅 {fecha_hoy}\n\n"
        
        mensaje += "🥳 Hoy celebramos a:\n\n"
        
        for nombre in cumpleaneros:
            mensaje += f"🎈 {nombre}\n"
        
        mensaje += "\n" + "━" * 30 + "\n"
        mensaje += "💐 Felicidades! Les deseamos un excelente dia.\n\n"
        mensaje += "👉 Saluda a los cumpleaneros en el subgrupo 'Cumpleanos, Eventos y Efemerides COFRADIA'"
        
        # Enviar al grupo SIN parse_mode
        if COFRADIA_GROUP_ID:
            await context.bot.send_message(
                chat_id=COFRADIA_GROUP_ID,
                text=mensaje
            )
            logger.info(f"✅ Enviado mensaje de cumpleaños: {len(cumpleaneros)} cumpleañeros")
        
    except Exception as e:
        logger.error(f"Error enviando cumpleaños: {e}")


async def enviar_resumen_nocturno(context: ContextTypes.DEFAULT_TYPE):
    """Tarea programada para enviar resumen del día a las 20:00"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("No se pudo conectar a BD para resumen nocturno")
            return
        
        c = conn.cursor()
        fecha_hoy = datetime.now().strftime("%d/%m/%Y")
        
        if DATABASE_URL:
            # Estadísticas del día
            c.execute("SELECT COUNT(*) as total FROM mensajes WHERE fecha >= CURRENT_DATE")
            total_hoy = c.fetchone()['total']
            
            c.execute("SELECT COUNT(DISTINCT user_id) as total FROM mensajes WHERE fecha >= CURRENT_DATE")
            usuarios_hoy = c.fetchone()['total']
            
            # Top usuarios del día
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        WHERE fecha >= CURRENT_DATE 
                        GROUP BY user_id ORDER BY msgs DESC LIMIT 5""")
            top_usuarios = [((r['nombre_completo'] or 'Usuario').strip(), r['msgs']) for r in c.fetchall()]
            
            # Categorías del día
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY total DESC LIMIT 5""")
            categorias = [(r['categoria'], r['total']) for r in c.fetchall()]
            
            # Mensajes por tema/subgrupo (topic_id)
            c.execute("""SELECT topic_id, COUNT(*) as msgs FROM mensajes 
                        WHERE fecha >= CURRENT_DATE AND topic_id IS NOT NULL
                        GROUP BY topic_id ORDER BY msgs DESC LIMIT 5""")
            por_tema = [(r['topic_id'], r['msgs']) for r in c.fetchall()]
            
            # Mensajes para análisis IA
            c.execute("""SELECT first_name, message, categoria FROM mensajes 
                        WHERE fecha >= CURRENT_DATE ORDER BY fecha DESC LIMIT 30""")
            mensajes_dia = [(r['first_name'], r['message'], r['categoria']) for r in c.fetchall()]
        else:
            c.execute("SELECT COUNT(*) FROM mensajes WHERE DATE(fecha) = DATE('now')")
            total_hoy = c.fetchone()[0]
            
            c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes WHERE DATE(fecha) = DATE('now')")
            usuarios_hoy = c.fetchone()[0]
            
            c.execute("""SELECT COALESCE(MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') AND first_name IS NOT NULL THEN first_name ELSE NULL END) || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), MAX(first_name), 'Usuario') as nombre_completo, 
                        COUNT(*) as msgs FROM mensajes 
                        WHERE DATE(fecha) = DATE('now') 
                        GROUP BY user_id ORDER BY msgs DESC LIMIT 5""")
            top_usuarios = c.fetchall()
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE DATE(fecha) = DATE('now') AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC LIMIT 5""")
            categorias = c.fetchall()
            
            c.execute("""SELECT topic_id, COUNT(*) FROM mensajes 
                        WHERE DATE(fecha) = DATE('now') AND topic_id IS NOT NULL
                        GROUP BY topic_id ORDER BY COUNT(*) DESC LIMIT 5""")
            por_tema = c.fetchall()
            
            c.execute("""SELECT first_name, message, categoria FROM mensajes 
                        WHERE DATE(fecha) = DATE('now') ORDER BY fecha DESC LIMIT 30""")
            mensajes_dia = c.fetchall()
        
        conn.close()
        
        if total_hoy == 0:
            logger.info("No hay mensajes hoy para el resumen nocturno")
            return
        
        # Construir mensaje de resumen nocturno
        mensaje = "━" * 30 + "\n"
        mensaje += "🌙 RESUMEN DEL DIA\n"
        mensaje += "━" * 30 + "\n\n"
        mensaje += f"📅 {fecha_hoy} | 🕗 20:00 hrs\n\n"
        
        mensaje += "📊 ACTIVIDAD DE HOY\n"
        mensaje += f"   💬 Mensajes: {total_hoy}\n"
        mensaje += f"   👥 Participantes: {usuarios_hoy}\n\n"
        
        if top_usuarios:
            mensaje += "🏆 MAS ACTIVOS\n"
            medallas = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣']
            for i, item in enumerate(top_usuarios[:5]):
                nombre = item[0] if isinstance(item, tuple) else item.get('nombre_completo', item.get('first_name', ''))
                msgs = item[1] if isinstance(item, tuple) else item.get('msgs', 0)
                nombre_limpio = str(nombre).replace('_', ' ').strip()
                if not nombre_limpio or nombre_limpio.lower() in ['group', 'grupo', 'channel', 'cofradía']:
                    nombre_limpio = "Usuario"
                mensaje += f"   {medallas[i]} {nombre_limpio}: {msgs}\n"
            mensaje += "\n"
        
        if categorias:
            # Usar IA para temas reales
            insights_temas = generar_insights_temas(dias=1)
            if insights_temas:
                mensaje += "🏷️ TEMAS DEL DIA\n"
                for tema in insights_temas:
                    tema_limpio = tema.replace('*', '').replace('_', '').strip()
                    if tema_limpio:
                        mensaje += f"   {tema_limpio}\n"
                mensaje += "\n"
            else:
                mensaje += "🏷️ TEMAS DEL DIA\n"
                emojis_cat = {'Empleo': '💼', 'Networking': '🤝', 'Consulta': '❓', 
                            'Emprendimiento': '🚀', 'Evento': '📅', 'Saludo': '👋', 'General': '💬'}
                for cat, count in categorias[:5]:
                    emoji = emojis_cat.get(cat, '📌')
                    mensaje += f"   {emoji} {cat}: {count}\n"
                mensaje += "\n"
        
        # Generar insights con IA si está disponible
        if ia_disponible and mensajes_dia:
            contexto = "\n".join([f"- {m[0]}: {m[1][:60]}" for m in mensajes_dia[:15]])
            
            prompt = f"""Resume la actividad del día en Cofradía de Networking en 3-4 puntos clave:
{contexto}

Menciona brevemente: temas discutidos, tendencias, oportunidades de networking.
Máximo 100 palabras. Sin introducción. No uses asteriscos ni guiones bajos."""
            
            insights = llamar_groq(prompt, max_tokens=200, temperature=0.3)
            
            if insights:
                insights_limpio = insights.replace('*', '').replace('_', ' ')
                mensaje += "💡 RESUMEN IA\n"
                mensaje += insights_limpio + "\n\n"
        
        mensaje += "━" * 30 + "\n"
        mensaje += "🌟 Gracias por participar! Nos vemos manana.\n"
        mensaje += "━" * 30
        
        # Enviar al grupo SIN parse_mode para evitar errores
        if COFRADIA_GROUP_ID:
            await context.bot.send_message(
                chat_id=COFRADIA_GROUP_ID,
                text=mensaje
            )
            logger.info(f"✅ Enviado resumen nocturno: {total_hoy} mensajes del día")
        
    except Exception as e:
        logger.error(f"Error enviando resumen nocturno: {e}")


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
    async def post_init(app):
        """Eliminar webhook anterior para evitar error Conflict en Render"""
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            logger.info("🧹 Webhook anterior eliminado - sin conflictos")
        except Exception as e:
            logger.warning(f"Nota al limpiar webhook: {e}")
    
    application = Application.builder().token(TOKEN_BOT).post_init(post_init).build()
    
    # Configurar comandos (SIN mostrar mi_cuenta, renovar, activar - son privados)
    async def setup_commands(app):
        commands = [
            BotCommand("start", "Iniciar bot"),
            BotCommand("ayuda", "Ver comandos"),
            BotCommand("registrarse", "Activar cuenta"),
            BotCommand("buscar", "Buscar en historial"),
            BotCommand("buscar_ia", "Búsqueda con IA"),
            BotCommand("buscar_profesional", "Buscar profesionales"),
            BotCommand("buscar_apoyo", "Buscar en busqueda laboral"),
            BotCommand("buscar_especialista_sec", "Buscar en SEC"),
            BotCommand("graficos", "Ver gráficos"),
            BotCommand("empleo", "Buscar empleos"),
            BotCommand("estadisticas", "Ver estadísticas"),
        ]
        try:
            await app.bot.set_my_commands(commands)
            
            if COFRADIA_GROUP_ID:
                from telegram import BotCommandScopeChat
                comandos_grupo = [
                    BotCommand("registrarse", "Activar cuenta"),
                    BotCommand("buscar", "Buscar"),
                    BotCommand("buscar_ia", "Búsqueda IA"),
                    BotCommand("buscar_profesional", "Buscar profesionales"),
                    BotCommand("buscar_apoyo", "Buscar en busqueda laboral"),
                    BotCommand("buscar_especialista_sec", "Buscar en SEC"),
                    BotCommand("graficos", "Ver gráficos"),
                    BotCommand("empleo", "Buscar empleos"),
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
    
    # Handlers básicos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ayuda", ayuda))
    application.add_handler(CommandHandler("registrarse", registrarse_comando))
    application.add_handler(CommandHandler("mi_cuenta", mi_cuenta_comando))
    application.add_handler(CommandHandler("renovar", renovar_comando))
    application.add_handler(CommandHandler("activar", activar_codigo_comando))
    
    # Handlers de búsqueda
    application.add_handler(CommandHandler("buscar", buscar_comando))
    application.add_handler(CommandHandler("buscar_ia", buscar_ia_comando))
    application.add_handler(CommandHandler("buscar_profesional", buscar_profesional_comando))
    application.add_handler(CommandHandler("buscar_apoyo", buscar_apoyo_comando))
    application.add_handler(CommandHandler("buscar_especialista_sec", buscar_especialista_sec_comando))
    application.add_handler(CommandHandler("empleo", empleo_comando))
    
    # Handlers de estadísticas
    application.add_handler(CommandHandler("graficos", graficos_comando))
    application.add_handler(CommandHandler("estadisticas", estadisticas_comando))
    application.add_handler(CommandHandler("top_usuarios", top_usuarios_comando))
    application.add_handler(CommandHandler("categorias", categorias_comando))
    application.add_handler(CommandHandler("mi_perfil", mi_perfil_comando))
    
    # Handlers de resumen
    application.add_handler(CommandHandler("resumen", resumen_comando))
    application.add_handler(CommandHandler("resumen_semanal", resumen_semanal_comando))
    application.add_handler(CommandHandler("resumen_mes", resumen_mes_comando))
    
    # Handlers de RRHH
    application.add_handler(CommandHandler("dotacion", dotacion_comando))
    
    # Handlers admin
    application.add_handler(CommandHandler("cobros_admin", cobros_admin_comando))
    application.add_handler(CommandHandler("generar_codigo", generar_codigo_comando))
    
    # Handlers RAG PDF
    application.add_handler(CommandHandler("subir_pdf", subir_pdf_comando))
    application.add_handler(CommandHandler("rag_status", rag_status_comando))
    application.add_handler(CommandHandler("rag_consulta", rag_consulta_comando))
    application.add_handler(CommandHandler("rag_reindexar", rag_reindexar_comando))
    application.add_handler(CommandHandler("eliminar_pdf", eliminar_pdf_comando))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(callback_plan, pattern='^plan_'))
    application.add_handler(CallbackQueryHandler(callback_generar_codigo, pattern='^gencodigo_'))
    application.add_handler(CallbackQueryHandler(callback_aprobar_rechazar, pattern='^(aprobar|rechazar)_'))
    
    # Mensajes y documentos
    application.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, recibir_comprobante))
    application.add_handler(MessageHandler(filters.Document.PDF & filters.ChatType.PRIVATE, recibir_documento_pdf))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'@'), responder_mencion))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS & ~filters.COMMAND, guardar_mensaje_grupo))
    
    # Programar tarea de cumpleaños diaria a las 8:00 AM (hora Chile)
    job_queue = application.job_queue
    if job_queue:
        from datetime import time as dt_time
        try:
            from zoneinfo import ZoneInfo
            chile_tz = ZoneInfo('America/Santiago')
        except ImportError:
            try:
                import pytz
                chile_tz = pytz.timezone('America/Santiago')
            except ImportError:
                chile_tz = None
                logger.warning("⚠️ No se pudo cargar timezone Chile, usando UTC offsets")
        
        # Cumpleaños a las 8:00 AM hora Chile
        if chile_tz:
            job_queue.run_daily(
                enviar_cumpleanos_diario,
                time=dt_time(hour=8, minute=0, second=0, tzinfo=chile_tz),
                name='cumpleanos_diario'
            )
        else:
            job_queue.run_daily(
                enviar_cumpleanos_diario,
                time=dt_time(hour=12, minute=0, second=0),  # ~8AM Chile
                name='cumpleanos_diario'
            )
        logger.info("🎂 Tarea de cumpleaños programada para las 8:00 AM Chile")
        
        # Resumen nocturno a las 20:00 hora Chile
        if chile_tz:
            job_queue.run_daily(
                enviar_resumen_nocturno,
                time=dt_time(hour=20, minute=0, second=0, tzinfo=chile_tz),
                name='resumen_nocturno'
            )
        else:
            job_queue.run_daily(
                enviar_resumen_nocturno,
                time=dt_time(hour=0, minute=0, second=0),  # ~20:00 Chile
                name='resumen_nocturno'
            )
        logger.info("🌙 Tarea de resumen nocturno programada para las 20:00 Chile")
        
        # RAG indexación cada 6 horas
        job_queue.run_repeating(
            indexar_rag_job,
            interval=21600,  # 6 horas en segundos
            first=60,  # Primera ejecución después de 60 segundos
            name='rag_indexacion'
        )
        logger.info("🧠 Tarea de indexación RAG programada cada 6 horas")
    
    logger.info("✅ Bot iniciado!")
    
    application.run_polling(
        allowed_updates=Update.ALL_TYPES, 
        drop_pending_updates=True
    )


if __name__ == '__main__':
    main()
