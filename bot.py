#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot Cofradía Premium - Versión con Supabase PostgreSQL
Desarrollado para @Cofradia_de_Networking
"""

import os
import math
import re
import io
import json
import logging
import secrets
import string
import threading
import base64
import asyncio
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

try:
    from PIL import Image, ImageDraw, ImageFont
    pil_disponible = True
except ImportError:
    pil_disponible = False
    logging.warning("⚠️ Pillow no instalado - tarjetas imagen no disponibles")

try:
    import qrcode
    qr_disponible = True
except ImportError:
    qr_disponible = False
    logging.warning("⚠️ qrcode no instalado - QR en tarjetas no disponible")

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, MenuButtonCommands
from telegram.ext import (
    Application, MessageHandler, CommandHandler, 
    filters, ContextTypes, CallbackQueryHandler,
    ConversationHandler, ChatJoinRequestHandler
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
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')  # Obtener en platform.deepseek.com (gratis)
TOKEN_BOT = os.environ.get('TOKEN_BOT')
OWNER_ID = int(os.environ.get('OWNER_TELEGRAM_ID', '0'))
COFRADIA_GROUP_ID = int(os.environ.get('COFRADIA_GROUP_ID', '0'))
COFRADIA_INVITE_LINK = os.environ.get('COFRADIA_INVITE_LINK', 'https://t.me/+MSQuQxeVpsExMThh')
DATABASE_URL = os.environ.get('DATABASE_URL')  # URL de Supabase PostgreSQL
BOT_USERNAME = "Cofradia_Premium_Bot"
DIAS_PRUEBA_GRATIS = 90

# Estados de conversación para onboarding
ONBOARD_NOMBRE, ONBOARD_GENERACION, ONBOARD_RECOMENDADO, ONBOARD_PREGUNTA4, ONBOARD_PREGUNTA5, ONBOARD_PREGUNTA6 = range(6)

# ==================== CONFIGURACIÓN DE LLMs ====================
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

# ==================== CONFIGURACIÓN DE GEMINI (OCR + texto fallback) ====================
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
GEMINI_TEXT_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

# ==================== CONFIGURACIÓN DE JSEARCH (EMPLEOS REALES) ====================
RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY')
JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"

# ==================== CONFIGURACIÓN CMF (Comisión para el Mercado Financiero) ====================
# API Key CMF — registrar en https://api.cmfchile.cl  (gratuita)
CMF_API_KEY  = os.environ.get('CMF_API_KEY', 'SBIF9990SBIF44b7SBIF7f4c5a537d02358e1099')
CMF_BASE_URL = 'https://api.cmfchile.cl/api-sbifv3/recursos_api'

# ==================== CONFIGURACIÓN AFP (QueTalMiAFP) ====================
AFP_API_KEY    = '5afad3802d3f2936c722fd41e9932eb34cef39468e'
AFP_CUOTAS_URL = 'https://www.quetalmiafp.cl/api/Cuota/ObtenerCuotas'
AFP_LISTA      = 'CAPITAL,CUPRUM,HABITAT,MODELO,PLANVITAL,PROVIDA,UNO'
AFP_FONDOS     = 'A,B,C,D,E'

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

# Gemini 2.0 Flash — fallback gratuito (reemplaza DeepSeek)
deepseek_disponible = False
gemini_texto_disponible = False
if GEMINI_API_KEY:
    gemini_disponible = True
    gemini_texto_disponible = True
    if not ia_disponible:
        ia_disponible = True
    logger.info("✅ Gemini 2.0 Flash activo (texto + OCR)")
else:
    logger.warning("⚠️ GEMINI_API_KEY no configurada")

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
            try:
                c.execute("ALTER TABLE suscripciones ADD COLUMN IF NOT EXISTS fecha_incorporacion TIMESTAMP")
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
            
            c.execute('''CREATE TABLE IF NOT EXISTS nuevos_miembros (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                username TEXT,
                nombre TEXT,
                apellido TEXT,
                generacion TEXT,
                recomendado_por TEXT,
                estado TEXT DEFAULT 'pendiente',
                fecha_solicitud TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_aprobacion TIMESTAMP
            )''')
            
            # === NUEVAS TABLAS v3.0 ===
            
            c.execute('''CREATE TABLE IF NOT EXISTS tarjetas_profesional (
                user_id BIGINT PRIMARY KEY,
                nombre_completo TEXT,
                profesion TEXT,
                empresa TEXT,
                servicios TEXT,
                telefono TEXT,
                email TEXT,
                ciudad TEXT,
                linkedin TEXT,
                nro_kdt TEXT DEFAULT '',
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            try:
                c.execute("ALTER TABLE tarjetas_profesional ADD COLUMN IF NOT EXISTS nro_kdt TEXT DEFAULT ''")
            except Exception:
                pass
            
            c.execute('''CREATE TABLE IF NOT EXISTS alertas_usuario (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                palabras_clave TEXT,
                activa BOOLEAN DEFAULT TRUE,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS anuncios (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                nombre_autor TEXT,
                categoria TEXT DEFAULT 'general',
                titulo TEXT,
                descripcion TEXT,
                contacto TEXT,
                fecha_publicacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_expiracion TIMESTAMP,
                activo BOOLEAN DEFAULT TRUE
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS eventos (
                id SERIAL PRIMARY KEY,
                titulo TEXT,
                descripcion TEXT,
                fecha_evento TIMESTAMP,
                lugar TEXT,
                creado_por BIGINT,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                activo BOOLEAN DEFAULT TRUE
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS eventos_asistencia (
                id SERIAL PRIMARY KEY,
                evento_id INTEGER REFERENCES eventos(id),
                user_id BIGINT,
                nombre TEXT,
                confirmado BOOLEAN DEFAULT TRUE,
                fecha_confirmacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS recomendaciones (
                id SERIAL PRIMARY KEY,
                autor_id BIGINT,
                autor_nombre TEXT,
                destinatario_id BIGINT,
                destinatario_nombre TEXT,
                texto TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS consultas_cofrades (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                nombre_autor TEXT,
                titulo TEXT,
                descripcion TEXT,
                anonima BOOLEAN DEFAULT FALSE,
                resuelta BOOLEAN DEFAULT FALSE,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS respuestas_consultas (
                id SERIAL PRIMARY KEY,
                consulta_id INTEGER REFERENCES consultas_cofrades(id),
                user_id BIGINT,
                nombre_autor TEXT,
                respuesta TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            logger.info("✅ Base de datos PostgreSQL (Supabase) inicializada con migraciones v3.0")
            
            # === NUEVAS TABLAS v4.0: Cofradía Coins y Servicios Premium ===
            c.execute('''CREATE TABLE IF NOT EXISTS cofradia_coins (
                user_id BIGINT PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                total_ganado INTEGER DEFAULT 0,
                total_gastado INTEGER DEFAULT 0,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS coins_historial (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                cantidad INTEGER,
                tipo TEXT,
                descripcion TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS precios_servicios (
                servicio TEXT PRIMARY KEY,
                precio_pesos INTEGER DEFAULT 0,
                precio_coins INTEGER DEFAULT 0,
                descripcion TEXT,
                activo BOOLEAN DEFAULT TRUE
            )''')
            for srv, pesos, coins, desc in [
                ('generar_cv', 2500, 25, 'Generador de CV profesional con IA'),
                ('entrevista', 5000, 50, 'Simulador de entrevista laboral'),
                ('analisis_linkedin', 3000, 30, 'Análisis de perfil LinkedIn con IA'),
                ('mentor', 4000, 40, 'Plan de mentoría IA personalizado'),
            ]:
                c.execute("""INSERT INTO precios_servicios (servicio, precio_pesos, precio_coins, descripcion)
                            VALUES (%s, %s, %s, %s) ON CONFLICT (servicio) DO UPDATE SET 
                            precio_pesos = EXCLUDED.precio_pesos, precio_coins = EXCLUDED.precio_coins""", (srv, pesos, coins, desc))
            # Eliminar mi_dashboard de premium si existía
            c.execute("DELETE FROM precios_servicios WHERE servicio = 'mi_dashboard'")
            conn.commit()
            logger.info("✅ Tablas v4.0 (Coins, Precios) inicializadas")
            
            # === TABLAS v5.0: Agente Inteligente de Networking ===
            c.execute('''CREATE TABLE IF NOT EXISTS agenda_personal (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                titulo TEXT,
                descripcion TEXT,
                fecha_evento TIMESTAMP,
                lugar TEXT DEFAULT '',
                tipo TEXT DEFAULT 'reunion',
                participantes TEXT DEFAULT '',
                recordatorio_enviado BOOLEAN DEFAULT FALSE,
                completada BOOLEAN DEFAULT FALSE,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS tareas_networking (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                tarea TEXT,
                prioridad TEXT DEFAULT 'normal',
                categoria TEXT DEFAULT 'general',
                completada BOOLEAN DEFAULT FALSE,
                fecha_vencimiento TIMESTAMP,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS conexiones_propuestas (
                id SERIAL PRIMARY KEY,
                user_id_a BIGINT,
                user_id_b BIGINT,
                razon TEXT,
                aceptada BOOLEAN DEFAULT NULL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.commit()
            logger.info("✅ Tablas v5.0 (Agente Networking) inicializadas")
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
            try:
                c.execute("ALTER TABLE suscripciones ADD COLUMN fecha_incorporacion DATETIME")
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
            
            c.execute('''CREATE TABLE IF NOT EXISTS nuevos_miembros (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                nombre TEXT,
                apellido TEXT,
                generacion TEXT,
                recomendado_por TEXT,
                estado TEXT DEFAULT 'pendiente',
                fecha_solicitud DATETIME DEFAULT CURRENT_TIMESTAMP,
                fecha_aprobacion DATETIME
            )''')
            
            # === NUEVAS TABLAS v3.0 ===
            
            c.execute('''CREATE TABLE IF NOT EXISTS tarjetas_profesional (
                user_id INTEGER PRIMARY KEY,
                nombre_completo TEXT,
                profesion TEXT,
                empresa TEXT,
                servicios TEXT,
                telefono TEXT,
                email TEXT,
                ciudad TEXT,
                linkedin TEXT,
                nro_kdt TEXT DEFAULT '',
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            try:
                c.execute("ALTER TABLE tarjetas_profesional ADD COLUMN nro_kdt TEXT DEFAULT ''")
            except Exception:
                pass
            
            c.execute('''CREATE TABLE IF NOT EXISTS alertas_usuario (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                palabras_clave TEXT,
                activa INTEGER DEFAULT 1,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS anuncios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                nombre_autor TEXT,
                categoria TEXT DEFAULT 'general',
                titulo TEXT,
                descripcion TEXT,
                contacto TEXT,
                fecha_publicacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                fecha_expiracion DATETIME,
                activo INTEGER DEFAULT 1
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS eventos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                titulo TEXT,
                descripcion TEXT,
                fecha_evento DATETIME,
                lugar TEXT,
                creado_por INTEGER,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                activo INTEGER DEFAULT 1
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS eventos_asistencia (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evento_id INTEGER,
                user_id INTEGER,
                nombre TEXT,
                confirmado INTEGER DEFAULT 1,
                fecha_confirmacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS recomendaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                autor_id INTEGER,
                autor_nombre TEXT,
                destinatario_id INTEGER,
                destinatario_nombre TEXT,
                texto TEXT,
                fecha DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS consultas_cofrades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                nombre_autor TEXT,
                titulo TEXT,
                descripcion TEXT,
                anonima INTEGER DEFAULT 0,
                resuelta INTEGER DEFAULT 0,
                fecha DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS respuestas_consultas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consulta_id INTEGER,
                user_id INTEGER,
                nombre_autor TEXT,
                respuesta TEXT,
                fecha DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            
            logger.info("✅ Base de datos SQLite inicializada con migraciones v3.0 (modo local)")
            
            # === NUEVAS TABLAS v4.0 SQLite ===
            c.execute('''CREATE TABLE IF NOT EXISTS cofradia_coins (
                user_id INTEGER PRIMARY KEY, balance INTEGER DEFAULT 0,
                total_ganado INTEGER DEFAULT 0, total_gastado INTEGER DEFAULT 0,
                fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS coins_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
                cantidad INTEGER, tipo TEXT, descripcion TEXT,
                fecha DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS precios_servicios (
                servicio TEXT PRIMARY KEY, precio_pesos INTEGER DEFAULT 0,
                precio_coins INTEGER DEFAULT 0, descripcion TEXT, activo INTEGER DEFAULT 1
            )''')
            for srv, pesos, coins, desc in [
                ('generar_cv', 2500, 25, 'Generador de CV profesional con IA'),
                ('entrevista', 5000, 50, 'Simulador de entrevista laboral'),
                ('analisis_linkedin', 3000, 30, 'Análisis de perfil LinkedIn con IA'),
                ('mentor', 4000, 40, 'Plan de mentoría IA personalizado'),
            ]:
                c.execute("INSERT OR REPLACE INTO precios_servicios (servicio, precio_pesos, precio_coins, descripcion) VALUES (?,?,?,?)",
                         (srv, pesos, coins, desc))
            c.execute("DELETE FROM precios_servicios WHERE servicio = 'mi_dashboard'")
            conn.commit()
            logger.info("✅ Tablas v4.0 SQLite inicializadas")
            
            # === TABLAS v5.0 SQLite: Agente Inteligente ===
            c.execute('''CREATE TABLE IF NOT EXISTS agenda_personal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, titulo TEXT, descripcion TEXT,
                fecha_evento DATETIME, lugar TEXT DEFAULT '',
                tipo TEXT DEFAULT 'reunion', participantes TEXT DEFAULT '',
                recordatorio_enviado INTEGER DEFAULT 0,
                completada INTEGER DEFAULT 0,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS tareas_networking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, tarea TEXT,
                prioridad TEXT DEFAULT 'normal', categoria TEXT DEFAULT 'general',
                completada INTEGER DEFAULT 0,
                fecha_vencimiento DATETIME,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS conexiones_propuestas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id_a INTEGER, user_id_b INTEGER, razon TEXT,
                aceptada INTEGER DEFAULT NULL,
                fecha DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.commit()
            logger.info("✅ Tablas v5.0 SQLite (Agente) inicializadas")
        
        # ═══ SETUP OWNER: INSERT + UPDATE (causa raíz: owner nunca pasaba por registrar_usuario) ═══
        try:
            if OWNER_ID and OWNER_ID != 0:
                c2 = conn.cursor()
                owner_int = int(OWNER_ID)
                logger.info(f"🔧 Setup owner ID={owner_int}")
                
                # PASO 0: INSERTAR owner en suscripciones SI NO EXISTE
                if DATABASE_URL:
                    c2.execute("SELECT user_id FROM suscripciones WHERE user_id = %s", (owner_int,))
                    if not c2.fetchone():
                        c2.execute("""INSERT INTO suscripciones 
                            (user_id, first_name, last_name, username, es_admin, fecha_registro,
                             fecha_expiracion, estado, mensajes_engagement, servicios_usados,
                             fecha_incorporacion)
                            VALUES (%s, 'Germán', 'Perey', '', 1, '2020-09-22',
                                    '2099-12-31 23:59:59', 'activo', 0, '[]', '2020-09-22')""",
                            (owner_int,))
                        logger.info("✅ Owner INSERTADO en suscripciones (fecha_inc=2020-09-22)")
                else:
                    c2.execute("SELECT user_id FROM suscripciones WHERE user_id = ?", (owner_int,))
                    if not c2.fetchone():
                        c2.execute("""INSERT INTO suscripciones 
                            (user_id, first_name, last_name, username, es_admin, fecha_registro,
                             fecha_expiracion, estado, mensajes_engagement, servicios_usados,
                             fecha_incorporacion)
                            VALUES (?, 'Germán', 'Perey', '', 1, '2020-09-22',
                                    '2099-12-31 23:59:59', 'activo', 0, '[]', '2020-09-22')""",
                            (owner_int,))
                        logger.info("✅ Owner INSERTADO en suscripciones SQLite")
                
                # PASO 1: Transferir mensajes admin anónimo
                if DATABASE_URL:
                    c2.execute("""UPDATE mensajes SET user_id = %s, first_name = 'Germán', last_name = 'Perey' 
                                WHERE first_name IN ('Group', 'Grupo', 'Cofradía', 'Cofradía de Networking')""", 
                              (owner_int,))
                    c2.execute("""UPDATE mensajes SET first_name = 'Germán', last_name = 'Perey' 
                                WHERE user_id = %s""", (owner_int,))
                else:
                    c2.execute("""UPDATE mensajes SET user_id = ?, first_name = 'Germán', last_name = 'Perey' 
                                WHERE first_name IN ('Group', 'Grupo', 'Cofradía')""", (owner_int,))
                    c2.execute("""UPDATE mensajes SET first_name = 'Germán', last_name = 'Perey' 
                                WHERE user_id = ?""", (owner_int,))
                
                # PASO 2: FORZAR datos owner (la fila ya existe por PASO 0)
                if DATABASE_URL:
                    c2.execute("""UPDATE suscripciones SET first_name = 'Germán', last_name = 'Perey',
                                fecha_expiracion = '2099-12-31 23:59:59', estado = 'activo',
                                fecha_incorporacion = '2020-09-22'
                                WHERE user_id = %s""", (owner_int,))
                else:
                    c2.execute("""UPDATE suscripciones SET first_name = 'Germán', last_name = 'Perey',
                                fecha_expiracion = '2099-12-31 23:59:59', estado = 'activo',
                                fecha_incorporacion = '2020-09-22'
                                WHERE user_id = ?""", (owner_int,))
                
                # PASO 3: Asegurar nuevos_miembros con generacion 2000
                try:
                    if DATABASE_URL:
                        c2.execute("SELECT id FROM nuevos_miembros WHERE user_id = %s LIMIT 1", (owner_int,))
                        if not c2.fetchone():
                            c2.execute("""INSERT INTO nuevos_miembros 
                                (user_id, nombre, apellido, generacion, recomendado_por, estado, fecha_solicitud)
                                VALUES (%s, 'Germán', 'Perey', '2000', 'Fundador', 'aprobado', '2020-09-22')""", (owner_int,))
                            logger.info("✅ Owner en nuevos_miembros gen=2000")
                        else:
                            c2.execute("UPDATE nuevos_miembros SET generacion = '2000', nombre = 'Germán', apellido = 'Perey' WHERE user_id = %s", (owner_int,))
                    else:
                        c2.execute("SELECT id FROM nuevos_miembros WHERE user_id = ? LIMIT 1", (owner_int,))
                        if not c2.fetchone():
                            c2.execute("""INSERT INTO nuevos_miembros 
                                (user_id, nombre, apellido, generacion, recomendado_por, estado, fecha_solicitud)
                                VALUES (?, 'Germán', 'Perey', '2000', 'Fundador', 'aprobado', '2020-09-22')""", (owner_int,))
                        else:
                            c2.execute("UPDATE nuevos_miembros SET generacion = '2000', nombre = 'Germán', apellido = 'Perey' WHERE user_id = ?", (owner_int,))
                except Exception as e_nm:
                    logger.warning(f"Error en owner nuevos_miembros: {e_nm}")
                
                conn.commit()
                logger.info("✅ Owner setup COMPLETO: suscripción + fecha + gen")
        except Exception as e:
            logger.warning(f"Error configurando owner: {e}")
        
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
    
    # FALLBACK: Groq falló → Gemini 2.0 Flash
    logger.warning("⚠️ Groq falló → Gemini 2.0 Flash")
    return llamar_gemini_texto(prompt, max_tokens, temperature)


def llamar_deepseek(prompt: str, max_tokens: int = 1024, temperature: float = 0.7) -> str:
    """Redirige a Gemini 2.0 Flash."""
    return llamar_gemini_texto(prompt, max_tokens, temperature)


def llamar_gemini_texto(prompt: str, max_tokens: int = 1024, temperature: float = 0.7) -> str:
    """Gemini 2.0 Flash — fallback de Groq (1500 req/día, gratis)."""
    if not GEMINI_API_KEY:
        logger.warning("⚠️ GEMINI_API_KEY no configurada")
        return None
    try:
        url = f"{GEMINI_TEXT_URL}?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{
                "parts": [{
                    "text": (
                        "Eres el asistente de IA de Cofradía de Networking, "
                        "comunidad profesional chilena de alto nivel. "
                        "Responde siempre en español chileno, profesional y cercano.\n\n"
                        + prompt
                    )
                }]
            }],
            "generationConfig": {
                "maxOutputTokens": max_tokens,
                "temperature": temperature,
            }
        }
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code == 200:
            texto = r.json()["candidates"][0]["content"]["parts"][0]["text"]
            if texto and texto.strip():
                logger.info("✅ Respuesta Gemini 2.0 Flash")
                return texto.strip()
        else:
            logger.warning(f"Gemini error: {r.status_code} — {r.text[:300]}")
    except Exception as e:
        logger.warning(f"Error Gemini: {e}")
    return None


# ==================== FUNCIONES DE VOZ (STT + TTS) ====================

# Configuración de voz
VOZ_TTS = os.environ.get('VOZ_TTS', 'es-CL-CatalinaNeural')  # Voz chilena femenina
GROQ_WHISPER_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
GROQ_WHISPER_MODEL = "whisper-large-v3-turbo"

def transcribir_audio_groq(audio_bytes: bytes, filename: str = "audio.ogg") -> str:
    """Transcribe audio a texto usando Groq Whisper API"""
    if not GROQ_API_KEY:
        logger.warning("GROQ_API_KEY no disponible para Whisper")
        return None
    
    try:
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
        files = {
            'file': (filename, audio_bytes, 'audio/ogg'),
        }
        data = {
            'model': GROQ_WHISPER_MODEL,
            'language': 'es',  # Español
            'response_format': 'json',
            'temperature': 0.0,
            'prompt': 'Transcripción de mensaje de voz en español chileno sobre networking profesional.'
        }
        
        response = requests.post(GROQ_WHISPER_URL, headers=headers, files=files, data=data, timeout=30)
        
        if response.status_code == 200:
            resultado = response.json()
            texto = resultado.get('text', '').strip()
            if texto:
                logger.info(f"🎤 Whisper transcribió: {texto[:80]}...")
                return texto
            else:
                logger.warning("Whisper devolvió texto vacío")
                return None
        else:
            logger.error(f"Error Whisper API: {response.status_code} - {response.text[:200]}")
            return None
    except Exception as e:
        logger.error(f"Error transcribiendo audio: {e}")
        return None


async def generar_audio_tts(texto: str, filename: str = "/tmp/respuesta_tts.mp3") -> str:
    """Genera audio con voz natural — usa Chatterbox (HF) con fallback a edge-TTS"""
    import re

    # Limitar largo del texto
    if len(texto) > 2000:
        texto = texto[:1997] + "..."

    # Limpiar emojis y símbolos
    texto_voz = re.sub(r'[📊📈📉💰🪙🏅🏆⭐🔵🟢⚪💬💡📱📧🔗📇💎📋📅🔔📢🎂🎉👤👥🎤🔍💼🏢📍🛠️✅❌⏰♾️✏️━═⚓]', '', texto)
    texto_voz = re.sub(r'[#*_~`|]', '', texto_voz)
    texto_voz = texto_voz.replace(' ej:', ', por ejemplo:')
    texto_voz = texto_voz.replace(' Ej:', ', por ejemplo:')
    texto_voz = texto_voz.replace(' etc.', ', etcétera.')
    texto_voz = texto_voz.replace(' vs ', ' versus ')
    texto_voz = re.sub(r'\s+', ' ', texto_voz).strip()

    # ── INTENTO 1: Chatterbox vía Hugging Face Space (voz ultra-natural) ──
    try:
        from tts_chatterbox import texto_a_voz
        audio_bytes = await texto_a_voz(texto_voz, estilo="normal")
        if audio_bytes:
            # Guardar bytes en archivo temporal
            ext = ".ogg" if audio_bytes[:4] == b'OggS' or audio_bytes[:4] != b'RIFF' else ".wav"
            fname = filename.replace(".mp3", ext)
            with open(fname, "wb") as f:
                f.write(audio_bytes)
            if os.path.getsize(fname) > 0:
                logger.info(f"🎙️ Audio Chatterbox generado: {os.path.getsize(fname)} bytes")
                return fname
    except Exception as e:
        logger.warning(f"⚠️ Chatterbox falló, usando Catalina: {e}")

    # ── INTENTO 2: edge-TTS mejorado (Catalina más natural) ──
    try:
        import edge_tts
        # Pausas naturales
        texto_voz = texto_voz.replace('. ', '... ')
        texto_voz = texto_voz.replace(': ', ':... ')
        texto_voz = texto_voz.replace('; ', ';... ')

        communicate = edge_tts.Communicate(
            texto_voz,
            VOZ_TTS,
            rate  = "-12%",   # más pausada y natural
            pitch = "-4Hz",   # tono más grave y humano
            volume = "+8%"    # más presencia
        )
        await communicate.save(filename)
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            logger.info(f"🔊 Audio edge-TTS generado: {os.path.getsize(filename)} bytes")
            return filename
    except Exception as e:
        logger.error(f"Error generando TTS: {e}")

    return None


# Mapeo de comandos por voz → comandos reales del bot
COMANDOS_VOZ = {
    # Búsqueda
    'buscar': 'buscar',
    'buscar ia': 'buscar_ia',
    'buscar inteligente': 'buscar_ia',
    'búsqueda ia': 'buscar_ia',
    'búsqueda inteligente': 'buscar_ia',
    'busqueda ia': 'buscar_ia',
    'rag consulta': 'rag_consulta',
    'consulta rag': 'rag_consulta',
    'consultar documentos': 'rag_consulta',
    'consultar libros': 'rag_consulta',
    'buscar profesional': 'buscar_profesional',
    'buscar profesionales': 'buscar_profesional',
    'buscar apoyo': 'buscar_apoyo',
    'buscar especialista': 'buscar_especialista_sec',
    'buscar especialista sec': 'buscar_especialista_sec',
    'empleo': 'empleo',
    'buscar empleo': 'empleo',
    # Directorio
    'mi tarjeta': 'mi_tarjeta',
    'tarjeta': 'mi_tarjeta',
    'directorio': 'directorio',
    'conectar': 'conectar',
    'conexiones': 'conectar',
    'conexiones inteligentes': 'conectar',
    # Alertas
    'alertas': 'alertas',
    'mis alertas': 'alertas',
    'alerta': 'alertas',
    # Comunidad
    'publicar': 'publicar',
    'anuncios': 'anuncios',
    'eventos': 'eventos',
    'consultas': 'consultas',
    'consultas abiertas': 'consultas',
    'mis recomendaciones': 'mis_recomendaciones',
    'recomendaciones': 'mis_recomendaciones',
    'encuesta': 'encuesta',
    # Estadísticas
    'gráficos': 'graficos',
    'graficos': 'graficos',
    'estadísticas': 'estadisticas',
    'estadisticas': 'estadisticas',
    'top usuarios': 'top_usuarios',
    'ranking': 'top_usuarios',
    'mi perfil': 'mi_perfil',
    'perfil': 'mi_perfil',
    # Indicadores económicos
    'indicadores': 'indicadores',
    'indicadores económicos': 'indicadores',
    'indicadores economicos': 'indicadores',
    'economía': 'indicadores',
    'economia chile': 'indicadores',
    'dólar hoy': 'indicadores',
    'dolar hoy': 'indicadores',
    'uf hoy': 'indicadores',
    'ipc': 'indicadores',
    'tpm': 'indicadores',
    # Resúmenes
    'resumen': 'resumen',
    'resumen del día': 'resumen',
    'resumen del dia': 'resumen',
    'resumen semanal': 'resumen_semanal',
    'resumen de la semana': 'resumen_semanal',
    'resumen mes': 'resumen_mes',
    'resumen mensual': 'resumen_mes',
    'resumen del mes': 'resumen_mes',
    # Grupo
    'dotación': 'dotacion',
    'dotacion': 'dotacion',
    'categorías': 'categorias',
    'categorias': 'categorias',
    'cumpleaños': 'cumpleanos_mes',
    'cumpleaños mes': 'cumpleanos_mes',
    'cumpleaños del mes': 'cumpleanos_mes',
    'cumpleanos': 'cumpleanos_mes',
    'cumpleanos mes': 'cumpleanos_mes',
    'mi cuenta': 'mi_cuenta',
    'ayuda': 'ayuda',
    # Admin
    'rag status': 'rag_status',
    'estado rag': 'rag_status',
    'estado del rag': 'rag_status',
    'rag backup': 'rag_backup',
    'respaldo rag': 'rag_backup',
    'backup rag': 'rag_backup',
    'ver solicitudes': 'ver_solicitudes',
    'solicitudes': 'ver_solicitudes',
    'solicitudes pendientes': 'ver_solicitudes',
    'cobros admin': 'cobros_admin',
    'panel admin': 'cobros_admin',
    'panel de cobros': 'cobros_admin',
    'ver topics': 'ver_topics',
    'topics': 'ver_topics',
    'rag reindexar': 'rag_reindexar',
    'reindexar': 'rag_reindexar',
    'reindexar rag': 'rag_reindexar',
    'buscar usuario': 'buscar_usuario',
    # v4.0 Premium
    'finanzas': 'finanzas',
    'consulta financiera': 'finanzas',
    'asesor financiero': 'finanzas',
    'generar cv': 'generar_cv',
    'generar currículum': 'generar_cv',
    'curriculum': 'generar_cv',
    'entrevista': 'entrevista',
    'simulador entrevista': 'entrevista',
    'análisis linkedin': 'analisis_linkedin',
    'analisis linkedin': 'analisis_linkedin',
    'mi dashboard': 'mi_dashboard',
    'dashboard': 'mi_dashboard',
    'mentor': 'mentor',
    'mentoría': 'mentor',
    'mentoria': 'mentor',
    'mis coins': 'mis_coins',
    'coins': 'mis_coins',
    'monedas': 'mis_coins',
}


def detectar_comando_por_voz(texto_transcrito: str):
    """Detecta si el texto transcrito contiene 'comando [nombre]' y extrae argumentos.
    Whisper transcribe con puntuación: 'Cofradía Bot, Comando, Resumen, Mes.'
    Debemos limpiar comas, puntos, etc. antes de buscar.
    Returns: (nombre_comando, argumentos) o None"""
    
    # PASO 1: Limpiar TODA la puntuación que Whisper agrega
    texto_limpio = texto_transcrito.lower().strip()
    texto_limpio = re.sub(r'[,.:;!?¿¡\-–—\"\'()…]', ' ', texto_limpio)  # Reemplazar puntuación por espacios
    texto_limpio = re.sub(r'\s+', ' ', texto_limpio).strip()  # Colapsar espacios múltiples
    
    # PASO 2: Detectar la palabra "comando" en cualquier posición
    prefijos = [
        'cofradía bot comando ', 'cofradia bot comando ',
        'cofradía comando ', 'cofradia comando ',
        'bot comando ', 'comando ', 'commando ',
        'ejecuta comando ', 'ejecutar comando ',
        'ejecuta ', 'ejecutar '
    ]
    
    texto_sin_prefijo = None
    for prefijo in prefijos:
        if prefijo in texto_limpio:
            idx = texto_limpio.index(prefijo) + len(prefijo)
            texto_sin_prefijo = texto_limpio[idx:].strip()
            break
    
    if not texto_sin_prefijo:
        return None
    
    # PASO 3: Buscar el comando más largo que coincida (greedy match)
    mejor_match = None
    mejor_longitud = 0
    
    for voz_cmd, real_cmd in COMANDOS_VOZ.items():
        if texto_sin_prefijo.startswith(voz_cmd):
            if len(voz_cmd) > mejor_longitud:
                mejor_longitud = len(voz_cmd)
                argumentos = texto_sin_prefijo[len(voz_cmd):].strip()
                mejor_match = (real_cmd, argumentos)
    
    # PASO 4: Si no hubo match exacto, intentar match flexible (palabras individuales)
    if not mejor_match:
        palabras_restantes = texto_sin_prefijo.split()
        if palabras_restantes:
            # Intentar con 1 palabra, luego 2, luego 3
            for n_palabras in [3, 2, 1]:
                if len(palabras_restantes) >= n_palabras:
                    intento = ' '.join(palabras_restantes[:n_palabras])
                    if intento in COMANDOS_VOZ:
                        real_cmd = COMANDOS_VOZ[intento]
                        argumentos = ' '.join(palabras_restantes[n_palabras:]).strip()
                        mejor_match = (real_cmd, argumentos)
                        break
    
    if mejor_match:
        logger.info(f"🎤 Comando por voz detectado: /{mejor_match[0]} {mejor_match[1]} (de: '{texto_transcrito[:80]}')")
    
    return mejor_match


async def ejecutar_comando_voz(comando: str, argumentos: str, update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Ejecuta un comando del bot por voz y retorna texto para TTS"""
    try:
        # Mapeo de comandos a funciones
        funciones_comando = {
            'buscar': buscar_comando,
            'buscar_ia': buscar_ia_comando,
            'rag_consulta': rag_consulta_comando,
            'buscar_profesional': buscar_profesional_comando,
            'buscar_apoyo': buscar_apoyo_comando,
            'buscar_especialista_sec': buscar_especialista_sec_comando,
            'empleo': empleo_comando,
            'mi_tarjeta': mi_tarjeta_comando,
            'directorio': directorio_comando,
            'conectar': conectar_comando,
            'alertas': alertas_comando,
            'publicar': publicar_comando,
            'anuncios': anuncios_comando,
            'encuesta': encuesta_comando,
            'eventos': eventos_comando,
            'asistir': asistir_comando,
            'nuevo_evento': nuevo_evento_comando,
            'recomendar': recomendar_comando,
            'mis_recomendaciones': mis_recomendaciones_comando,
            'consultar': consultar_comando,
            'consultas': consultas_comando,
            'responder': responder_consulta_comando,
            'ver_consulta': ver_consulta_comando,
            'graficos': graficos_comando,
            'estadisticas': estadisticas_comando,
            'indicadores': indicadores_comando,
            'top_usuarios': top_usuarios_comando,
            'mi_perfil': mi_perfil_comando,
            'resumen': resumen_comando,
            'resumen_semanal': resumen_semanal_comando,
            'resumen_mes': resumen_mes_comando,
            'dotacion': dotacion_comando,
            'categorias': categorias_comando,
            'cumpleanos_mes': cumpleanos_mes_comando,
            'mi_cuenta': mi_cuenta_comando,
            'ayuda': ayuda,
            'rag_status': rag_status_comando,
            'rag_backup': rag_backup_comando,
            'rag_reindexar': rag_reindexar_comando,
            'eliminar_pdf': eliminar_pdf_comando,
            'ver_solicitudes': aprobar_solicitud_comando,
            'cobros_admin': cobros_admin_comando,
            'ver_topics': ver_topics_comando,
            'buscar_usuario': buscar_usuario_comando,
            'finanzas': finanzas_comando,
            'generar_cv': generar_cv_comando,
            'entrevista': entrevista_comando,
            'analisis_linkedin': analisis_linkedin_comando,
            'mi_dashboard': mi_dashboard_comando,
            'mentor': mentor_comando,
            'mis_coins': mis_coins_comando,
        }
        
        func = funciones_comando.get(comando)
        if not func:
            await update.message.reply_text(f"❌ Comando '{comando}' no reconocido por voz.")
            return None
        
        # Simular context.args con los argumentos extraídos
        original_args = context.args
        context.args = argumentos.split() if argumentos else []
        
        try:
            await func(update, context)
        finally:
            # Restaurar args originales
            context.args = original_args
        
        return f"Comando {comando} ejecutado correctamente con argumentos: {argumentos}" if argumentos else f"Comando {comando} ejecutado correctamente."
    
    except Exception as e:
        logger.warning(f"Error ejecutando comando voz '{comando}': {e}")
        await update.message.reply_text(f"❌ Error ejecutando /{comando}: {str(e)[:100]}")
        return None


async def manejar_mensaje_voz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja mensajes de voz: transcribe con Whisper, responde con IA, genera audio"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    es_privado = es_chat_privado(update)
    
    # Verificar suscripción (excepto owner)
    if not es_owner and not verificar_suscripcion_activa(user_id):
        if es_privado:
            await update.message.reply_text(
                "👋 ¡Hola! Para usar el asistente de voz necesitas una cuenta activa.\n\n"
                "👉 Escribe /start para registrarte."
            )
        return
    
    # No interferir con onboarding
    if context.user_data.get('onboard_activo'):
        return
    
    msg = await update.message.reply_text("🎤 Escuchando tu mensaje de voz...")
    
    try:
        # PASO 1: Descargar audio de Telegram
        voice = update.message.voice or update.message.audio
        if not voice:
            await msg.edit_text("❌ No se pudo obtener el audio.")
            return
        
        file = await context.bot.get_file(voice.file_id)
        audio_bytes = await file.download_as_bytearray()
        
        if not audio_bytes or len(audio_bytes) < 100:
            await msg.edit_text("❌ El audio está vacío o es muy corto.")
            return
        
        await msg.edit_text("🎤 Transcribiendo tu mensaje...")
        
        # PASO 2: Transcribir con Groq Whisper
        texto_transcrito = transcribir_audio_groq(bytes(audio_bytes))
        
        if not texto_transcrito:
            await msg.edit_text(
                "❌ No pude entender el audio. Intenta:\n"
                "• Hablar más cerca del micrófono\n"
                "• Reducir el ruido de fondo\n"
                "• Enviar un mensaje más largo"
            )
            return
        
        # FILTRO: Solo en GRUPOS se requiere mencionar "bot" para no interrumpir conversaciones
        # En chat PRIVADO el bot siempre responde (el usuario le habla directamente)
        if not es_privado:
            texto_check = re.sub(r'[,.:;!?¿¡\-–—\"\'()…]', ' ', texto_transcrito.lower())
            palabras_audio = texto_check.split()
            # Aceptar "bot", "cofradía", "asistente" o "cofradia" como activadores
            activadores = {'bot', 'cofradía', 'cofradia', 'asistente'}
            if not any(p in activadores for p in palabras_audio):
                # No lo nombraron — silenciosamente ignorar y borrar mensaje de "escuchando"
                try:
                    await msg.delete()
                except:
                    pass
                return
        
        await msg.edit_text(f"🧠 Procesando: \"{texto_transcrito[:80]}{'...' if len(texto_transcrito) > 80 else ''}\"")
        
        # PASO 2.5: Detectar si el usuario dijo "comando [nombre]" para ejecutar un comando real
        comando_detectado = detectar_comando_por_voz(texto_transcrito)
        if comando_detectado:
            comando_nombre, argumentos = comando_detectado
            await msg.edit_text(f"🎤 Detectado: /{comando_nombre} {argumentos}\n⚙️ Ejecutando comando...")
            
            # Simular ejecución del comando inyectando como texto
            try:
                texto_comando = f"/{comando_nombre} {argumentos}".strip()
                # Crear un mensaje falso con el comando para que Telegram lo procese
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"🎤 Comando de voz detectado:\n`{texto_comando}`\n\nEjecutando...",
                    parse_mode='Markdown'
                )
                # Ejecutar el comando directamente llamando la función correspondiente
                resultado_cmd = await ejecutar_comando_voz(comando_nombre, argumentos, update, context)
                if resultado_cmd:
                    # Generar audio de la respuesta del comando
                    try:
                        audio_file = await generar_audio_tts(resultado_cmd[:1500])
                        if audio_file:
                            with open(audio_file, 'rb') as f:
                                await update.message.reply_voice(voice=f, caption="🔊 Respuesta de voz")
                            try:
                                os.remove(audio_file)
                            except:
                                pass
                    except:
                        pass
                registrar_servicio_usado(user_id, 'voz_comando')
                return
            except Exception as e:
                logger.warning(f"Error ejecutando comando por voz: {e}")
                # Si falla, continuar con procesamiento normal de IA
        
        # PASO 3: Procesar consulta con IA - búsqueda exhaustiva en todas las fuentes
        # Primero mejorar la intención del mensaje transcrito (invisible para el usuario)
        intencion_voz = mejorar_intencion(texto_transcrito, user.first_name, user_id, canal='audio')
        texto_para_busqueda = intencion_voz['query_mejorada']
        
        # Si detectó un comando claro, ejecutarlo directamente
        if intencion_voz['ejecutar_comando'] and intencion_voz['comando']:
            ejecutado = await ejecutar_comando_desde_intencion(
                intencion_voz['comando'], intencion_voz['args'], update, context
            )
            if ejecutado:
                try:
                    await msg.delete()
                except:
                    pass
                registrar_servicio_usado(user_id, 'voz_comando')
                return
        
        resultados = busqueda_unificada(texto_para_busqueda, limit_historial=15, limit_rag=40)
        contexto = formatear_contexto_unificado(resultados, texto_para_busqueda)
        
        # Generar también contexto de tarjetas profesionales si hay nombres relevantes
        contexto_tarjetas = ""
        try:
            conn_tc = get_db_connection()
            if conn_tc:
                c_tc = conn_tc.cursor()
                # Usar query mejorada para mejor búsqueda de tarjetas
                palabras_busq = [p for p in texto_para_busqueda.lower().split() if len(p) > 3][:5]
                if palabras_busq:
                    cond_tc = []
                    params_tc = []
                    for p in palabras_busq:
                        if DATABASE_URL:
                            cond_tc.append("(LOWER(nombre_completo) LIKE %s OR LOWER(profesion) LIKE %s OR LOWER(empresa) LIKE %s OR LOWER(servicios) LIKE %s)")
                            params_tc.extend([f'%{p}%', f'%{p}%', f'%{p}%', f'%{p}%'])
                        else:
                            cond_tc.append("(LOWER(nombre_completo) LIKE ? OR LOWER(profesion) LIKE ? OR LOWER(empresa) LIKE ? OR LOWER(servicios) LIKE ?)")
                            params_tc.extend([f'%{p}%', f'%{p}%', f'%{p}%', f'%{p}%'])
                    if cond_tc:
                        wh = " OR ".join(cond_tc)
                        if DATABASE_URL:
                            c_tc.execute(f"SELECT nombre_completo, profesion, empresa, ciudad, servicios FROM tarjetas_profesional WHERE {wh} LIMIT 5", params_tc)
                        else:
                            c_tc.execute(f"SELECT nombre_completo, profesion, empresa, ciudad, servicios FROM tarjetas_profesional WHERE {wh} LIMIT 5", params_tc)
                        tarjetas = c_tc.fetchall()
                        if tarjetas:
                            ctx_list = []
                            for t in tarjetas:
                                if DATABASE_URL:
                                    ctx_list.append(f"{t['nombre_completo']} - {t['profesion']} en {t['empresa']} ({t['ciudad']}): {t['servicios']}")
                                else:
                                    ctx_list.append(f"{t[0]} - {t[1]} en {t[2]} ({t[3]}): {t[4]}")
                            contexto_tarjetas = "\n=== PROFESIONALES COFRADÍA (Tarjetas) ===\n" + "\n".join(ctx_list)
                conn_tc.close()
        except Exception as e_tc:
            logger.debug(f"No se pudo buscar tarjetas para audio: {e_tc}")
        
        fuentes_info = ", ".join(resultados.get('fuentes_usadas', [])) or "conocimiento propio"
        rag_conf = resultados.get('rag_confianza', 'ninguna')
        
        modo_audio = ("Usa el contexto indexado como base." if rag_conf in ('alta','media') 
                      else "No hay docs relevantes — responde desde tu conocimiento propio como LLM experto.")
        
        # Contexto de intención para el prompt
        intent_audio_hint = ""
        if texto_para_busqueda != texto_transcrito:
            intent_audio_hint = f"\n(La búsqueda usó la consulta enriquecida: \"{texto_para_busqueda[:100]}\")"
        
        prompt = f"""Eres el asistente IA de la Cofradía de Networking. Tu respuesta será convertida a audio.

{user.first_name}, responde de forma conversacional (máximo 5 oraciones, sin emojis ni listas).
Empieza directamente: "{user.first_name}, [respuesta]..."

MODO: {modo_audio}
NUNCA digas "no tengo información" si sabes del tema como LLM.
NUNCA inventes nombres de personas — solo menciona a quienes aparezcan en el contexto.

He buscado en: {fuentes_info} (confianza RAG: {rag_conf}){intent_audio_hint}

{contexto}
{contexto_tarjetas}

{user.first_name} pregunta (audio): {texto_transcrito}"""

        respuesta_texto = llamar_groq(prompt, max_tokens=900, temperature=0.7)
        
        if not respuesta_texto:
            respuesta_texto = llamar_gemini_texto(prompt, max_tokens=600, temperature=0.7)
        
        if not respuesta_texto:
            respuesta_texto = f"Recibí tu mensaje: \"{texto_transcrito}\". Lamentablemente no pude generar una respuesta en este momento."
        
        # PASO 4: Enviar respuesta en texto
        texto_respuesta_display = f"🎤 *Tu mensaje:*\n_{texto_transcrito}_\n\n💬 *Respuesta:*\n{respuesta_texto}"
        try:
            await msg.edit_text(texto_respuesta_display, parse_mode='Markdown')
        except Exception:
            await msg.edit_text(f"🎤 Tu mensaje:\n{texto_transcrito}\n\n💬 Respuesta:\n{respuesta_texto}")
        
        # PASO 5: Generar y enviar audio de respuesta
        try:
            audio_file = await generar_audio_tts(respuesta_texto)
            if audio_file:
                with open(audio_file, 'rb') as f:
                    await update.message.reply_voice(
                        voice=f,
                        caption="🔊 Respuesta de voz"
                    )
                # Limpiar archivo temporal
                try:
                    os.remove(audio_file)
                except:
                    pass
        except Exception as e:
            logger.warning(f"No se pudo enviar audio TTS: {e}")
            # No es crítico - ya se envió la respuesta en texto
        
        # Registrar uso del servicio
        registrar_servicio_usado(user_id, 'voz')
        
        # Guardar mensaje en historial si es grupo
        if not es_privado:
            guardar_mensaje(
                user_id,
                user.username or "sin_username",
                user.first_name or "Usuario",
                f"[AUDIO] {texto_transcrito}",
                update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None,
                last_name=user.last_name or ''
            )
        
    except Exception as e:
        logger.error(f"Error procesando voz: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            await msg.edit_text(f"❌ Error procesando audio: {str(e)[:100]}")
        except:
            pass


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
    # Owner siempre tiene suscripción ilimitada
    if user_id == OWNER_ID:
        return 99999
    
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
        
        # Si es el owner/fundador, forzar fecha_incorporacion al 22-09-2020
        if user_id == OWNER_ID:
            try:
                if DATABASE_URL:
                    c.execute("UPDATE suscripciones SET fecha_incorporacion = '2020-09-22', first_name = 'Germán', last_name = 'Perey', fecha_expiracion = '2099-12-31 23:59:59', estado = 'activo' WHERE user_id = %s", (user_id,))
                else:
                    c.execute("UPDATE suscripciones SET fecha_incorporacion = '2020-09-22', first_name = 'Germán', last_name = 'Perey', fecha_expiracion = '2099-12-31 23:59:59', estado = 'activo' WHERE user_id = ?", (user_id,))
                logger.info(f"✅ Owner fecha_incorporacion forzada a 2020-09-22")
            except Exception as e:
                logger.warning(f"Error forzando fecha owner: {e}")
        
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
    """Categoriza un mensaje según su contenido - categorías específicas"""
    texto_lower = texto.lower()
    
    categorias = {
        'Oferta Laboral': ['oferta laboral', 'vacante', 'estamos buscando', 'se necesita', 'oportunidad laboral', 'cargo disponible'],
        'Búsqueda Empleo': ['busco trabajo', 'busco empleo', 'estoy buscando', 'cv', 'currículum', 'postular', 'transición laboral'],
        'Recomendación Profesional': ['recomiendo', 'les comparto', 'contacto de', 'excelente servicio', 'buen profesional', 'maestro', 'técnico'],
        'Consulta Profesional': ['alguien sabe', 'alguien conoce', 'necesito', 'busco un', 'recomienden', 'ayuda con', 'consulta'],
        'Servicios y Productos': ['vendo', 'ofrezco', 'servicio de', 'cotización', 'presupuesto', 'precio', 'descuento', 'proveedor'],
        'Networking': ['contacto', 'networking', 'conectar', 'alianza', 'colaboración', 'red de'],
        'Emprendimiento': ['emprendimiento', 'negocio', 'startup', 'empresa propia', 'proyecto', 'inversión', 'socio'],
        'Capacitación': ['curso', 'capacitación', 'taller', 'diplomado', 'certificación', 'formación', 'webinar'],
        'Evento': ['evento', 'charla', 'meetup', 'conferencia', 'seminario', 'feria'],
        'Información': ['les informo', 'dato', 'comparto', 'información', 'noticia', 'artículo', 'link', 'www', 'http'],
        'Opinión': ['creo que', 'opino', 'mi experiencia', 'en mi caso', 'a mi juicio', 'considero'],
        'Conversación': ['gracias', 'excelente', 'buena idea', 'de acuerdo', 'así es', 'correcto', 'claro'],
        'Saludo': ['hola', 'buenos días', 'buenas tardes', 'buenas noches', 'saludos', 'bienvenido', 'felicitaciones']
    }
    
    for categoria, palabras in categorias.items():
        if any(palabra in texto_lower for palabra in palabras):
            return categoria
    
    # Intento adicional: detectar temas por contexto
    if any(w in texto_lower for w in ['panel', 'construcción', 'instalación', 'obra']):
        return 'Construcción'
    if any(w in texto_lower for w in ['finanza', 'banco', 'crédito', 'inversión', 'contabilidad']):
        return 'Finanzas'
    if any(w in texto_lower for w in ['tecnología', 'software', 'sistema', 'app', 'digital']):
        return 'Tecnología'
    if any(w in texto_lower for w in ['inmobiliaria', 'propiedad', 'arriendo', 'departamento']):
        return 'Inmobiliaria'
    if any(w in texto_lower for w in ['seguridad', 'cámara', 'alarma', 'vigilancia']):
        return 'Seguridad'
    if any(w in texto_lower for w in ['combustible', 'energía', 'gas', 'electricidad']):
        return 'Energía'
    if any(w in texto_lower for w in ['marítimo', 'naviera', 'puerto', 'armada', 'naval']):
        return 'Sector Marítimo'
    if len(texto) < 20:
        return 'Conversación'
    return 'Otro'


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


def limpiar_nombre_display(nombre):
    """Limpia un nombre para mostrar"""
    if not nombre:
        return "Usuario"
    nombre = str(nombre).replace('_', ' ').strip()
    if not nombre:
        return "Usuario"
    # Solo estos son admin anónimo del grupo (Telegram envía el nombre del grupo/canal)
    if nombre.lower() in ['group', 'grupo', 'channel', 'canal', 'cofradía', 
                           'cofradía de networking']:
        return "Germán Perey"
    # Nombres genéricos inválidos — NO son Germán, son usuarios reales sin nombre
    if nombre.lower() in ['usuario', 'anónimo', 'sin nombre', 'no name', 'none', 'null']:
        return "Usuario"
    return nombre


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
    """Busca en el historial de mensajes - retorna nombre completo"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        c = conn.cursor()
        query_like = f'%{query.lower()}%'
        
        # Stemming básico: buscar con y sin 's' final
        query_l = query.lower().strip()
        variantes = [query_like]
        if query_l.endswith('s'):
            variantes.append(f'%{query_l[:-1]}%')
        else:
            variantes.append(f'%{query_l}s%')
        
        if DATABASE_URL:
            like_clause = " OR ".join(["LOWER(message) LIKE %s"] * len(variantes))
            if topic_id:
                c.execute(f"""SELECT first_name || ' ' || COALESCE(NULLIF(last_name, ''), '') as nombre_completo, 
                             message, fecha FROM mensajes 
                             WHERE ({like_clause}) AND topic_id = %s
                             ORDER BY fecha DESC LIMIT %s""", (*variantes, topic_id, limit))
            else:
                c.execute(f"""SELECT first_name || ' ' || COALESCE(NULLIF(last_name, ''), '') as nombre_completo, 
                             message, fecha FROM mensajes 
                             WHERE ({like_clause})
                             ORDER BY fecha DESC LIMIT %s""", (*variantes, limit))
        else:
            like_clause = " OR ".join(["LOWER(message) LIKE ?"] * len(variantes))
            if topic_id:
                c.execute(f"""SELECT first_name || ' ' || COALESCE(NULLIF(last_name, ''), '') as nombre_completo, 
                             message, fecha FROM mensajes 
                             WHERE ({like_clause}) AND topic_id = ?
                             ORDER BY fecha DESC LIMIT ?""", (*variantes, topic_id, limit))
            else:
                c.execute(f"""SELECT first_name || ' ' || COALESCE(NULLIF(last_name, ''), '') as nombre_completo, 
                             message, fecha FROM mensajes 
                             WHERE ({like_clause})
                             ORDER BY fecha DESC LIMIT ?""", (*variantes, limit))
        
        resultados = c.fetchall()
        conn.close()
        
        if DATABASE_URL:
            return [((r['nombre_completo'] or '').strip(), r['message'], r['fecha']) for r in resultados]
        else:
            return [((r[0] or '').strip(), r[1], r[2]) for r in resultados]
            
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

async def enviar_mensaje_largo(update: Update, texto: str, parse_mode=None):
    """Envía mensajes largos dividiéndolos si es necesario. Sin Markdown por defecto para evitar errores."""
    if len(texto) <= 4000:
        try:
            await update.message.reply_text(texto, parse_mode=parse_mode)
        except Exception:
            # Si falla con parse_mode, reintentar sin formato
            try:
                await update.message.reply_text(texto)
            except Exception as e:
                logger.error(f"Error enviando mensaje: {e}")
    else:
        partes = [texto[i:i+4000] for i in range(0, len(texto), 4000)]
        for parte in partes:
            try:
                await update.message.reply_text(parte, parse_mode=parse_mode)
            except Exception:
                try:
                    await update.message.reply_text(parte)
                except Exception as e:
                    logger.error(f"Error enviando parte: {e}")


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
    """Comando /start - Detecta si es usuario nuevo o registrado"""
    user = update.message.from_user
    user_id = user.id
    
    # DEEP LINK: /start tarjeta_USERID → mostrar tarjeta profesional de alguien
    if context.args and len(context.args) > 0 and context.args[0].startswith('tarjeta_'):
        try:
            target_user_id = int(context.args[0].replace('tarjeta_', ''))
            await mostrar_tarjeta_publica(update, context, target_user_id)
            return ConversationHandler.END
        except (ValueError, Exception) as e:
            logger.debug(f"Error procesando deep link tarjeta: {e}")
    
    # DEEP LINK: /start verificar_USERID → verificar autenticidad de usuario
    if context.args and len(context.args) > 0 and context.args[0].startswith('verificar_'):
        try:
            target_id = int(context.args[0].replace('verificar_', ''))
            stats = obtener_stats_tarjeta(target_id)
            if stats['nombre_completo']:
                estado_txt = '✅ Usuario ACTIVO' if stats['estado'] == 'activo' else '❌ Usuario ELIMINADO'
                anio_gen = f"⚓ Generación: {stats['generacion']}\n" if stats['generacion'] else ""
                verif = (f"🔒 VERIFICACIÓN DE USUARIO\n{'━' * 30}\n\n"
                         f"👤 {stats['nombre_completo']}\n"
                         f"{anio_gen}"
                         f"📋 {estado_txt}\n"
                         f"📅 Incorporación: {stats['fecha_incorporacion'] or 'No registrada'}\n\n"
                         f"✅ Verificado por Cofradía de Networking")
            else:
                verif = "❌ USUARIO INEXISTENTE\n\nEste código no corresponde a ningún miembro de Cofradía."
            await update.message.reply_text(verif)
            return ConversationHandler.END
        except Exception as e_verif:
            logger.warning(f"Error verificación QR: {e_verif}")
            await update.message.reply_text("❌ Error verificando usuario.")
            return ConversationHandler.END
    
    
    # Owner siempre tiene acceso completo
    if user_id == OWNER_ID:
        registrar_usuario_suscripcion(user_id, 'Germán', user.username or '', es_admin=True, dias_gratis=999999, last_name='Perey')
        await update.message.reply_text(
            f"👑 Bienvenido Germán!\n\n"
            f"Panel completo disponible.\n"
            f"Escribe /cobros_admin para ver el panel de administración.\n"
            f"Escribe /ayuda para ver todos los comandos."
        )
        return ConversationHandler.END
    
    # Verificar si el usuario ya está registrado (tiene suscripción)
    es_registrado = verificar_suscripcion_activa(user_id)
    
    if es_registrado:
        # Usuario ya registrado - bienvenida normal
        mensaje = f"""🎉 Bienvenido/a {user.first_name} al Bot Cofradia Premium!

============================
🛠️ QUE PUEDO HACER?
============================

🔍 Buscar informacion - /buscar o /buscar_ia
👥 Encontrar profesionales - /buscar_profesional
💼 Buscar empleos - /empleo
📊 Ver estadisticas - /graficos
📈 Indicadores económicos - /indicadores
📝 Resumenes diarios - /resumen
🤖 Preguntarme - @Cofradia_Premium_Bot + pregunta

Escribe /ayuda para ver todos los comandos.
"""
        await update.message.reply_text(mensaje)
        return ConversationHandler.END
    
    # Usuario NO registrado - verificar si ya tiene solicitud pendiente
    tiene_solicitud = False
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT estado FROM nuevos_miembros WHERE user_id = %s ORDER BY fecha_solicitud DESC LIMIT 1", (user_id,))
            else:
                c.execute("SELECT estado FROM nuevos_miembros WHERE user_id = ? ORDER BY fecha_solicitud DESC LIMIT 1", (user_id,))
            resultado = c.fetchone()
            if resultado:
                estado = resultado['estado'] if DATABASE_URL else resultado[0]
                if estado == 'pendiente':
                    tiene_solicitud = True
            conn.close()
    except Exception as e:
        logger.warning(f"Error verificando solicitud: {e}")
    
    if tiene_solicitud:
        await update.message.reply_text(
            "⏳ Ya tienes una solicitud de ingreso pendiente.\n\n"
            "El administrador está revisando tu solicitud.\n"
            "Te notificaremos cuando sea aprobada."
        )
        return ConversationHandler.END
    
    # Usuario nuevo sin solicitud - iniciar onboarding
    context.user_data['onboard_user_id'] = user_id
    context.user_data['onboard_username'] = user.username or ''
    context.user_data['onboard_activo'] = True
    
    await update.message.reply_text(
        f"⚓ Bienvenido/a {user.first_name} a Cofradía de Networking!\n\n"
        f"Cofradía es un grupo exclusivo de Marinos en materia laboral, "
        f"para fortalecer apoyos, fomentar el intercambio comercial y cultivar la amistad.\n\n"
        f"Para solicitar tu ingreso necesito que respondas 5 breves preguntas "
        f"(3 de información personal y 2 de verificación).\n\n"
        f"📝 Pregunta 1 de 6:\n"
        f"¿Cuál es tu Nombre y Apellido completo?\n"
        f"(Nombre + Apellido paterno + Apellido materno)"
    )
    
    return ONBOARD_NOMBRE


async def start_no_registrado_texto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja cualquier texto de usuario no registrado en chat privado"""
    user = update.message.from_user
    user_id = user.id
    
    # Si está en medio del onboarding, no interferir
    if context.user_data.get('onboard_activo'):
        return
    
    # Owner y registrados no pasan por aquí
    if user_id == OWNER_ID or verificar_suscripcion_activa(user_id):
        return
    
    # Usuario no registrado escribiendo algo - redirigir a /start
    context.user_data['onboard_user_id'] = user_id
    context.user_data['onboard_username'] = user.username or ''
    context.user_data['onboard_activo'] = True
    
    await update.message.reply_text(
        f"⚓ Hola {user.first_name}! Veo que aún no eres miembro de Cofradía de Networking.\n\n"
        f"Cofradía es un grupo exclusivo de Marinos en materia laboral, "
        f"para fortalecer apoyos, fomentar el intercambio comercial y cultivar la amistad.\n\n"
        f"Para solicitar tu ingreso necesito que respondas 5 breves preguntas "
        f"(3 de información personal y 2 de verificación).\n\n"
        f"📝 Pregunta 1 de 6:\n"
        f"¿Cuál es tu Nombre y Apellido completo?\n"
        f"(Nombre + Apellido paterno + Apellido materno)"
    )
    
    return ONBOARD_NOMBRE


@solo_chat_privado
async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /ayuda - Lista de comandos con ejemplos expandibles"""
    user_id = update.effective_user.id
    
    texto = """📚 COMANDOS DISPONIBLES
============================

🔍 BÚSQUEDA
/buscar [texto] - Buscar en historial
/buscar_ia [consulta] - Búsqueda con IA
/rag_consulta [pregunta] - Buscar en documentos
/buscar_profesional [área] - Buscar profesionales
/buscar_apoyo [área] - Buscar en bolsa laboral
/empleo [cargo] - Buscar empleos

📇 DIRECTORIO PROFESIONAL
/mi_tarjeta - Crear/ver tu tarjeta profesional
/directorio [búsqueda] - Buscar en directorio
/conectar - Conexiones inteligentes sugeridas
/recomendar @user [texto] - Recomendar cofrade
/mis_recomendaciones - Ver recomendaciones

📢 COMUNIDAD
/publicar [cat] titulo | desc - Publicar anuncio
/anuncios [categoría] - Ver tablón de anuncios
/consultar titulo | desc - Consulta profesional
/consultas - Ver consultas abiertas
/responder [ID] [resp] - Responder consulta
/encuesta pregunta | opc1 | opc2 - Crear encuesta

📅 EVENTOS
/eventos - Ver próximos eventos
/asistir [ID] - Confirmar asistencia

🔔 ALERTAS
/alertas - Ver/gestionar alertas
/alertas [palabras] - Crear alerta

📊 ESTADÍSTICAS
/graficos - Gráficos de actividad y KPIs
/estadisticas - Estadísticas generales
/top_usuarios - Ranking de participación
/mi_perfil - Tu perfil, coins y trust score
/cumpleanos_mes [1-12] - Cumpleaños del mes

💰 ASISTENTE FINANCIERO
/finanzas [consulta] - Asesoría basada en libros (gratis)

📊 TU DASHBOARD (GRATIS)
⭐ /mi_dashboard - Tu dashboard personal ⭐

💎 SERVICIOS PREMIUM (Coins o pesos)
/generar_cv [orientación] - CV profesional ($2.500 / 25 coins)
/entrevista [cargo] - Simulador entrevista ($5.000 / 50 coins)
/analisis_linkedin - Análisis de perfil ($3.000 / 30 coins)
/mentor - Plan de mentoría IA ($4.000 / 40 coins)

🪙 COFRADÍA COINS
/mis_coins - Balance y servicios canjeables

📋 RESÚMENES
/resumen - Resumen del día
/resumen_semanal - Resumen de 7 días
/resumen_mes - Resumen mensual

🎤 VOZ: Envía audio mencionando "Bot" y te respondo!

============================
💡 Toca un botón para ver ejemplos paso a paso:
"""
    
    # Botones expandibles por sección
    keyboard = [
        [InlineKeyboardButton("🔍 Ejemplos Búsqueda ＋", callback_data="ayuda_ej_busqueda")],
        [InlineKeyboardButton("📇 Ejemplos Directorio ＋", callback_data="ayuda_ej_directorio")],
        [InlineKeyboardButton("📢 Ejemplos Comunidad ＋", callback_data="ayuda_ej_comunidad")],
        [InlineKeyboardButton("📊 Ejemplos Estadísticas ＋", callback_data="ayuda_ej_estadisticas")],
        [InlineKeyboardButton("💎 Ejemplos Premium ＋", callback_data="ayuda_ej_premium")],
        [InlineKeyboardButton("🪙 Ejemplos Coins ＋", callback_data="ayuda_ej_coins")],
    ]
    
    await update.message.reply_text(texto, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # Admin commands in separate message (no buttons needed)
    if user_id == OWNER_ID:
        admin_txt = """
👑 COMANDOS ADMIN
============================
/aprobar_solicitud [ID] - Aprobar ingreso
/editar_usuario [ID] [campo] [valor] - Editar datos
/eliminar_solicitud [ID] - Eliminar usuario
/buscar_usuario [nombre] - Buscar ID de usuario
/cobros_admin - Panel de cobros
/ver_solicitudes - Ver solicitudes pendientes
/generar_codigo - Generar código de activación
/nuevo_evento fecha | título | lugar | desc - Crear evento

🧠 RAG (Base de conocimiento)
/rag_status - Estado del sistema RAG
/rag_backup - Verificar integridad datos RAG
/rag_reindexar - Re-indexar documentos
/eliminar_pdf [nombre] - Eliminar PDF indexado

💰 PRECIOS Y COINS
/set_precio [srv] [pesos] [coins] - Editar precios
/dar_coins [user_id] [cant] - Regalar coins
"""
        await update.message.reply_text(admin_txt)


# Diccionario de ejemplos para /ayuda
AYUDA_EJEMPLOS = {
    'busqueda': """🔍 EJEMPLOS DE BÚSQUEDA
━━━━━━━━━━━━━━━━━━━━━━━━

1️⃣ /buscar inversiones
   Busca mensajes del grupo que contengan "inversiones"

2️⃣ /buscar_ia cómo mejorar mi currículum
   La IA busca en el historial y responde con contexto

3️⃣ /rag_consulta qué dice Kiyosaki sobre deuda buena
   Busca en los 100+ libros de la biblioteca

4️⃣ /buscar_profesional abogado
   Busca abogados en la base de datos de Drive

5️⃣ /empleo gerente logística
   Busca ofertas laborales de gerente de logística

💡 Tip: Usa términos específicos para mejores resultados""",

    'directorio': """📇 EJEMPLOS DE DIRECTORIO
━━━━━━━━━━━━━━━━━━━━━━━━

1️⃣ /mi_tarjeta
   Paso 1: Escribe /mi_tarjeta para ver tu tarjeta
   Paso 2: Si no tienes, te pedirá crearla
   Paso 3: Recibes imagen + archivo descargable + contacto

2️⃣ /mi_tarjeta profesion Ingeniero Civil
   Actualiza tu profesión en la tarjeta

3️⃣ /directorio consultoría
   Muestra cofrades que ofrecen consultoría

4️⃣ /conectar
   La IA analiza tu perfil y sugiere 3 cofrades ideales

5️⃣ /recomendar @juanperez Excelente profesional, muy confiable
   Deja una recomendación pública (+5 coins para ti)

💡 Tip: Completa todos los campos de tu tarjeta para aparecer en más búsquedas""",

    'comunidad': """📢 EJEMPLOS DE COMUNIDAD
━━━━━━━━━━━━━━━━━━━━━━━━

1️⃣ /publicar servicios Asesoría Tributaria | Ofrezco asesoría en declaraciones de renta y planificación fiscal
   Publica un anuncio en categoría "servicios"

2️⃣ /anuncios servicios
   Ver todos los anuncios de servicios activos

3️⃣ /consultar Recomendación de contador | Necesito un contador para empresa PYME en Viña
   Crea una consulta pública que otros cofrades pueden responder

4️⃣ /responder 5 Te recomiendo a Juan Pérez, excelente contador
   Responde a la consulta #5 (+10 coins)

5️⃣ /encuesta ¿Cuándo hacemos la junta? | Viernes 19h | Sábado 12h | Domingo 11h
   Crea una encuesta con 3 opciones

💡 Tip: Las consultas respondidas te dan 10 Cofradía Coins""",

    'estadisticas': """📊 EJEMPLOS DE ESTADÍSTICAS
━━━━━━━━━━━━━━━━━━━━━━━━

1️⃣ /graficos
   Genera gráficos visuales de actividad del grupo

2️⃣ /mi_perfil
   Tu perfil completo: Trust Score, Coins, servicios canjeables y cómo ganar más

3️⃣ /mi_dashboard
   Dashboard detallado: ranking, métricas, recomendaciones (GRATIS)

4️⃣ /cumpleanos_mes 3
   Muestra todos los cumpleaños de Marzo

5️⃣ /top_usuarios
   Ranking de los cofrades más activos

6️⃣ /alertas inversiones finanzas
   Recibirás notificación cuando alguien hable de "inversiones" o "finanzas"

💡 Tip: /mi_dashboard es gratis y te muestra todo lo que puedes mejorar""",

    'premium': """💎 EJEMPLOS SERVICIOS PREMIUM
━━━━━━━━━━━━━━━━━━━━━━━━

1️⃣ /generar_cv logística marítima (25 coins / $2.500)
   Paso 1: Necesitas tener tu /mi_tarjeta creada
   Paso 2: Escribe /generar_cv y la orientación del CV
   Paso 3: La IA genera un CV completo basado en tu perfil

2️⃣ /entrevista Gerente de Operaciones (50 coins / $5.000)
   La IA simula 5 preguntas de entrevista con guías de respuesta

3️⃣ /analisis_linkedin (30 coins / $3.000)
   Analiza tu perfil y sugiere headline, keywords y mejoras

4️⃣ /mentor (40 coins / $4.000)
   Plan de desarrollo: diagnóstico, metas, tareas semanales y lecturas recomendadas

5️⃣ /finanzas conviene más APV o fondo mutuo (GRATIS)
   Asesoría financiera basada en los 100+ libros de la biblioteca

💡 Tip: Cada servicio se paga con Coins o pesos chilenos""",

    'coins': """🪙 CÓMO FUNCIONAN LOS COFRADÍA COINS
━━━━━━━━━━━━━━━━━━━━━━━━

1️⃣ /mis_coins
   Ver tu balance, historial y qué servicios puedes canjear

CÓMO GANAR COINS:
  💬 Escribir en el grupo: +1 coin por mensaje
  💡 Responder consulta (/responder): +10 coins
  ⭐ Recomendar cofrade (/recomendar): +5 coins
  📅 Asistir a evento (/asistir): +20 coins
  📇 Crear/actualizar tarjeta: +15 coins
  💰 Consulta financiera: +1 coin

EJEMPLO PRÁCTICO:
  Si envías 25 mensajes al grupo = 25 coins
  + recomiendas a 1 cofrade = 5 coins
  Total: 30 coins → alcanza para /analisis_linkedin

💡 Tip: /mi_perfil te muestra cuánto te falta para cada servicio"""
}


async def callback_ayuda_ejemplos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para mostrar ejemplos expandibles de /ayuda"""
    query = update.callback_query
    await query.answer()
    
    seccion = query.data.replace('ayuda_ej_', '')
    ejemplo = AYUDA_EJEMPLOS.get(seccion, 'Ejemplos no disponibles.')
    
    await query.message.reply_text(ejemplo)


async def registrarse_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /registrarse - Redirige al proceso de onboarding"""
    user = update.message.from_user
    
    # Verificar si ya está registrado con cuenta activa
    if verificar_suscripcion_activa(user.id):
        nombre_completo = f"{user.first_name or ''} {user.last_name or ''}".strip()
        await update.message.reply_text(
            f"✅ {nombre_completo}, ya estas registrado con una cuenta activa!"
        )
        return
    
    # TODOS los usuarios nuevos deben pasar por onboarding (5 preguntas)
    await update.message.reply_text(
        "⚓ Para registrarte en Cofradía de Networking debes completar "
        "un breve proceso de verificación (5 preguntas).\n\n"
        "👉 Escríbeme en privado a @Cofradia_Premium_Bot y presiona /start\n\n"
        "O haz clic aquí: https://t.me/Cofradia_Premium_Bot?start=registro"
    )


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
        await update.message.reply_text(f"❌ No se encontraron resultados para: {query}")
        return
    
    mensaje = f"🔍 RESULTADOS PARA: {query}\n\n"
    
    for nombre, texto, fecha in resultados[:10]:
        nombre_limpio = limpiar_nombre_display(nombre)
        texto_corto = texto[:150] + "..." if len(texto) > 150 else texto
        try:
            if hasattr(fecha, 'strftime'):
                fecha_str = fecha.strftime("%d/%m/%Y %H:%M")
            else:
                fecha_str = str(fecha)[:16]
        except:
            fecha_str = str(fecha)[:16] if fecha else ""
        mensaje += f"👤 {nombre_limpio}\n📅 {fecha_str}\n{texto_corto}\n\n"
    
    await enviar_mensaje_largo(update, mensaje)
    registrar_servicio_usado(update.effective_user.id, 'buscar')


@requiere_suscripcion
async def graficos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /graficos - Dashboard interactivo ECharts con análisis del grupo"""
    msg = await update.message.reply_text("📊 Generando dashboard interactivo ECharts...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error conectando a la base de datos")
            return
        
        c = conn.cursor()
        dias = 7
        
        # Verificar datos
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM mensajes")
            total_general = c.fetchone()['total']
        else:
            c.execute("SELECT COUNT(*) FROM mensajes")
            total_general = c.fetchone()[0]
        
        if total_general == 0:
            conn.close()
            await msg.edit_text(
                "📊 No hay datos para mostrar\n\n"
                "La base de datos está vacía. Los gráficos estarán disponibles cuando el bot "
                "comience a guardar mensajes del grupo."
            )
            return
        
        # ===== RECOLECTAR TODOS LOS DATOS =====
        fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
        
        # 1. Mensajes por día
        if DATABASE_URL:
            c.execute("""SELECT DATE(fecha) as date, COUNT(*) as count FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
                        GROUP BY DATE(fecha) ORDER BY DATE(fecha)""")
            por_dia = [(str(r['date']), r['count']) for r in c.fetchall()]
        else:
            c.execute("SELECT DATE(fecha), COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY DATE(fecha) ORDER BY DATE(fecha)", (fecha_inicio,))
            por_dia = [(r[0], r[1]) for r in c.fetchall()]
        
        # 2. Actividad por hora
        if DATABASE_URL:
            c.execute("""SELECT EXTRACT(HOUR FROM fecha)::int as hora, COUNT(*) as count 
                        FROM mensajes WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
                        GROUP BY EXTRACT(HOUR FROM fecha)::int ORDER BY hora""")
            por_hora = [(r['hora'], r['count']) for r in c.fetchall()]
        else:
            c.execute("SELECT CAST(strftime('%%H', fecha) AS INTEGER), COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY 1 ORDER BY 1", (fecha_inicio,))
            por_hora = [(r[0], r[1]) for r in c.fetchall()]
        
        # 3. Top usuarios
        if DATABASE_URL:
            c.execute("""SELECT COALESCE(
                            MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') 
                            AND first_name IS NOT NULL THEN first_name ELSE NULL END) 
                            || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), 
                            MAX(first_name), 'Usuario') as nombre, 
                        COUNT(*) as count FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'
                        GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 10""")
            usuarios = [((r['nombre'] or 'Usuario').strip(), r['count']) for r in c.fetchall()]
        else:
            c.execute("""SELECT COALESCE(
                            MAX(CASE WHEN first_name NOT IN ('Group','Grupo','Channel','Canal','') 
                            AND first_name IS NOT NULL THEN first_name ELSE NULL END) 
                            || ' ' || COALESCE(MAX(NULLIF(last_name, '')), ''), 
                            MAX(first_name), 'Usuario'), 
                        COUNT(*) FROM mensajes WHERE fecha >= ? 
                        GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 10""", (fecha_inicio,))
            usuarios = [((r[0] or 'Usuario').strip(), r[1]) for r in c.fetchall()]
        
        # 4. Categorías
        if DATABASE_URL:
            c.execute("""SELECT categoria, COUNT(*) as count FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days' AND categoria IS NOT NULL
                        GROUP BY categoria ORDER BY COUNT(*) DESC""")
            categorias = [(r['categoria'], r['count']) for r in c.fetchall()]
        else:
            c.execute("SELECT categoria, COUNT(*) FROM mensajes WHERE fecha >= ? AND categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC", (fecha_inicio,))
            categorias = [(r[0], r[1]) for r in c.fetchall()]
        
        # 5. Miembros totales y nuevos
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as t FROM suscripciones WHERE estado = 'activo'")
            total_miembros = c.fetchone()['t']
            c.execute("""SELECT COUNT(*) as t FROM suscripciones 
                        WHERE fecha_registro >= CURRENT_DATE - INTERVAL '7 days' AND estado = 'activo'""")
            nuevos_7d = c.fetchone()['t']
        else:
            c.execute("SELECT COUNT(*) FROM suscripciones WHERE estado = 'activo'")
            total_miembros = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM suscripciones WHERE fecha_registro >= ? AND estado = 'activo'", (fecha_inicio,))
            nuevos_7d = c.fetchone()[0]
        
        # 6. Datos Drive si disponible
        drive_data = None
        try:
            drive_data = obtener_datos_excel_drive()
        except:
            pass
        
        drive_stats = {}
        if drive_data is not None and len(drive_data) > 0:
            try:
                ciudades = {}
                generaciones = {}
                profesiones = {}
                estados_laborales = {}
                total_drive = len(drive_data)
                for _, row in drive_data.iterrows():
                    ciudad = str(row.iloc[7]).strip() if len(row) > 7 and pd.notna(row.iloc[7]) else ''
                    # Columna B (iloc[1]) = Generación (Año de Guardiamarina)
                    gen = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
                    # Columna Y (iloc[24]) = Profesión/Actividad
                    profesion = str(row.iloc[24]).strip() if len(row) > 24 and pd.notna(row.iloc[24]) else ''
                    # Columna I (iloc[8]) = Situación Laboral
                    estado_lab = str(row.iloc[8]).strip() if len(row) > 8 and pd.notna(row.iloc[8]) else ''
                    if ciudad and ciudad.lower() not in ['nan', 'none', '']:
                        ciudades[ciudad] = ciudades.get(ciudad, 0) + 1
                    # Extraer año de 4 dígitos del valor de generación
                    if gen and gen.lower() not in ['nan', 'none', '']:
                        gen_digits = ''.join(c for c in gen if c.isdigit())
                        if len(gen_digits) == 4 and 1950 <= int(gen_digits) <= 2025:
                            generaciones[gen_digits] = generaciones.get(gen_digits, 0) + 1
                    if profesion and profesion.lower() not in ['nan', 'none', '']:
                        profesiones[profesion[:30]] = profesiones.get(profesion[:30], 0) + 1
                    if estado_lab and estado_lab.lower() not in ['nan', 'none', '']:
                        estados_laborales[estado_lab[:25]] = estados_laborales.get(estado_lab[:25], 0) + 1
                drive_stats['ciudades'] = sorted(ciudades.items(), key=lambda x: -x[1])[:12]
                drive_stats['generaciones'] = sorted(generaciones.items(), key=lambda x: x[0])
                drive_stats['profesiones'] = sorted(profesiones.items(), key=lambda x: -x[1])[:10]
                drive_stats['estados_laborales'] = sorted(estados_laborales.items(), key=lambda x: -x[1])[:8]
                drive_stats['total_registros'] = total_drive
            except Exception as e:
                logger.warning(f"Error extrayendo drive_stats: {e}")
        
        conn.close()
        
        # ===== GENERAR HTML CON ECHARTS =====
        total_msgs_7d = sum(d[1] for d in por_dia) if por_dia else 0
        promedio_diario = round(total_msgs_7d / max(len(por_dia), 1), 1)
        hora_pico = max(por_hora, key=lambda x: x[1])[0] if por_hora else 0
        
        # Limpiar nombres usuarios
        usuarios_clean = []
        for u in usuarios[:8]:
            n = str(u[0]).replace('_', ' ').strip()
            if n.lower() in ['group', 'grupo', 'channel', 'canal', 'none', 'null', '']:
                n = 'Cofradía'
            usuarios_clean.append((n[:20], u[1]))
        
        # JSON data
        import json as _json
        dias_labels = _json.dumps([d[0][-5:] for d in por_dia])
        dias_values = _json.dumps([d[1] for d in por_dia])
        horas_labels = _json.dumps([f"{h[0]:02d}:00" for h in por_hora])
        horas_values = _json.dumps([h[1] for h in por_hora])
        users_labels = _json.dumps([u[0] for u in usuarios_clean])
        users_values = _json.dumps([u[1] for u in usuarios_clean])
        cats_data = _json.dumps([{'name': c[0] or 'General', 'value': c[1]} for c in categorias[:8]])
        
        # Drive data JSON
        ciudades_json = _json.dumps([{'name': c[0], 'value': c[1]} for c in drive_stats.get('ciudades', [])])
        gen_labels = _json.dumps([g[0] for g in drive_stats.get('generaciones', [])])
        gen_values = _json.dumps([g[1] for g in drive_stats.get('generaciones', [])])
        profesiones_json = _json.dumps([{'name': p[0], 'value': p[1]} for p in drive_stats.get('profesiones', [])])
        estados_json = _json.dumps([{'name': e[0], 'value': e[1]} for e in drive_stats.get('estados_laborales', [])])
        total_drive_reg = drive_stats.get('total_registros', 0)
        
        has_drive = 'true' if drive_stats else 'false'
        
        html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dashboard Cofradía de Networking</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ 
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    background: linear-gradient(135deg, #0a1628 0%, #0f2f59 50%, #1a3a6a 100%);
    color: #e0e6ed; min-height: 100vh; padding: 20px;
}}
.header {{
    text-align: center; padding: 30px 0 20px;
    border-bottom: 2px solid rgba(195,165,90,0.4);
    margin-bottom: 25px;
}}
.header h1 {{
    font-size: 2.2em; color: #c3a55a;
    text-shadow: 0 2px 10px rgba(195,165,90,0.3);
    letter-spacing: 2px;
}}
.header .subtitle {{ color: #8899aa; font-size: 1em; margin-top: 5px; }}
.kpi-row {{
    display: flex; gap: 15px; margin-bottom: 25px; flex-wrap: wrap; justify-content: center;
}}
.kpi {{
    background: linear-gradient(135deg, rgba(15,47,89,0.8), rgba(30,80,140,0.4));
    border: 1px solid rgba(195,165,90,0.3); border-radius: 12px;
    padding: 20px 30px; text-align: center; flex: 1; min-width: 180px; max-width: 220px;
    backdrop-filter: blur(10px);
}}
.kpi .value {{
    font-size: 2.5em; font-weight: 800; color: #c3a55a;
    text-shadow: 0 0 20px rgba(195,165,90,0.4);
}}
.kpi .label {{ font-size: 0.85em; color: #8899aa; margin-top: 5px; text-transform: uppercase; letter-spacing: 1px; }}
.charts-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
    gap: 20px; margin-bottom: 20px;
}}
.chart-card {{
    background: linear-gradient(145deg, rgba(15,47,89,0.6), rgba(10,22,40,0.8));
    border: 1px solid rgba(52,120,195,0.2); border-radius: 14px;
    padding: 15px; min-height: 380px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.3);
}}
.chart-card .title {{
    font-size: 1.1em; color: #c3a55a; margin-bottom: 10px;
    padding-bottom: 8px; border-bottom: 1px solid rgba(195,165,90,0.2);
    font-weight: 600; letter-spacing: 0.5px;
}}
.chart {{ width: 100%; height: 320px; }}
.footer {{
    text-align: center; padding: 20px 0; color: #556677;
    font-size: 0.85em; border-top: 1px solid rgba(195,165,90,0.2);
    margin-top: 20px;
}}
.footer span {{ color: #c3a55a; }}
</style>
</head>
<body>
<div class="header">
    <h1>⚓ COFRADÍA DE NETWORKING</h1>
    <div class="subtitle">Dashboard de Actividad — Últimos 7 días</div>
</div>

<div class="kpi-row">
    <div class="kpi"><div class="value">{total_msgs_7d}</div><div class="label">Mensajes</div></div>
    <div class="kpi"><div class="value">{len(usuarios_clean)}</div><div class="label">Usuarios Activos</div></div>
    <div class="kpi"><div class="value">{promedio_diario}</div><div class="label">Promedio/Día</div></div>
    <div class="kpi"><div class="value">{hora_pico:02d}:00</div><div class="label">Hora Pico</div></div>
    <div class="kpi"><div class="value">{total_miembros}</div><div class="label">Miembros</div></div>
    <div class="kpi"><div class="value">+{nuevos_7d}</div><div class="label">Nuevos 7d</div></div>
    <div class="kpi"><div class="value">{total_drive_reg}</div><div class="label">BD Excel Drive</div></div>
</div>

<div class="charts-grid">
    <div class="chart-card">
        <div class="title">📈 Actividad Diaria</div>
        <div id="chart-diario" class="chart"></div>
    </div>
    <div class="chart-card">
        <div class="title">🕐 Actividad por Hora</div>
        <div id="chart-hora" class="chart"></div>
    </div>
    <div class="chart-card">
        <div class="title">👥 Top Usuarios Activos</div>
        <div id="chart-usuarios" class="chart"></div>
    </div>
    <div class="chart-card">
        <div class="title">📂 Categorías de Mensajes</div>
        <div id="chart-categorias" class="chart"></div>
    </div>
</div>

<div id="drive-section" class="charts-grid" style="display:none;">
    <div class="chart-card">
        <div class="title">🌎 Distribución por Ciudad</div>
        <div id="chart-ciudades" class="chart"></div>
    </div>
    <div class="chart-card">
        <div class="title">⚓ Distribución por Generación (Col B Excel)</div>
        <div id="chart-generaciones" class="chart"></div>
    </div>
    <div class="chart-card">
        <div class="title">💼 Top Profesiones / Cargos</div>
        <div id="chart-profesiones" class="chart"></div>
    </div>
    <div class="chart-card">
        <div class="title">📋 Situación Laboral</div>
        <div id="chart-estados" class="chart"></div>
    </div>
</div>

<div class="footer">
    Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')} · <span>Cofradía de Networking</span> · Bot Premium v4.3 ECharts
</div>

<script>
const gold = '#c3a55a';
const goldLight = '#d4b86a';
const blue = '#3478c3';
const blueLight = '#5a9fd4';
const navy = '#0f2f59';
const textColor = '#c0c8d4';

// ===== CHART 1: Actividad Diaria =====
var c1 = echarts.init(document.getElementById('chart-diario'));
c1.setOption({{
    tooltip: {{ trigger: 'axis', backgroundColor: 'rgba(15,47,89,0.95)', borderColor: gold, textStyle: {{ color: textColor }} }},
    grid: {{ left: '8%', right: '5%', bottom: '12%', top: '10%' }},
    xAxis: {{ type: 'category', data: {dias_labels}, axisLabel: {{ color: textColor, fontSize: 11 }}, axisLine: {{ lineStyle: {{ color: '#2a4a6a' }} }} }},
    yAxis: {{ type: 'value', axisLabel: {{ color: textColor }}, splitLine: {{ lineStyle: {{ color: 'rgba(52,120,195,0.15)' }} }} }},
    series: [{{
        type: 'line', data: {dias_values}, smooth: true,
        lineStyle: {{ color: gold, width: 3 }},
        areaStyle: {{ color: {{ type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [{{ offset: 0, color: 'rgba(195,165,90,0.4)' }}, {{ offset: 1, color: 'rgba(195,165,90,0.02)' }}] }} }},
        itemStyle: {{ color: gold, borderWidth: 2 }},
        symbol: 'circle', symbolSize: 8,
        emphasis: {{ itemStyle: {{ borderWidth: 3, borderColor: '#fff' }} }}
    }}]
}});

// ===== CHART 2: Por Hora =====
var c2 = echarts.init(document.getElementById('chart-hora'));
c2.setOption({{
    tooltip: {{ trigger: 'axis', backgroundColor: 'rgba(15,47,89,0.95)', borderColor: blue, textStyle: {{ color: textColor }} }},
    grid: {{ left: '8%', right: '5%', bottom: '12%', top: '10%' }},
    xAxis: {{ type: 'category', data: {horas_labels}, axisLabel: {{ color: textColor, fontSize: 10, rotate: 45 }}, axisLine: {{ lineStyle: {{ color: '#2a4a6a' }} }} }},
    yAxis: {{ type: 'value', axisLabel: {{ color: textColor }}, splitLine: {{ lineStyle: {{ color: 'rgba(52,120,195,0.15)' }} }} }},
    series: [{{
        type: 'bar', data: {horas_values},
        itemStyle: {{
            color: {{ type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
                colorStops: [{{ offset: 0, color: blueLight }}, {{ offset: 1, color: 'rgba(52,120,195,0.3)' }}] }},
            borderRadius: [4, 4, 0, 0]
        }},
        emphasis: {{ itemStyle: {{ color: gold }} }}
    }}]
}});

// ===== CHART 3: Usuarios =====
var c3 = echarts.init(document.getElementById('chart-usuarios'));
c3.setOption({{
    tooltip: {{ trigger: 'axis', backgroundColor: 'rgba(15,47,89,0.95)', borderColor: gold, textStyle: {{ color: textColor }} }},
    grid: {{ left: '30%', right: '12%', bottom: '5%', top: '5%' }},
    xAxis: {{ type: 'value', axisLabel: {{ color: textColor }}, splitLine: {{ lineStyle: {{ color: 'rgba(52,120,195,0.15)' }} }} }},
    yAxis: {{ type: 'category', data: {users_labels}, inverse: true, axisLabel: {{ color: textColor, fontSize: 11 }}, axisLine: {{ lineStyle: {{ color: '#2a4a6a' }} }} }},
    series: [{{
        type: 'bar', data: {users_values},
        itemStyle: {{
            color: {{ type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
                colorStops: [{{ offset: 0, color: 'rgba(195,165,90,0.3)' }}, {{ offset: 1, color: gold }}] }},
            borderRadius: [0, 6, 6, 0]
        }},
        label: {{ show: true, position: 'right', color: gold, fontWeight: 'bold', fontSize: 13 }}
    }}]
}});

// ===== CHART 4: Categorías =====
var c4 = echarts.init(document.getElementById('chart-categorias'));
c4.setOption({{
    tooltip: {{ trigger: 'item', backgroundColor: 'rgba(15,47,89,0.95)', borderColor: gold, textStyle: {{ color: textColor }},
        formatter: '{{b}}: {{c}} ({{d}}%)' }},
    series: [{{
        type: 'pie', radius: ['35%', '70%'], center: ['50%', '55%'],
        data: {cats_data},
        itemStyle: {{ borderColor: 'rgba(10,22,40,0.8)', borderWidth: 2, borderRadius: 6 }},
        label: {{ color: textColor, fontSize: 11, formatter: '{{b}}\\n{{d}}%' }},
        emphasis: {{ itemStyle: {{ shadowBlur: 20, shadowColor: 'rgba(195,165,90,0.5)' }},
            label: {{ fontSize: 14, fontWeight: 'bold' }} }},
        color: [gold, blue, blueLight, '#2ecc71', '#e74c3c', '#9b59b6', '#f39c12', '#1abc9c']
    }}]
}});

// ===== DRIVE CHARTS =====
if ({has_drive}) {{
    document.getElementById('drive-section').style.display = 'grid';
    
    var c5 = echarts.init(document.getElementById('chart-ciudades'));
    c5.setOption({{
        tooltip: {{ trigger: 'item', backgroundColor: 'rgba(15,47,89,0.95)', borderColor: gold, textStyle: {{ color: textColor }} }},
        series: [{{
            type: 'pie', radius: ['25%', '65%'], center: ['50%', '55%'],
            roseType: 'area', data: {ciudades_json},
            itemStyle: {{ borderColor: 'rgba(10,22,40,0.8)', borderWidth: 2, borderRadius: 4 }},
            label: {{ color: textColor, fontSize: 10, formatter: '{{b}}: {{c}}' }},
            color: [gold, blue, blueLight, '#2ecc71', '#e74c3c', '#9b59b6', '#f39c12', '#1abc9c', '#3498db', '#e67e22', '#c0392b', '#16a085']
        }}]
    }});
    
    var c6 = echarts.init(document.getElementById('chart-generaciones'));
    c6.setOption({{
        tooltip: {{ trigger: 'axis', backgroundColor: 'rgba(15,47,89,0.95)', borderColor: gold, textStyle: {{ color: textColor }} }},
        grid: {{ left: '10%', right: '5%', bottom: '15%', top: '10%' }},
        xAxis: {{ type: 'category', data: {gen_labels}, axisLabel: {{ color: textColor, fontSize: 10, rotate: 45 }}, axisLine: {{ lineStyle: {{ color: '#2a4a6a' }} }} }},
        yAxis: {{ type: 'value', axisLabel: {{ color: textColor }}, splitLine: {{ lineStyle: {{ color: 'rgba(52,120,195,0.15)' }} }} }},
        series: [{{
            type: 'bar', data: {gen_values},
            itemStyle: {{
                color: {{ type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
                    colorStops: [{{ offset: 0, color: gold }}, {{ offset: 1, color: 'rgba(195,165,90,0.2)' }}] }},
                borderRadius: [4, 4, 0, 0]
            }},
            label: {{ show: true, position: 'top', color: gold, fontSize: 11 }}
        }}]
    }});
    
    // Chart 7: Profesiones (horizontal bar)
    var c7 = echarts.init(document.getElementById('chart-profesiones'));
    var profData = {profesiones_json};
    c7.setOption({{
        tooltip: {{ trigger: 'axis', backgroundColor: 'rgba(15,47,89,0.95)', borderColor: blue, textStyle: {{ color: textColor }} }},
        grid: {{ left: '35%', right: '10%', bottom: '5%', top: '5%' }},
        xAxis: {{ type: 'value', axisLabel: {{ color: textColor }}, splitLine: {{ lineStyle: {{ color: 'rgba(52,120,195,0.15)' }} }} }},
        yAxis: {{ type: 'category', data: profData.map(p => p.name), inverse: true, axisLabel: {{ color: textColor, fontSize: 10 }}, axisLine: {{ lineStyle: {{ color: '#2a4a6a' }} }} }},
        series: [{{
            type: 'bar', data: profData.map(p => p.value),
            itemStyle: {{
                color: {{ type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
                    colorStops: [{{ offset: 0, color: 'rgba(52,120,195,0.3)' }}, {{ offset: 1, color: blueLight }}] }},
                borderRadius: [0, 6, 6, 0]
            }},
            label: {{ show: true, position: 'right', color: blueLight, fontWeight: 'bold', fontSize: 12 }}
        }}]
    }});
    
    // Chart 8: Situación Laboral (pie)
    var c8 = echarts.init(document.getElementById('chart-estados'));
    c8.setOption({{
        tooltip: {{ trigger: 'item', backgroundColor: 'rgba(15,47,89,0.95)', borderColor: gold, textStyle: {{ color: textColor }},
            formatter: '{{b}}: {{c}} ({{d}}%)' }},
        series: [{{
            type: 'pie', radius: ['30%', '65%'], center: ['50%', '55%'],
            data: {estados_json},
            itemStyle: {{ borderColor: 'rgba(10,22,40,0.8)', borderWidth: 2, borderRadius: 5 }},
            label: {{ color: textColor, fontSize: 10, formatter: '{{b}}\\n{{d}}%' }},
            emphasis: {{ itemStyle: {{ shadowBlur: 15, shadowColor: 'rgba(195,165,90,0.4)' }} }},
            color: ['#2ecc71', '#e74c3c', '#f39c12', gold, blue, '#9b59b6', '#1abc9c', blueLight]
        }}]
    }});
}}

// Responsive
window.addEventListener('resize', function() {{
    c1.resize(); c2.resize(); c3.resize(); c4.resize();
    if ({has_drive}) {{ c5.resize(); c6.resize(); c7.resize(); c8.resize(); }}
}});
</script>
</body>
</html>"""
        
        # Guardar HTML
        html_path = f"/tmp/cofradia_dashboard_{update.effective_user.id}.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        # También generar imagen preview con matplotlib
        try:
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            fig.patch.set_facecolor('#0a1628')
            fig.suptitle('COFRADÍA DE NETWORKING — Dashboard 7 días', fontsize=16, fontweight='bold', color='#c3a55a')
            
            for ax in axes.flat:
                ax.set_facecolor('#0f2244')
                ax.tick_params(colors='#8899aa')
                ax.spines['bottom'].set_color('#2a4a6a')
                ax.spines['left'].set_color('#2a4a6a')
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
            
            # G1: Actividad diaria
            if por_dia:
                axes[0,0].fill_between(range(len(por_dia)), [d[1] for d in por_dia], alpha=0.3, color='#c3a55a')
                axes[0,0].plot(range(len(por_dia)), [d[1] for d in por_dia], color='#c3a55a', linewidth=2, marker='o', markersize=6)
                axes[0,0].set_xticks(range(len(por_dia)))
                axes[0,0].set_xticklabels([d[0][-5:] for d in por_dia], fontsize=8, color='#8899aa')
            axes[0,0].set_title('Actividad Diaria', color='#c3a55a', fontsize=12)
            axes[0,0].set_ylabel('Mensajes', color='#8899aa')
            
            # G2: Por hora
            if por_hora:
                colors_h = ['#FFD700' if 6<=h[0]<12 else '#3478c3' if 12<=h[0]<18 else '#5a9fd4' if 18<=h[0]<22 else '#2C3E50' for h in por_hora]
                axes[0,1].bar([h[0] for h in por_hora], [h[1] for h in por_hora], color=colors_h, alpha=0.85)
            axes[0,1].set_title('Actividad por Hora', color='#c3a55a', fontsize=12)
            axes[0,1].set_xlabel('Hora', color='#8899aa')
            
            # G3: Top usuarios
            if usuarios_clean:
                names = [u[0][:15] for u in usuarios_clean[:8]]
                vals = [u[1] for u in usuarios_clean[:8]]
                bars = axes[1,0].barh(names, vals, color='#c3a55a', alpha=0.8, edgecolor='#d4b86a')
                for bar, val in zip(bars, vals):
                    axes[1,0].text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2, str(val), va='center', color='#c3a55a', fontsize=10, fontweight='bold')
            axes[1,0].set_title('Top Usuarios', color='#c3a55a', fontsize=12)
            axes[1,0].invert_yaxis()
            
            # G4: Categorías
            if categorias:
                cat_names = [c[0] or 'General' for c in categorias[:6]]
                cat_vals = [c[1] for c in categorias[:6]]
                wedge_colors = ['#c3a55a', '#3478c3', '#5a9fd4', '#2ecc71', '#e74c3c', '#9b59b6']
                axes[1,1].pie(cat_vals, labels=cat_names, colors=wedge_colors[:len(cat_names)], 
                            autopct='%1.0f%%', textprops={'color': '#e0e6ed', 'fontsize': 9},
                            wedgeprops={'edgecolor': '#0a1628', 'linewidth': 2})
            axes[1,1].set_title('Categorías', color='#c3a55a', fontsize=12)
            
            plt.tight_layout()
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor='#0a1628')
            buf.seek(0)
            plt.close()
            
            # Enviar imagen preview
            await msg.delete()
            await update.message.reply_photo(
                photo=buf,
                caption=f"📊 Dashboard Cofradía — Últimos 7 días\n\n"
                        f"📨 {total_msgs_7d} mensajes · 👥 {len(usuarios_clean)} usuarios activos\n"
                        f"📈 Promedio: {promedio_diario}/día · 🕐 Pico: {hora_pico:02d}:00\n\n"
                        f"⬇️ Descarga el archivo HTML adjunto para ver el dashboard interactivo completo con ECharts."
            )
        except Exception as e:
            logger.warning(f"Error matplotlib preview: {e}")
            await msg.edit_text("📊 Dashboard generado. Descarga el archivo HTML adjunto.")
        
        # Enviar HTML como documento
        with open(html_path, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=f"cofradia_dashboard_{datetime.now().strftime('%Y%m%d')}.html",
                caption="📊 Dashboard Interactivo ECharts\n\nAbre este archivo en tu navegador para ver gráficos interactivos con animaciones y tooltips."
            )
        
        # Limpiar
        try:
            os.remove(html_path)
        except:
            pass
        
        registrar_servicio_usado(update.effective_user.id, 'graficos')
        
    except Exception as e:
        logger.error(f"Error en graficos_comando: {e}")
        await msg.edit_text(f"❌ Error generando gráficos.\n\nDetalle: {str(e)[:100]}")


async def buscar_ia_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /buscar_ia - Búsqueda inteligente UNIFICADA (historial + RAG + IA)"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso: /buscar_ia [tu consulta]\n\n"
            "Ejemplo: /buscar_ia aniversario\n\n"
            "Busca en TODAS las fuentes: historial del grupo, "
            "documentos PDF indexados, y base de datos de profesionales."
        )
        return
    
    consulta = ' '.join(context.args)
    msg = await update.message.reply_text("🔍 Buscando en todas las fuentes de conocimiento...")
    
    # Búsqueda unificada en todas las fuentes (máximo contexto)
    resultados = busqueda_unificada(consulta, limit_historial=15, limit_rag=30)
    
    tiene_historial = bool(resultados.get('historial'))
    tiene_rag = bool(resultados.get('rag'))
    rag_conf = resultados.get('rag_confianza', 'ninguna')
    
    if not tiene_historial and not tiene_rag:
        # Aunque no haya RAG, la IA puede responder con su propio conocimiento
        if ia_disponible:
            await msg.edit_text("🧠 Consultando conocimiento del asistente IA...")
            prompt_fallback = f"""Eres el asistente IA de la Cofradía de Networking. No encontré información sobre "{consulta}" en los documentos indexados de la comunidad, pero respondo desde mi conocimiento general como LLM experto.

Pregunta: {consulta}

Responde de forma completa y útil en español. No menciones que "no hay documentos" — simplemente responde lo que sabes. Máximo 350 palabras."""
            respuesta_fb = llamar_groq(prompt_fallback, max_tokens=800, temperature=0.5)
            if respuesta_fb:
                await msg.delete()
                await enviar_mensaje_largo(update, f"🔍 {consulta}\n📊 Fuente: Conocimiento IA\n{'━'*25}\n\n{respuesta_fb}")
                registrar_servicio_usado(update.effective_user.id, 'buscar_ia')
                return
        
        await msg.edit_text(
            f"❌ No se encontraron resultados para: {consulta}\n\n"
            f"💡 Intenta con otras palabras clave.\n"
            f"💡 Usa /rag_status para ver documentos indexados."
        )
        return
    
    fuentes = ', '.join(resultados.get('fuentes_usadas', []))
    
    # Si no hay IA, mostrar resultados crudos
    if not ia_disponible:
        await msg.delete()
        mensaje = f"🔍 RESULTADOS PARA: {consulta}\n"
        mensaje += f"📊 Fuentes: {fuentes}\n\n"
        
        if tiene_historial:
            mensaje += "💬 HISTORIAL DEL GRUPO:\n"
            for nombre, texto, fecha in resultados['historial'][:8]:
                nombre_limpio = limpiar_nombre_display(nombre)
                fecha_str = fecha.strftime("%d/%m/%Y %H:%M") if hasattr(fecha, 'strftime') else str(fecha)[:16]
                texto_corto = texto[:150] + "..." if len(texto) > 150 else texto
                mensaje += f"  {nombre_limpio} ({fecha_str}): {texto_corto}\n\n"
        
        if tiene_rag:
            mensaje += "📄 DOCUMENTOS:\n"
            for i, item in enumerate(resultados['rag'][:5], 1):
                chunk_texto = item[0] if isinstance(item, tuple) else item
                mensaje += f"  [{i}] {chunk_texto[:200]}...\n\n"
        
        await enviar_mensaje_largo(update, mensaje)
        registrar_servicio_usado(update.effective_user.id, 'buscar_ia')
        return
    
    # Preparar contexto COMPLETO para la IA
    await msg.edit_text("🧠 Analizando resultados con IA...")
    
    contexto_completo = formatear_contexto_unificado(resultados, consulta)
    modo_buscar = ("USA el contexto encontrado como base." if rag_conf in ('alta','media')
                   else "El RAG tiene relevancia baja/ninguna → si sabes del tema como LLM, responde desde tu conocimiento.")
    
    prompt = f"""Eres el asistente IA de la Cofradía de Networking, comunidad de oficiales de la Armada de Chile.

MODO: {modo_buscar}
REGLAS: (1) NUNCA digas "no tengo información" si conoces el tema. (2) NUNCA inventes personas de la Cofradía. (3) NUNCA modifiques datos.

CONSULTA: "{consulta}"

CONTEXTO (fuentes: {fuentes}, confianza RAG: {rag_conf}):
{contexto_completo if contexto_completo else "(Sin contexto relevante)"}

Responde de forma completa, útil y natural. Sin asteriscos. Máximo 400 palabras."""

    respuesta = llamar_groq(prompt, max_tokens=1000, temperature=0.3)
    
    await msg.delete()
    
    if respuesta:
        respuesta_limpia = respuesta.replace('*', '').replace('_', ' ')
        mensaje_final = f"🔍 BÚSQUEDA IA: {consulta}\n"
        mensaje_final += f"📊 Fuentes consultadas: {fuentes}\n"
        mensaje_final += "━" * 25 + "\n\n"
        mensaje_final += respuesta_limpia
        
        await enviar_mensaje_largo(update, mensaje_final)
        registrar_servicio_usado(update.effective_user.id, 'buscar_ia')
    else:
        # Fallback: mostrar resultados crudos
        mensaje = f"🔍 RESULTADOS PARA: {consulta}\n"
        mensaje += f"(IA no pudo generar resumen)\n\n"
        if tiene_historial:
            for nombre, texto, fecha in resultados['historial'][:6]:
                texto_corto = texto[:150] + "..." if len(texto) > 150 else texto
                mensaje += f"👤 {nombre}: {texto_corto}\n\n"
        if tiene_rag:
            for item in resultados['rag'][:4]:
                chunk_texto = item[0] if isinstance(item, tuple) else item
                mensaje += f"📄 {chunk_texto[:200]}...\n\n"
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
        
        # MOTOR DE INTENCIÓN: Mejorar query del grupo silenciosamente
        intencion_mencion = mejorar_intencion(pregunta, user_name, user_id, canal='grupo')
        pregunta_mejorada = intencion_mencion['query_mejorada']
        
        # Si detectó un comando claro (tipo=='comando'), ejecutarlo directamente
        if (intencion_mencion['ejecutar_comando'] and intencion_mencion['comando'] 
                and intencion_mencion['tipo'] == 'comando'):
            ejecutado = await ejecutar_comando_desde_intencion(
                intencion_mencion['comando'], intencion_mencion['args'], update, context
            )
            if ejecutado:
                await msg.delete()
                return
        
        # BÚSQUEDA UNIFICADA usando query mejorada
        resultados_unificados = busqueda_unificada(pregunta_mejorada, limit_historial=10, limit_rag=25)
        contexto_completo = formatear_contexto_unificado(resultados_unificados, pregunta_mejorada)
        fuentes = ', '.join(resultados_unificados.get('fuentes_usadas', []))
        rag_conf = resultados_unificados.get('rag_confianza', 'ninguna')
        rag_score = resultados_unificados.get('rag_score_max', 0.0)
        
        modo = ("✅ Usa el contexto RAG como base principal de tu respuesta." if rag_conf in ('alta','media')
                else "🔵 El RAG no tiene documentos sobre este tema — responde COMPLETAMENTE desde tu conocimiento como LLM experto (no digas 'no tengo información' si sabes del tema).")
        
        prompt = f"""Eres el asistente IA de la Cofradía de Networking — comunidad profesional chilena de oficiales de la Armada (activos y retirados). Eres un modelo LLaMA 3.3 70B con conocimiento hasta 2024.

MODO ACTUAL: {modo}
(Confianza RAG: {rag_conf}, score: {rag_score:.1f})

REGLAS:
1. NUNCA digas "no tengo información" sobre temas que conoces (libros, economía, historia, ciencia, tecnología, etc.)
2. NUNCA inventes nombres de personas de la Cofradía — solo los del contexto
3. NUNCA modifiques datos de usuarios
4. Cuando el RAG tiene info relevante → úsala; cuando no → usa tu conocimiento propio sin disculparte
5. Responde de forma completa, clara y útil en máximo 3-4 párrafos
6. Sin asteriscos ni formatos Markdown

PREGUNTA DE {user_name}: "{pregunta}"

CONTEXTO ENCONTRADO (fuentes: {fuentes}):
{contexto_completo if contexto_completo else "(Sin contexto relevante en documentos indexados)"}"""

        respuesta = llamar_groq(prompt, max_tokens=1000, temperature=0.5)
        
        await msg.delete()
        
        if respuesta:
            respuesta_limpia = respuesta.replace('*', '').replace('_', ' ')
            await enviar_mensaje_largo(update, respuesta_limpia)
            registrar_servicio_usado(user_id, 'ia_mencion')
        else:
            await update.message.reply_text(
                "❌ No pude generar respuesta.\n\n"
                "Comandos útiles:\n"
                "/buscar_profesional [profesión]\n"
                "/empleo [cargo]\n"
                "/buscar_ia [tema]\n"
                "/rag_consulta [tema]"
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
    
    # Detectar nombre correcto
    first_name = user.first_name or ""
    last_name = user.last_name or ""
    user_id = user.id
    
    # DETECCIÓN DE ADMIN ANÓNIMO: Si from_user es un grupo/canal (ID negativo o nombre 'Group')
    # Telegram envía from_user.id = ID del grupo cuando admin postea anónimamente
    sender_chat = getattr(update.message, 'sender_chat', None)
    es_admin_anonimo = (
        sender_chat is not None or 
        user_id < 0 or 
        first_name.lower() in ['group', 'grupo', 'channel', 'canal', 'cofradía', 'cofradía de networking']
    )
    
    if es_admin_anonimo:
        # Asumir que es el owner (único admin del grupo)
        user_id = OWNER_ID
        first_name = "Germán"
        last_name = "Perey"
    elif user_id == OWNER_ID:
        # Si el owner postea con su cuenta normal, forzar nombre
        first_name = "Germán"
        last_name = "Perey"
    
    # Si el nombre parece ser un grupo/canal, usar username o ID
    if first_name.lower() in ['group', 'grupo', 'channel', 'canal'] or not first_name:
        if user.username:
            first_name = user.username
        else:
            first_name = f"ID_{user_id}"
    
    guardar_mensaje(
        user_id,
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
                             (last_name, first_name, str(user_id)))
                else:
                    c.execute("""UPDATE mensajes SET last_name = ?, first_name = ?
                                WHERE user_id = ? AND (last_name IS NULL OR last_name = '')""",
                             (last_name, first_name, str(user_id)))
                conn.commit()
                conn.close()
        except Exception:
            pass
    
    # Verificar alertas de otros usuarios (en background, no bloquea)
    try:
        nombre_display = f"{first_name} {last_name}".strip()
        asyncio.create_task(verificar_alertas_mensaje(user_id, update.message.text, nombre_display, context))
        
        # Cofradía Coins: +1 por mensaje en grupo
        otorgar_coins(user_id, 1, 'Mensaje en grupo')
    except Exception:
        pass


# ==================== COMANDOS ADMIN ====================

async def buscar_usuario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /buscar_usuario [nombre] - Buscar ID de usuario por nombre (admin)"""
    if update.effective_user.id != OWNER_ID:
        return
    
    if not context.args:
        await update.message.reply_text(
            "🔍 Uso: /buscar_usuario [nombre o apellido]\n\n"
            "Ejemplo:\n"
            "/buscar_usuario Pérez\n"
            "/buscar_usuario Juan Carlos\n"
            "/buscar_usuario @username"
        )
        return
    
    busqueda = ' '.join(context.args).lower().replace('@', '')
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión")
            return
        c = conn.cursor()
        
        resultados = []
        
        # Buscar en suscripciones
        if DATABASE_URL:
            c.execute("""SELECT user_id, first_name, last_name, username, estado, fecha_expiracion 
                        FROM suscripciones 
                        WHERE LOWER(first_name) LIKE %s OR LOWER(COALESCE(last_name,'')) LIKE %s 
                        OR LOWER(COALESCE(username,'')) LIKE %s
                        ORDER BY first_name LIMIT 15""",
                     (f"%{busqueda}%", f"%{busqueda}%", f"%{busqueda}%"))
        else:
            c.execute("""SELECT user_id, first_name, last_name, username, estado, fecha_expiracion 
                        FROM suscripciones 
                        WHERE LOWER(first_name) LIKE ? OR LOWER(COALESCE(last_name,'')) LIKE ? 
                        OR LOWER(COALESCE(username,'')) LIKE ?
                        ORDER BY first_name LIMIT 15""",
                     (f"%{busqueda}%", f"%{busqueda}%", f"%{busqueda}%"))
        
        for r in c.fetchall():
            uid = r['user_id'] if DATABASE_URL else r[0]
            fname = r['first_name'] if DATABASE_URL else r[1]
            lname = r['last_name'] if DATABASE_URL else r[2]
            uname = r['username'] if DATABASE_URL else r[3]
            estado = r['estado'] if DATABASE_URL else r[4]
            exp = r['fecha_expiracion'] if DATABASE_URL else r[5]
            resultados.append({
                'uid': uid, 'nombre': f"{fname or ''} {lname or ''}".strip(),
                'username': uname, 'estado': estado, 'exp': str(exp)[:10] if exp else '?',
                'origen': 'suscripción'
            })
        
        # Buscar también en nuevos_miembros (solicitudes)
        if DATABASE_URL:
            c.execute("""SELECT user_id, nombre, apellido, username, estado, generacion 
                        FROM nuevos_miembros 
                        WHERE LOWER(COALESCE(nombre,'')) LIKE %s OR LOWER(COALESCE(apellido,'')) LIKE %s 
                        OR LOWER(COALESCE(username,'')) LIKE %s
                        ORDER BY nombre LIMIT 10""",
                     (f"%{busqueda}%", f"%{busqueda}%", f"%{busqueda}%"))
        else:
            c.execute("""SELECT user_id, nombre, apellido, username, estado, generacion 
                        FROM nuevos_miembros 
                        WHERE LOWER(COALESCE(nombre,'')) LIKE ? OR LOWER(COALESCE(apellido,'')) LIKE ? 
                        OR LOWER(COALESCE(username,'')) LIKE ?
                        ORDER BY nombre LIMIT 10""",
                     (f"%{busqueda}%", f"%{busqueda}%", f"%{busqueda}%"))
        
        for r in c.fetchall():
            uid = r['user_id'] if DATABASE_URL else r[0]
            nombre = r['nombre'] if DATABASE_URL else r[1]
            apellido = r['apellido'] if DATABASE_URL else r[2]
            uname = r['username'] if DATABASE_URL else r[3]
            estado = r['estado'] if DATABASE_URL else r[4]
            gen = r['generacion'] if DATABASE_URL else r[5]
            # Evitar duplicados
            if not any(x['uid'] == uid for x in resultados):
                resultados.append({
                    'uid': uid, 'nombre': f"{nombre or ''} {apellido or ''}".strip(),
                    'username': uname, 'estado': estado, 'exp': f"Gen: {gen}" if gen else '?',
                    'origen': 'solicitud'
                })
        
        conn.close()
        
        if not resultados:
            await update.message.reply_text(f"❌ No se encontró ningún usuario con: \"{busqueda}\"")
            return
        
        msg = f"🔍 USUARIOS ENCONTRADOS: \"{busqueda}\"\n{'━' * 30}\n\n"
        for r in resultados:
            estado_icon = "✅" if r['estado'] == 'activo' else "⏳" if r['estado'] == 'pendiente' else "❌"
            msg += f"{estado_icon} {r['nombre']}\n"
            msg += f"   🆔 {r['uid']}\n"
            if r['username']: msg += f"   👤 @{r['username']}\n"
            msg += f"   📋 {r['estado']} | {r['exp']} ({r['origen']})\n\n"
        
        msg += f"📊 {len(resultados)} resultado(s)\n\n"
        msg += "💡 Usa el ID para:\n"
        msg += "/aprobar_solicitud [ID]\n"
        msg += "/editar_usuario [ID] [campo] [valor]\n"
        msg += "/eliminar_solicitud [ID]"
        
        await enviar_mensaje_largo(update, msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def cobros_admin_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /cobros_admin - Panel admin"""
    if update.effective_user.id != OWNER_ID:
        return
    
    await update.message.reply_text(
        "👑 PANEL DE ADMINISTRACIÓN\n\n"
        "💰 COBROS:\n"
        "  /generar_codigo - Crear código de activación\n"
        "  /precios - Ver precios actuales\n"
        "  /set_precios [dias] [precio] [nombre] - Configurar\n"
        "  /pagos_pendientes - Ver pagos por aprobar\n\n"
        "📅 SUSCRIPCIONES:\n"
        "  /vencimientos - Próximos a vencer\n"
        "  /vencimientos_mes [1-12] - Por mes\n"
        "  /ingresos - Resumen de ingresos\n\n"
        "📈 CRECIMIENTO:\n"
        "  /crecimiento_mes [mes] - Suscriptores por mes\n"
        "  /crecimiento_anual - Resumen anual\n\n"
        "👥 USUARIOS:\n"
        "  /resumen_usuario @username - Actividad de un usuario\n"
        "  /dotacion - Miembros del grupo\n"
        "  /aprobar_solicitud - Solicitudes pendientes\n\n"
        "📂 TOPICS:\n"
        "  /ver_topics - Ver actividad por topic\n\n"
        "📚 RAG:\n"
        "  /subir_pdf - Subir documento\n"
        "  /rag_status - Estado del sistema RAG\n"
        "  /rag_reindexar - Reindexar documentos\n"
        "  /eliminar_pdf - Eliminar documento"
    )


async def generar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /generar_codigo - Admin"""
    if update.effective_user.id != OWNER_ID:
        return
    
    precios = obtener_precios()
    keyboard = [
        [InlineKeyboardButton(f"{nombre} ({dias}d) - {formato_clp(precio)}", callback_data=f"gencodigo_{dias}")]
        for dias, precio, nombre in precios
    ]
    
    await update.message.reply_text(
        "👑 GENERAR CÓDIGO\n\nSelecciona el plan:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# ==================== COMANDOS ADMIN FALTANTES ====================

async def precios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /precios - Ver precios actuales"""
    if update.effective_user.id != OWNER_ID:
        return
    
    precios = obtener_precios()
    if not precios:
        await update.message.reply_text("❌ No hay planes configurados.")
        return
    
    mensaje = "💰 PRECIOS ACTUALES\n\n"
    for dias, precio, nombre in precios:
        mensaje += f"📋 {nombre} ({dias} días): {formato_clp(precio)}\n"
    
    await update.message.reply_text(mensaje)


async def set_precios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /set_precios [dias] [precio] [nombre] - Configurar precios"""
    if update.effective_user.id != OWNER_ID:
        return
    
    if not context.args or len(context.args) < 3:
        await update.message.reply_text(
            "❌ Uso: /set_precios [dias] [precio] [nombre]\n\n"
            "Ejemplo: /set_precios 30 5000 Plan Mensual"
        )
        return
    
    try:
        dias = int(context.args[0])
        precio = int(context.args[1])
        nombre = ' '.join(context.args[2:])
        
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("DELETE FROM precios_planes WHERE dias = %s", (dias,))
                c.execute("INSERT INTO precios_planes (dias, precio, nombre) VALUES (%s, %s, %s)", (dias, precio, nombre))
            else:
                c.execute("DELETE FROM precios_planes WHERE dias = ?", (dias,))
                c.execute("INSERT INTO precios_planes (dias, precio, nombre) VALUES (?, ?, ?)", (dias, precio, nombre))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"✅ Plan actualizado: {nombre} ({dias}d) = {formato_clp(precio)}")
    except ValueError:
        await update.message.reply_text("❌ Formato inválido. Días y precio deben ser números.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def pagos_pendientes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /pagos_pendientes - Ver pagos por aprobar"""
    if update.effective_user.id != OWNER_ID:
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""SELECT id, user_id, first_name, dias_plan, precio, estado, fecha_envio 
                        FROM pagos_pendientes WHERE estado = 'pendiente' 
                        ORDER BY fecha_envio DESC LIMIT 20""")
            pagos = c.fetchall()
        else:
            c.execute("""SELECT id, user_id, first_name, dias_plan, precio, estado, fecha_envio 
                        FROM pagos_pendientes WHERE estado = 'pendiente' 
                        ORDER BY fecha_envio DESC LIMIT 20""")
            pagos = c.fetchall()
        conn.close()
        
        if not pagos:
            await update.message.reply_text("✅ No hay pagos pendientes de aprobación.")
            return
        
        mensaje = "💳 PAGOS PENDIENTES\n\n"
        for p in pagos:
            if DATABASE_URL:
                mensaje += (f"🆔 ID: {p['id']}\n"
                           f"👤 {p['first_name']} (ID: {p['user_id']})\n"
                           f"📋 Plan: {p['dias_plan']} días\n"
                           f"💰 Precio: {formato_clp(p['precio'] or 0)}\n"
                           f"📅 Fecha: {str(p['fecha_envio'])[:16]}\n\n")
            else:
                mensaje += (f"🆔 ID: {p[0]}\n"
                           f"👤 {p[2]} (ID: {p[1]})\n"
                           f"📋 Plan: {p[3]} días\n"
                           f"💰 Precio: {formato_clp(p[4] or 0)}\n"
                           f"📅 Fecha: {str(p[6])[:16]}\n\n")
        
        await enviar_mensaje_largo(update, mensaje)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def vencimientos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /vencimientos - Ver suscripciones próximas a vencer"""
    if update.effective_user.id != OWNER_ID:
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""SELECT user_id, first_name, last_name, fecha_expiracion,
                        fecha_expiracion - CURRENT_DATE as dias_restantes
                        FROM suscripciones 
                        WHERE estado = 'activo' AND fecha_expiracion IS NOT NULL
                        ORDER BY fecha_expiracion ASC LIMIT 20""")
            subs = c.fetchall()
        else:
            c.execute("""SELECT user_id, first_name, last_name, fecha_expiracion
                        FROM suscripciones 
                        WHERE estado = 'activo' AND fecha_expiracion IS NOT NULL
                        ORDER BY fecha_expiracion ASC LIMIT 20""")
            subs = c.fetchall()
        conn.close()
        
        if not subs:
            await update.message.reply_text("📋 No hay suscripciones activas con fecha de vencimiento.")
            return
        
        mensaje = "📅 VENCIMIENTOS DE SUSCRIPCIONES\n\n"
        for s in subs:
            if DATABASE_URL:
                uid = s['user_id']
                nombre = f"{s['first_name'] or ''} {s['last_name'] or ''}".strip()
                fecha = str(s['fecha_expiracion'])[:10]
                dias = s['dias_restantes']
                if hasattr(dias, 'days'):
                    dias = dias.days
            else:
                uid = s[0]
                nombre = f"{s[1] or ''} {s[2] or ''}".strip()
                fecha = str(s[3])[:10]
                dias = obtener_dias_restantes(s[0])
            
            nombre = limpiar_nombre_display(nombre)
            
            # Owner tiene suscripción ilimitada
            if uid == OWNER_ID:
                mensaje += f"👑 {nombre}: ♾️ Sin límite (Owner)\n"
                continue
            
            if dias and dias <= 7:
                emoji = "🔴"
            elif dias and dias <= 15:
                emoji = "🟡"
            else:
                emoji = "🟢"
            
            mensaje += f"{emoji} {nombre}: vence {fecha} ({dias} días)\n"
        
        await update.message.reply_text(mensaje)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def vencimientos_mes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /vencimientos_mes [mes] - Ver vencimientos de un mes específico"""
    if update.effective_user.id != OWNER_ID:
        return
    
    mes = datetime.now().month
    if context.args:
        try:
            mes = int(context.args[0])
            if mes < 1 or mes > 12:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Ingresa un mes válido (1-12)")
            return
    
    meses_nombre = ['', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        anio = datetime.now().year
        
        if DATABASE_URL:
            c.execute("""SELECT user_id, first_name, last_name, fecha_expiracion
                        FROM suscripciones 
                        WHERE estado = 'activo' 
                        AND EXTRACT(MONTH FROM fecha_expiracion) = %s
                        AND EXTRACT(YEAR FROM fecha_expiracion) = %s
                        ORDER BY fecha_expiracion ASC""", (mes, anio))
            subs = c.fetchall()
        else:
            c.execute("""SELECT user_id, first_name, last_name, fecha_expiracion
                        FROM suscripciones 
                        WHERE estado = 'activo'
                        AND strftime('%%m', fecha_expiracion) = ?
                        AND strftime('%%Y', fecha_expiracion) = ?
                        ORDER BY fecha_expiracion ASC""", (f"{mes:02d}", str(anio)))
            subs = c.fetchall()
        conn.close()
        
        if not subs:
            await update.message.reply_text(f"📋 No hay vencimientos en {meses_nombre[mes]} {anio}.")
            return
        
        mensaje = f"📅 VENCIMIENTOS - {meses_nombre[mes]} {anio}\n\n"
        for s in subs:
            if DATABASE_URL:
                nombre = f"{s['first_name'] or ''} {s['last_name'] or ''}".strip()
                fecha = str(s['fecha_expiracion'])[:10]
            else:
                nombre = f"{s[1] or ''} {s[2] or ''}".strip()
                fecha = str(s[3])[:10]
            nombre = limpiar_nombre_display(nombre)
            mensaje += f"👤 {nombre}: {fecha}\n"
        
        mensaje += f"\nTotal: {len(subs)} suscripciones"
        await update.message.reply_text(mensaje)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def ingresos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /ingresos - Resumen de ingresos"""
    if update.effective_user.id != OWNER_ID:
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM suscripciones WHERE estado = 'activo'")
            activas = c.fetchone()['total']
            c.execute("SELECT COUNT(*) as total FROM suscripciones")
            total = c.fetchone()['total']
            c.execute("""SELECT COALESCE(SUM(precio), 0) as total_ingresos FROM pagos_pendientes 
                        WHERE estado = 'aprobado'""")
            total_ingresos = c.fetchone()['total_ingresos'] or 0
            c.execute("""SELECT COALESCE(SUM(precio), 0) as total_mes FROM pagos_pendientes 
                        WHERE estado = 'aprobado' 
                        AND fecha_envio >= DATE_TRUNC('month', CURRENT_DATE)""")
            ingresos_mes = c.fetchone()['total_mes'] or 0
        else:
            c.execute("SELECT COUNT(*) FROM suscripciones WHERE estado = 'activo'")
            activas = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM suscripciones")
            total = c.fetchone()[0]
            c.execute("SELECT COALESCE(SUM(precio), 0) FROM pagos_pendientes WHERE estado = 'aprobado'")
            total_ingresos = c.fetchone()[0] or 0
            primer_dia = datetime.now().replace(day=1).strftime("%Y-%m-%d")
            c.execute("SELECT COALESCE(SUM(precio), 0) FROM pagos_pendientes WHERE estado = 'aprobado' AND fecha_envio >= ?", (primer_dia,))
            ingresos_mes = c.fetchone()[0] or 0
        
        conn.close()
        
        mensaje = (f"💰 RESUMEN DE INGRESOS\n\n"
                  f"👥 Suscripciones activas: {activas}\n"
                  f"📊 Total registrados: {total}\n"
                  f"💵 Ingresos totales: {formato_clp(total_ingresos)}\n"
                  f"📅 Ingresos este mes: {formato_clp(ingresos_mes)}\n")
        
        await update.message.reply_text(mensaje)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def crecimiento_mes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /crecimiento_mes [mes] - Crecimiento de suscriptores por mes"""
    if update.effective_user.id != OWNER_ID:
        return
    
    mes = datetime.now().month
    if context.args:
        try:
            mes = int(context.args[0])
            if mes < 1 or mes > 12:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Ingresa un mes válido (1-12)")
            return
    
    meses_nombre = ['', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        anio = datetime.now().year
        
        if DATABASE_URL:
            c.execute("""SELECT COUNT(*) as nuevos FROM suscripciones 
                        WHERE EXTRACT(MONTH FROM fecha_registro) = %s
                        AND EXTRACT(YEAR FROM fecha_registro) = %s""", (mes, anio))
            nuevos = c.fetchone()['nuevos']
            c.execute("""SELECT COUNT(*) as total FROM suscripciones 
                        WHERE fecha_registro <= (DATE '%s-%s-01' + INTERVAL '1 month' - INTERVAL '1 day')""" % (anio, f"{mes:02d}"))
            acumulado = c.fetchone()['total']
        else:
            c.execute("""SELECT COUNT(*) FROM suscripciones 
                        WHERE strftime('%%m', fecha_registro) = ? AND strftime('%%Y', fecha_registro) = ?""", 
                     (f"{mes:02d}", str(anio)))
            nuevos = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM suscripciones")
            acumulado = c.fetchone()[0]
        
        conn.close()
        
        mensaje = (f"📈 CRECIMIENTO - {meses_nombre[mes]} {anio}\n\n"
                  f"🆕 Nuevos suscriptores: {nuevos}\n"
                  f"📊 Total acumulado: {acumulado}\n")
        
        await update.message.reply_text(mensaje)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def crecimiento_anual_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /crecimiento_anual - Crecimiento anual de suscriptores"""
    if update.effective_user.id != OWNER_ID:
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        anio = datetime.now().year
        meses_nombre = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
        
        mensaje = f"📊 CRECIMIENTO ANUAL {anio}\n\n"
        
        for mes in range(1, 13):
            if DATABASE_URL:
                c.execute("""SELECT COUNT(*) as total FROM suscripciones 
                            WHERE EXTRACT(MONTH FROM fecha_registro) = %s
                            AND EXTRACT(YEAR FROM fecha_registro) = %s""", (mes, anio))
                count = c.fetchone()['total']
            else:
                c.execute("""SELECT COUNT(*) FROM suscripciones 
                            WHERE strftime('%%m', fecha_registro) = ? AND strftime('%%Y', fecha_registro) = ?""",
                         (f"{mes:02d}", str(anio)))
                count = c.fetchone()[0]
            
            barra = "█" * count if count > 0 else ""
            if mes <= datetime.now().month:
                mensaje += f"  {meses_nombre[mes-1]}: {barra} {count}\n"
        
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM suscripciones WHERE EXTRACT(YEAR FROM fecha_registro) = %s", (anio,))
            total_anio = c.fetchone()['total']
        else:
            c.execute("SELECT COUNT(*) FROM suscripciones WHERE strftime('%%Y', fecha_registro) = ?", (str(anio),))
            total_anio = c.fetchone()[0]
        
        conn.close()
        mensaje += f"\n📈 Total {anio}: {total_anio} suscriptores"
        
        await update.message.reply_text(mensaje)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def resumen_usuario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /resumen_usuario @username - Resumen de actividad de un usuario"""
    if update.effective_user.id != OWNER_ID:
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ Uso: /resumen_usuario @username\n\n"
            "Ejemplo: /resumen_usuario @francisco_clavel"
        )
        return
    
    busqueda = ' '.join(context.args).replace('@', '').lower().strip()
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        
        # Buscar por username o nombre
        if DATABASE_URL:
            c.execute("""SELECT user_id, 
                        MAX(first_name) as first_name, 
                        MAX(COALESCE(last_name, '')) as last_name,
                        MAX(username) as username,
                        COUNT(*) as total_msgs,
                        MIN(fecha) as primera_fecha,
                        MAX(fecha) as ultima_fecha
                        FROM mensajes 
                        WHERE LOWER(username) LIKE %s 
                        OR LOWER(first_name) LIKE %s 
                        OR LOWER(last_name) LIKE %s
                        GROUP BY user_id
                        ORDER BY total_msgs DESC LIMIT 1""",
                     (f"%{busqueda}%", f"%{busqueda}%", f"%{busqueda}%"))
        else:
            c.execute("""SELECT user_id, 
                        MAX(first_name) as first_name, 
                        MAX(COALESCE(last_name, '')) as last_name,
                        MAX(username) as username,
                        COUNT(*) as total_msgs,
                        MIN(fecha) as primera_fecha,
                        MAX(fecha) as ultima_fecha
                        FROM mensajes 
                        WHERE LOWER(username) LIKE ? 
                        OR LOWER(first_name) LIKE ? 
                        OR LOWER(last_name) LIKE ?
                        GROUP BY user_id
                        ORDER BY total_msgs DESC LIMIT 1""",
                     (f"%{busqueda}%", f"%{busqueda}%", f"%{busqueda}%"))
        
        user = c.fetchone()
        
        if not user:
            conn.close()
            await update.message.reply_text(f"❌ No se encontró usuario: {busqueda}")
            return
        
        if DATABASE_URL:
            uid = user['user_id']
            nombre = f"{user['first_name'] or ''} {user['last_name'] or ''}".strip()
            username = user['username'] or ''
            total = user['total_msgs']
            primera = str(user['primera_fecha'])[:10]
            ultima = str(user['ultima_fecha'])[:16]
            
            # Categorías
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE user_id = %s AND categoria IS NOT NULL 
                        GROUP BY categoria ORDER BY total DESC LIMIT 5""", (uid,))
            cats = [(r['categoria'], r['total']) for r in c.fetchall()]
        else:
            uid = user[0]
            nombre = f"{user[1] or ''} {user[2] or ''}".strip()
            username = user[3] or ''
            total = user[4]
            primera = str(user[5])[:10]
            ultima = str(user[6])[:16]
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE user_id = ? AND categoria IS NOT NULL 
                        GROUP BY categoria ORDER BY COUNT(*) DESC LIMIT 5""", (uid,))
            cats = c.fetchall()
        
        conn.close()
        nombre = limpiar_nombre_display(nombre)
        
        mensaje = (f"👤 RESUMEN DE USUARIO\n\n"
                  f"📛 Nombre: {nombre}\n"
                  f"📱 Username: @{username}\n"
                  f"💬 Total mensajes: {total}\n"
                  f"📅 Primer mensaje: {primera}\n"
                  f"🕐 Último mensaje: {ultima}\n")
        
        if cats:
            mensaje += "\n📊 Temas más frecuentes:\n"
            for cat, cnt in cats:
                cat_name = cat if DATABASE_URL else cat[0]
                cat_count = cnt if DATABASE_URL else cat[1]
                mensaje += f"  📌 {cat_name}: {cat_count}\n"
        
        # Suscripción
        dias = obtener_dias_restantes(uid)
        if dias > 0:
            mensaje += f"\n⏰ Suscripción: {dias} días restantes"
        
        await update.message.reply_text(mensaje)
    except Exception as e:
        logger.error(f"Error en resumen_usuario: {e}")
        await update.message.reply_text(f"❌ Error: {e}")


async def ver_topics_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /ver_topics - Ver temas/topics del grupo con nombres"""
    if update.effective_user.id != OWNER_ID:
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""SELECT topic_id, COUNT(*) as msgs, 
                        MAX(fecha) as ultimo_msg,
                        MAX(message) as ultimo_texto
                        FROM mensajes 
                        WHERE topic_id IS NOT NULL
                        GROUP BY topic_id 
                        ORDER BY msgs DESC""")
            topics = c.fetchall()
        else:
            c.execute("""SELECT topic_id, COUNT(*) as msgs, MAX(fecha) as ultimo_msg,
                        MAX(message) as ultimo_texto
                        FROM mensajes WHERE topic_id IS NOT NULL
                        GROUP BY topic_id ORDER BY msgs DESC""")
            topics = c.fetchall()
        conn.close()
        
        if not topics:
            await update.message.reply_text("📋 No hay topics registrados.")
            return
        
        # Intentar obtener nombres de topics del grupo
        topic_names = {}
        try:
            if COFRADIA_GROUP_ID:
                # Intentar obtener forum topics via API
                forum_topics = await context.bot.get_forum_topic_icon_stickers()
                # Si no funciona, usamos los mensajes para inferir el tema
        except Exception:
            pass
        
        mensaje = "📂 TOPICS DEL GRUPO\n\n"
        for t in topics:
            if DATABASE_URL:
                tid = t['topic_id']
                msgs = t['msgs']
                ultimo = str(t['ultimo_msg'])[:10]
                # Usar el último mensaje como referencia del tema
                ultimo_texto = (t['ultimo_texto'] or '')[:50]
            else:
                tid = t[0]
                msgs = t[1]
                ultimo = str(t[2])[:10]
                ultimo_texto = (t[3] or '')[:50]
            
            # Inferir nombre del topic basado en ID conocidos o último mensaje
            nombre_topic = topic_names.get(tid, f"Topic #{tid}")
            
            mensaje += (f"🔹 {nombre_topic}\n"
                       f"   💬 {msgs} mensajes | Último: {ultimo}\n"
                       f"   📝 \"{ultimo_texto}...\"\n\n")
        
        mensaje += ("💡 Los nombres de los topics se configuran en la\n"
                    "   configuración del grupo de Telegram.")
        
        await update.message.reply_text(mensaje)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")


async def set_topic_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /set_topic - Información sobre topics de Telegram"""
    if update.effective_user.id != OWNER_ID:
        return
    
    await update.message.reply_text(
        "📝 GESTIÓN DE TOPICS\n\n"
        "Los topics se gestionan directamente en la configuración del grupo de Telegram.\n\n"
        "Para crear/editar topics:\n"
        "1. Abre el grupo en Telegram\n"
        "2. Toca el nombre del grupo\n"
        "3. Selecciona 'Topics'\n"
        "4. Crea o edita los temas\n\n"
        "El bot registra automáticamente los mensajes por topic.\n"
        "Usa /ver_topics para ver la actividad por topic."
    )


async def set_topic_emoji_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /set_topic_emoji - Info sobre emojis de topics"""
    if update.effective_user.id != OWNER_ID:
        return
    
    await update.message.reply_text(
        "🎨 EMOJIS DE TOPICS\n\n"
        "Los emojis de los topics se configuran directamente en Telegram:\n"
        "1. Abre la configuración del grupo\n"
        "2. Selecciona el topic a editar\n"
        "3. Cambia el emoji del topic\n\n"
        "El bot automáticamente detecta los topics por su ID."
    )


# ==================== COMANDOS DE ESTADÍSTICAS ====================

@requiere_suscripcion
async def estadisticas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /estadisticas - Estadísticas generales + mini-dashboard ECharts"""
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
            c.execute("SELECT COUNT(*) as total FROM recomendaciones")
            total_recs = c.fetchone()['total']
            c.execute("SELECT COUNT(*) as total FROM tarjetas_profesional")
            total_tarjetas = c.fetchone()['total']
            c.execute("""SELECT COUNT(*) as total FROM mensajes 
                        WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'""")
            msgs_7d = c.fetchone()['total']
        else:
            c.execute("SELECT COUNT(*) FROM mensajes")
            total_msgs = c.fetchone()[0]
            c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes")
            total_usuarios = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM suscripciones WHERE estado = 'activo'")
            suscriptores = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM mensajes WHERE DATE(fecha) = DATE('now')")
            msgs_hoy = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM recomendaciones")
            total_recs = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM tarjetas_profesional")
            total_tarjetas = c.fetchone()[0]
            fecha_7d = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            c.execute("SELECT COUNT(*) FROM mensajes WHERE fecha >= ?", (fecha_7d,))
            msgs_7d = c.fetchone()[0]
        
        conn.close()
        
        promedio_7d = round(msgs_7d / 7, 1) if msgs_7d else 0
        pct_tarjetas = round(total_tarjetas / max(suscriptores, 1) * 100) if suscriptores else 0
        
        # Generar mini-dashboard HTML con gauges ECharts
        import json as _json
        html = f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Estadísticas Cofradía</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:linear-gradient(135deg,#0a1628,#0f2f59);color:#e0e6ed;padding:20px;min-height:100vh}}
h1{{text-align:center;color:#c3a55a;font-size:1.8em;margin:20px 0 5px;letter-spacing:2px}}
.sub{{text-align:center;color:#667788;margin-bottom:25px}}
.gauges{{display:flex;flex-wrap:wrap;gap:15px;justify-content:center;margin-bottom:25px}}
.gauge-box{{background:rgba(15,47,89,0.6);border:1px solid rgba(195,165,90,0.2);border-radius:12px;padding:10px;width:280px;height:240px}}
.gauge{{width:100%;height:100%}}
.stats-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;max-width:900px;margin:0 auto}}
.stat{{background:rgba(15,47,89,0.6);border:1px solid rgba(52,120,195,0.2);border-radius:10px;padding:18px;text-align:center}}
.stat .val{{font-size:2em;font-weight:800;color:#c3a55a}}
.stat .lbl{{font-size:0.8em;color:#667788;text-transform:uppercase;letter-spacing:1px;margin-top:4px}}
.foot{{text-align:center;color:#445566;font-size:0.8em;margin-top:25px;padding-top:15px;border-top:1px solid rgba(195,165,90,0.15)}}
</style></head><body>
<h1>⚓ ESTADÍSTICAS COFRADÍA</h1>
<div class="sub">Resumen General — {datetime.now().strftime('%d/%m/%Y')}</div>

<div class="gauges">
<div class="gauge-box"><div id="g1" class="gauge"></div></div>
<div class="gauge-box"><div id="g2" class="gauge"></div></div>
<div class="gauge-box"><div id="g3" class="gauge"></div></div>
</div>

<div class="stats-grid">
<div class="stat"><div class="val">{total_msgs:,}</div><div class="lbl">Mensajes Totales</div></div>
<div class="stat"><div class="val">{total_usuarios:,}</div><div class="lbl">Usuarios Únicos</div></div>
<div class="stat"><div class="val">{suscriptores:,}</div><div class="lbl">Miembros Activos</div></div>
<div class="stat"><div class="val">{msgs_hoy:,}</div><div class="lbl">Mensajes Hoy</div></div>
<div class="stat"><div class="val">{total_recs:,}</div><div class="lbl">Recomendaciones</div></div>
<div class="stat"><div class="val">{total_tarjetas:,}</div><div class="lbl">Tarjetas Creadas</div></div>
</div>

<div class="foot">Bot Premium v4.3 ECharts · Cofradía de Networking</div>

<script>
var gold='#c3a55a',blue='#3478c3';
function gauge(id,val,max,title,color){{
  var c=echarts.init(document.getElementById(id));
  c.setOption({{series:[{{type:'gauge',startAngle:200,endAngle:-20,min:0,max:max,
    pointer:{{show:true,length:'60%',width:4,itemStyle:{{color:color}}}},
    progress:{{show:true,width:12,itemStyle:{{color:color}}}},
    axisLine:{{lineStyle:{{width:12,color:[[1,'rgba(52,120,195,0.15)']]}}}},
    axisTick:{{show:false}},splitLine:{{show:false}},
    axisLabel:{{show:false}},
    title:{{show:true,offsetCenter:[0,'75%'],fontSize:13,color:'#8899aa'}},
    detail:{{valueAnimation:true,fontSize:28,fontWeight:'bold',color:color,
      offsetCenter:[0,'40%'],formatter:'{{value}}'}},
    data:[{{value:{val},name:title}}]
  }}]}});
  window.addEventListener('resize',()=>c.resize());
}}
gauge('g1',{msgs_hoy},{max(msgs_hoy*3,100)},'Mensajes Hoy',gold);
gauge('g2',{promedio_7d},{max(int(promedio_7d*3),50)},'Promedio/Día',blue);
gauge('g3',{pct_tarjetas},100,'% Tarjetas',gold);
</script></body></html>"""
        
        html_path = f"/tmp/cofradia_stats_{update.effective_user.id}.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        mensaje = (
            f"📊 ESTADÍSTICAS COFRADÍA\n"
            f"{'━' * 28}\n\n"
            f"📝 Mensajes totales: {total_msgs:,}\n"
            f"👥 Usuarios únicos: {total_usuarios:,}\n"
            f"✅ Miembros activos: {suscriptores:,}\n"
            f"📅 Mensajes hoy: {msgs_hoy:,}\n"
            f"⭐ Recomendaciones: {total_recs:,}\n"
            f"📇 Tarjetas creadas: {total_tarjetas:,}\n"
            f"📈 Promedio 7 días: {promedio_7d}/día\n\n"
            f"💡 Usa /graficos para dashboard completo."
        )
        await update.message.reply_text(mensaje)
        
        with open(html_path, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=f"cofradia_estadisticas_{datetime.now().strftime('%Y%m%d')}.html",
                caption="📊 Dashboard ECharts interactivo con gauges"
            )
        
        try:
            os.remove(html_path)
        except:
            pass
        
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
            nombre_limpio = limpiar_nombre_display(nombre)
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
                  'Emprendimiento': '🚀', 'Evento': '📅', 'Saludo': '👋',
                  'Oferta Laboral': '💼', 'Búsqueda Empleo': '🔍', 'Recomendación Profesional': '⭐',
                  'Consulta Profesional': '❓', 'Servicios y Productos': '🛒', 'Capacitación': '📚',
                  'Información': '📰', 'Opinión': '💭', 'Conversación': '💬', 'Construcción': '🏗️',
                  'Finanzas': '💰', 'Tecnología': '💻', 'Inmobiliaria': '🏠', 'Seguridad': '🔒',
                  'Energía': '⚡', 'Sector Marítimo': '⚓', 'Otro': '📌'}
        
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
        user_id = user.id
        
        # Nombre completo
        if user_id == OWNER_ID:
            nombre_display = "Germán Perey"
        else:
            nombre_display = f"{user.first_name or ''} {user.last_name or ''}".strip() or "Usuario"
        
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as total FROM mensajes WHERE user_id = %s", (user_id,))
            total_msgs = c.fetchone()['total']
            
            c.execute("""SELECT categoria, COUNT(*) as total FROM mensajes 
                        WHERE user_id = %s AND categoria IS NOT NULL 
                        GROUP BY categoria ORDER BY total DESC LIMIT 3""", (user_id,))
            top_cats = [(r['categoria'], r['total']) for r in c.fetchall()]
            
            c.execute("SELECT fecha_registro, fecha_expiracion FROM suscripciones WHERE user_id = %s", (user_id,))
            sus = c.fetchone()
        else:
            c.execute("SELECT COUNT(*) FROM mensajes WHERE user_id = ?", (user_id,))
            total_msgs = c.fetchone()[0]
            
            c.execute("""SELECT categoria, COUNT(*) FROM mensajes 
                        WHERE user_id = ? AND categoria IS NOT NULL 
                        GROUP BY categoria ORDER BY COUNT(*) DESC LIMIT 3""", (user_id,))
            top_cats = c.fetchall()
            
            c.execute("SELECT fecha_registro, fecha_expiracion FROM suscripciones WHERE user_id = ?", (user_id,))
            sus = c.fetchone()
        
        conn.close()
        
        # === v4.0: Coins y Trust Score ===
        coins_info = get_coins_balance(user.id)
        trust = calcular_trust_score(user.id)
        
        mensaje = f"👤 MI PERFIL\n\n"
        mensaje += f"📛 Nombre: {nombre_display}\n"
        mensaje += f"🏅 Trust Score: {trust['score']}/100 {trust['nivel']}\n"
        mensaje += f"📝 Mensajes totales: {total_msgs}\n"
        
        if top_cats:
            mensaje += f"\n📊 Tus temas favoritos:\n"
            for cat, total in top_cats:
                mensaje += f"  📌 {cat}: {total}\n"
        
        if sus:
            dias = obtener_dias_restantes(user_id)
            if dias >= 99999:
                mensaje += f"\n⏰ Suscripción: ♾️ Sin límite (Owner)\n"
            else:
                mensaje += f"\n⏰ Días restantes: {dias}\n"
        
        # Cofradía Coins
        mensaje += f"\n🪙 COFRADÍA COINS\n"
        mensaje += f"  💰 Balance: {coins_info['balance']} Coins\n"
        mensaje += f"  📈 Ganados: {coins_info['total_ganado']} | 📉 Gastados: {coins_info['total_gastado']}\n"
        
        # Servicios canjeables
        try:
            conn2 = get_db_connection()
            if conn2:
                c2 = conn2.cursor()
                if DATABASE_URL:
                    c2.execute("SELECT servicio, precio_coins, descripcion FROM precios_servicios WHERE activo = TRUE ORDER BY precio_coins")
                else:
                    c2.execute("SELECT servicio, precio_coins, descripcion FROM precios_servicios WHERE activo = 1 ORDER BY precio_coins")
                servicios_premium = c2.fetchall()
                conn2.close()
                
                if servicios_premium:
                    mensaje += "\n🛒 SERVICIOS CANJEABLES\n"
                    for s in servicios_premium:
                        srv = s['servicio'] if DATABASE_URL else s[0]
                        pc = s['precio_coins'] if DATABASE_URL else s[1]
                        desc = s['descripcion'] if DATABASE_URL else s[2]
                        if coins_info['balance'] >= pc:
                            mensaje += f"  ✅ /{srv} ({pc} coins) — {desc}\n"
                        else:
                            faltan = pc - coins_info['balance']
                            mensaje += f"  🔒 /{srv} ({pc} coins, faltan {faltan}) — {desc}\n"
        except:
            pass
        
        # Cómo ganar más
        mensaje += "\n💡 GANAR MÁS COINS\n"
        mensaje += "  💬 Mensaje en grupo: +1\n"
        mensaje += "  💡 Responder consulta: +10\n"
        mensaje += "  ⭐ Recomendar cofrade: +5\n"
        mensaje += "  📅 Asistir evento: +20\n"
        mensaje += "  📇 Crear tarjeta: +15\n"
        
        await enviar_mensaje_largo(update, mensaje)
        
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
                    nombre_limpio = limpiar_nombre_display(nombre)
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
                nombre_limpio = limpiar_nombre_display(nombre)
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
                     'Emprendimiento': '🚀', 'Evento': '📅', 'Saludo': '👋',
                  'Oferta Laboral': '💼', 'Búsqueda Empleo': '🔍', 'Recomendación Profesional': '⭐',
                  'Consulta Profesional': '❓', 'Servicios y Productos': '🛒', 'Capacitación': '📚',
                  'Información': '📰', 'Opinión': '💭', 'Conversación': '💬', 'Construcción': '🏗️',
                  'Finanzas': '💰', 'Tecnología': '💻', 'Inmobiliaria': '🏠', 'Seguridad': '🔒',
                  'Energía': '⚡', 'Sector Marítimo': '⚓', 'Otro': '📌'}
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
                nombre_limpio = limpiar_nombre_display(nombre)
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
            "❌ Uso: /buscar_profesional [profesión o nombre]\n\n"
            "Ejemplos:\n"
            "  /buscar_profesional abogado\n"
            "  /buscar_profesional contador\n"
            "  /buscar_profesional diseñador"
        )
        return
    
    query = ' '.join(context.args)
    msg = await update.message.reply_text(f"🔍 Buscando profesionales: {query}...")
    
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
        
        # Stemming básico español: generar variaciones de la palabra
        def generar_variaciones(palabra):
            """Genera variaciones de una palabra para búsqueda flexible"""
            variaciones = {palabra}
            # Quitar/agregar 's' final
            if palabra.endswith('s'):
                variaciones.add(palabra[:-1])
            else:
                variaciones.add(palabra + 's')
            # Quitar/agregar 'es' final
            if palabra.endswith('es'):
                variaciones.add(palabra[:-2])
            # Quitar 'ción'/'cion' → agregar otras formas
            if palabra.endswith('ción') or palabra.endswith('cion'):
                raiz = palabra.replace('ción', '').replace('cion', '')
                variaciones.update([raiz, raiz + 'ciones', raiz + 'cionista'])
            # Quitar 'ista' → agregar otras formas
            if palabra.endswith('ista'):
                raiz = palabra[:-4]
                variaciones.update([raiz, raiz + 'ismo', raiz + 'ístico'])
            # Quitar 'ero/era'
            if palabra.endswith('ero') or palabra.endswith('era'):
                variaciones.add(palabra[:-3])
                variaciones.add(palabra[:-3] + 'ería')
            return variaciones
        
        # Agregar variaciones del query original
        for var in generar_variaciones(query_lower):
            palabras_busqueda.add(var)
        
        for categoria, sinonimos in SINONIMOS.items():
            if any(palabra in query_lower or query_lower in palabra for palabra in sinonimos):
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
            
            msg = f"❌ No se encontraron profesionales para: {query}\n\n"
            msg += f"📊 Total en BD: {len(profesionales)} profesionales\n\n"
            
            if profesiones:
                msg += "💡 **Algunas profesiones (col Y):**\n"
                for p in sorted(profesiones)[:10]:
                    msg += f"• {p}\n"
            
            if industrias:
                msg += "\n💼 Algunas industrias:\n"
                for i in sorted(industrias)[:10]:
                    msg += f"• {i}\n"
            
            return msg
        
        # Formatear resultados
        resultado = "━" * 30 + "\n"
        resultado += "👥 PROFESIONALES ENCONTRADOS\n"
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
        
        # Buscar carpeta existente (incluir Shared Drives)
        query = f"name = '{nombre_carpeta}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {
            'q': query, 
            'fields': 'files(id, name, driveId)',
            'includeItemsFromAllDrives': 'true',
            'supportsAllDrives': 'true',
            'corpora': 'allDrives' if not parent_id else 'allDrives'
        }
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
        
        create_url = "https://www.googleapis.com/drive/v3/files?supportsAllDrives=true"
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
        params = {
            'q': query, 
            'fields': 'files(id, name)',
            'includeItemsFromAllDrives': 'true',
            'supportsAllDrives': 'true'
        }
        resp = requests.get(search_url, headers=headers, params=params, timeout=30)
        existentes = resp.json().get('files', [])
        
        if existentes:
            # Actualizar archivo existente
            file_id = existentes[0]['id']
            upload_url = f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media&supportsAllDrives=true"
            resp = requests.patch(upload_url, 
                                headers={**headers, 'Content-Type': 'application/pdf'},
                                data=file_bytes, timeout=120)
            if resp.status_code == 200:
                logger.info(f"📄 PDF actualizado en Drive: {filename} ({file_id})")
                return file_id, "actualizado"
            else:
                logger.warning(f"Error actualizando PDF en Drive: {resp.status_code} {resp.text[:200]}")
        
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
        
        upload_url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true"
        resp = requests.post(upload_url,
                           headers={**headers, 'Content-Type': f'multipart/related; boundary={boundary}'},
                           data=body.getvalue(), timeout=120)
        
        if resp.status_code in [200, 201]:
            file_id = resp.json().get('id')
            logger.info(f"📄 PDF subido a Drive: {filename} ({file_id})")
            return file_id, "subido"
        elif resp.status_code == 403:
            error_detail = resp.json().get('error', {})
            error_msg = error_detail.get('message', 'Sin permisos')
            errors_list = error_detail.get('errors', [])
            reason = errors_list[0].get('reason', '') if errors_list else ''
            logger.error(f"Error 403 subiendo PDF: {error_msg} (reason: {reason})")
            
            if 'insufficientPermissions' in reason or 'forbidden' in reason.lower():
                return None, f"Error HTTP 403 - Service Account sin permisos de Editor en la carpeta"
            return None, f"Error HTTP 403 - {error_msg[:100]}"
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


def listar_pdfs_rag(incluir_drive=False):
    """Lista todos los PDFs indexados en el sistema RAG (desde BD, opcionalmente Drive)"""
    pdfs = []
    
    # Primero listar desde la base de datos (siempre disponible, rápido)
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("""SELECT source, COUNT(*) as chunks, MAX(fecha_indexado) as ultimo
                            FROM rag_chunks WHERE source LIKE 'PDF:%%'
                            GROUP BY source ORDER BY ultimo DESC""")
            else:
                c.execute("""SELECT source, COUNT(*) as chunks, MAX(fecha_indexado) as ultimo
                            FROM rag_chunks WHERE source LIKE 'PDF:%'
                            GROUP BY source ORDER BY ultimo DESC""")
            resultados = c.fetchall()
            conn.close()
            
            for r in resultados:
                if DATABASE_URL:
                    nombre = r['source'].replace('PDF:', '') if r['source'].startswith('PDF:') else r['source']
                    pdfs.append({'name': nombre, 'chunks': r['chunks'], 
                                'modified': str(r['ultimo'])[:16], 'origen': 'BD'})
                else:
                    nombre = r[0].replace('PDF:', '') if str(r[0]).startswith('PDF:') else r[0]
                    pdfs.append({'name': nombre, 'chunks': r[1], 
                                'modified': str(r[2])[:16], 'origen': 'BD'})
    except Exception as e:
        logger.warning(f"Error listando PDFs desde BD: {e}")
    
    # Solo consultar Drive si se pide explícitamente (evita bloquear rag_status)
    if incluir_drive:
        try:
            headers = obtener_drive_auth_headers()
            if headers:
                rag_folder_id = obtener_carpeta_rag_pdf()
                if rag_folder_id:
                    query = f"'{rag_folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
                    search_url = "https://www.googleapis.com/drive/v3/files"
                    params = {
                        'q': query,
                        'fields': 'files(id, name, size, createdTime, modifiedTime)',
                        'orderBy': 'modifiedTime desc',
                        'pageSize': 100,
                        'includeItemsFromAllDrives': 'true',
                        'supportsAllDrives': 'true'
                    }
                    resp = requests.get(search_url, headers=headers, params=params, timeout=10)
                    archivos_drive = resp.json().get('files', [])
                    
                    # Agregar los que están en Drive pero no en BD
                    nombres_bd = {p['name'] for p in pdfs}
                    for archivo in archivos_drive:
                        if archivo['name'] not in nombres_bd:
                            pdfs.append({'name': archivo['name'], 'size': archivo.get('size', 0),
                                        'modified': archivo.get('modifiedTime', '')[:16], 'origen': 'Drive',
                                        'id': archivo.get('id', '')})
        except Exception as e:
            logger.warning(f"Error listando PDFs desde Drive: {e}")
    
    return pdfs


def descargar_pdf_drive(file_id):
    """Descarga contenido de un PDF desde Google Drive"""
    try:
        headers = obtener_drive_auth_headers()
        if not headers:
            return None
        
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true"
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
    """Genera keywords de un chunk de texto para búsqueda (con normalización)"""
    import re
    import unicodedata
    
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
    
    # Deduplicar manteniendo orden, agregar versión sin tildes
    seen = set()
    unique = []
    for k in keywords:
        if k not in seen:
            seen.add(k)
            unique.append(k)
            # Agregar versión sin tildes para búsquedas sin acentos
            k_norm = unicodedata.normalize('NFKD', k)
            k_norm = ''.join(c for c in k_norm if not unicodedata.combining(c))
            if k_norm != k and k_norm not in seen:
                seen.add(k_norm)
                unique.append(k_norm)
    
    return ' '.join(unique[:70])


def indexar_pdf_en_rag(filename, texto, file_id=None):
    """Indexa un PDF en la tabla rag_chunks para búsqueda RAG.
    IMPORTANTE: inyecta términos del nombre de archivo en keywords de CADA chunk,
    así búsquedas por autor/título siempre encuentran el documento correcto.
    """
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
        
        # Extraer keywords del nombre de archivo para inyectarlos en CADA chunk
        # Esto es crítico: permite encontrar chunks aunque el autor no aparezca en el texto
        import unicodedata, re as _re
        def normalizar_kw(t):
            t = unicodedata.normalize('NFKD', t.lower())
            t = ''.join(c for c in t if not unicodedata.combining(c))
            return t
        
        # Limpiar nombre de archivo: quitar extensión, separar por guiones/puntos/espacios
        nombre_sin_ext = _re.sub(r'\.(pdf|docx?|txt)$', '', filename, flags=_re.IGNORECASE)
        partes_nombre = _re.split(r'[\s\-_.,]+', nombre_sin_ext)
        stopwords_archivo = {'el','la','los','las','de','del','un','una','y','a','en','fin','the','of'}
        # Keywords del nombre: palabras de 3+ letras que no sean stopwords
        keywords_filename = ' '.join([
            normalizar_kw(p) for p in partes_nombre
            if len(p) >= 3 and normalizar_kw(p) not in stopwords_archivo
        ])
        
        logger.info(f"📎 PDF '{filename}' → keywords_filename: '{keywords_filename}'")
        
        # Crear chunks
        chunks = crear_chunks_texto(texto)
        chunks_creados = 0
        
        for i, chunk_text in enumerate(chunks):
            # Keywords del contenido del chunk
            keywords_contenido = generar_keywords_chunk(chunk_text)
            # ENRIQUECER: añadir keywords del filename a CADA chunk
            keywords_enriquecidas = f"{keywords_filename} {keywords_contenido}".strip()
            
            metadata = json.dumps({
                'filename': filename,
                'file_id': file_id or '',
                'chunk_index': i,
                'total_chunks': len(chunks),
                'tipo': 'pdf',
                'filename_keywords': keywords_filename,
            })
            
            if DATABASE_URL:
                c.execute("""INSERT INTO rag_chunks (source, chunk_text, metadata, keywords) 
                           VALUES (%s, %s, %s, %s)""",
                         (source, chunk_text, metadata, keywords_enriquecidas))
            else:
                c.execute("""INSERT INTO rag_chunks (source, chunk_text, metadata, keywords) 
                           VALUES (?, ?, ?, ?)""",
                         (source, chunk_text, metadata, keywords_enriquecidas))
            chunks_creados += 1
        
        conn.commit()
        conn.close()
        logger.info(f"✅ PDF '{filename}' indexado: {chunks_creados} chunks (keywords enriquecidas con nombre)")
        return chunks_creados
    
    except Exception as e:
        logger.error(f"Error indexando PDF en RAG: {e}")
        return 0


def indexar_todos_pdfs_rag():
    """Re-indexa PDFs desde Drive (los que están en BD ya están indexados)"""
    try:
        # Solo obtener PDFs de Drive para re-indexar
        pdfs = listar_pdfs_rag(incluir_drive=True)
        pdfs_drive = [p for p in pdfs if p.get('origen') == 'Drive' and p.get('id')]
        
        if not pdfs_drive:
            logger.info("RAG PDF: No hay PDFs nuevos en Drive para indexar")
            return 0
        
        total_chunks = 0
        pdfs_procesados = 0
        pdfs_error = 0
        
        for pdf_info in pdfs_drive:
            file_id = pdf_info.get('id', '')
            filename = pdf_info['name']
            
            if not file_id:
                continue
            
            try:
                logger.info(f"📄 Indexando PDF desde Drive: {filename}...")
                
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
            # Total chunks
            c.execute("SELECT COUNT(*) as total FROM rag_chunks")
            stats['total_chunks'] = int(c.fetchone()['total'] or 0)
            
            # Por fuente
            c.execute("SELECT source, COUNT(*) as total FROM rag_chunks GROUP BY source ORDER BY total DESC")
            stats['por_fuente'] = [(r['source'], int(r['total'])) for r in c.fetchall()]
            
            # Total PDFs
            c.execute("SELECT COUNT(DISTINCT source) as total FROM rag_chunks WHERE source LIKE 'PDF:%%'")
            stats['total_pdfs'] = int(c.fetchone()['total'] or 0)
            
            # Tamaño de datos RAG en BD (texto de chunks + keywords)
            c.execute("""SELECT 
                COALESCE(SUM(LENGTH(chunk_text)), 0) as texto_bytes,
                COALESCE(SUM(LENGTH(keywords)), 0) as keywords_bytes,
                COALESCE(SUM(LENGTH(metadata)), 0) as metadata_bytes
                FROM rag_chunks""")
            size_row = c.fetchone()
            stats['rag_texto_bytes'] = int(size_row['texto_bytes'] or 0)
            stats['rag_keywords_bytes'] = int(size_row['keywords_bytes'] or 0)
            stats['rag_metadata_bytes'] = int(size_row['metadata_bytes'] or 0)
            
            # Tamaño total de la BD (todas las tablas)
            c.execute("""SELECT 
                SUM(pg_total_relation_size(quote_ident(table_name))) as total_bytes
                FROM information_schema.tables 
                WHERE table_schema = 'public'""")
            db_size = c.fetchone()
            stats['db_total_bytes'] = int(db_size['total_bytes']) if db_size and db_size['total_bytes'] else 0
            
            # Tamaño solo de rag_chunks
            c.execute("SELECT pg_total_relation_size('rag_chunks') as rag_bytes")
            rag_table = c.fetchone()
            stats['rag_table_bytes'] = int(rag_table['rag_bytes']) if rag_table and rag_table['rag_bytes'] else 0
            
        else:
            c.execute("SELECT COUNT(*) FROM rag_chunks")
            stats['total_chunks'] = c.fetchone()[0]
            
            c.execute("SELECT source, COUNT(*) as total FROM rag_chunks GROUP BY source ORDER BY total DESC")
            stats['por_fuente'] = c.fetchall()
            
            c.execute("SELECT COUNT(DISTINCT source) FROM rag_chunks WHERE source LIKE 'PDF:%'")
            stats['total_pdfs'] = c.fetchone()[0]
            
            # Tamaño de datos RAG
            c.execute("""SELECT 
                COALESCE(SUM(LENGTH(chunk_text)), 0),
                COALESCE(SUM(LENGTH(keywords)), 0),
                COALESCE(SUM(LENGTH(metadata)), 0)
                FROM rag_chunks""")
            size_row = c.fetchone()
            stats['rag_texto_bytes'] = size_row[0] or 0
            stats['rag_keywords_bytes'] = size_row[1] or 0
            stats['rag_metadata_bytes'] = size_row[2] or 0
            
            # SQLite: tamaño del archivo de BD
            stats['db_total_bytes'] = 0
            stats['rag_table_bytes'] = stats['rag_texto_bytes'] + stats['rag_keywords_bytes'] + stats['rag_metadata_bytes']
        
        # Calcular totales RAG (asegurar int para evitar Decimal)
        stats['rag_data_bytes'] = int(stats['rag_texto_bytes']) + int(stats['rag_keywords_bytes']) + int(stats['rag_metadata_bytes'])
        
        # Límite según plan (Supabase free = 500 MB)
        stats['db_limite_bytes'] = 500 * 1024 * 1024  # 500 MB
        
        conn.close()
        
        # Info de PDFs desde BD (NO consultar Drive para mantener velocidad)
        pdfs_indexados = listar_pdfs_rag(incluir_drive=False)
        stats['pdfs_lista'] = pdfs_indexados
        stats['pdfs_en_drive'] = 0  # Se muestra solo si Drive responde rápido
        
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
    
    # Solo owner puede subir PDFs al RAG (seguridad)
    es_owner = (user_id == OWNER_ID)
    if not es_owner:
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
            "🔍 Extrayendo texto del PDF..."
        )
        
        # PASO 1: Extraer texto PRIMERO (no depende de Drive)
        texto = extraer_texto_pdf(file_bytes)
        
        if not texto:
            await msg.edit_text(
                f"⚠️ No se pudo extraer texto de {filename}.\n\n"
                "Posibles causas:\n"
                "- PDF escaneado (imagen sin texto)\n"
                "- PDF protegido/encriptado\n"
                "- PDF sin contenido de texto\n\n"
                "Solo se pueden indexar PDFs con texto seleccionable."
            )
            return
        
        await msg.edit_text(
            f"📥 {filename}\n"
            f"📝 {len(texto):,} caracteres extraidos\n"
            "🧠 Indexando en sistema RAG..."
        )
        
        # PASO 2: Indexar en RAG (usa base de datos, no depende de Drive)
        # Guardar file_id de Telegram como referencia
        tg_file_id = document.file_id or "local"
        chunks_creados = indexar_pdf_en_rag(filename, texto, tg_file_id)
        
        # PASO 3: Intentar subir a Drive como BACKUP (opcional, no bloquea)
        drive_status = ""
        try:
            file_id_drive, status_drive = subir_pdf_a_drive(file_bytes, filename)
            if file_id_drive:
                drive_status = f"☁️ Backup en Drive: INBESTU/RAG_PDF ({status_drive})"
                # Actualizar el drive_file_id en la BD
                try:
                    conn = get_db_connection()
                    if conn:
                        c = conn.cursor()
                        if DATABASE_URL:
                            c.execute("UPDATE rag_documentos SET drive_file_id = %s WHERE nombre_archivo = %s", 
                                     (file_id_drive, filename))
                        else:
                            c.execute("UPDATE rag_documentos SET drive_file_id = ? WHERE nombre_archivo = ?", 
                                     (file_id_drive, filename))
                        conn.commit()
                        conn.close()
                except:
                    pass
            else:
                if 'HTTP 403' in str(status_drive):
                    drive_status = (f"⚠️ Drive backup omitido (Error 403: Sin permisos de escritura)\n"
                                  f"   💡 Comparte la carpeta INBESTU con la Service Account como Editor\n"
                                  f"   💡 Usa /rag_status para ver el diagnostico completo")
                else:
                    drive_status = f"⚠️ Drive backup omitido ({status_drive})"
                logger.warning(f"Drive backup falló para {filename}: {status_drive}")
        except Exception as e:
            drive_status = f"⚠️ Drive backup omitido ({str(e)[:50]})"
            logger.warning(f"Drive backup error: {e}")
        
        # RESULTADO FINAL
        resultado = "━" * 30 + "\n"
        resultado += "✅ PDF PROCESADO EXITOSAMENTE\n"
        resultado += "━" * 30 + "\n\n"
        resultado += f"📄 Archivo: {filename}\n"
        resultado += f"📏 Tamano: {file_size_mb:.1f} MB\n"
        resultado += f"📝 Texto extraido: {len(texto):,} caracteres\n"
        resultado += f"🧩 Chunks RAG creados: {chunks_creados}\n"
        resultado += f"{drive_status}\n\n"
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
async def rag_debug_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /rag_debug [query] — Solo para OWNER. Diagnóstico completo del RAG."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("🔒 Solo disponible para el administrador.")
        return
    
    if not context.args:
        await update.message.reply_text(
            "Uso: /rag_debug [consulta]\n"
            "Ejemplo: /rag_debug milei inflacion\n\n"
            "También: /rag_debug --docs (lista todos los documentos indexados)"
        )
        return
    
    query = ' '.join(context.args)
    
    # Modo especial: listar todos los documentos
    if query.strip() == '--docs':
        msg = await update.message.reply_text("📚 Listando todos los documentos indexados...")
        try:
            conn = get_db_connection()
            if not conn:
                await msg.edit_text("❌ Sin conexión a BD")
                return
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("""SELECT source, COUNT(*) as chunks, 
                            MAX(LENGTH(chunk_text)) as max_len
                            FROM rag_chunks GROUP BY source ORDER BY chunks DESC""")
            else:
                c.execute("""SELECT source, COUNT(*) as chunks,
                            MAX(LENGTH(chunk_text)) as max_len
                            FROM rag_chunks GROUP BY source ORDER BY chunks DESC""")
            docs = c.fetchall()
            conn.close()
            
            texto = f"📚 DOCUMENTOS INDEXADOS EN RAG ({len(docs)} total)\n"
            texto += "━" * 40 + "\n\n"
            for d in docs:
                if DATABASE_URL:
                    src, n, ml = d['source'], d['chunks'], d['max_len']
                else:
                    src, n, ml = d[0], d[1], d[2]
                src_clean = src.replace('PDF:', '📄 ').replace('EXCEL:', '📊 ')
                texto += f"{src_clean}\n   → {n} chunks, max {ml} chars\n\n"
            
            if len(texto) > 4000:
                partes = [texto[i:i+3900] for i in range(0, len(texto), 3900)]
                await msg.edit_text(partes[0])
                for p in partes[1:]:
                    await update.message.reply_text(p)
            else:
                await msg.edit_text(texto)
        except Exception as e:
            await msg.edit_text(f"❌ Error: {e}")
        return
    
    msg = await update.message.reply_text(f"🔬 Debug RAG completo para: '{query}'...")
    
    try:
        # 1. Buscar en nombres de documentos directamente
        conn = get_db_connection()
        docs_directos = []
        if conn:
            c = conn.cursor()
            for term in query.lower().split():
                if len(term) >= 3:
                    if DATABASE_URL:
                        c.execute("SELECT DISTINCT source, COUNT(*) as n FROM rag_chunks WHERE LOWER(source) LIKE %s GROUP BY source",
                                 (f'%{term}%',))
                    else:
                        c.execute("SELECT DISTINCT source, COUNT(*) as n FROM rag_chunks WHERE LOWER(source) LIKE ? GROUP BY source",
                                 (f'%{term}%',))
                    rows = c.fetchall()
                    for r in rows:
                        src = r['source'] if DATABASE_URL else r[0]
                        n = r['n'] if DATABASE_URL else r[1]
                        if src not in [d[0] for d in docs_directos]:
                            docs_directos.append((src, n, term))
            conn.close()
        
        # 2. Ejecutar buscar_rag normal
        chunks, score_max = buscar_rag(query, limit=15)
        
        # Clasificar confianza
        if score_max >= 45.0:
            conf = "ALTA-FASE1 ✅ (documento encontrado por nombre)"
        elif score_max >= 8.0:
            conf = "ALTA ✅"
        elif score_max >= 4.0:
            conf = "MEDIA 🟡"
        elif score_max >= 1.5:
            conf = "BAJA 🟠"
        else:
            conf = "NINGUNA ❌"
        
        texto = f"🔬 DEBUG RAG COMPLETO: '{query}'\n"
        texto += "━" * 40 + "\n\n"
        
        # Documentos encontrados por nombre
        texto += f"🎯 FASE 1 — Búsqueda por nombre de documento:\n"
        if docs_directos:
            for src, n, term in docs_directos:
                texto += f"  ✅ '{term}' → {src} ({n} chunks)\n"
        else:
            texto += f"  ❌ Ningún documento tiene los términos '{query}' en su nombre\n"
            texto += f"  ⚠️ Esto significa que el PDF de Milei tiene otro nombre\n"
        
        texto += f"\n📊 FASE 2 — Búsqueda semántica:\n"
        texto += f"  Score máximo: {score_max:.2f} → {conf}\n"
        texto += f"  Chunks retornados: {len(chunks)}\n\n"
        
        # Distribución por documento
        dist = {}
        for item in chunks:
            if isinstance(item, tuple):
                src = item[1]
            else:
                src = "?"
            dist[src] = dist.get(src, 0) + 1
        
        texto += "📋 DISTRIBUCIÓN POR DOCUMENTO:\n"
        for src, n in sorted(dist.items(), key=lambda x: -x[1]):
            src_clean = src.replace('PDF:', '').replace('EXCEL:', '')
            texto += f"  {n}x → {src_clean}\n"
        
        texto += "\n━" * 20 + "\n"
        texto += "🔍 PRIMEROS 5 CHUNKS:\n\n"
        
        for i, item in enumerate(chunks[:5], 1):
            if isinstance(item, tuple):
                chunk_texto, chunk_source = item[0], item[1]
            else:
                chunk_texto, chunk_source = item, "?"
            fuente = chunk_source.replace('PDF:', '').replace('EXCEL:', '')
            texto += f"[{i}] {fuente}\n{chunk_texto[:250]}...\n\n"
        
        # Enviar en partes si es largo
        if len(texto) > 4000:
            partes = [texto[i:i+3900] for i in range(0, len(texto), 3900)]
            await msg.edit_text(partes[0])
            for parte in partes[1:]:
                await update.message.reply_text(parte)
        else:
            await msg.edit_text(texto)
        
    except Exception as e:
        import traceback
        await msg.edit_text(f"❌ Error en debug: {e}\n{traceback.format_exc()[:500]}")


async def rag_status_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /rag_status - Ver estado del sistema RAG (paginado para 100+ PDFs)"""
    msg = await update.message.reply_text("🔍 Consultando estado del sistema RAG...")
    
    try:
        stats = obtener_estadisticas_rag()
        
        if not stats:
            await msg.edit_text("❌ Error obteniendo estadisticas RAG.\nVerifica que la base de datos esté activa.")
            return
        
        # ===== MENSAJE 1: RESUMEN GENERAL =====
        resultado = "━" * 30 + "\n"
        resultado += "🧠 ESTADO DEL SISTEMA RAG\n"
        resultado += "━" * 30 + "\n\n"
        
        total_chunks = stats.get('total_chunks', 0)
        total_pdfs = stats.get('total_pdfs', 0)
        resultado += f"📊 Total chunks indexados: {total_chunks:,}\n"
        resultado += f"📄 PDFs/Libros indexados: {total_pdfs}\n\n"
        
        # Almacenamiento
        rag_data_bytes = stats.get('rag_data_bytes', 0)
        rag_table_bytes = stats.get('rag_table_bytes', 0)
        db_total_bytes = stats.get('db_total_bytes', 0)
        db_limite_bytes = stats.get('db_limite_bytes', 500 * 1024 * 1024)
        
        rag_mb = rag_data_bytes / (1024 * 1024) if rag_data_bytes else 0
        rag_table_mb = rag_table_bytes / (1024 * 1024) if rag_table_bytes else 0
        db_total_mb = db_total_bytes / (1024 * 1024) if db_total_bytes else 0
        db_limite_mb = db_limite_bytes / (1024 * 1024) if db_limite_bytes else 500
        
        resultado += "💾 ALMACENAMIENTO\n"
        if db_total_mb > 0:
            db_pct = (db_total_mb / db_limite_mb) * 100 if db_limite_mb > 0 else 0
            resultado += f"   📦 BD total: {db_total_mb:.1f} MB de {db_limite_mb:.0f} MB ({db_pct:.1f}%)\n"
            bloques_llenos = min(20, int(db_pct / 5))
            bloques_vacios = 20 - bloques_llenos
            color = "🟢" if db_pct < 50 else ("🟡" if db_pct < 80 else "🔴")
            barra = "▓" * bloques_llenos + "░" * bloques_vacios
            resultado += f"   {color} [{barra}] {db_pct:.1f}%\n"
        else:
            resultado += f"   📦 BD: (tamaño no disponible en SQLite)\n"
        
        if rag_table_mb > 0:
            resultado += f"   🧠 Tabla RAG: {rag_table_mb:.1f} MB\n"
        resultado += f"   📝 Texto indexado: {rag_mb:.2f} MB\n"
        
        if total_pdfs > 0 and rag_mb > 0:
            mb_por_pdf = rag_mb / total_pdfs
            espacio_libre_mb = db_limite_mb - db_total_mb if db_total_mb > 0 else db_limite_mb - rag_mb
            pdfs_estimados = int(espacio_libre_mb / mb_por_pdf) if mb_por_pdf > 0 else 999
            resultado += f"   📈 Promedio por PDF: {mb_por_pdf:.2f} MB\n"
            resultado += f"   🔮 Capacidad estimada: ~{pdfs_estimados:,} PDFs mas\n"
        resultado += "\n"
        
        # Diagnóstico de Drive (rápido)
        resultado += "☁️ BACKUP GOOGLE DRIVE:\n"
        try:
            headers = obtener_drive_auth_headers()
            if headers:
                test_url = "https://www.googleapis.com/drive/v3/about?fields=storageQuota"
                test_resp = requests.get(test_url, headers=headers, timeout=5)
                if test_resp.status_code == 200:
                    quota = test_resp.json().get('storageQuota', {})
                    usado_gb = int(quota.get('usage', 0)) / (1024**3)
                    limite_gb = int(quota.get('limit', 0)) / (1024**3)
                    if limite_gb > 0:
                        resultado += f"   ✅ Conectado ({usado_gb:.1f} GB de {limite_gb:.0f} GB)\n"
                    else:
                        resultado += f"   ✅ Conectado (uso: {usado_gb:.1f} GB)\n"
                    
                    rag_id = obtener_carpeta_rag_pdf()
                    if rag_id:
                        resultado += f"   📁 Carpeta RAG_PDF: OK\n"
                        test_meta = {'name': '.test_write', 'parents': [rag_id]}
                        wr = requests.post("https://www.googleapis.com/drive/v3/files?supportsAllDrives=true",
                                          headers={**headers, 'Content-Type': 'application/json'},
                                          json=test_meta, timeout=5)
                        if wr.status_code in [200, 201]:
                            test_id = wr.json().get('id')
                            if test_id:
                                requests.delete(f"https://www.googleapis.com/drive/v3/files/{test_id}?supportsAllDrives=true",
                                              headers=headers, timeout=5)
                            resultado += f"   ✅ Permisos de escritura: OK\n"
                        else:
                            resultado += f"   ❌ Sin permisos escritura (HTTP {wr.status_code})\n"
                            resultado += f"   💡 Comparte INBESTU con Service Account como Editor\n"
                    else:
                        resultado += f"   ⚠️ Carpeta RAG_PDF no encontrada\n"
                elif test_resp.status_code == 403:
                    resultado += f"   ❌ Error 403: Sin permisos en Drive\n"
                else:
                    resultado += f"   ⚠️ Error HTTP {test_resp.status_code}\n"
            else:
                resultado += f"   ⚠️ Sin credenciales Drive configuradas\n"
        except requests.exceptions.Timeout:
            resultado += f"   ⚠️ Timeout conectando a Drive\n"
        except Exception as e:
            resultado += f"   ⚠️ {str(e)[:50]}\n"
        resultado += "\n"
        
        resultado += "━" * 30 + "\n"
        resultado += "📤 Envia un PDF al bot para indexarlo\n"
        resultado += "💡 /rag_consulta [pregunta]\n"
        resultado += "💡 /eliminar_pdf - Ver/eliminar PDFs\n"
        resultado += "💡 /rag_backup - Ver detalle de cada libro"
        
        # Enviar mensaje 1 (resumen)
        await msg.edit_text(resultado)
        
        # ===== MENSAJE 2+: LISTA DE FUENTES (paginada) =====
        por_fuente = stats.get('por_fuente', [])
        if por_fuente:
            # Separar PDFs y otras fuentes
            pdfs_lista = []
            otras_lista = []
            for fuente_data in por_fuente:
                try:
                    if isinstance(fuente_data, (list, tuple)):
                        fuente = str(fuente_data[0])
                        total = fuente_data[1]
                    elif isinstance(fuente_data, dict):
                        fuente = str(fuente_data.get('source', ''))
                        total = fuente_data.get('total', 0)
                    else:
                        continue
                    
                    if fuente.startswith('PDF:'):
                        nombre_corto = fuente.replace('PDF:', '')[:45]
                        pdfs_lista.append(f"📄 {nombre_corto}: {total}")
                    else:
                        otras_lista.append(f"📊 {fuente[:45]}: {total}")
                except:
                    continue
            
            # Construir lista paginada (máx ~3800 chars por mensaje)
            paginas = []
            pagina_actual = f"📁 FUENTES INDEXADAS ({len(pdfs_lista)} PDFs)\n\n"
            
            # Primero las otras fuentes
            for linea in otras_lista:
                pagina_actual += linea + " chunks\n"
            if otras_lista:
                pagina_actual += "\n"
            
            # Luego los PDFs
            for i, linea in enumerate(pdfs_lista, 1):
                nueva_linea = f"{i:3d}. {linea} chunks\n"
                if len(pagina_actual) + len(nueva_linea) > 3800:
                    paginas.append(pagina_actual)
                    pagina_actual = f"📁 FUENTES (cont. pag {len(paginas)+1})\n\n"
                pagina_actual += nueva_linea
            
            if pagina_actual.strip():
                paginas.append(pagina_actual)
            
            # Enviar cada página como mensaje separado
            for pagina in paginas:
                try:
                    await update.message.reply_text(pagina)
                except Exception as e:
                    logger.warning(f"Error enviando página rag_status: {e}")
        
    except Exception as e:
        logger.error(f"Error en rag_status: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await msg.edit_text(f"❌ Error en rag_status: {str(e)[:200]}")


@requiere_suscripcion
async def rag_consulta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /rag_consulta - Consulta UNIFICADA al sistema de conocimiento con IA"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso: /rag_consulta [tu pregunta]\n\n"
            "Ejemplos:\n"
            "  /rag_consulta reglas del grupo\n"
            "  /rag_consulta libro de Milei\n"
            "  /rag_consulta como hacer networking\n\n"
            "Busca en TODAS las fuentes: PDFs indexados, "
            "historial del grupo y base de datos."
        )
        return
    
    query = ' '.join(context.args)
    msg = await update.message.reply_text(f"🧠 Buscando en todas las fuentes: {query}...")
    
    try:
        # Búsqueda unificada (máximo contexto posible)
        resultados = busqueda_unificada(query, limit_historial=10, limit_rag=30)
        
        tiene_historial = bool(resultados.get('historial'))
        tiene_rag = bool(resultados.get('rag'))
        fuentes = ', '.join(resultados.get('fuentes_usadas', []))
        
        if not tiene_historial and not tiene_rag:
            await msg.edit_text(
                f"🔍 No se encontraron resultados para: {query}\n\n"
                "💡 Prueba con palabras clave diferentes.\n"
                "💡 Usa /rag_status para ver documentos indexados."
            )
            return
        
        # Generar respuesta con IA
        if ia_disponible:
            contexto_completo = formatear_contexto_unificado(resultados, query)
            rag_conf = resultados.get('rag_confianza', 'ninguna')
            total_rag = len(resultados.get('rag', []))
            
            prompt = f"""Eres el asistente IA de la Cofradía de Networking.

CONSULTA: "{query}"

{"━"*50}
⚠️ INSTRUCCIÓN CRÍTICA — DEBES SEGUIRLA AL PIE DE LA LETRA:

El motor de búsqueda encontró {total_rag} fragmentos con confianza {rag_conf.upper()}.
ESTOS FRAGMENTOS CONTIENEN LA INFORMACIÓN QUE EL USUARIO BUSCA.

TU TAREA:
1. LEE cada fragmento del contexto con atención
2. SINTETIZA el contenido en una respuesta completa y útil
3. NUNCA digas "no encontré información" — la información ESTÁ en los fragmentos
4. Si los fragmentos hablan de economía/inflación → ÚSALOS aunque no repitan el nombre del autor en cada línea
5. Identifica el tema central de cada documento por su NOMBRE DE ARCHIVO y su CONTENIDO
6. Responde en español, claro, completo, máximo 600 palabras
7. Sin asteriscos ni markdown
{"━"*50}

CONTEXTO COMPLETO:
{contexto_completo}

REGLA FINAL: Si el sistema dice que estos fragmentos son del tema buscado → CONFÍA en el sistema y ÚSALOS."""
            
            respuesta = llamar_groq(prompt, max_tokens=2000, temperature=0.3)
            
            if respuesta:
                respuesta_limpia = respuesta.replace('*', '').replace('_', ' ')
                texto_final = "🧠 CONSULTA INTELIGENTE\n"
                texto_final += "━" * 30 + "\n\n"
                texto_final += f"🔍 Pregunta: {query}\n\n"
                texto_final += respuesta_limpia + "\n\n"
                texto_final += "━" * 30 + "\n"
                texto_final += f"📚 Fuentes: {fuentes}"
                
                await msg.edit_text(texto_final)
                registrar_servicio_usado(update.effective_user.id, 'rag_consulta')
                return
        
        # Sin IA: mostrar resultados directos
        texto_final = "🧠 RESULTADOS DE BÚSQUEDA\n"
        texto_final += "━" * 30 + "\n\n"
        texto_final += f"🔍 Busqueda: {query}\n"
        texto_final += f"📊 Fuentes: {fuentes}\n\n"
        
        if tiene_rag:
            texto_final += "📄 DOCUMENTOS:\n"
            for i, item in enumerate(resultados['rag'][:5], 1):
                r = item[0] if isinstance(item, tuple) else item
                texto_final += f"  [{i}] {r[:250]}...\n\n" if len(r) > 250 else f"  [{i}] {r}\n\n"
        
        if tiene_historial:
            texto_final += "💬 HISTORIAL:\n"
            for nombre, texto, fecha in resultados['historial'][:5]:
                texto_final += f"  {nombre}: {texto[:150]}...\n\n"
        
        await msg.edit_text(texto_final)
        registrar_servicio_usado(update.effective_user.id, 'rag_consulta')
        
    except Exception as e:
        logger.error(f"Error en rag_consulta: {e}")
        await msg.edit_text(f"❌ Error consultando: {str(e)[:200]}")


async def rag_backup_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /rag_backup - Verificar integridad de datos RAG (solo admin) - paginado"""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Comando solo disponible para el administrador.")
        return
    
    msg = await update.message.reply_text("🔍 Verificando integridad de datos RAG en Supabase...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ No se pudo conectar a la base de datos.")
            return
        
        c = conn.cursor()
        
        if DATABASE_URL:
            # Total chunks
            c.execute("SELECT COUNT(*) as total FROM rag_chunks")
            total = int(c.fetchone()['total'] or 0)
            
            # Por fuente
            c.execute("""SELECT source, COUNT(*) as chunks, 
                        MIN(fecha_indexado) as primera, MAX(fecha_indexado) as ultima,
                        SUM(LENGTH(chunk_text)) as bytes_texto
                        FROM rag_chunks 
                        GROUP BY source 
                        ORDER BY ultima DESC""")
            fuentes = c.fetchall()
            
            # Tamaño tabla
            c.execute("SELECT pg_total_relation_size('rag_chunks') as size")
            table_size = int(c.fetchone()['size'] or 0)
        else:
            c.execute("SELECT COUNT(*) FROM rag_chunks")
            total = c.fetchone()[0]
            fuentes = []
            table_size = 0
        
        conn.close()
        
        # ===== MENSAJE 1: RESUMEN =====
        resumen = "━" * 30 + "\n"
        resumen += "🔒 VERIFICACIÓN INTEGRIDAD RAG\n"
        resumen += "━" * 30 + "\n\n"
        resumen += f"📊 Total chunks en Supabase: {total:,}\n\n"
        
        total_bytes = 0
        pdf_count = 0
        for f in fuentes:
            if DATABASE_URL:
                bytes_t = int(f['bytes_texto'] or 0)
                nombre = f['source']
            else:
                bytes_t = 0
                nombre = ''
            total_bytes += bytes_t
            if nombre.startswith('PDF:'):
                pdf_count += 1
        
        resumen += f"📈 RESUMEN:\n"
        resumen += f"  📚 PDFs/Libros: {pdf_count}\n"
        resumen += f"  📊 Total fuentes: {len(fuentes)}\n"
        resumen += f"  🧩 Total chunks: {total:,}\n"
        resumen += f"  💾 Texto total: {total_bytes / (1024*1024):.1f} MB\n"
        if table_size > 0:
            resumen += f"  📦 Tabla RAG en disco: {table_size / (1024*1024):.1f} MB\n"
        resumen += f"\n✅ DATOS SEGUROS EN SUPABASE\n"
        resumen += f"Los {pdf_count} libros/PDFs están indexados\n"
        resumen += f"en la base de datos PostgreSQL.\n"
        resumen += "━" * 30
        
        await msg.edit_text(resumen)
        
        # ===== MENSAJES 2+: DETALLE POR FUENTE (paginado) =====
        if fuentes:
            paginas = []
            pagina_actual = f"📄 DETALLE ({len(fuentes)} fuentes)\n\n"
            num = 0
            
            for f in fuentes:
                if DATABASE_URL:
                    nombre = f['source']
                    chunks = int(f['chunks'])
                    bytes_t = int(f['bytes_texto'] or 0)
                    ultima = str(f['ultima'])[:10] if f['ultima'] else '?'
                else:
                    continue
                
                kb = bytes_t / 1024
                num += 1
                
                if nombre.startswith('PDF:'):
                    nombre_corto = nombre.replace('PDF:', '')[:42]
                    linea = f"{num:3d}. 📕 {nombre_corto}\n     {chunks} chunks | {kb:.0f} KB | {ultima}\n"
                else:
                    linea = f"{num:3d}. 📊 {nombre[:42]}\n     {chunks} chunks | {kb:.0f} KB\n"
                
                if len(pagina_actual) + len(linea) > 3800:
                    paginas.append(pagina_actual)
                    pagina_actual = f"📄 DETALLE (cont. pag {len(paginas)+1})\n\n"
                pagina_actual += linea
            
            if pagina_actual.strip():
                paginas.append(pagina_actual)
            
            for pagina in paginas:
                try:
                    await update.message.reply_text(pagina)
                except Exception as e:
                    logger.warning(f"Error enviando página rag_backup: {e}")
        
    except Exception as e:
        logger.error(f"Error en rag_backup: {e}")
        await msg.edit_text(f"❌ Error: {str(e)[:200]}")


async def rag_enriquecer_keywords_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /rag_enriquecer — Enriquece keywords de PDFs YA indexados con términos del nombre de archivo.
    Resuelve el problema de PDFs cuyo contenido no menciona al autor (ej: "El_fin_de_la_inflacion.pdf"
    cuyos chunks no repiten "Milei" en el cuerpo).
    Solo owner.
    """
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("🔒 Solo el administrador puede ejecutar esto.")
        return
    
    msg = await update.message.reply_text("🔑 Enriqueciendo keywords de todos los PDFs indexados...")
    
    try:
        import unicodedata, re as _re
        
        def normalizar_kw(t):
            t = unicodedata.normalize('NFKD', t.lower())
            return ''.join(ch for ch in t if not unicodedata.combining(ch))
        
        stopwords_arch = {'el','la','los','las','de','del','un','una','y','a','en','fin',
                         'the','of','and','to','in','por','para','con','que','es'}
        
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Sin conexión a BD")
            return
        
        c = conn.cursor()
        
        # Obtener todos los PDFs únicos
        if DATABASE_URL:
            c.execute("SELECT DISTINCT source FROM rag_chunks WHERE source LIKE 'PDF:%%'")
        else:
            c.execute("SELECT DISTINCT source FROM rag_chunks WHERE source LIKE 'PDF:%'")
        sources = [r['source'] if DATABASE_URL else r[0] for r in c.fetchall()]
        
        total_actualizados = 0
        log_docs = []
        
        for source in sources:
            filename = source.replace('PDF:', '')
            nombre_sin_ext = _re.sub(r'\.(pdf|docx?|txt)$', '', filename, flags=_re.IGNORECASE)
            partes = _re.split(r'[\s\-_.,]+', nombre_sin_ext)
            kw_filename = ' '.join([
                normalizar_kw(p) for p in partes
                if len(p) >= 3 and normalizar_kw(p) not in stopwords_arch
            ])
            
            if not kw_filename.strip():
                continue
            
            # Actualizar keywords de cada chunk: anteponer keywords del filename
            if DATABASE_URL:
                c.execute("""UPDATE rag_chunks 
                            SET keywords = %s || ' ' || COALESCE(keywords, '')
                            WHERE source = %s 
                            AND keywords NOT LIKE %s""",
                         (kw_filename, source, f'%{kw_filename[:20]}%'))
            else:
                c.execute("""UPDATE rag_chunks 
                            SET keywords = ? || ' ' || COALESCE(keywords, '')
                            WHERE source = ? 
                            AND keywords NOT LIKE ?""",
                         (kw_filename, source, f'%{kw_filename[:20]}%'))
            
            n = c.rowcount
            if n > 0:
                total_actualizados += n
                log_docs.append(f"  ✅ {filename[:50]} → '{kw_filename}' ({n} chunks)")
        
        conn.commit()
        conn.close()
        
        texto = f"✅ KEYWORDS ENRIQUECIDAS\n{'━'*35}\n\n"
        texto += f"📄 Documentos procesados: {len(sources)}\n"
        texto += f"🔑 Chunks actualizados: {total_actualizados}\n\n"
        if log_docs:
            texto += "DOCUMENTOS ENRIQUECIDOS:\n" + "\n".join(log_docs[:20])
        texto += "\n\n💡 Ahora las búsquedas por autor/título funcionarán correctamente."
        texto += "\n💡 NO es necesario re-indexar los PDFs."
        
        if len(texto) > 4000:
            partes = [texto[i:i+3900] for i in range(0, len(texto), 3900)]
            await msg.edit_text(partes[0])
            for p in partes[1:]:
                await update.message.reply_text(p)
        else:
            await msg.edit_text(texto)
            
    except Exception as e:
        import traceback
        logger.error(f"Error enriqueciendo keywords: {e}\n{traceback.format_exc()}")
        await msg.edit_text(f"❌ Error: {e}")


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
        # Listar PDFs disponibles desde BD (paginado)
        pdfs = listar_pdfs_rag()
        if not pdfs:
            await update.message.reply_text("📁 No hay PDFs indexados en el sistema RAG")
            return
        
        # Construir lista paginada
        paginas = []
        pagina_actual = f"📁 PDFs indexados ({len(pdfs)} total)\n\n"
        
        for i, pdf in enumerate(pdfs, 1):
            chunks = pdf.get('chunks', '?')
            origen = pdf.get('origen', '')
            linea = f"{i}. {pdf['name']} ({chunks} ch) [{origen}]\n"
            
            if len(pagina_actual) + len(linea) > 3800:
                paginas.append(pagina_actual)
                pagina_actual = f"📁 PDFs (cont. pag {len(paginas)+1})\n\n"
            pagina_actual += linea
        
        pagina_actual += "\n💡 Uso: /eliminar_pdf [nombre exacto]"
        paginas.append(pagina_actual)
        
        for pagina in paginas:
            try:
                await update.message.reply_text(pagina)
            except Exception as e:
                logger.warning(f"Error enviando lista PDFs: {e}")
        return
    
    filename = ' '.join(context.args)
    
    try:
        chunks_eliminados = 0
        
        # PASO 1: Eliminar chunks del RAG en la BD (principal)
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            # Buscar con diferentes formatos de source
            for source_pattern in [filename, f"PDF:{filename}"]:
                if DATABASE_URL:
                    c.execute("DELETE FROM rag_chunks WHERE source = %s", (source_pattern,))
                else:
                    c.execute("DELETE FROM rag_chunks WHERE source = ?", (source_pattern,))
                chunks_eliminados += c.rowcount
            conn.commit()
            conn.close()
        
        if chunks_eliminados == 0:
            await update.message.reply_text(f"❌ No se encontró: {filename}\n\nUsa /eliminar_pdf sin argumentos para ver la lista.")
            return
        
        # PASO 2: Intentar eliminar de Drive (opcional)
        drive_status = ""
        try:
            headers = obtener_drive_auth_headers()
            if headers:
                rag_folder_id = obtener_carpeta_rag_pdf()
                if rag_folder_id:
                    query = f"name = '{filename}' and '{rag_folder_id}' in parents and trashed = false"
                    search_url = "https://www.googleapis.com/drive/v3/files"
                    params = {'q': query, 'fields': 'files(id, name)'}
                    resp = requests.get(search_url, headers=headers, params=params, timeout=30)
                    archivos = resp.json().get('files', [])
                    
                    if archivos:
                        file_id = archivos[0]['id']
                        delete_url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
                        resp = requests.patch(delete_url, headers={**headers, 'Content-Type': 'application/json'},
                                            json={'trashed': True}, timeout=30)
                        if resp.status_code == 200:
                            drive_status = "\n☁️ También eliminado de Google Drive"
        except Exception as e:
            logger.warning(f"Error eliminando de Drive: {e}")
        
        await update.message.reply_text(
            f"✅ PDF eliminado: {filename}\n"
            f"🧩 {chunks_eliminados} chunks RAG eliminados{drive_status}"
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
    """Indexa datos del Excel de Google Drive en chunks para RAG.
    IMPORTANTE: Solo borra y re-crea chunks con source='BD_Grupo_Laboral'.
    NUNCA toca los chunks de PDFs (source LIKE 'PDF:%').
    """
    try:
        df = obtener_datos_excel_drive()
        if df is None or len(df) == 0:
            logger.info("RAG: No hay datos para indexar")
            return
        
        conn = get_db_connection()
        if not conn:
            return
        
        c = conn.cursor()
        
        # SOLO limpiar chunks del Excel (PRESERVAR PDFs y otros)
        if DATABASE_URL:
            c.execute("DELETE FROM rag_chunks WHERE source = %s", ('BD_Grupo_Laboral',))
        else:
            c.execute("DELETE FROM rag_chunks WHERE source = ?", ('BD_Grupo_Laboral',))
        
        logger.info("🔄 RAG: Chunks de Excel eliminados para re-indexar (PDFs preservados)")
        
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
    """
    Búsqueda RAG bifásica:
    Fase 1 — Búsqueda por NOMBRE DE DOCUMENTO (nombres propios, títulos, autores)
    Fase 2 — Búsqueda semántica general balanceada (máx. 3 chunks por documento)
    
    Retorna: (lista de (texto, source), score_maximo)
    """
    try:
        conn = get_db_connection()
        if not conn:
            return [], 0.0
        
        c = conn.cursor()
        
        import unicodedata
        def normalizar(texto):
            texto = unicodedata.normalize('NFKD', texto.lower())
            return ''.join(ch for ch in texto if not unicodedata.combining(ch))
        
        STOPWORDS = {'de', 'la', 'el', 'en', 'los', 'las', 'del', 'al', 'un', 'una',
                    'por', 'con', 'para', 'que', 'es', 'se', 'no', 'su', 'lo', 'como',
                    'mas', 'pero', 'sus', 'le', 'ya', 'este', 'si', 'ha', 'son',
                    'muy', 'hay', 'fue', 'ser', 'han', 'esta', 'tan', 'sin', 'sobre',
                    'trata', 'libro', 'libros', 'sobre', 'acerca', 'habla', 'dice',
                    'cuéntame', 'cuentame', 'cual', 'cuál', 'qué', 'que',
                    'a', 'y', 'o', 'e', 'u', 'the', 'of', 'and', 'to', 'in'}
        
        query_norm = normalizar(query)
        
        # Extraer términos relevantes — priorizar palabras largas (nombres propios, títulos)
        todas_palabras = [p for p in query_norm.split() if len(p) > 1 and p not in STOPWORDS]
        # Palabras clave: las más largas y/o únicas (nombres propios suelen ser >= 4 letras)
        palabras_clave = sorted(set(todas_palabras), key=len, reverse=True)
        
        if not palabras_clave:
            conn.close()
            return [], 0.0
        
        # ═══════════════════════════════════════════════════
        # FASE 1: BÚSQUEDA POR NOMBRE DE DOCUMENTO
        # Si alguna palabra clave aparece en el SOURCE (nombre del archivo),
        # traer TODOS los chunks de ese documento — máxima prioridad
        # ═══════════════════════════════════════════════════
        docs_fase1 = []    # sources encontrados en fase 1
        chunks_fase1 = []  # (texto, score, source) con score alto
        
        for palabra in palabras_clave:
            if len(palabra) < 3:
                continue
            try:
                if DATABASE_URL:
                    c.execute("SELECT DISTINCT source FROM rag_chunks WHERE LOWER(source) LIKE %s LIMIT 5",
                             (f'%{palabra}%',))
                else:
                    c.execute("SELECT DISTINCT source FROM rag_chunks WHERE LOWER(source) LIKE ? LIMIT 5",
                             (f'%{palabra}%',))
                sources_encontrados = c.fetchall()
                for row in sources_encontrados:
                    src = row['source'] if DATABASE_URL else row[0]
                    if src and src not in docs_fase1:
                        docs_fase1.append(src)
                        logger.info(f"🎯 Fase 1 RAG: '{palabra}' → documento '{src}'")
            except Exception as e:
                logger.debug(f"Error fase1 RAG: {e}")
        
        if docs_fase1:
            # Traer TODOS los chunks de los documentos encontrados en fase 1
            for src in docs_fase1[:3]:  # Máximo 3 documentos en fase 1
                try:
                    if DATABASE_URL:
                        c.execute("SELECT chunk_text, keywords, source FROM rag_chunks WHERE source = %s ORDER BY id ASC",
                                 (src,))
                    else:
                        c.execute("SELECT chunk_text, keywords, source FROM rag_chunks WHERE source = ? ORDER BY id ASC",
                                 (src,))
                    filas_doc = c.fetchall()
                    
                    for fila in filas_doc:
                        if DATABASE_URL:
                            texto = fila['chunk_text'] or ''
                            source = fila['source'] or ''
                        else:
                            texto, _, source = fila[0] or '', fila[1] or '', fila[2] or ''
                        
                        # Score altísimo para documentos de fase 1 — garantiza que sean top
                        score = 50.0 + len(texto) / 100
                        chunks_fase1.append((texto, score, source))
                except Exception as e:
                    logger.debug(f"Error trayendo chunks de '{src}': {e}")
        
        # ═══════════════════════════════════════════════════
        # FASE 2: BÚSQUEDA SEMÁNTICA GENERAL BALANCEADA
        # Busca por keywords/texto, pero limita chunks por documento
        # ═══════════════════════════════════════════════════
        # Solo términos que no sean stopwords contextuales
        terminos_semanticos = [p for p in palabras_clave if len(p) >= 3][:8]
        
        chunks_fase2 = []
        
        if terminos_semanticos:
            condiciones = []
            params = []
            for term in terminos_semanticos:
                if DATABASE_URL:
                    condiciones.append("(LOWER(keywords) LIKE %s OR LOWER(chunk_text) LIKE %s)")
                    params.extend([f'%{term}%', f'%{term}%'])
                else:
                    condiciones.append("(LOWER(keywords) LIKE ? OR LOWER(chunk_text) LIKE ?)")
                    params.extend([f'%{term}%', f'%{term}%'])
            
            where_clause = " OR ".join(condiciones)
            max_candidates = limit * 20
            
            # Excluir documentos ya encontrados en fase 1
            if docs_fase1:
                exclusion = " AND source NOT IN ({})".format(
                    ','.join(['%s'] * len(docs_fase1) if DATABASE_URL else ['?'] * len(docs_fase1))
                )
                where_clause_f2 = f"({where_clause}){exclusion}"
                params_f2 = params + docs_fase1
            else:
                where_clause_f2 = where_clause
                params_f2 = params
            
            try:
                if DATABASE_URL:
                    c.execute(f"""SELECT chunk_text, keywords, metadata, source FROM rag_chunks 
                                WHERE {where_clause_f2} LIMIT %s""", params_f2 + [max_candidates])
                else:
                    c.execute(f"""SELECT chunk_text, keywords, metadata, source FROM rag_chunks 
                                WHERE {where_clause_f2} LIMIT ?""", params_f2 + [max_candidates])
                filas_f2 = c.fetchall()
                
                # Scoring semántico
                for fila in filas_f2:
                    if DATABASE_URL:
                        texto = fila['chunk_text'] or ''
                        keywords = fila['keywords'] or ''
                        source = fila['source'] or ''
                    else:
                        texto = fila[0] or ''
                        keywords = fila[1] or ''
                        source = fila[3] if len(fila) > 3 else ''
                    
                    texto_n = normalizar(texto)
                    kw_n = normalizar(keywords)
                    
                    score = 0.0
                    for p in terminos_semanticos:
                        if p in kw_n:
                            score += 3.0
                        if p in texto_n:
                            score += 2.0 + min(texto_n.count(p) * 0.3, 2.0)
                    
                    if len(texto) < 100:
                        score *= 0.4
                    elif len(texto) < 200:
                        score *= 0.7
                    if len(texto) > 500:
                        score *= 1.1
                    if source.startswith('PDF:'):
                        score *= 1.1
                    
                    if score > 0:
                        chunks_fase2.append((texto, score, source))
                        
            except Exception as e:
                logger.warning(f"Error fase2 RAG: {e}")
        
        conn.close()
        
        # ═══════════════════════════════════════════════════
        # COMBINAR RESULTADOS
        # ═══════════════════════════════════════════════════
        resultados = []
        seen = set()
        score_maximo = 0.0
        
        # Primero: todos los chunks de documentos fase 1 (en orden del documento)
        for texto, score, source in chunks_fase1:
            key = texto[:80]
            if key not in seen and texto.strip():
                seen.add(key)
                resultados.append((texto, source))
                score_maximo = max(score_maximo, score)
                if len(resultados) >= limit:
                    break
        
        # FASE 2 con promoción automática:
        # Si un documento domina los mejores resultados semánticos → promoverlo como Fase 1
        # (esto resuelve el caso de PDFs con nombre sin el término buscado, ej: "El_fin_de_la_inflacion.pdf" para query "milei")
        if chunks_fase2:
            chunks_fase2.sort(key=lambda x: x[1], reverse=True)
            
            # Analizar distribución en los TOP-20 chunks por score
            top_chunks = chunks_fase2[:20]
            score_por_doc = {}
            count_por_doc = {}
            for txt, sc, src in top_chunks:
                score_por_doc[src] = score_por_doc.get(src, 0) + sc
                count_por_doc[src] = count_por_doc.get(src, 0) + 1
            
            # Identificar documento dominante: > 40% de los top chunks O score acumulado > 2x el segundo
            docs_ordenados = sorted(score_por_doc.items(), key=lambda x: -x[1])
            doc_dominante = None
            
            if docs_ordenados:
                top_doc, top_score = docs_ordenados[0]
                top_count = count_por_doc.get(top_doc, 0)
                
                segundo_score = docs_ordenados[1][1] if len(docs_ordenados) > 1 else 0
                
                # Criterio de dominancia: muchos chunks del mismo doc en top resultados
                if (top_count >= 5 or  # 5+ chunks en top-20
                    (top_count >= 3 and top_score > segundo_score * 2) or  # 3+ chunks y doble score
                    (top_count / max(len(top_chunks), 1)) >= 0.4):  # 40%+ del top son del mismo doc
                    doc_dominante = top_doc
                    logger.info(f"🚀 RAG Fase2→Fase1 promoción: '{top_doc}' "
                               f"(count={top_count}, score={top_score:.1f}, ratio={top_count/max(len(top_chunks),1):.0%})")
            
            # Si hay documento dominante NO encontrado en fase 1, traer TODOS sus chunks
            if doc_dominante and doc_dominante not in docs_fase1:
                try:
                    conn2 = get_db_connection()
                    if conn2:
                        c2 = conn2.cursor()
                        if DATABASE_URL:
                            c2.execute("SELECT chunk_text, source FROM rag_chunks WHERE source = %s ORDER BY id ASC",
                                      (doc_dominante,))
                        else:
                            c2.execute("SELECT chunk_text, source FROM rag_chunks WHERE source = ? ORDER BY id ASC",
                                      (doc_dominante,))
                        filas_dom = c2.fetchall()
                        conn2.close()
                        
                        for fila in filas_dom:
                            if len(resultados) >= limit:
                                break
                            txt = fila['chunk_text'] if DATABASE_URL else fila[0]
                            src = fila['source'] if DATABASE_URL else fila[1]
                            key = txt[:80] if txt else ''
                            if key and key not in seen and txt.strip():
                                seen.add(key)
                                resultados.append((txt, src))
                                score_maximo = max(score_maximo, 50.0)  # Promovido = score alto
                        
                        docs_fase1.append(doc_dominante)  # Marcar como ya procesado
                except Exception as e_dom:
                    logger.debug(f"Error promoviendo doc dominante: {e_dom}")
            
            # Chunks restantes de fase 2: máximo 3 por documento (excluir ya procesados)
            if len(resultados) < limit:
                conteo_por_doc = {}
                MAX_POR_DOC = 3
                
                for texto, score, source in chunks_fase2:
                    if len(resultados) >= limit:
                        break
                    if source in docs_fase1:  # Ya procesado (fase 1 o promovido)
                        continue
                    key = texto[:80]
                    if key in seen or not texto.strip():
                        continue
                    doc_count = conteo_por_doc.get(source, 0)
                    if doc_count >= MAX_POR_DOC:
                        continue
                    seen.add(key)
                    conteo_por_doc[source] = doc_count + 1
                    resultados.append((texto, source))
                    score_maximo = max(score_maximo, score)
        
        logger.info(f"🔍 RAG resultado final: {len(resultados)} chunks "
                   f"(fase1_docs={len(docs_fase1)}, f2_candidates={len(chunks_fase2)}), "
                   f"score_max={score_maximo:.1f}")
        
        return resultados, score_maximo
        
    except Exception as e:
        logger.error(f"Error en buscar_rag: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return [], 0.0


def busqueda_unificada(query, limit_historial=10, limit_rag=25):
    """Busca en TODAS las fuentes de conocimiento simultáneamente.
    Incluye verificación de relevancia temática: si los docs no tratan el tema
    consultado, marca rag_confianza='irrelevante' para que el LLM use su conocimiento.
    """
    resultados = {
        'historial': [],
        'rag': [],
        'fuentes_usadas': [],
        'rag_score_max': 0.0,
        'rag_confianza': 'ninguna',
        'rag_tematica': 'desconocida',   # 'relevante' | 'irrelevante' | 'desconocida'
    }
    
    # 1. Historial del grupo (mensajes de usuarios)
    try:
        historial = buscar_en_historial(query, limit=limit_historial)
        if historial:
            resultados['historial'] = historial
            resultados['fuentes_usadas'].append(f"Historial ({len(historial)} msgs)")
    except Exception as e:
        logger.warning(f"Error buscando historial unificado: {e}")
    
    # 2. RAG (PDFs indexados + Excel indexado en BD)
    try:
        chunks_rag, score_max = buscar_rag(query, limit=limit_rag)
        resultados['rag_score_max'] = score_max
        
        if score_max >= 45.0:
            resultados['rag_confianza'] = 'alta'
        elif score_max >= 8.0:
            resultados['rag_confianza'] = 'alta'
        elif score_max >= 4.0:
            resultados['rag_confianza'] = 'media'
        elif score_max >= 1.5:
            resultados['rag_confianza'] = 'baja'
        else:
            resultados['rag_confianza'] = 'ninguna'
        
        if chunks_rag:
            # ── VERIFICACIÓN DE RELEVANCIA TEMÁTICA ──────────────────────────────
            # Comparar términos clave del query contra términos clave del contenido
            # Si no hay overlap significativo → los docs no son sobre el tema consultado
            import unicodedata as _ud, re as _re
            def _norm(t):
                t = _ud.normalize('NFKD', t.lower())
                return ''.join(c for c in t if not _ud.combining(c))
            
            STOPWORDS_TEMA = {
                'de','la','el','en','los','las','del','al','un','una','por','con','para',
                'que','es','se','no','su','lo','como','mas','pero','sus','le','ya','este',
                'si','ha','son','muy','hay','fue','ser','han','esta','tan','sin','sobre',
                'trata','libro','libros','acerca','habla','dice','cual','que','como','esto',
                'a','y','o','e','u','the','of','and','to','in','me','te','le','nos'
            }
            
            # Términos clave del query (palabras significativas de 4+ letras)
            query_norm = _norm(query)
            terminos_query = set(
                w for w in _re.findall(r'\b\w{4,}\b', query_norm)
                if w not in STOPWORDS_TEMA
            )
            
            # Términos del contenido de los chunks retornados
            texto_chunks = ' '.join(
                (item[0] if isinstance(item, tuple) else item)
                for item in chunks_rag[:10]  # Analizar primeros 10 chunks
            )
            terminos_chunks = set(
                w for w in _re.findall(r'\b\w{4,}\b', _norm(texto_chunks))
                if w not in STOPWORDS_TEMA
            )
            
            # Nombres de documentos retornados (también como señal de relevancia)
            sources_retornados = set()
            for item in chunks_rag:
                src = (item[1] if isinstance(item, tuple) else '')
                src_norm = _norm(src.replace('PDF:', '').replace('EXCEL:', '').replace('_', ' '))
                sources_retornados.update(w for w in src_norm.split() if len(w) >= 4)
            
            # Calcular overlap: cuántos términos del query aparecen en chunks O en nombres de docs
            terminos_en_contenido = terminos_query & terminos_chunks
            terminos_en_sources = terminos_query & sources_retornados
            overlap_total = terminos_en_contenido | terminos_en_sources
            
            ratio_overlap = len(overlap_total) / max(len(terminos_query), 1)
            
            if ratio_overlap >= 0.4 or len(terminos_en_contenido) >= 2:
                resultados['rag_tematica'] = 'relevante'
            elif ratio_overlap >= 0.2 or len(terminos_en_sources) >= 1:
                resultados['rag_tematica'] = 'posible'
            else:
                resultados['rag_tematica'] = 'irrelevante'
                # Si los docs son irrelevantes, degradar confianza para que LLM use su conocimiento
                resultados['rag_confianza'] = 'irrelevante'
                logger.info(f"⚠️ RAG temática IRRELEVANTE para '{query[:50]}': "
                           f"overlap={ratio_overlap:.0%}, términos_query={terminos_query}, "
                           f"en_chunks={terminos_en_contenido}, en_sources={terminos_en_sources}")
            
            logger.info(f"📊 RAG temática: {resultados['rag_tematica']} "
                       f"(overlap={ratio_overlap:.0%}, query_terms={terminos_query}, "
                       f"en_contenido={terminos_en_contenido})")
            # ── FIN VERIFICACIÓN ──────────────────────────────────────────────────
            
            resultados['rag'] = chunks_rag
            confianza_str = resultados['rag_confianza']
            resultados['fuentes_usadas'].append(
                f"RAG/Documentos ({len(chunks_rag)} fragmentos, confianza: {confianza_str})"
            )
    except Exception as e:
        logger.warning(f"Error buscando RAG unificado: {e}")
    
    return resultados


def formatear_contexto_unificado(resultados, query):
    """Formatea resultados de búsqueda unificada en contexto para el LLM.
    Incluye el máximo de información posible para respuestas completas."""
    contexto = ""
    
    # RAG (PDFs y documentos) - PRIMERO y con fuente MUY VISIBLE, agrupado por documento
    if resultados.get('rag'):
        # Agrupar chunks por documento fuente
        docs_agrupados = {}
        for item in resultados['rag']:
            if isinstance(item, tuple):
                chunk_texto, chunk_source = item[0], item[1]
            else:
                chunk_texto, chunk_source = item, "documento"
            fuente_key = chunk_source.replace('PDF:', '').replace('EXCEL:', '').strip()
            if fuente_key not in docs_agrupados:
                docs_agrupados[fuente_key] = []
            docs_agrupados[fuente_key].append(chunk_texto)
        
        contexto += "\n\n╔══════════════════════════════════════════════════╗\n"
        contexto += "║  FRAGMENTOS DE DOCUMENTOS INDEXADOS (FUENTE RAG) ║\n"
        contexto += "╚══════════════════════════════════════════════════╝\n\n"
        
        # INSTRUCCIÓN EXPLÍCITA: conectar nombre del documento con la consulta
        nombres_docs = list(docs_agrupados.keys())
        contexto += f"⚠️ INSTRUCCIÓN PARA EL ASISTENTE:\n"
        contexto += f"Los siguientes fragmentos provienen de estos documentos indexados:\n"
        for nd in nombres_docs:
            contexto += f"  → \"{nd}\"\n"
        contexto += f"\nESTOS FRAGMENTOS CONTIENEN LA INFORMACIÓN SOLICITADA.\n"
        contexto += f"Tu tarea: LEER cada fragmento y SINTETIZAR una respuesta completa.\n"
        contexto += f"El nombre del documento identifica de qué trata — úsalo para contextualizar el contenido.\n"
        contexto += f"NO digas 'no encontré información' — la información ESTÁ en los fragmentos.\n\n"
        
        for doc_nombre, chunks in docs_agrupados.items():
            # Título del documento con nombre limpio y visible
            titulo_limpio = doc_nombre.replace('_', ' ').replace('-', ' ')
            # Quitar extensión para mejor legibilidad
            import re as _re
            titulo_limpio = _re.sub(r'\.(pdf|docx?|txt)$', '', titulo_limpio, flags=_re.IGNORECASE)
            contexto += f"{'═'*50}\n"
            contexto += f"📄 DOCUMENTO: \"{titulo_limpio}\"\n"
            contexto += f"   (archivo: {doc_nombre})\n"
            contexto += f"{'─'*50}\n"
            for i, chunk in enumerate(chunks, 1):
                contexto += f"[Fragmento {i}]:\n{chunk[:800]}\n\n"
            contexto += "\n"
    
    # Historial del grupo (al final, menos prioritario)
    if resultados.get('historial'):
        contexto += "\n\n── CONVERSACIONES DEL GRUPO (referencia) ──\n"
        for i, (nombre, texto, fecha) in enumerate(resultados['historial'][:8], 1):
            nombre_limpio = limpiar_nombre_display(nombre) if callable(limpiar_nombre_display) else nombre
            fecha_str = fecha.strftime("%d/%m/%Y") if hasattr(fecha, 'strftime') else str(fecha)[:10]
            contexto += f"{i}. {nombre_limpio} ({fecha_str}): {texto[:350]}\n"
    
    return contexto


async def indexar_rag_job(context: ContextTypes.DEFAULT_TYPE):
    """Job programado para re-indexar RAG cada 6 horas.
    SOLO re-indexa Excel. Los PDFs se preservan en la BD.
    """
    logger.info("🔄 Ejecutando re-indexación RAG (solo Excel, PDFs preservados)...")
    
    # Contar PDFs antes para verificar que no se pierdan
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT COUNT(*) as total FROM rag_chunks WHERE source LIKE 'PDF:%%'")
                pdfs_antes = int(c.fetchone()['total'] or 0)
            else:
                c.execute("SELECT COUNT(*) as total FROM rag_chunks WHERE source LIKE 'PDF:%'")
                pdfs_antes = c.fetchone()[0] if c.fetchone() else 0
            conn.close()
            logger.info(f"🔒 PDFs en BD antes de re-indexar Excel: {pdfs_antes} chunks")
    except:
        pdfs_antes = -1
    
    # 1. Re-indexar SOLO Excel de BD Grupo Laboral (NO borra PDFs)
    indexar_google_drive_rag()
    
    # 2. Verificar PDFs siguen intactos
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT COUNT(*) as total FROM rag_chunks WHERE source LIKE 'PDF:%%'")
                pdfs_despues = int(c.fetchone()['total'] or 0)
            else:
                c.execute("SELECT COUNT(*) as total FROM rag_chunks WHERE source LIKE 'PDF:%'")
                pdfs_despues = c.fetchone()[0] if c.fetchone() else 0
            conn.close()
            logger.info(f"🔒 PDFs en BD después de re-indexar: {pdfs_despues} chunks (antes: {pdfs_antes})")
            if pdfs_antes > 0 and pdfs_despues < pdfs_antes:
                logger.error(f"⚠️ ALERTA: Se perdieron chunks de PDFs! Antes={pdfs_antes}, Después={pdfs_despues}")
    except Exception as e:
        logger.warning(f"Error verificando PDFs: {e}")


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
                nombre_limpio = limpiar_nombre_display(nombre)
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
                            'Emprendimiento': '🚀', 'Evento': '📅', 'Saludo': '👋',
                  'Oferta Laboral': '💼', 'Búsqueda Empleo': '🔍', 'Recomendación Profesional': '⭐',
                  'Consulta Profesional': '❓', 'Servicios y Productos': '🛒', 'Capacitación': '📚',
                  'Información': '📰', 'Opinión': '💭', 'Conversación': '💬', 'Construcción': '🏗️',
                  'Finanzas': '💰', 'Tecnología': '💻', 'Inmobiliaria': '🏠', 'Seguridad': '🔒',
                  'Energía': '⚡', 'Sector Marítimo': '⚓', 'Otro': '📌'}
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

# ==================== 1. DIRECTORIO PROFESIONAL ====================

COFRADIA_LOGO_URL = "https://image2url.com/r2/default/images/1771472537472-e500a03d-8d53-4737-af3e-97bc77a7656a.png"
_logo_cache = {'img': None, 'loaded': False}


def descargar_logo_cofradia():
    """Descarga y cachea el logo de Cofradía (solo una vez)"""
    if _logo_cache['loaded']:
        return _logo_cache['img']
    
    try:
        resp = requests.get(COFRADIA_LOGO_URL, timeout=10)
        if resp.status_code == 200:
            logo = Image.open(BytesIO(resp.content)).convert('RGBA')
            _logo_cache['img'] = logo
            _logo_cache['loaded'] = True
            logger.info(f"✅ Logo Cofradía descargado: {logo.size}")
            return logo
        else:
            logger.warning(f"No se pudo descargar logo: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Error descargando logo Cofradía: {e}")
    
    _logo_cache['loaded'] = True  # No reintentar infinitamente
    return None


def generar_vcard(datos: dict) -> BytesIO:
    """Genera archivo vCard (.vcf) para guardar contacto en el teléfono"""
    nombre_completo = datos.get('nombre_completo', '')
    partes_nombre = nombre_completo.split(' ', 1)
    first = partes_nombre[0] if partes_nombre else ''
    last = partes_nombre[1] if len(partes_nombre) > 1 else ''
    
    telefono = datos.get('telefono', '').replace(' ', '')
    email = datos.get('email', '')
    empresa = datos.get('empresa', '')
    profesion = datos.get('profesion', '')
    ciudad = datos.get('ciudad', '')
    linkedin = datos.get('linkedin', '')
    
    vcf = "BEGIN:VCARD\n"
    vcf += "VERSION:3.0\n"
    vcf += f"N:{last};{first};;;\n"
    vcf += f"FN:{nombre_completo}\n"
    if profesion:
        vcf += f"TITLE:{profesion}\n"
    if empresa:
        vcf += f"ORG:{empresa}\n"
    if telefono:
        vcf += f"TEL;TYPE=CELL:{telefono}\n"
    if email:
        vcf += f"EMAIL:{email}\n"
    if ciudad:
        vcf += f"ADR;TYPE=WORK:;;{ciudad};;;;\n"
    if linkedin:
        url_li = linkedin if linkedin.startswith('http') else f"https://{linkedin}"
        vcf += f"URL:{url_li}\n"
    vcf += "NOTE:Contacto de Cofradía de Networking\n"
    vcf += "END:VCARD\n"
    
    buffer = BytesIO(vcf.encode('utf-8'))
    buffer.name = f"{re.sub(r'[^a-zA-Z0-9]', '_', nombre_completo)[:30]}.vcf"
    return buffer


def generar_qr_verificacion(url: str, size: int = 65):
    """Genera QR de verificación: fondo azul oscuro con puntos blancos"""
    if not qr_disponible:
        return None
    try:
        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_H, box_size=8, border=2)
        qr.add_data(url)
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="white", back_color="#0F2F59").convert('RGB')
        qr_img = qr_img.resize((size, size), Image.NEAREST)
        return qr_img
    except:
        return None


def obtener_stats_tarjeta(user_id_param: int) -> dict:
    """Obtiene antigüedad, recomendaciones y referidos para la tarjeta"""
    import unicodedata as _ud
    stats = {'antiguedad': '0,0', 'recomendaciones': 0, 'referidos': 0,
             'fecha_incorporacion': '', 'estado': 'activo', 'nombre_completo': '', 'generacion': ''}
    try:
        conn = get_db_connection()
        if not conn:
            return stats
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT fecha_incorporacion, fecha_registro, estado, first_name, last_name FROM suscripciones WHERE user_id = %s", (user_id_param,))
        else:
            c.execute("SELECT fecha_incorporacion, fecha_registro, estado, first_name, last_name FROM suscripciones WHERE user_id = ?", (user_id_param,))
        row = c.fetchone()
        if row:
            fecha_inc = (row['fecha_incorporacion'] if DATABASE_URL else row[0])
            fecha_reg = (row['fecha_registro'] if DATABASE_URL else row[1])
            estado = (row['estado'] if DATABASE_URL else row[2]) or 'activo'
            fn = (row['first_name'] if DATABASE_URL else row[3]) or ''
            ln = (row['last_name'] if DATABASE_URL else row[4]) or ''
            stats['estado'] = estado
            stats['nombre_completo'] = f"{fn} {ln}".strip()
            # --- Parseo robusto de fecha ---
            fecha_base = fecha_inc or fecha_reg
            if fecha_base:
                try:
                    fecha_dt = None
                    if hasattr(fecha_base, 'year'):
                        fecha_dt = datetime(fecha_base.year, fecha_base.month, fecha_base.day)
                    else:
                        fb = str(fecha_base).strip()
                        for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d',
                                    '%Y-%m-%dT%H:%M:%S', '%d-%m-%Y', '%d/%m/%Y']:
                            try:
                                fecha_dt = datetime.strptime(fb[:26], fmt)
                                break
                            except:
                                continue
                    if fecha_dt:
                        delta = datetime.now() - fecha_dt
                        total_meses = delta.days // 30
                        anios = total_meses // 12
                        meses = total_meses % 12
                        stats['antiguedad'] = f"{anios},{meses}"
                        stats['fecha_incorporacion'] = fecha_dt.strftime('%d-%m-%Y')
                except Exception as e:
                    logger.warning(f"Error parsing fecha stats: {e} val={fecha_base} type={type(fecha_base)}")
            else:
                logger.info(f"Stats user {user_id_param}: fecha_base is empty/None (inc={fecha_inc}, reg={fecha_reg})")
        # --- Generación (buscar en nuevos_miembros) ---
        try:
            if DATABASE_URL:
                c.execute("SELECT generacion FROM nuevos_miembros WHERE user_id = %s ORDER BY id DESC LIMIT 1", (user_id_param,))
            else:
                c.execute("SELECT generacion FROM nuevos_miembros WHERE user_id = ? ORDER BY id DESC LIMIT 1", (user_id_param,))
            gen_row = c.fetchone()
            if gen_row:
                stats['generacion'] = (gen_row['generacion'] if DATABASE_URL else gen_row[0]) or ''
            logger.info(f"Stats user {user_id_param}: gen={stats['generacion']}, ant={stats['antiguedad']}, fecha={stats['fecha_incorporacion']}")
        except Exception as e:
            logger.warning(f"Error obteniendo generacion user {user_id_param}: {e}")
        # --- Recomendaciones recibidas ---
        try:
            if DATABASE_URL:
                c.execute("SELECT COUNT(*) as t FROM recomendaciones WHERE destinatario_id = %s", (user_id_param,))
                stats['recomendaciones'] = c.fetchone()['t']
            else:
                c.execute("SELECT COUNT(*) FROM recomendaciones WHERE destinatario_id = ?", (user_id_param,))
                stats['recomendaciones'] = c.fetchone()[0]
        except:
            pass
        # --- Referidos: búsqueda insensible a acentos (todos los registros) ---
        try:
            nombre_full = stats['nombre_completo']
            if nombre_full and len(nombre_full) > 2:
                def _quitar_acentos(s):
                    return ''.join(ch for ch in _ud.normalize('NFD', s.lower()) if _ud.category(ch) != 'Mn')
                nf_clean = _quitar_acentos(nombre_full)
                partes_n = nombre_full.split()
                # Buscar en TODOS los registros (aprobados y pendientes) para contar referidos
                if DATABASE_URL:
                    c.execute("SELECT recomendado_por, user_id FROM nuevos_miembros WHERE recomendado_por IS NOT NULL AND recomendado_por != ''")
                else:
                    c.execute("SELECT recomendado_por, user_id FROM nuevos_miembros WHERE recomendado_por IS NOT NULL AND recomendado_por != ''")
                total_refs = 0
                for reg in c.fetchall():
                    rec = (reg['recomendado_por'] if DATABASE_URL else reg[0]) or ''
                    ref_uid = (reg['user_id'] if DATABASE_URL else reg[1])
                    if not rec or ref_uid == user_id_param:
                        continue
                    rec_clean = _quitar_acentos(rec)
                    # Match 1: nombre completo contenido
                    if nf_clean in rec_clean or rec_clean in nf_clean:
                        total_refs += 1
                    elif len(partes_n) >= 2:
                        # Match 2: nombre + apellido (ambos presentes)
                        n_clean = _quitar_acentos(partes_n[0])
                        a_clean = _quitar_acentos(partes_n[-1])
                        if len(n_clean) > 2 and len(a_clean) > 2:
                            if n_clean in rec_clean and a_clean in rec_clean:
                                total_refs += 1
                            # Match 3: solo apellido si es largo y único
                            elif a_clean == rec_clean or (len(a_clean) > 4 and a_clean in rec_clean):
                                total_refs += 1
                stats['referidos'] = total_refs
                logger.debug(f"Referidos user {user_id_param} ({nombre_full}): {total_refs} encontrados")
        except Exception as e:
            logger.warning(f"Error contando referidos: {e}")
        conn.close()
    except Exception as e:
        logger.warning(f"Error obtener_stats_tarjeta: {e}")
    return stats


def generar_qr_simple(url: str, size: int = 150):
    """Genera QR code limpio y escaneable — sin manipulación de píxeles"""
    if not qr_disponible:
        return None
    
    try:
        qr = qrcode.QRCode(
            version=2,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=10,
            border=3
        )
        qr.add_data(url)
        qr.make(fit=True)
        
        # Generar en blanco y negro puro (máxima compatibilidad con escáneres)
        qr_img = qr.make_image(fill_color="black", back_color="white").convert('RGB')
        
        # Redimensionar con NEAREST para mantener bordes nítidos (crucial para QR)
        resample = Image.NEAREST
        qr_img = qr_img.resize((size, size), resample)
        
        return qr_img
    except Exception as e:
        logger.warning(f"Error generando QR: {e}")
        return None


def generar_tarjeta_imagen(datos: dict) -> BytesIO:
    """Genera imagen PNG con QR verificación inferior + iconos dorados + NRO-GEN"""
    if not pil_disponible:
        return None
    W, H = 900, 620
    AZUL_OSCURO = (15, 47, 89)
    AZUL_MEDIO = (30, 80, 140)
    AZUL_CLARO = (52, 120, 195)
    BLANCO = (255, 255, 255)
    GRIS_TEXTO = (80, 80, 80)
    GRIS_SUTIL = (150, 155, 165)
    DORADO = (195, 165, 90)
    DORADO_OSCURO = (170, 140, 60)
    img = Image.new('RGB', (W, H), BLANCO)
    draw = ImageDraw.Draw(img)
    def cargar_fuente(size, bold=False):
        rutas = [f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
                 f"/usr/share/fonts/truetype/liberation/LiberationSans{'-Bold' if bold else '-Regular'}.ttf",
                 f"/usr/share/fonts/truetype/freefont/FreeSans{'Bold' if bold else ''}.ttf"]
        for r in rutas:
            try: return ImageFont.truetype(r, size)
            except: continue
        return ImageFont.load_default()
    font_nombre = cargar_fuente(32, bold=True)
    font_profesion = cargar_fuente(18, bold=True)
    font_campo = cargar_fuente(15)
    font_label = cargar_fuente(12, bold=True)
    font_miembro = cargar_fuente(13)
    font_cofradia_title = cargar_fuente(22, bold=True)
    font_stats_val = cargar_fuente(16, bold=True)
    font_stats_lbl = cargar_fuente(13)
    font_kdt = cargar_fuente(20, bold=True)
    # --- Franja superior azul ---
    draw.rectangle([0, 0, W, 130], fill=AZUL_OSCURO)
    draw.rectangle([0, 130, W, 134], fill=DORADO)
    # --- Logo ---
    logo = descargar_logo_cofradia()
    logo_end_x = 40
    if logo:
        try:
            logo_h = 90
            ratio = logo_h / logo.height
            logo_w = int(logo.width * ratio)
            resample = Image.LANCZOS if hasattr(Image, 'LANCZOS') else Image.BILINEAR
            logo_resized = logo.resize((logo_w, logo_h), resample)
            if logo_resized.mode == 'RGBA':
                img.paste(logo_resized, (30, 20), logo_resized)
            else:
                img.paste(logo_resized, (30, 20))
            logo_end_x = 30 + logo_w + 15
        except:
            logo_end_x = 40
    draw.text((logo_end_x, 30), "COFRADÍA DE NETWORKING", fill=DORADO, font=font_cofradia_title)
    draw.text((logo_end_x, 62), "Red Profesional de Ex-cadetes y Oficiales", fill=GRIS_SUTIL, font=font_campo)
    # --- NRO KDT - GENERACIÓN (esquina superior derecha, letras blancas sobre azul) ---
    user_id = datos.get('user_id', '')
    nro_kdt = datos.get('nro_kdt', '')
    stats = obtener_stats_tarjeta(int(user_id)) if user_id else {'antiguedad': '0,0', 'recomendaciones': 0, 'referidos': 0, 'generacion': ''}
    generacion = stats.get('generacion', '')
    if nro_kdt and generacion:
        kdt_texto = f"{nro_kdt}-{generacion}"
        try:
            bbox = draw.textbbox((0, 0), kdt_texto, font=font_kdt)
            tw = bbox[2] - bbox[0]
            draw.text((W - tw - 25, 50), kdt_texto, fill=BLANCO, font=font_kdt)
        except:
            draw.text((W - 180, 50), kdt_texto, fill=BLANCO, font=font_kdt)
    elif nro_kdt:
        draw.text((W - 100, 50), nro_kdt, fill=BLANCO, font=font_kdt)
    elif generacion:
        draw.text((W - 100, 50), generacion, fill=BLANCO, font=font_kdt)
    # --- Nombre ---
    nombre = datos.get('nombre_completo', 'Sin nombre')
    y_nombre = 155
    draw.text((40, y_nombre), nombre, fill=AZUL_MEDIO, font=font_nombre)
    profesion = datos.get('profesion', '')
    if profesion:
        draw.text((40, y_nombre + 42), profesion.upper(), fill=AZUL_CLARO, font=font_profesion)
    y_sep = y_nombre + 72
    draw.line([40, y_sep, 560, y_sep], fill=(220, 225, 235), width=1)
    # --- Campos ---
    y_info = y_sep + 15
    campos = [('empresa', datos.get('empresa', '')), ('servicios', datos.get('servicios', '')),
              ('ciudad', datos.get('ciudad', '')), ('telefono', datos.get('telefono', '')),
              ('email', datos.get('email', '')), ('linkedin', datos.get('linkedin', ''))]
    labels_d = {'empresa': 'Empresa', 'servicios': 'Servicios', 'ciudad': 'Ciudad',
                'telefono': 'Teléfono', 'email': 'Email', 'linkedin': 'LinkedIn'}
    for label, valor in campos:
        if valor:
            draw.text((42, y_info), f"{labels_d[label]}:", fill=GRIS_SUTIL, font=font_label)
            draw.text((130, y_info), valor[:50], fill=GRIS_TEXTO, font=font_campo)
            y_info += 24
    # --- QR principal (tarjeta compartible, lado derecho) ---
    qr_x, qr_y = 640, 155
    qr_size = 150
    qr_url = f"https://t.me/Cofradia_Premium_Bot?start=tarjeta_{user_id}" if user_id else "https://t.me/Cofradia_Premium_Bot"
    qr_img = generar_qr_simple(qr_url, size=qr_size)
    if qr_img:
        try:
            img.paste(qr_img, (qr_x, qr_y))
            draw.rectangle([qr_x - 4, qr_y - 4, qr_x + qr_size + 4, qr_y + qr_size + 4], outline=AZUL_CLARO, width=2)
        except:
            pass
    badge_y = qr_y + qr_size + 12
    if logo:
        try:
            mini_h = 28
            ratio = mini_h / logo.height
            mini_w = int(logo.width * ratio)
            resample = Image.LANCZOS if hasattr(Image, 'LANCZOS') else Image.BILINEAR
            mini_logo = logo.resize((mini_w, mini_h), resample)
            if mini_logo.mode == 'RGBA':
                img.paste(mini_logo, (qr_x + 10, badge_y), mini_logo)
            else:
                img.paste(mini_logo, (qr_x + 10, badge_y))
            draw.text((qr_x + 10 + mini_w + 8, badge_y + 7), "Miembro Cofradía", fill=GRIS_SUTIL, font=font_miembro)
        except:
            draw.text((qr_x + 20, badge_y + 5), "Miembro Cofradía", fill=GRIS_SUTIL, font=font_miembro)
    else:
        draw.text((qr_x + 20, badge_y + 5), "Miembro Cofradía", fill=GRIS_SUTIL, font=font_miembro)
    # === BARRA DE ESTADÍSTICAS — 3 iconos dorados (SIN línea horizontal) ===
    bar_y = H - 100
    # ICONO 1: Reloj analógico dorado (antigüedad)
    cx1, cy1 = 75, bar_y + 8
    r = 16
    draw.ellipse([cx1 - r, cy1 - r, cx1 + r, cy1 + r], outline=DORADO, width=2)
    draw.line([cx1, cy1, cx1, cy1 - 10], fill=DORADO, width=2)
    draw.line([cx1, cy1, cx1 + 7, cy1 + 3], fill=DORADO, width=2)
    draw.ellipse([cx1 - 2, cy1 - 2, cx1 + 2, cy1 + 2], fill=DORADO)
    for ang in [0, 90, 180, 270]:
        rad_a = math.radians(ang)
        mx = cx1 + int((r - 3) * math.sin(rad_a))
        my = cy1 - int((r - 3) * math.cos(rad_a))
        draw.ellipse([mx - 1, my - 1, mx + 1, my + 1], fill=DORADO)
    draw.text((cx1 + r + 8, cy1 - 16), stats['antiguedad'], fill=GRIS_TEXTO, font=font_stats_val)
    draw.text((cx1 + r + 8, cy1 + 4), "años", fill=GRIS_SUTIL, font=font_stats_lbl)
    # ICONO 2: Estrella 5 puntas dorada (recomendaciones)
    cx2, cy2 = 280, bar_y + 8
    pts = []
    for i in range(10):
        ang = math.pi / 2 + i * math.pi / 5
        rad_s = 16 if i % 2 == 0 else 7
        pts.append((cx2 + rad_s * math.cos(ang), cy2 - rad_s * math.sin(ang)))
    draw.polygon(pts, fill=DORADO, outline=DORADO_OSCURO)
    draw.text((cx2 + 24, cy2 - 16), str(stats['recomendaciones']), fill=GRIS_TEXTO, font=font_stats_val)
    draw.text((cx2 + 24, cy2 + 4), "recomendaciones", fill=GRIS_SUTIL, font=font_stats_lbl)
    # ICONO 3: Trofeo dorado (copa con asas y base)
    cx3, cy3 = 510, bar_y + 6
    # Copa (semicírculo superior relleno)
    draw.pieslice([cx3 - 14, cy3 - 16, cx3 + 14, cy3 + 4], start=0, end=180, fill=DORADO, outline=DORADO_OSCURO)
    # Borde superior copa
    draw.rectangle([cx3 - 16, cy3 - 16, cx3 + 16, cy3 - 12], fill=DORADO, outline=DORADO_OSCURO)
    # Asas laterales
    draw.arc([cx3 - 24, cy3 - 12, cx3 - 10, cy3 + 2], start=90, end=270, fill=DORADO, width=2)
    draw.arc([cx3 + 10, cy3 - 12, cx3 + 24, cy3 + 2], start=270, end=90, fill=DORADO, width=2)
    # Tallo
    draw.rectangle([cx3 - 3, cy3 + 4, cx3 + 3, cy3 + 14], fill=DORADO)
    # Base
    draw.rectangle([cx3 - 10, cy3 + 14, cx3 + 10, cy3 + 18], fill=DORADO, outline=DORADO_OSCURO)
    draw.text((cx3 + 28, cy3 - 14), str(stats['referidos']), fill=GRIS_TEXTO, font=font_stats_val)
    draw.text((cx3 + 28, cy3 + 6), "referidos", fill=GRIS_SUTIL, font=font_stats_lbl)
    # --- QR VERIFICACIÓN (extremo inferior derecho, discreto azul+blanco) ---
    if user_id:
        verif_url = f"https://t.me/Cofradia_Premium_Bot?start=verificar_{user_id}"
        qr_verif = generar_qr_verificacion(verif_url, size=65)
        if qr_verif:
            try:
                img.paste(qr_verif, (W - 82, H - 110))
            except:
                pass
    # --- Franja inferior ---
    draw.rectangle([0, H - 35, W, H], fill=AZUL_OSCURO)
    draw.text((40, H - 27), "cofradía de networking", fill=GRIS_SUTIL, font=font_miembro)
    draw.text((W - 280, H - 27), "Conectando profesionales", fill=GRIS_SUTIL, font=font_miembro)
    draw.rectangle([0, 0, W - 1, H - 1], outline=(200, 205, 215), width=1)
    buffer = BytesIO()
    img.save(buffer, format='PNG', quality=95)
    buffer.seek(0)
    return buffer


async def mostrar_tarjeta_publica(update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
    """Muestra la tarjeta profesional de un usuario (activada por deep link QR)"""
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión. Intenta nuevamente.")
            return
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (target_user_id,))
        else:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (target_user_id,))
        tarjeta = c.fetchone()
        conn.close()
        
        if not tarjeta:
            await update.message.reply_text(
                "📇 Este cofrade aún no ha creado su tarjeta profesional.\n\n"
                "¿Eres miembro de Cofradía? Crea tu tarjeta con /mi_tarjeta"
            )
            return
        
        nombre = tarjeta['nombre_completo'] if DATABASE_URL else tarjeta[1]
        profesion = tarjeta['profesion'] if DATABASE_URL else tarjeta[2]
        empresa = tarjeta['empresa'] if DATABASE_URL else tarjeta[3]
        servicios = tarjeta['servicios'] if DATABASE_URL else tarjeta[4]
        telefono = tarjeta['telefono'] if DATABASE_URL else tarjeta[5]
        email = tarjeta['email'] if DATABASE_URL else tarjeta[6]
        ciudad = tarjeta['ciudad'] if DATABASE_URL else tarjeta[7]
        linkedin = tarjeta['linkedin'] if DATABASE_URL else tarjeta[8]
        
        # Obtener username del dueño de la tarjeta
        username_target = ''
        try:
            conn2 = get_db_connection()
            if conn2:
                c2 = conn2.cursor()
                if DATABASE_URL:
                    c2.execute("SELECT username FROM suscripciones WHERE user_id = %s", (target_user_id,))
                else:
                    c2.execute("SELECT username FROM suscripciones WHERE user_id = ?", (target_user_id,))
                row = c2.fetchone()
                if row:
                    username_target = row['username'] if DATABASE_URL else row[0]
                conn2.close()
        except:
            pass
        
        # Extraer nro_kdt
        _nro_kdt = ''
        try:
            if DATABASE_URL:
                _nro_kdt = tarjeta.get('nro_kdt', '') or ''
            else:
                _nro_kdt = tarjeta[11] if len(tarjeta) > 11 else ''
        except:
            pass
        datos_tarjeta = {
            'nombre_completo': nombre, 'profesion': profesion,
            'empresa': empresa, 'servicios': servicios,
            'telefono': telefono, 'email': email,
            'ciudad': ciudad, 'linkedin': linkedin,
            'username': username_target or '',
            'user_id': target_user_id,
            'nro_kdt': _nro_kdt
        }
        
        img_buffer = None
        try:
            img_buffer = generar_tarjeta_imagen(datos_tarjeta)
        except Exception as e:
            logger.warning(f"Error generando tarjeta pública: {e}")
        
        if img_buffer:
            # Caption con links clicables (HTML)
            caption = f"📇 <b>{nombre}</b>\n"
            if profesion: caption += f"💼 {profesion}\n"
            if empresa: caption += f"🏢 {empresa}\n"
            if ciudad: caption += f"📍 {ciudad}\n"
            if telefono: caption += f"📱 <a href=\"tel:{telefono.replace(' ', '')}\">{telefono}</a>\n"
            if email: caption += f"📧 <a href=\"mailto:{email}\">{email}</a>\n"
            if linkedin:
                url_li = linkedin if linkedin.startswith('http') else f"https://{linkedin}"
                caption += f"🔗 <a href=\"{url_li}\">LinkedIn</a>\n"
            caption += "\n🔗 Cofradía de Networking"
            
            await update.message.reply_photo(photo=img_buffer, caption=caption, parse_mode='HTML')
            
            # Enviar como archivo descargable
            try:
                img_buffer.seek(0)
                nombre_archivo = re.sub(r'[^a-zA-Z0-9]', '_', nombre)[:30]
                await update.message.reply_document(
                    document=img_buffer,
                    filename=f"Tarjeta_{nombre_archivo}.png",
                    caption="📥 Imagen exportable — guárdala o compártela"
                )
            except Exception as e:
                logger.debug(f"Error enviando documento tarjeta pública: {e}")
            
            # Enviar vCard para guardar en contactos
            try:
                vcf_buffer = generar_vcard(datos_tarjeta)
                await update.message.reply_document(
                    document=vcf_buffer,
                    filename=vcf_buffer.name,
                    caption="📱 Guardar en contactos — toca el archivo para agregar a tu agenda"
                )
            except Exception as e:
                logger.debug(f"Error enviando vCard: {e}")
        else:
            # Fallback texto
            msg = f"📇 TARJETA PROFESIONAL\n{'━' * 28}\n\n"
            msg += f"👤 {nombre}\n"
            if profesion: msg += f"💼 {profesion}\n"
            if empresa: msg += f"🏢 {empresa}\n"
            if servicios: msg += f"🛠️ {servicios}\n"
            if ciudad: msg += f"📍 {ciudad}\n"
            if telefono: msg += f"📱 {telefono}\n"
            if email: msg += f"📧 {email}\n"
            if linkedin: msg += f"🔗 {linkedin}\n"
            msg += f"\n🔗 Cofradía de Networking"
            await update.message.reply_text(msg)
    except Exception as e:
        logger.error(f"Error mostrando tarjeta pública: {e}")
        await update.message.reply_text("❌ Error al mostrar la tarjeta. Intenta nuevamente.")


@requiere_suscripcion
async def mi_tarjeta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mi_tarjeta - Crear/ver tarjeta profesional"""
    user = update.effective_user
    user_id = user.id
    
    if not context.args:
        # Mostrar tarjeta actual
        try:
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
                else:
                    c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
                tarjeta = c.fetchone()
                conn.close()
                
                if tarjeta:
                    t = tarjeta if not DATABASE_URL else tarjeta
                    nombre = t['nombre_completo'] if DATABASE_URL else t[1]
                    profesion = t['profesion'] if DATABASE_URL else t[2]
                    empresa = t['empresa'] if DATABASE_URL else t[3]
                    servicios = t['servicios'] if DATABASE_URL else t[4]
                    telefono = t['telefono'] if DATABASE_URL else t[5]
                    email = t['email'] if DATABASE_URL else t[6]
                    ciudad = t['ciudad'] if DATABASE_URL else t[7]
                    linkedin = t['linkedin'] if DATABASE_URL else t[8]
                    
                    # Enviar tarjeta como IMAGEN profesional
                    # Extraer nro_kdt
                    _nro_kdt2 = ''
                    try:
                        if DATABASE_URL:
                            _nro_kdt2 = t.get('nro_kdt', '') or ''
                        else:
                            _nro_kdt2 = t[11] if len(t) > 11 else ''
                    except:
                        pass
                    datos_tarjeta = {
                        'nombre_completo': nombre or f"{user.first_name or ''} {user.last_name or ''}".strip(),
                        'profesion': profesion, 'empresa': empresa,
                        'servicios': servicios, 'telefono': telefono,
                        'email': email, 'ciudad': ciudad, 'linkedin': linkedin,
                        'username': user.username or '',
                        'user_id': user_id,
                        'nro_kdt': _nro_kdt2
                    }
                    
                    img_buffer = None
                    try:
                        img_buffer = generar_tarjeta_imagen(datos_tarjeta)
                    except Exception as e:
                        logger.warning(f"Error generando tarjeta imagen: {e}")
                    
                    if img_buffer:
                        # Construir caption con links clicables (HTML)
                        caption = f"📇 <b>Tarjeta de {nombre}</b>\n\n"
                        if profesion: caption += f"💼 {profesion}\n"
                        if empresa: caption += f"🏢 {empresa}\n"
                        if ciudad: caption += f"📍 {ciudad}\n"
                        if telefono: caption += f"📱 <a href=\"tel:{telefono.replace(' ', '')}\">{telefono}</a>\n"
                        if email: caption += f"📧 <a href=\"mailto:{email}\">{email}</a>\n"
                        if linkedin:
                            url_li = linkedin if linkedin.startswith('http') else f"https://{linkedin}"
                            caption += f"🔗 <a href=\"{url_li}\">LinkedIn</a>\n"
                        caption += "\n✏️ Editar: /mi_tarjeta [campo] [valor]"
                        
                        await update.message.reply_photo(
                            photo=img_buffer,
                            caption=caption,
                            parse_mode='HTML'
                        )
                        
                        # Enviar también como archivo descargable (exportar imagen)
                        try:
                            img_buffer.seek(0)
                            nombre_archivo = re.sub(r'[^a-zA-Z0-9]', '_', nombre)[:30]
                            await update.message.reply_document(
                                document=img_buffer,
                                filename=f"Tarjeta_{nombre_archivo}.png",
                                caption="📥 Imagen exportable — guárdala o compártela por cualquier medio"
                            )
                        except Exception as e:
                            logger.debug(f"Error enviando documento tarjeta: {e}")
                        
                        # Enviar vCard para guardar en contactos
                        try:
                            vcf_buffer = generar_vcard(datos_tarjeta)
                            await update.message.reply_document(
                                document=vcf_buffer,
                                filename=vcf_buffer.name,
                                caption="📱 Guardar en contactos — toca el archivo para agregar a tu agenda"
                            )
                        except Exception as e:
                            logger.debug(f"Error enviando vCard: {e}")
                    else:
                        # Fallback: enviar como texto si no hay PIL
                        msg = f"📇 TU TARJETA PROFESIONAL\n{'━' * 28}\n\n"
                        msg += f"👤 {nombre}\n"
                        if profesion: msg += f"💼 {profesion}\n"
                        if empresa: msg += f"🏢 {empresa}\n"
                        if servicios: msg += f"🛠️ {servicios}\n"
                        if ciudad: msg += f"📍 {ciudad}\n"
                        if telefono: msg += f"📱 {telefono}\n"
                        if email: msg += f"📧 {email}\n"
                        if linkedin: msg += f"🔗 {linkedin}\n"
                        msg += f"\n💡 Para editar: /mi_tarjeta [campo] [valor]\n"
                        msg += f"Campos: profesion, empresa, servicios, telefono, email, ciudad, linkedin"
                        await update.message.reply_text(msg)
                else:
                    await update.message.reply_text(
                        "📇 Aún no tienes tarjeta profesional.\n\n"
                        "Créala paso a paso:\n"
                        "/mi_tarjeta profesion Ingeniero Civil Industrial\n"
                        "/mi_tarjeta empresa ACME S.A.\n"
                        "/mi_tarjeta servicios Consultoría en logística\n"
                        "/mi_tarjeta telefono +56912345678\n"
                        "/mi_tarjeta email tu@correo.com\n"
                        "/mi_tarjeta ciudad Santiago\n"
                        "/mi_tarjeta linkedin linkedin.com/in/tuperfil\n"
                        "/mi_tarjeta nro_kdt 322\n\n"
                        "💡 Cada campo es opcional."
                    )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)[:100]}")
        return
    
    # Editar campo
    campo = context.args[0].lower()
    campos_validos = ['profesion', 'empresa', 'servicios', 'telefono', 'email', 'ciudad', 'linkedin', 'nro_kdt']
    
    if campo not in campos_validos:
        await update.message.reply_text(
            "❌ Campo no válido.\n\n"
            "Campos: profesion, empresa, servicios, telefono,\n"
            "email, ciudad, linkedin, nro_kdt\n\n"
            "Ejemplo: /mi_tarjeta nro_kdt 322"
        )
        return
    
    valor = ' '.join(context.args[1:])
    if not valor:
        await update.message.reply_text(f"❌ Uso: /mi_tarjeta {campo} [valor]")
        return
    
    # nro_kdt: validar y rellenar a 3 dígitos
    if campo == 'nro_kdt':
        digitos = ''.join(ch for ch in valor if ch.isdigit())
        if not digitos or int(digitos) < 1 or int(digitos) > 999:
            await update.message.reply_text(
                "❌ Número de cadete: entre 1 y 999.\n\n"
                "Ejemplo: /mi_tarjeta nro_kdt 322\n"
                "Si tu número es 7, se guardará como 007"
            )
            return
        valor = digitos.zfill(3)
    
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            nombre_completo = f"{user.first_name or ''} {user.last_name or ''}".strip()
            
            if DATABASE_URL:
                c.execute("""INSERT INTO tarjetas_profesional (user_id, nombre_completo, """ + campo + """)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (user_id) DO UPDATE SET """ + campo + """ = %s, 
                            nombre_completo = %s, fecha_actualizacion = CURRENT_TIMESTAMP""",
                         (user_id, nombre_completo, valor, valor, nombre_completo))
            else:
                c.execute("SELECT user_id FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
                if c.fetchone():
                    c.execute(f"UPDATE tarjetas_profesional SET {campo} = ?, nombre_completo = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE user_id = ?",
                             (valor, nombre_completo, user_id))
                else:
                    c.execute(f"INSERT INTO tarjetas_profesional (user_id, nombre_completo, {campo}) VALUES (?, ?, ?)",
                             (user_id, nombre_completo, valor))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"✅ {campo.capitalize()} actualizado: {valor}\n\nVer tarjeta: /mi_tarjeta")
            otorgar_coins(user_id, 15, 'Actualizar tarjeta profesional')
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


@requiere_suscripcion
async def directorio_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /directorio [búsqueda] - Buscar en directorio profesional"""
    busqueda = ' '.join(context.args).lower() if context.args else ''
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión")
            return
        c = conn.cursor()
        
        if busqueda:
            if DATABASE_URL:
                c.execute("""SELECT nombre_completo, profesion, empresa, servicios, ciudad, telefono, email 
                            FROM tarjetas_profesional 
                            WHERE LOWER(profesion) LIKE %s OR LOWER(empresa) LIKE %s 
                            OR LOWER(servicios) LIKE %s OR LOWER(nombre_completo) LIKE %s
                            OR LOWER(ciudad) LIKE %s
                            ORDER BY nombre_completo LIMIT 15""",
                         tuple(f"%{busqueda}%" for _ in range(5)))
            else:
                c.execute("""SELECT nombre_completo, profesion, empresa, servicios, ciudad, telefono, email 
                            FROM tarjetas_profesional 
                            WHERE LOWER(profesion) LIKE ? OR LOWER(empresa) LIKE ? 
                            OR LOWER(servicios) LIKE ? OR LOWER(nombre_completo) LIKE ?
                            OR LOWER(ciudad) LIKE ?
                            ORDER BY nombre_completo LIMIT 15""",
                         tuple(f"%{busqueda}%" for _ in range(5)))
        else:
            if DATABASE_URL:
                c.execute("SELECT nombre_completo, profesion, empresa, servicios, ciudad, telefono, email FROM tarjetas_profesional ORDER BY nombre_completo LIMIT 20")
            else:
                c.execute("SELECT nombre_completo, profesion, empresa, servicios, ciudad, telefono, email FROM tarjetas_profesional ORDER BY nombre_completo LIMIT 20")
        
        resultados = c.fetchall()
        conn.close()
        
        if not resultados:
            await update.message.reply_text(f"📇 No se encontraron tarjetas{f' para: {busqueda}' if busqueda else ''}.\n\n💡 Crea tu tarjeta: /mi_tarjeta")
            return
        
        msg = f"📇 DIRECTORIO PROFESIONAL{f' — {busqueda}' if busqueda else ''}\n{'━' * 28}\n\n"
        for r in resultados:
            nombre = r['nombre_completo'] if DATABASE_URL else r[0]
            prof = r['profesion'] if DATABASE_URL else r[1]
            emp = r['empresa'] if DATABASE_URL else r[2]
            serv = r['servicios'] if DATABASE_URL else r[3]
            ciudad = r['ciudad'] if DATABASE_URL else r[4]
            msg += f"👤 {nombre}\n"
            if prof: msg += f"   💼 {prof}\n"
            if emp: msg += f"   🏢 {emp}\n"
            if serv: msg += f"   🛠️ {serv[:60]}\n"
            if ciudad: msg += f"   📍 {ciudad}\n"
            msg += "\n"
        
        msg += f"📊 {len(resultados)} resultado(s)\n💡 /mi_tarjeta para crear/editar tu tarjeta"
        await enviar_mensaje_largo(update, msg)
        registrar_servicio_usado(update.effective_user.id, 'directorio')
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


# ==================== 2. ALERTAS PERSONALIZADAS ====================

@requiere_suscripcion
async def alertas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /alertas [palabras] - Configurar alertas de palabras clave"""
    user_id = update.effective_user.id
    
    if not context.args:
        # Mostrar alertas actuales
        try:
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("SELECT id, palabras_clave, activa FROM alertas_usuario WHERE user_id = %s ORDER BY id", (user_id,))
                else:
                    c.execute("SELECT id, palabras_clave, activa FROM alertas_usuario WHERE user_id = ? ORDER BY id", (user_id,))
                alertas = c.fetchall()
                conn.close()
                
                if alertas:
                    msg = "🔔 TUS ALERTAS ACTIVAS\n\n"
                    for a in alertas:
                        aid = a['id'] if DATABASE_URL else a[0]
                        palabras = a['palabras_clave'] if DATABASE_URL else a[1]
                        activa = a['activa'] if DATABASE_URL else a[2]
                        estado = "✅" if activa else "⏸️"
                        msg += f"{estado} #{aid}: {palabras}\n"
                    msg += "\n💡 Agregar: /alertas empleo gerente logística\n"
                    msg += "💡 Eliminar: /alertas eliminar [#ID]"
                else:
                    msg = "🔔 No tienes alertas configuradas.\n\n"
                    msg += "Cuando alguien publique en el grupo un mensaje con tus palabras clave, te avisaré en privado.\n\n"
                    msg += "💡 Crear alerta: /alertas empleo gerente logística\n"
                    msg += "Puedes poner varias palabras separadas por espacio."
                await update.message.reply_text(msg)
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)[:100]}")
        return
    
    # Eliminar alerta
    if context.args[0].lower() == 'eliminar' and len(context.args) > 1:
        try:
            alerta_id = int(context.args[1].replace('#', ''))
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("DELETE FROM alertas_usuario WHERE id = %s AND user_id = %s", (alerta_id, user_id))
                else:
                    c.execute("DELETE FROM alertas_usuario WHERE id = ? AND user_id = ?", (alerta_id, user_id))
                conn.commit()
                conn.close()
                await update.message.reply_text(f"✅ Alerta #{alerta_id} eliminada.")
        except:
            await update.message.reply_text("❌ Uso: /alertas eliminar [#ID]")
        return
    
    # Crear nueva alerta
    palabras = ' '.join(context.args).lower()
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT COUNT(*) as total FROM alertas_usuario WHERE user_id = %s", (user_id,))
                count = c.fetchone()['total']
            else:
                c.execute("SELECT COUNT(*) FROM alertas_usuario WHERE user_id = ?", (user_id,))
                count = c.fetchone()[0]
            
            if count >= 10:
                conn.close()
                await update.message.reply_text("❌ Máximo 10 alertas por usuario. Elimina alguna primero.")
                return
            
            if DATABASE_URL:
                c.execute("INSERT INTO alertas_usuario (user_id, palabras_clave) VALUES (%s, %s)", (user_id, palabras))
            else:
                c.execute("INSERT INTO alertas_usuario (user_id, palabras_clave) VALUES (?, ?)", (user_id, palabras))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"✅ Alerta creada: \"{palabras}\"\n\nTe avisaré cuando se mencionen estas palabras en el grupo.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def verificar_alertas_mensaje(user_id_autor, texto_mensaje, nombre_autor, context):
    """Verifica si un mensaje del grupo coincide con alertas de usuarios"""
    if not texto_mensaje or len(texto_mensaje) < 5:
        return
    texto_lower = texto_mensaje.lower()
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT user_id, palabras_clave FROM alertas_usuario WHERE activa = TRUE")
        else:
            c.execute("SELECT user_id, palabras_clave FROM alertas_usuario WHERE activa = 1")
        alertas = c.fetchall()
        conn.close()
        
        for alerta in alertas:
            alert_user_id = alerta['user_id'] if DATABASE_URL else alerta[0]
            palabras = alerta['palabras_clave'] if DATABASE_URL else alerta[1]
            
            if alert_user_id == user_id_autor:
                continue
            
            palabras_lista = palabras.lower().split()
            if any(p in texto_lower for p in palabras_lista):
                palabras_encontradas = [p for p in palabras_lista if p in texto_lower]
                try:
                    await context.bot.send_message(
                        chat_id=alert_user_id,
                        text=f"🔔 ALERTA: Se mencionó \"{', '.join(palabras_encontradas)}\" en el grupo\n\n"
                             f"👤 {nombre_autor} escribió:\n"
                             f"📝 {texto_mensaje[:300]}{'...' if len(texto_mensaje) > 300 else ''}\n\n"
                             f"💡 /alertas para gestionar tus alertas"
                    )
                except:
                    pass
    except Exception as e:
        logger.debug(f"Error verificando alertas: {e}")


# ==================== 3. TABLÓN DE ANUNCIOS ====================

@requiere_suscripcion
async def publicar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /publicar [categoría] [título] | [descripción] - Publicar anuncio"""
    user = update.effective_user
    
    if not context.args:
        await update.message.reply_text(
            "📢 PUBLICAR ANUNCIO\n\n"
            "Formato:\n"
            "/publicar [categoría] [título] | [descripción]\n\n"
            "Categorías: servicio, empleo, necesidad, venta, otro\n\n"
            "Ejemplo:\n"
            "/publicar servicio Asesoría Legal | Ofrezco servicios de asesoría legal para empresas. Contacto: 912345678"
        )
        return
    
    texto = ' '.join(context.args)
    categorias_validas = ['servicio', 'empleo', 'necesidad', 'venta', 'otro']
    
    primera_palabra = context.args[0].lower()
    if primera_palabra in categorias_validas:
        categoria = primera_palabra
        texto = ' '.join(context.args[1:])
    else:
        categoria = 'otro'
    
    if '|' in texto:
        partes = texto.split('|', 1)
        titulo = partes[0].strip()
        descripcion = partes[1].strip()
    else:
        titulo = texto[:80]
        descripcion = texto
    
    if len(titulo) < 5:
        await update.message.reply_text("❌ El título debe tener al menos 5 caracteres.")
        return
    
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            nombre = f"{user.first_name or ''} {user.last_name or ''}".strip()
            fecha_exp = datetime.now() + timedelta(days=30)
            
            if DATABASE_URL:
                c.execute("""INSERT INTO anuncios (user_id, nombre_autor, categoria, titulo, descripcion, contacto, fecha_expiracion)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                         (user.id, nombre, categoria, titulo, descripcion, f"@{user.username}" if user.username else nombre, fecha_exp))
            else:
                c.execute("""INSERT INTO anuncios (user_id, nombre_autor, categoria, titulo, descripcion, contacto, fecha_expiracion)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                         (user.id, nombre, categoria, titulo, descripcion, f"@{user.username}" if user.username else nombre, fecha_exp.strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
            conn.close()
            
            await update.message.reply_text(
                f"✅ Anuncio publicado!\n\n"
                f"📌 {titulo}\n"
                f"📂 Categoría: {categoria}\n"
                f"⏰ Vigencia: 30 días\n\n"
                f"Los cofrades pueden verlo con /anuncios"
            )
            registrar_servicio_usado(user.id, 'publicar')
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


@requiere_suscripcion
async def anuncios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /anuncios [categoría] - Ver tablón de anuncios"""
    categoria = context.args[0].lower() if context.args else None
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión")
            return
        c = conn.cursor()
        
        if DATABASE_URL:
            if categoria:
                c.execute("""SELECT titulo, descripcion, nombre_autor, contacto, categoria, fecha_publicacion 
                            FROM anuncios WHERE activo = TRUE AND fecha_expiracion > CURRENT_TIMESTAMP AND categoria = %s
                            ORDER BY fecha_publicacion DESC LIMIT 15""", (categoria,))
            else:
                c.execute("""SELECT titulo, descripcion, nombre_autor, contacto, categoria, fecha_publicacion 
                            FROM anuncios WHERE activo = TRUE AND fecha_expiracion > CURRENT_TIMESTAMP
                            ORDER BY fecha_publicacion DESC LIMIT 15""")
        else:
            if categoria:
                c.execute("""SELECT titulo, descripcion, nombre_autor, contacto, categoria, fecha_publicacion 
                            FROM anuncios WHERE activo = 1 AND categoria = ?
                            ORDER BY fecha_publicacion DESC LIMIT 15""", (categoria,))
            else:
                c.execute("""SELECT titulo, descripcion, nombre_autor, contacto, categoria, fecha_publicacion 
                            FROM anuncios WHERE activo = 1
                            ORDER BY fecha_publicacion DESC LIMIT 15""")
        
        anuncios = c.fetchall()
        conn.close()
        
        if not anuncios:
            await update.message.reply_text(f"📢 No hay anuncios activos{f' en {categoria}' if categoria else ''}.\n\n💡 Publica uno: /publicar")
            return
        
        msg = f"📢 TABLÓN DE ANUNCIOS{f' — {categoria}' if categoria else ''}\n{'━' * 28}\n\n"
        for a in anuncios:
            titulo = a['titulo'] if DATABASE_URL else a[0]
            desc = a['descripcion'] if DATABASE_URL else a[1]
            autor = a['nombre_autor'] if DATABASE_URL else a[2]
            contacto = a['contacto'] if DATABASE_URL else a[3]
            cat = a['categoria'] if DATABASE_URL else a[4]
            msg += f"📌 {titulo}\n"
            msg += f"   📂 {cat} | 👤 {autor}\n"
            if desc and desc != titulo: msg += f"   📝 {desc[:100]}\n"
            msg += f"   📱 {contacto}\n\n"
        
        msg += "💡 Filtrar: /anuncios servicio\n💡 Publicar: /publicar"
        await enviar_mensaje_largo(update, msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


# ==================== 4. CONEXIONES INTELIGENTES ====================

@requiere_suscripcion
async def conectar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /conectar - Sugerir conexiones profesionales"""
    user_id = update.effective_user.id
    msg = await update.message.reply_text("🔗 Analizando tu perfil y buscando conexiones...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error de conexión")
            return
        c = conn.cursor()
        
        # Obtener perfil del usuario
        if DATABASE_URL:
            c.execute("SELECT profesion, empresa, servicios, ciudad FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT profesion, empresa, servicios, ciudad FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
        mi_tarjeta = c.fetchone()
        
        if not mi_tarjeta:
            conn.close()
            await msg.edit_text("📇 Primero crea tu tarjeta profesional con /mi_tarjeta para que pueda sugerirte conexiones.\n\n"
                               "Ejemplo: /mi_tarjeta profesion Ingeniero Civil")
            return
        
        mi_prof = (mi_tarjeta['profesion'] if DATABASE_URL else mi_tarjeta[0]) or ''
        mi_emp = (mi_tarjeta['empresa'] if DATABASE_URL else mi_tarjeta[1]) or ''
        mi_serv = (mi_tarjeta['servicios'] if DATABASE_URL else mi_tarjeta[2]) or ''
        mi_ciudad = (mi_tarjeta['ciudad'] if DATABASE_URL else mi_tarjeta[3]) or ''
        
        # Buscar conexiones potenciales con IA
        if DATABASE_URL:
            c.execute("""SELECT user_id, nombre_completo, profesion, empresa, servicios, ciudad 
                        FROM tarjetas_profesional WHERE user_id != %s LIMIT 50""", (user_id,))
        else:
            c.execute("""SELECT user_id, nombre_completo, profesion, empresa, servicios, ciudad 
                        FROM tarjetas_profesional WHERE user_id != ? LIMIT 50""", (user_id,))
        otros = c.fetchall()
        conn.close()
        
        if not otros:
            await msg.edit_text("📇 Aún no hay suficientes tarjetas en el directorio.\n\nInvita a tus compañeros a crear su tarjeta con /mi_tarjeta")
            return
        
        # Construir prompt para IA
        otros_texto = "\n".join([
            f"- {o['nombre_completo'] if DATABASE_URL else o[1]}: {o['profesion'] if DATABASE_URL else o[2] or '?'} en {o['empresa'] if DATABASE_URL else o[3] or '?'}, {o['servicios'] if DATABASE_URL else o[4] or ''}, {o['ciudad'] if DATABASE_URL else o[5] or ''}"
            for o in otros[:30]
        ])
        
        prompt = f"""Analiza el perfil profesional y sugiere las 5 mejores conexiones de networking.

MI PERFIL:
- Profesión: {mi_prof}
- Empresa: {mi_emp}
- Servicios: {mi_serv}
- Ciudad: {mi_ciudad}

OTROS PROFESIONALES EN COFRADÍA:
{otros_texto}

Para cada conexión sugerida, indica brevemente por qué sería útil conectarse (sinergia comercial, misma industria, servicios complementarios, misma ciudad, etc).
Responde en español, de forma concisa. No uses asteriscos ni formatos."""

        respuesta = llamar_groq(prompt, max_tokens=600, temperature=0.5)
        if not respuesta:
            respuesta = "No pude generar sugerencias en este momento."
        
        await msg.edit_text(f"🔗 CONEXIONES SUGERIDAS\n{'━' * 28}\n\n{respuesta}\n\n💡 /directorio para ver el directorio completo")
        registrar_servicio_usado(user_id, 'conectar')
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


# ==================== 5. CUMPLEAÑOS MEJORADO ====================

@requiere_suscripcion
async def cumpleanos_mes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /cumpleanos_mes [1-12] - Cumpleaños de un mes desde Google Drive (columna X: DD-MMM)"""
    
    # Parsear mes: si viene argumento usar ese, si no, mes actual
    mes_consulta = datetime.now().month
    if context.args:
        try:
            mes_input = int(context.args[0])
            if 1 <= mes_input <= 12:
                mes_consulta = mes_input
            else:
                await update.message.reply_text(
                    "❌ Mes inválido. Usa un número del 1 al 12.\n\n"
                    "Ejemplo:\n"
                    "/cumpleanos_mes 3  →  Marzo\n"
                    "/cumpleanos_mes 12  →  Diciembre"
                )
                return
        except ValueError:
            await update.message.reply_text(
                "❌ Escribe un número del 1 al 12.\n\n"
                "Ejemplo:\n"
                "/cumpleanos_mes 3  →  Marzo\n"
                "/cumpleanos_mes 12  →  Diciembre"
            )
            return
    
    meses_nombres = ['', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                     'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    
    msg = await update.message.reply_text(f"🎂 Buscando cumpleaños de {meses_nombres[mes_consulta]}...")
    
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDS')
        if not creds_json:
            await msg.edit_text("❌ Credenciales de Google Drive no configuradas.")
            return
        
        creds_dict = json.loads(creds_json)
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        access_token = creds.get_access_token().access_token
        headers = {'Authorization': f'Bearer {access_token}'}
        
        # Buscar archivo BD Grupo Laboral
        search_url = "https://www.googleapis.com/drive/v3/files"
        params = {
            'q': "name contains 'BD Grupo Laboral' and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed=false",
            'fields': 'files(id, name)',
            'supportsAllDrives': 'true',
            'includeItemsFromAllDrives': 'true'
        }
        response = requests.get(search_url, headers=headers, params=params, timeout=30)
        archivos = response.json().get('files', [])
        
        if not archivos:
            await msg.edit_text("❌ No se encontró el archivo BD Grupo Laboral en Drive.")
            return
        
        file_id = archivos[0]['id']
        file_name = archivos[0]['name']
        logger.info(f"🎂 Leyendo cumpleaños desde: {file_name}")
        
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true"
        response = requests.get(download_url, headers=headers, timeout=60)
        
        if response.status_code != 200:
            await msg.edit_text("❌ Error descargando datos desde Drive.")
            return
        
        df = pd.read_excel(BytesIO(response.content), engine='openpyxl', header=0)
        logger.info(f"🎂 Excel: {len(df)} filas, {len(df.columns)} columnas")
        
        # Log primeras filas de columna X para debug
        if len(df.columns) > 23:
            col_x_name = df.columns[23]
            logger.info(f"🎂 Columna X (idx 23): '{col_x_name}'")
            muestras = df.iloc[:5, 23].tolist()
            logger.info(f"🎂 Muestras col X: {muestras}")
        
        def get_col(row, idx):
            try:
                val = row.iloc[idx] if idx < len(row) else ''
                # Si es un Timestamp o datetime de pandas, retornar como objeto especial
                if hasattr(val, 'month') and hasattr(val, 'day'):
                    return val  # Retornar el datetime directamente
                val_str = str(val).strip()
                if val_str.lower() in ['nan', 'none', '', 'null', 'n/a', '-', 'nat']:
                    return ''
                return val_str
            except:
                return ''
        
        # Abreviaciones de meses español/inglés → número
        abrev_a_mes = {
            'ene': 1, 'jan': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'apr': 4,
            'may': 5, 'jun': 6, 'jul': 7, 'ago': 8, 'aug': 8,
            'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dic': 12, 'dec': 12
        }
        
        cumples = []
        filas_con_fecha = 0
        
        for idx, row in df.iterrows():
            nombre = str(row.iloc[2]).strip() if len(row) > 2 and str(row.iloc[2]).strip().lower() not in ['nan', 'none', '', 'nat'] else ''
            apellido = str(row.iloc[3]).strip() if len(row) > 3 and str(row.iloc[3]).strip().lower() not in ['nan', 'none', '', 'nat'] else ''
            fecha_val = get_col(row, 23)  # Columna X
            
            if not fecha_val or not nombre:
                continue
            
            filas_con_fecha += 1
            dia = 0
            mes_num = 0
            
            try:
                # CASO 1: pandas Timestamp / datetime object
                if hasattr(fecha_val, 'month') and hasattr(fecha_val, 'day'):
                    dia = fecha_val.day
                    mes_num = fecha_val.month
                else:
                    fecha_str = str(fecha_val).strip()
                    
                    # CASO 2: Formato ISO "2024-03-15" o "2024-03-15 00:00:00"
                    if len(fecha_str) >= 10 and fecha_str[4] == '-':
                        try:
                            dt = datetime.strptime(fecha_str[:10], '%Y-%m-%d')
                            dia = dt.day
                            mes_num = dt.month
                        except:
                            pass
                    
                    # CASO 3: DD-MMM (15-Mar, 03-Jul, 7-Ene)
                    if mes_num == 0:
                        fecha_clean = fecha_str.replace('/', '-').replace('.', '-').replace(' ', '-').strip()
                        partes = [p.strip() for p in fecha_clean.split('-') if p.strip()]
                        
                        if len(partes) >= 2:
                            dia_str = partes[0]
                            mes_str = partes[1].lower()
                            
                            if not dia_str.isdigit() and len(partes) > 1:
                                dia_str = partes[1]
                                mes_str = partes[0].lower()
                            
                            try:
                                dia = int(dia_str)
                            except:
                                dia = 0
                            mes_num = abrev_a_mes.get(mes_str[:3], 0)
                            
                            if mes_num == 0:
                                try:
                                    mes_num = int(partes[1])
                                except:
                                    pass
                
                if mes_num == mes_consulta and 1 <= dia <= 31:
                    cumples.append({
                        'nombre': f"{nombre} {apellido}".strip(),
                        'dia': dia
                    })
            except (ValueError, IndexError, TypeError):
                continue
        
        logger.info(f"🎂 Filas con fecha: {filas_con_fecha}, Cumpleaños en mes {mes_consulta}: {len(cumples)}")
        
        cumples.sort(key=lambda x: x['dia'])
        
        if not cumples:
            await msg.edit_text(
                f"🎂 No se encontraron cumpleaños para {meses_nombres[mes_consulta]}.\n\n"
                f"📊 Se revisaron {filas_con_fecha} registros con fecha en columna X.\n\n"
                f"💡 Formato esperado en columna X: DD-MMM (ej: 15-Mar)\n"
                f"💡 Consulta otro mes: /cumpleanos_mes [1-12]"
            )
            return
        
        texto = f"🎂 CUMPLEAÑOS DE {meses_nombres[mes_consulta].upper()}\n{'━' * 28}\n\n"
        for c in cumples:
            hoy = datetime.now()
            es_hoy = (c['dia'] == hoy.day and mes_consulta == hoy.month)
            marca = " 🎉 ¡HOY!" if es_hoy else ""
            texto += f"🎂 {c['dia']:02d}/{mes_consulta:02d} — {c['nombre']}{marca}\n"
        
        texto += f"\n🎉 {len(cumples)} cumpleaños en {meses_nombres[mes_consulta]}"
        texto += f"\n💡 Otro mes: /cumpleanos_mes [1-12]"
        
        await msg.edit_text(texto)
        
    except Exception as e:
        logger.error(f"Error cumpleaños mes: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await msg.edit_text(f"❌ Error obteniendo cumpleaños: {str(e)[:100]}")


# ==================== 6. ENCUESTAS ====================

async def encuesta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /encuesta [pregunta] | [opción1] | [opción2] | ... - Crear encuesta"""
    if update.effective_user.id != OWNER_ID:
        if not verificar_suscripcion_activa(update.effective_user.id):
            await update.message.reply_text("❌ Necesitas una suscripción activa.")
            return
    
    if not context.args:
        await update.message.reply_text(
            "📊 CREAR ENCUESTA\n\n"
            "Formato:\n"
            "/encuesta Pregunta? | Opción 1 | Opción 2 | Opción 3\n\n"
            "Ejemplo:\n"
            "/encuesta ¿Cuándo prefieren la junta? | Viernes 18:00 | Sábado 12:00 | Domingo 11:00"
        )
        return
    
    texto = ' '.join(context.args)
    partes = [p.strip() for p in texto.split('|')]
    
    if len(partes) < 3:
        await update.message.reply_text("❌ Necesitas al menos una pregunta y 2 opciones separadas por |")
        return
    
    pregunta = partes[0]
    opciones = partes[1:10]  # Máximo 10 opciones (límite Telegram)
    
    try:
        await context.bot.send_poll(
            chat_id=update.effective_chat.id,
            question=pregunta,
            options=opciones,
            is_anonymous=False,
            allows_multiple_answers=False
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error creando encuesta: {str(e)[:100]}")


# ==================== 7. AGENDA DE EVENTOS ====================

async def nuevo_evento_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /nuevo_evento - Crear evento (solo admin)"""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el administrador puede crear eventos.")
        return
    
    if not context.args:
        await update.message.reply_text(
            "📅 CREAR EVENTO\n\n"
            "Formato:\n"
            "/nuevo_evento [fecha DD/MM/YYYY HH:MM] | [título] | [lugar] | [descripción]\n\n"
            "Ejemplo:\n"
            "/nuevo_evento 15/03/2026 19:00 | Junta de Cofradía | Club Naval Valparaíso | Cena de camaradería y networking"
        )
        return
    
    texto = ' '.join(context.args)
    partes = [p.strip() for p in texto.split('|')]
    
    if len(partes) < 3:
        await update.message.reply_text("❌ Formato: /nuevo_evento [fecha] | [título] | [lugar] | [descripción]")
        return
    
    fecha_str = partes[0]
    titulo = partes[1]
    lugar = partes[2]
    descripcion = partes[3] if len(partes) > 3 else ''
    
    # Parsear fecha
    try:
        for fmt in ['%d/%m/%Y %H:%M', '%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M']:
            try:
                fecha_evento = datetime.strptime(fecha_str, fmt)
                break
            except:
                continue
        else:
            await update.message.reply_text("❌ Formato de fecha no válido. Usa DD/MM/YYYY HH:MM")
            return
    except:
        await update.message.reply_text("❌ Formato de fecha no válido. Usa DD/MM/YYYY HH:MM")
        return
    
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("""INSERT INTO eventos (titulo, descripcion, fecha_evento, lugar, creado_por)
                            VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                         (titulo, descripcion, fecha_evento, lugar, OWNER_ID))
                evento_id = c.fetchone()['id']
            else:
                c.execute("""INSERT INTO eventos (titulo, descripcion, fecha_evento, lugar, creado_por)
                            VALUES (?, ?, ?, ?, ?)""",
                         (titulo, descripcion, fecha_evento.strftime("%Y-%m-%d %H:%M:%S"), lugar, OWNER_ID))
                evento_id = c.lastrowid
            conn.commit()
            conn.close()
            
            await update.message.reply_text(
                f"✅ Evento #{evento_id} creado!\n\n"
                f"📅 {titulo}\n"
                f"📆 {fecha_evento.strftime('%d/%m/%Y %H:%M')}\n"
                f"📍 {lugar}\n"
                f"{'📝 ' + descripcion if descripcion else ''}\n\n"
                f"Los cofrades pueden verlo con /eventos\n"
                f"y confirmar asistencia con /asistir {evento_id}"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


@requiere_suscripcion
async def eventos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /eventos - Ver próximos eventos"""
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión")
            return
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("""SELECT e.id, e.titulo, e.descripcion, e.fecha_evento, e.lugar,
                        (SELECT COUNT(*) FROM eventos_asistencia WHERE evento_id = e.id) as asistentes
                        FROM eventos e WHERE e.activo = TRUE AND e.fecha_evento > CURRENT_TIMESTAMP
                        ORDER BY e.fecha_evento LIMIT 10""")
        else:
            c.execute("""SELECT e.id, e.titulo, e.descripcion, e.fecha_evento, e.lugar,
                        (SELECT COUNT(*) FROM eventos_asistencia WHERE evento_id = e.id) as asistentes
                        FROM eventos e WHERE e.activo = 1
                        ORDER BY e.fecha_evento LIMIT 10""")
        
        eventos = c.fetchall()
        conn.close()
        
        if not eventos:
            await update.message.reply_text("📅 No hay eventos próximos programados.")
            return
        
        msg = f"📅 PRÓXIMOS EVENTOS\n{'━' * 28}\n\n"
        for e in eventos:
            eid = e['id'] if DATABASE_URL else e[0]
            titulo = e['titulo'] if DATABASE_URL else e[1]
            desc = e['descripcion'] if DATABASE_URL else e[2]
            fecha = e['fecha_evento'] if DATABASE_URL else e[3]
            lugar = e['lugar'] if DATABASE_URL else e[4]
            asist = e['asistentes'] if DATABASE_URL else e[5]
            
            fecha_str = str(fecha)[:16] if fecha else '?'
            msg += f"📌 #{eid} {titulo}\n"
            msg += f"   📆 {fecha_str}\n"
            msg += f"   📍 {lugar}\n"
            if desc: msg += f"   📝 {desc[:80]}\n"
            msg += f"   👥 {asist} confirmado(s)\n"
            msg += f"   ✅ /asistir {eid}\n\n"
        
        await enviar_mensaje_largo(update, msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


@requiere_suscripcion
async def asistir_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /asistir [ID] - Confirmar asistencia a evento"""
    if not context.args:
        await update.message.reply_text("❌ Uso: /asistir [ID del evento]\n\nVer eventos: /eventos")
        return
    
    try:
        evento_id = int(context.args[0])
        user = update.effective_user
        nombre = f"{user.first_name or ''} {user.last_name or ''}".strip()
        
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            # Verificar evento existe
            if DATABASE_URL:
                c.execute("SELECT titulo FROM eventos WHERE id = %s AND activo = TRUE", (evento_id,))
            else:
                c.execute("SELECT titulo FROM eventos WHERE id = ? AND activo = 1", (evento_id,))
            evento = c.fetchone()
            
            if not evento:
                conn.close()
                await update.message.reply_text("❌ Evento no encontrado.")
                return
            
            titulo = evento['titulo'] if DATABASE_URL else evento[0]
            
            # Verificar si ya confirmó
            if DATABASE_URL:
                c.execute("SELECT id FROM eventos_asistencia WHERE evento_id = %s AND user_id = %s", (evento_id, user.id))
            else:
                c.execute("SELECT id FROM eventos_asistencia WHERE evento_id = ? AND user_id = ?", (evento_id, user.id))
            
            if c.fetchone():
                conn.close()
                await update.message.reply_text(f"✅ Ya habías confirmado tu asistencia a \"{titulo}\"")
                return
            
            if DATABASE_URL:
                c.execute("INSERT INTO eventos_asistencia (evento_id, user_id, nombre) VALUES (%s, %s, %s)",
                         (evento_id, user.id, nombre))
            else:
                c.execute("INSERT INTO eventos_asistencia (evento_id, user_id, nombre) VALUES (?, ?, ?)",
                         (evento_id, user.id, nombre))
            conn.commit()
            conn.close()
            
            await update.message.reply_text(f"✅ Asistencia confirmada a \"{titulo}\"\n\n📅 Te recordaremos 24h antes.")
            otorgar_coins(user_id, 20, f'Asistir evento: {titulo}')
    except ValueError:
        await update.message.reply_text("❌ Uso: /asistir [número ID]")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


# ==================== 8. RECOMENDACIONES ====================

@requiere_suscripcion
async def recomendar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /recomendar @usuario|nombre Texto — Recomendar a un cofrade"""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "⭐ RECOMENDAR COFRADE\n\n"
            "Formato:\n"
            "/recomendar @usuario Excelente profesional\n"
            "/recomendar Pedro González Excelente profesional\n\n"
            "Tu recomendación aparecerá en su tarjeta profesional."
        )
        return
    
    user = update.effective_user
    autor_nombre = f"{user.first_name or ''} {user.last_name or ''}".strip()
    primer_arg = context.args[0]
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión")
            return
        c = conn.cursor()
        dest = None
        texto_rec = ''
        
        if primer_arg.startswith('@'):
            objetivo_username = primer_arg.replace('@', '').lower()
            texto_rec = ' '.join(context.args[1:])
            if DATABASE_URL:
                c.execute("SELECT user_id, first_name, last_name FROM suscripciones WHERE LOWER(username) = %s", (objetivo_username,))
            else:
                c.execute("SELECT user_id, first_name, last_name FROM suscripciones WHERE LOWER(username) = ?", (objetivo_username,))
            dest = c.fetchone()
            if not dest:
                conn.close()
                await update.message.reply_text(f"❌ No se encontró al usuario @{objetivo_username}")
                return
        else:
            # Búsqueda por nombre
            busqueda_palabras = []
            for arg in context.args:
                if len(busqueda_palabras) < 3 and len(arg) > 1 and arg[0].isupper():
                    busqueda_palabras.append(arg)
                else:
                    break
            if len(busqueda_palabras) < 1:
                conn.close()
                await update.message.reply_text("❌ Indica un @usuario o nombre.\n\nEjemplo: /recomendar @usuario Texto")
                return
            nombre_buscar = ' '.join(busqueda_palabras).lower()
            texto_rec = ' '.join(context.args[len(busqueda_palabras):])
            busqueda = f"%{nombre_buscar}%"
            if DATABASE_URL:
                c.execute("""SELECT user_id, first_name, last_name FROM suscripciones 
                           WHERE LOWER(first_name || ' ' || COALESCE(last_name,'')) LIKE %s 
                           AND estado = 'activo' LIMIT 5""", (busqueda,))
            else:
                c.execute("""SELECT user_id, first_name, last_name FROM suscripciones 
                           WHERE LOWER(first_name || ' ' || COALESCE(last_name,'')) LIKE ? 
                           AND estado = 'activo' LIMIT 5""", (busqueda,))
            resultados = c.fetchall()
            if not resultados:
                conn.close()
                await update.message.reply_text(f"❌ No se encontró a \"{' '.join(busqueda_palabras)}\" entre los miembros.")
                return
            validos = [r for r in resultados if (r['user_id'] if DATABASE_URL else r[0]) != user.id]
            if not validos:
                conn.close()
                await update.message.reply_text("❌ No puedes recomendarte a ti mismo.")
                return
            if len(validos) > 1 and not texto_rec:
                lista = "\n".join([f"  👤 {(r['first_name'] if DATABASE_URL else r[1]) or ''} {(r['last_name'] if DATABASE_URL else r[2]) or ''}".strip() for r in validos])
                conn.close()
                await update.message.reply_text(
                    f"👥 Se encontraron {len(validos)} coincidencias:\n{lista}\n\n"
                    f"Sé más específico o usa @username:\n"
                    f"/recomendar @usuario Texto de recomendación"
                )
                return
            dest = validos[0]
        
        dest_id = dest['user_id'] if DATABASE_URL else dest[0]
        dest_nombre = f"{dest['first_name'] if DATABASE_URL else dest[1]} {dest['last_name'] if DATABASE_URL else dest[2]}".strip()
        
        if dest_id == user.id:
            conn.close()
            await update.message.reply_text("❌ No puedes recomendarte a ti mismo.")
            return
        
        if len(texto_rec) < 10:
            conn.close()
            await update.message.reply_text("❌ La recomendación debe tener al menos 10 caracteres.")
            return
        
        if DATABASE_URL:
            c.execute("INSERT INTO recomendaciones (autor_id, autor_nombre, destinatario_id, destinatario_nombre, texto) VALUES (%s, %s, %s, %s, %s)",
                     (user.id, autor_nombre, dest_id, dest_nombre, texto_rec[:500]))
        else:
            c.execute("INSERT INTO recomendaciones (autor_id, autor_nombre, destinatario_id, destinatario_nombre, texto) VALUES (?, ?, ?, ?, ?)",
                     (user.id, autor_nombre, dest_id, dest_nombre, texto_rec[:500]))
        conn.commit()
        conn.close()
        
        await update.message.reply_text(f"⭐ Recomendación enviada para {dest_nombre}!\n\nAparecerá en su perfil profesional.")
        otorgar_coins(update.effective_user.id, 5, f'Recomendar a {dest_nombre}')
        
        try:
            await context.bot.send_message(
                chat_id=dest_id,
                text=f"⭐ {autor_nombre} te ha dejado una recomendación:\n\n\"{texto_rec[:300]}\"\n\n💡 Ver tus recomendaciones: /mis_recomendaciones"
            )
        except:
            pass
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


@requiere_suscripcion
async def mis_recomendaciones_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mis_recomendaciones - Ver recomendaciones recibidas"""
    user_id = update.effective_user.id
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT autor_nombre, texto, fecha FROM recomendaciones WHERE destinatario_id = %s ORDER BY fecha DESC LIMIT 10", (user_id,))
            else:
                c.execute("SELECT autor_nombre, texto, fecha FROM recomendaciones WHERE destinatario_id = ? ORDER BY fecha DESC LIMIT 10", (user_id,))
            recs = c.fetchall()
            conn.close()
            
            if not recs:
                await update.message.reply_text("⭐ Aún no tienes recomendaciones.\n\nPide a tus cofrades que te recomienden con /recomendar")
                return
            
            msg = f"⭐ TUS RECOMENDACIONES ({len(recs)})\n{'━' * 28}\n\n"
            for r in recs:
                autor = r['autor_nombre'] if DATABASE_URL else r[0]
                texto = r['texto'] if DATABASE_URL else r[1]
                msg += f"👤 {autor}:\n\"{texto[:150]}\"\n\n"
            
            await enviar_mensaje_largo(update, msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


# ==================== 9. NEWSLETTER SEMANAL ====================

async def generar_newsletter_semanal(context):
    """Job: Genera y envía newsletter semanal los lunes a las 9AM"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        
        datos = {}
        
        if DATABASE_URL:
            # Mensajes de la semana
            c.execute("SELECT COUNT(*) as total FROM mensajes WHERE fecha > CURRENT_TIMESTAMP - INTERVAL '7 days'")
            datos['mensajes_semana'] = c.fetchone()['total']
            
            # Usuarios activos
            c.execute("SELECT COUNT(DISTINCT user_id) as total FROM mensajes WHERE fecha > CURRENT_TIMESTAMP - INTERVAL '7 days'")
            datos['usuarios_activos'] = c.fetchone()['total']
            
            # Nuevos miembros
            c.execute("SELECT COUNT(*) as total FROM nuevos_miembros WHERE estado = 'aprobado' AND fecha_aprobacion > CURRENT_TIMESTAMP - INTERVAL '7 days'")
            datos['nuevos_miembros'] = c.fetchone()['total']
            
            # Top 3 participantes
            c.execute("""SELECT first_name || ' ' || COALESCE(last_name, '') as nombre, COUNT(*) as msgs 
                        FROM mensajes WHERE fecha > CURRENT_TIMESTAMP - INTERVAL '7 days' 
                        GROUP BY first_name, last_name ORDER BY msgs DESC LIMIT 3""")
            datos['top_3'] = [(r['nombre'].strip(), r['msgs']) for r in c.fetchall()]
            
            # Anuncios nuevos
            c.execute("SELECT COUNT(*) as total FROM anuncios WHERE fecha_publicacion > CURRENT_TIMESTAMP - INTERVAL '7 days' AND activo = TRUE")
            datos['anuncios_nuevos'] = c.fetchone()['total']
            
            # Eventos próximos
            c.execute("SELECT titulo, fecha_evento FROM eventos WHERE activo = TRUE AND fecha_evento > CURRENT_TIMESTAMP ORDER BY fecha_evento LIMIT 3")
            datos['eventos'] = [(r['titulo'], str(r['fecha_evento'])[:10]) for r in c.fetchall()]
            
            # Total miembros
            c.execute("SELECT COUNT(*) as total FROM suscripciones WHERE estado = 'activo'")
            datos['total_activos'] = c.fetchone()['total']
        
        conn.close()
        
        # Generar newsletter
        newsletter = f"📰 NEWSLETTER SEMANAL COFRADÍA\n{'━' * 30}\n\n"
        newsletter += f"📊 ACTIVIDAD DE LA SEMANA\n"
        newsletter += f"💬 {datos.get('mensajes_semana', 0)} mensajes\n"
        newsletter += f"👥 {datos.get('usuarios_activos', 0)} cofrades activos\n"
        newsletter += f"🆕 {datos.get('nuevos_miembros', 0)} nuevos miembros\n\n"
        
        top = datos.get('top_3', [])
        if top:
            newsletter += "🏆 TOP PARTICIPANTES\n"
            medallas = ['🥇', '🥈', '🥉']
            for i, (nombre, msgs) in enumerate(top):
                nombre_limpio = limpiar_nombre_display(nombre)
                newsletter += f"{medallas[i]} {nombre_limpio}: {msgs} msgs\n"
            newsletter += "\n"
        
        if datos.get('anuncios_nuevos', 0) > 0:
            newsletter += f"📢 {datos['anuncios_nuevos']} anuncios nuevos — /anuncios\n\n"
        
        eventos = datos.get('eventos', [])
        if eventos:
            newsletter += "📅 PRÓXIMOS EVENTOS\n"
            for titulo, fecha in eventos:
                newsletter += f"📌 {titulo} — {fecha}\n"
            newsletter += "Ver detalles: /eventos\n\n"
        
        newsletter += f"👥 Total miembros activos: {datos.get('total_activos', 0)}\n\n"
        newsletter += "💡 ¿Sabías que puedes crear tu tarjeta profesional? /mi_tarjeta\n"
        newsletter += "━" * 30
        
        # Enviar al grupo
        if COFRADIA_GROUP_ID:
            try:
                await context.bot.send_message(chat_id=COFRADIA_GROUP_ID, text=newsletter)
                logger.info("📰 Newsletter semanal enviada al grupo")
            except Exception as e:
                logger.error(f"Error enviando newsletter: {e}")
    except Exception as e:
        logger.error(f"Error generando newsletter: {e}")


# ==================== 10. CONSULTAS ENTRE COFRADES ====================

@requiere_suscripcion
async def consultar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /consultar [título] | [descripción] - Publicar consulta profesional"""
    if not context.args:
        await update.message.reply_text(
            "❓ CONSULTAS PROFESIONALES\n\n"
            "Publica una consulta para que otros cofrades te ayuden.\n\n"
            "Formato:\n"
            "/consultar [título] | [descripción detallada]\n\n"
            "Ejemplo:\n"
            "/consultar Abogado laboral | Necesito asesoría sobre finiquito. ¿Alguien conoce un buen abogado?\n\n"
            "Para consulta anónima agrega 'anónimo' al inicio:\n"
            "/consultar anónimo Consulta médica | ¿Alguien recomienda traumatólogo en Santiago?"
        )
        return
    
    user = update.effective_user
    texto = ' '.join(context.args)
    
    anonima = False
    if texto.lower().startswith('anónim') or texto.lower().startswith('anonim'):
        anonima = True
        texto = re.sub(r'^an[oó]nim[oa]?\s*', '', texto, flags=re.IGNORECASE).strip()
    
    if '|' in texto:
        partes = texto.split('|', 1)
        titulo = partes[0].strip()
        descripcion = partes[1].strip()
    else:
        titulo = texto[:80]
        descripcion = texto
    
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            nombre = f"{user.first_name or ''} {user.last_name or ''}".strip()
            
            if DATABASE_URL:
                c.execute("""INSERT INTO consultas_cofrades (user_id, nombre_autor, titulo, descripcion, anonima)
                            VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                         (user.id, nombre, titulo, descripcion, anonima))
                consulta_id = c.fetchone()['id']
            else:
                c.execute("""INSERT INTO consultas_cofrades (user_id, nombre_autor, titulo, descripcion, anonima)
                            VALUES (?, ?, ?, ?, ?)""",
                         (user.id, nombre, titulo, descripcion, 1 if anonima else 0))
                consulta_id = c.lastrowid
            conn.commit()
            conn.close()
            
            autor_display = "Anónimo" if anonima else nombre
            await update.message.reply_text(
                f"✅ Consulta #{consulta_id} publicada!\n\n"
                f"❓ {titulo}\n"
                f"👤 {autor_display}\n\n"
                f"Los cofrades pueden responder con:\n"
                f"/responder {consulta_id} [tu respuesta]"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


@requiere_suscripcion
async def consultas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /consultas - Ver consultas abiertas"""
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión")
            return
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("""SELECT c.id, c.titulo, c.nombre_autor, c.anonima, c.fecha,
                        (SELECT COUNT(*) FROM respuestas_consultas WHERE consulta_id = c.id) as resp_count
                        FROM consultas_cofrades c WHERE c.resuelta = FALSE
                        ORDER BY c.fecha DESC LIMIT 15""")
        else:
            c.execute("""SELECT c.id, c.titulo, c.nombre_autor, c.anonima, c.fecha,
                        (SELECT COUNT(*) FROM respuestas_consultas WHERE consulta_id = c.id) as resp_count
                        FROM consultas_cofrades c WHERE c.resuelta = 0
                        ORDER BY c.fecha DESC LIMIT 15""")
        
        consultas = c.fetchall()
        conn.close()
        
        if not consultas:
            await update.message.reply_text("❓ No hay consultas abiertas.\n\n💡 Publica una: /consultar")
            return
        
        msg = f"❓ CONSULTAS ABIERTAS\n{'━' * 28}\n\n"
        for cq in consultas:
            cid = cq['id'] if DATABASE_URL else cq[0]
            titulo = cq['titulo'] if DATABASE_URL else cq[1]
            autor = cq['nombre_autor'] if DATABASE_URL else cq[2]
            anonima = cq['anonima'] if DATABASE_URL else cq[3]
            resps = cq['resp_count'] if DATABASE_URL else cq[5]
            autor_display = "Anónimo" if anonima else autor
            msg += f"❓ #{cid} {titulo}\n"
            msg += f"   👤 {autor_display} | 💬 {resps} respuesta(s)\n"
            msg += f"   ➡️ /responder {cid} [tu respuesta]\n\n"
        
        await enviar_mensaje_largo(update, msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


@requiere_suscripcion
async def responder_consulta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /responder [ID] [respuesta] - Responder a una consulta"""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("❌ Uso: /responder [ID consulta] [tu respuesta]")
        return
    
    try:
        consulta_id = int(context.args[0])
        respuesta_texto = ' '.join(context.args[1:])
        user = update.effective_user
        nombre = f"{user.first_name or ''} {user.last_name or ''}".strip()
        
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            
            # Verificar consulta existe
            if DATABASE_URL:
                c.execute("SELECT titulo, user_id, nombre_autor FROM consultas_cofrades WHERE id = %s", (consulta_id,))
            else:
                c.execute("SELECT titulo, user_id, nombre_autor FROM consultas_cofrades WHERE id = ?", (consulta_id,))
            consulta = c.fetchone()
            
            if not consulta:
                conn.close()
                await update.message.reply_text("❌ Consulta no encontrada.")
                return
            
            titulo = consulta['titulo'] if DATABASE_URL else consulta[0]
            autor_id = consulta['user_id'] if DATABASE_URL else consulta[1]
            
            if DATABASE_URL:
                c.execute("INSERT INTO respuestas_consultas (consulta_id, user_id, nombre_autor, respuesta) VALUES (%s, %s, %s, %s)",
                         (consulta_id, user.id, nombre, respuesta_texto[:1000]))
            else:
                c.execute("INSERT INTO respuestas_consultas (consulta_id, user_id, nombre_autor, respuesta) VALUES (?, ?, ?, ?)",
                         (consulta_id, user.id, nombre, respuesta_texto[:1000]))
            conn.commit()
            conn.close()
            
            await update.message.reply_text(f"✅ Respuesta enviada a consulta #{consulta_id}: \"{titulo}\"")
            otorgar_coins(update.effective_user.id, 10, f'Responder consulta #{consulta_id}')
            
            # Notificar al autor
            try:
                await context.bot.send_message(
                    chat_id=autor_id,
                    text=f"💬 Nueva respuesta a tu consulta #{consulta_id}:\n\"{titulo}\"\n\n"
                         f"👤 {nombre} respondió:\n{respuesta_texto[:300]}\n\n"
                         f"Ver todas: /ver_consulta {consulta_id}"
                )
            except:
                pass
    except ValueError:
        await update.message.reply_text("❌ Uso: /responder [número ID] [tu respuesta]")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


@requiere_suscripcion
async def ver_consulta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /ver_consulta [ID] - Ver consulta con respuestas"""
    if not context.args:
        await update.message.reply_text("❌ Uso: /ver_consulta [ID]")
        return
    
    try:
        consulta_id = int(context.args[0])
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("SELECT titulo, descripcion, nombre_autor, anonima, fecha FROM consultas_cofrades WHERE id = %s", (consulta_id,))
        else:
            c.execute("SELECT titulo, descripcion, nombre_autor, anonima, fecha FROM consultas_cofrades WHERE id = ?", (consulta_id,))
        consulta = c.fetchone()
        
        if not consulta:
            conn.close()
            await update.message.reply_text("❌ Consulta no encontrada.")
            return
        
        titulo = consulta['titulo'] if DATABASE_URL else consulta[0]
        desc = consulta['descripcion'] if DATABASE_URL else consulta[1]
        autor = consulta['nombre_autor'] if DATABASE_URL else consulta[2]
        anonima = consulta['anonima'] if DATABASE_URL else consulta[3]
        
        if DATABASE_URL:
            c.execute("SELECT nombre_autor, respuesta, fecha FROM respuestas_consultas WHERE consulta_id = %s ORDER BY fecha", (consulta_id,))
        else:
            c.execute("SELECT nombre_autor, respuesta, fecha FROM respuestas_consultas WHERE consulta_id = ? ORDER BY fecha", (consulta_id,))
        respuestas = c.fetchall()
        conn.close()
        
        autor_display = "Anónimo" if anonima else autor
        msg = f"❓ CONSULTA #{consulta_id}\n{'━' * 28}\n\n"
        msg += f"📌 {titulo}\n👤 {autor_display}\n📝 {desc}\n\n"
        
        if respuestas:
            msg += f"💬 RESPUESTAS ({len(respuestas)}):\n\n"
            for r in respuestas:
                r_autor = r['nombre_autor'] if DATABASE_URL else r[0]
                r_texto = r['respuesta'] if DATABASE_URL else r[1]
                msg += f"👤 {r_autor}:\n{r_texto[:200]}\n\n"
        else:
            msg += "💬 Sin respuestas aún.\n"
        
        msg += f"\n➡️ /responder {consulta_id} [tu respuesta]"
        await enviar_mensaje_largo(update, msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


# ==================== SISTEMA COFRADÍA COINS v4.0 ====================

def otorgar_coins(user_id: int, cantidad: int, descripcion: str):
    """Otorga Cofradía Coins a un usuario"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""INSERT INTO cofradia_coins (user_id, balance, total_ganado)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                        balance = cofradia_coins.balance + %s,
                        total_ganado = cofradia_coins.total_ganado + %s,
                        fecha_actualizacion = CURRENT_TIMESTAMP""",
                     (user_id, cantidad, cantidad, cantidad, cantidad))
            c.execute("INSERT INTO coins_historial (user_id, cantidad, tipo, descripcion) VALUES (%s, %s, %s, %s)",
                     (user_id, cantidad, 'ganado', descripcion))
        else:
            c.execute("SELECT user_id FROM cofradia_coins WHERE user_id = ?", (user_id,))
            if c.fetchone():
                c.execute("UPDATE cofradia_coins SET balance = balance + ?, total_ganado = total_ganado + ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE user_id = ?",
                         (cantidad, cantidad, user_id))
            else:
                c.execute("INSERT INTO cofradia_coins (user_id, balance, total_ganado) VALUES (?, ?, ?)",
                         (user_id, cantidad, cantidad))
            c.execute("INSERT INTO coins_historial (user_id, cantidad, tipo, descripcion) VALUES (?, ?, ?, ?)",
                     (user_id, cantidad, 'ganado', descripcion))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.debug(f"Error otorgando coins: {e}")
        return False


def gastar_coins(user_id: int, cantidad: int, descripcion: str) -> bool:
    """Gasta Cofradía Coins. Retorna True si tenía suficiente."""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT balance FROM cofradia_coins WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT balance FROM cofradia_coins WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        balance = (row['balance'] if DATABASE_URL else row[0]) if row else 0
        if balance < cantidad:
            conn.close()
            return False
        if DATABASE_URL:
            c.execute("UPDATE cofradia_coins SET balance = balance - %s, total_gastado = total_gastado + %s, fecha_actualizacion = CURRENT_TIMESTAMP WHERE user_id = %s",
                     (cantidad, cantidad, user_id))
            c.execute("INSERT INTO coins_historial (user_id, cantidad, tipo, descripcion) VALUES (%s, %s, %s, %s)",
                     (user_id, -cantidad, 'gastado', descripcion))
        else:
            c.execute("UPDATE cofradia_coins SET balance = balance - ?, total_gastado = total_gastado + ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE user_id = ?",
                     (cantidad, cantidad, user_id))
            c.execute("INSERT INTO coins_historial (user_id, cantidad, tipo, descripcion) VALUES (?, ?, ?, ?)",
                     (user_id, -cantidad, 'gastado', descripcion))
        conn.commit()
        conn.close()
        return True
    except:
        return False


def get_coins_balance(user_id: int) -> dict:
    """Obtiene balance de coins"""
    try:
        conn = get_db_connection()
        if not conn:
            return {'balance': 0, 'total_ganado': 0, 'total_gastado': 0}
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT balance, total_ganado, total_gastado FROM cofradia_coins WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT balance, total_ganado, total_gastado FROM cofradia_coins WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        if row:
            return {'balance': row['balance'] if DATABASE_URL else row[0],
                    'total_ganado': row['total_ganado'] if DATABASE_URL else row[1],
                    'total_gastado': row['total_gastado'] if DATABASE_URL else row[2]}
        return {'balance': 0, 'total_ganado': 0, 'total_gastado': 0}
    except:
        return {'balance': 0, 'total_ganado': 0, 'total_gastado': 0}


def get_precio_servicio(servicio: str) -> dict:
    """Obtiene precio de un servicio premium"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT precio_pesos, precio_coins, descripcion FROM precios_servicios WHERE servicio = %s", (servicio,))
        else:
            c.execute("SELECT precio_pesos, precio_coins, descripcion FROM precios_servicios WHERE servicio = ?", (servicio,))
        row = c.fetchone()
        conn.close()
        if row:
            return {'pesos': row['precio_pesos'] if DATABASE_URL else row[0],
                    'coins': row['precio_coins'] if DATABASE_URL else row[1],
                    'desc': row['descripcion'] if DATABASE_URL else row[2]}
        return None
    except:
        return None


def calcular_trust_score(user_id: int) -> dict:
    """Calcula el Trust Score de un usuario (0-100)"""
    score = 0
    detalles = {}
    try:
        conn = get_db_connection()
        if not conn:
            return {'score': 0, 'nivel': '⚪ Nuevo', 'detalles': {}}
        c = conn.cursor()

        # Antigüedad (máx 20 pts)
        if DATABASE_URL:
            c.execute("SELECT fecha_registro FROM suscripciones WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT fecha_registro FROM suscripciones WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if row:
            fecha = row['fecha_registro'] if DATABASE_URL else row[0]
            if fecha:
                try:
                    dias = (datetime.now() - datetime.fromisoformat(str(fecha)[:19])).days
                    pts = min(20, dias // 15)
                    score += pts
                    detalles['Antigüedad'] = pts
                except:
                    pass

        # Mensajes (máx 20 pts)
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as t FROM mensajes WHERE user_id = %s", (str(user_id),))
            msgs = c.fetchone()['t']
        else:
            c.execute("SELECT COUNT(*) FROM mensajes WHERE user_id = ?", (str(user_id),))
            msgs = c.fetchone()[0]
        pts = min(20, msgs // 10)
        score += pts
        detalles['Participación'] = pts

        # Recomendaciones recibidas (máx 20 pts)
        try:
            if DATABASE_URL:
                c.execute("SELECT COUNT(*) as t FROM recomendaciones WHERE destinatario_id = %s", (user_id,))
                recs = c.fetchone()['t']
            else:
                c.execute("SELECT COUNT(*) FROM recomendaciones WHERE destinatario_id = ?", (user_id,))
                recs = c.fetchone()[0]
            pts = min(20, recs * 5)
            score += pts
            detalles['Recomendaciones'] = pts
        except:
            pass

        # Consultas respondidas (máx 20 pts)
        try:
            if DATABASE_URL:
                c.execute("SELECT COUNT(*) as t FROM respuestas_consultas WHERE user_id = %s", (user_id,))
                resps = c.fetchone()['t']
            else:
                c.execute("SELECT COUNT(*) FROM respuestas_consultas WHERE user_id = ?", (user_id,))
                resps = c.fetchone()[0]
            pts = min(20, resps * 4)
            score += pts
            detalles['Ayuda a otros'] = pts
        except:
            pass

        # Tarjeta profesional (10 pts)
        if DATABASE_URL:
            c.execute("SELECT user_id FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT user_id FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
        if c.fetchone():
            score += 10
            detalles['Tarjeta profesional'] = 10

        # Coins ganados (máx 10 pts)
        coins = get_coins_balance(user_id)
        pts = min(10, coins['total_ganado'] // 20)
        score += pts
        detalles['Actividad Coins'] = pts

        conn.close()
        score = min(score, 100)

        if score >= 80: nivel = '🏆 Embajador'
        elif score >= 60: nivel = '⭐ Destacado'
        elif score >= 40: nivel = '🔵 Activo'
        elif score >= 20: nivel = '🟢 Participante'
        else: nivel = '⚪ Nuevo'

        return {'score': score, 'nivel': nivel, 'detalles': detalles}
    except:
        return {'score': 0, 'nivel': '⚪ Nuevo', 'detalles': {}}


# ==================== NIVEL 2: ASISTENTE FINANCIERO (gratis) ====================

@requiere_suscripcion
async def finanzas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /finanzas [consulta] - Asistente financiero basado en libros RAG"""
    if not context.args:
        await update.message.reply_text(
            "💰 ASISTENTE FINANCIERO\n"
            "Consulta basada en los 100+ libros de la biblioteca.\n\n"
            "Ejemplos:\n"
            "/finanzas qué dice Kiyosaki sobre inversiones\n"
            "/finanzas cómo diversificar con 5 millones\n"
            "/finanzas conviene APV o fondo mutuo\n\n"
            "💡 Gratuito — ganas 1 Coin por consulta."
        )
        return
    consulta = ' '.join(context.args)
    msg = await update.message.reply_text("💰 Consultando biblioteca financiera...")
    try:
        resultados = busqueda_unificada(consulta, limit_historial=0, limit_rag=25)
        contexto = formatear_contexto_unificado(resultados, consulta)
        nombre = update.effective_user.first_name
        prompt = f"""Eres un asesor financiero experto. Responde en primera persona, con ejemplos en pesos chilenos.
Inicia tu respuesta con el nombre "{nombre}". Sé claro, práctico y conciso (máx 6 oraciones).
Al final sugiere 1-2 libros de la biblioteca que profundicen el tema.
No uses asteriscos, emojis ni formatos especiales.

{contexto}

Consulta de {nombre}: {consulta}"""
        respuesta = llamar_groq(prompt, max_tokens=800, temperature=0.5)
        if not respuesta:
            respuesta = "No pude generar una respuesta en este momento."
        await msg.edit_text(f"💰 CONSULTA FINANCIERA\n{'━' * 28}\n\n{respuesta}")
        otorgar_coins(update.effective_user.id, 1, 'Consulta financiera')
        registrar_servicio_usado(update.effective_user.id, 'finanzas')
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


# ==================== NIVEL 2: GENERADOR CV (premium) ====================

@requiere_suscripcion
async def generar_cv_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /generar_cv [orientación] - CV profesional con IA + datos reales de Drive y tarjeta"""
    user_id = update.effective_user.id
    orientacion = ' '.join(context.args) if context.args else ''
    if user_id != OWNER_ID:
        precio = get_precio_servicio('generar_cv')
        if precio:
            coins = get_coins_balance(user_id)
            if coins['balance'] >= precio['coins']:
                if not gastar_coins(user_id, precio['coins'], 'Servicio: generar_cv'):
                    await update.message.reply_text("❌ Error procesando coins.")
                    return
                await update.message.reply_text(f"✅ {precio['coins']} Coins descontados.")
            else:
                faltan = precio['coins'] - coins['balance']
                await update.message.reply_text(
                    f"💎 SERVICIO PREMIUM: {precio['desc']}\n\n"
                    f"💰 Precio: ${precio['pesos']:,} CLP o {precio['coins']} Cofradía Coins\n"
                    f"🪙 Tu balance: {coins['balance']} Coins (faltan {faltan})\n\n"
                    f"💡 Cómo ganar Coins:\n"
                    f"  💬 Mensaje en grupo: +1\n  💡 Responder consulta: +10\n"
                    f"  ⭐ Recomendar cofrade: +5\n  📅 Asistir evento: +20\n"
                    f"  📇 Crear tarjeta: +15\n\n📱 Pago en pesos: contacta al admin."
                )
                return
    # ===== PASO 1: Obtener datos de tarjeta profesional =====
    conn = get_db_connection()
    tarjeta = {}
    if conn:
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
        t = c.fetchone()
        if t:
            tarjeta = {'nombre': t['nombre_completo'] if DATABASE_URL else t[1],
                       'profesion': t['profesion'] if DATABASE_URL else t[2],
                       'empresa': t['empresa'] if DATABASE_URL else t[3],
                       'servicios': t['servicios'] if DATABASE_URL else t[4],
                       'telefono': t['telefono'] if DATABASE_URL else t[5],
                       'email': t['email'] if DATABASE_URL else t[6],
                       'ciudad': t['ciudad'] if DATABASE_URL else t[7],
                       'linkedin': t['linkedin'] if DATABASE_URL else t[8]}
        conn.close()
    if not tarjeta:
        await update.message.reply_text("❌ Primero crea tu tarjeta profesional con /mi_tarjeta")
        return
    msg = await update.message.reply_text("📄 Recopilando datos profesionales de múltiples fuentes...")
    
    # ===== PASO 2: Buscar datos adicionales en Google Drive Excel =====
    drive_info = ""
    try:
        drive_data = obtener_datos_excel_drive()
        if drive_data is not None and len(drive_data) > 0:
            nombre_buscar = (tarjeta.get('nombre', '') or '').lower().strip()
            partes_nombre = nombre_buscar.split()
            for _, row in drive_data.iterrows():
                nombre_excel = str(row.iloc[2]).strip().lower() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
                apellido_excel = str(row.iloc[3]).strip().lower() if len(row) > 3 and pd.notna(row.iloc[3]) else ''
                full_excel = f"{nombre_excel} {apellido_excel}".strip()
                match = False
                if nombre_buscar and (nombre_buscar in full_excel or full_excel in nombre_buscar):
                    match = True
                elif len(partes_nombre) >= 2:
                    if partes_nombre[0] in full_excel and partes_nombre[-1] in full_excel:
                        match = True
                if match:
                    extras = []
                    # Mapeo real del Excel BD Grupo Laboral:
                    # B=1:Generación, F=5:Teléfono, G=6:Email, H=7:Ciudad, 
                    # I=8:Situación Laboral, K=10:Industria1, L=11:Empresa1,
                    # M=12:Industria2, N=13:Empresa2, O=14:Industria3, P=15:Empresa3,
                    # Y=24:Profesión/Actividad
                    for col_idx, label in [(1, 'Generación'), (24, 'Profesión/Actividad'),
                                           (8, 'Situación Laboral'), (7, 'Ciudad'),
                                           (10, 'Industria 1'), (11, 'Empresa 1'),
                                           (12, 'Industria 2'), (13, 'Empresa 2'),
                                           (14, 'Industria 3'), (15, 'Empresa 3'),
                                           (5, 'Teléfono'), (6, 'Email')]:
                        if len(row) > col_idx and pd.notna(row.iloc[col_idx]):
                            val = str(row.iloc[col_idx]).strip()
                            if val and val.lower() not in ['nan', 'none', '', 'no']:
                                extras.append(f"- {label}: {val}")
                    if extras:
                        drive_info = "\nDATOS ENCONTRADOS EN BASE DE DATOS PROFESIONAL:\n" + "\n".join(extras)
                    break
    except Exception as e:
        logger.debug(f"Error buscando datos Drive para CV: {e}")
    
    # ===== PASO 3: Obtener stats (generación, antigüedad, recomendaciones) =====
    stats = obtener_stats_tarjeta(user_id)
    stats_info = ""
    if stats['generacion']:
        stats_info += f"- Generación Escuela Naval: {stats['generacion']}\n"
    if stats['antiguedad'] != '0,0':
        stats_info += f"- Antigüedad en red profesional: {stats['antiguedad']} años\n"
    if stats['recomendaciones'] > 0:
        stats_info += f"- Recomendaciones profesionales recibidas: {stats['recomendaciones']}\n"
    if stats['referidos'] > 0:
        stats_info += f"- Profesionales referidos a la red: {stats['referidos']}\n"
    
    # ===== PASO 4: Obtener recomendaciones textuales =====
    recs_info = ""
    try:
        conn2 = get_db_connection()
        if conn2:
            c2 = conn2.cursor()
            if DATABASE_URL:
                c2.execute("SELECT autor_nombre, texto FROM recomendaciones WHERE destinatario_id = %s ORDER BY id DESC LIMIT 3", (user_id,))
            else:
                c2.execute("SELECT autor_nombre, texto FROM recomendaciones WHERE destinatario_id = ? ORDER BY id DESC LIMIT 3", (user_id,))
            recs = c2.fetchall()
            if recs:
                recs_list = []
                for r in recs:
                    autor = (r['autor_nombre'] if DATABASE_URL else r[0]) or 'Cofrade'
                    texto = (r['texto'] if DATABASE_URL else r[1]) or ''
                    if texto:
                        recs_list.append(f"  - {autor}: \"{texto[:120]}\"")
                if recs_list:
                    recs_info = "\nRECOMENDACIONES PROFESIONALES RECIBIDAS:\n" + "\n".join(recs_list)
            conn2.close()
    except:
        pass
    
    # ===== PASO 5: Buscar info de LinkedIn si URL disponible =====
    linkedin_info = ""
    linkedin_url = tarjeta.get('linkedin', '')
    if linkedin_url:
        linkedin_info = f"\nPERFIL LINKEDIN: {linkedin_url}\n(Considerar el perfil LinkedIn como fuente de información real del profesional)"
    
    await msg.edit_text("📄 Generando CV profesional con IA...")
    
    try:
        prompt = f"""Genera un Currículum Vitae PROFESIONAL de alto impacto en español.
Diseñado para atraer reclutadores y headhunters. ESTRICTAMENTE BASADO EN DATOS REALES.
{f'ORIENTACIÓN: Optimizado para postular a: {orientacion}' if orientacion else ''}

===== DATOS REALES PROPORCIONADOS =====
Nombre completo: {tarjeta.get('nombre', 'No disponible')}
Cargo/Profesión actual: {tarjeta.get('profesion', 'No disponible')}
Empresa actual: {tarjeta.get('empresa', 'No disponible')}
Servicios/Especialidades: {tarjeta.get('servicios', 'No disponible')}
Ciudad: {tarjeta.get('ciudad', 'Chile')}
Teléfono: {tarjeta.get('telefono', '')}
Email: {tarjeta.get('email', '')}
LinkedIn: {tarjeta.get('linkedin', '')}
{stats_info}
{drive_info}
{recs_info}

===== ESTRUCTURA OBLIGATORIA =====

ENCABEZADO
Nombre, ciudad, teléfono, email, LinkedIn.

PERFIL PROFESIONAL (3-4 líneas)
Resumen ejecutivo basado EXCLUSIVAMENTE en los datos proporcionados: cargo actual,
empresa, servicios, industria, ciudad. Incluir formación naval si hay generación.

COMPETENCIAS CLAVE
8-10 competencias derivadas lógicamente de: cargo actual + servicios + industria.
Solo habilidades coherentes con el perfil real.

EXPERIENCIA PROFESIONAL
- Posición actual: cargo + empresa proporcionados. Generar 3-4 logros REALISTAS
  basados en el tipo de cargo (no inventar nombres de proyectos ni cifras exactas).
- Si los datos de BD incluyen Empresa 1/2/3 e Industrias, usar esos datos reales
  para crear posiciones anteriores con logros coherentes.
- Si NO hay datos de empresas anteriores, incluir SOLO la posición actual.
  NO INVENTAR empresas, fechas ni posiciones que no estén en los datos.

FORMACIÓN ACADÉMICA
- Escuela Naval "Arturo Prat" - Oficial de Marina{f' (Generación {stats.get("generacion", "")})' if stats.get('generacion') else ''}
- Si los datos de BD incluyen universidad/formación/postgrado, incluirlos textualmente.
- Si NO hay datos de formación civil, escribir SOLAMENTE la Escuela Naval.
  NO INVENTAR universidades, carreras ni títulos.

CERTIFICACIONES Y DESARROLLO
- Si los datos de BD incluyen certificaciones, listarlas.
- Si NO hay datos, NO inventar. Omitir esta sección o escribir:
  "Disponible para compartir certificaciones relevantes al cargo."

IDIOMAS
Español nativo. Si hay datos de idiomas en BD, incluirlos. Si no, omitir sección.

INFORMACIÓN ADICIONAL
- Miembro de Cofradía de Networking - Red Profesional de Ex-cadetes y Oficiales de la Armada de Chile.
{f'- Recomendado por {stats["recomendaciones"]} profesionales de la red.' if stats.get('recomendaciones', 0) > 0 else ''}
{f'- Ha referido {stats["referidos"]} profesionales a la comunidad.' if stats.get('referidos', 0) > 0 else ''}

REGLAS ABSOLUTAS:
- NO uses asteriscos, negritas ni markdown. Usa MAYÚSCULAS para títulos.
- Usa guiones (-) para listas.
- PROHIBIDO INVENTAR: universidades, títulos, empresas anteriores, certificaciones,
  nombres de proyectos, cifras exactas de facturación, o cualquier dato no proporcionado.
- Los logros deben ser genéricos pero profesionales (ej: "Optimicé procesos operativos
  logrando mejoras significativas en eficiencia") NO cifras inventadas.
- Si una sección no tiene datos reales, OMÍTELA o indica brevemente que está disponible.
- Redacción orientada a ATS. Lenguaje ejecutivo. Máximo 2 páginas."""
        cv = llamar_groq(prompt, max_tokens=3000, temperature=0.4)
        if cv:
            fuentes = ["Tarjeta profesional"]
            if drive_info:
                fuentes.append("Base de datos Drive")
            if stats.get('generacion') or stats.get('antiguedad', '0,0') != '0,0':
                fuentes.append("Stats comunidad")
            if recs_info:
                fuentes.append(f"{stats['recomendaciones']} recomendaciones")
            fuentes_txt = " + ".join(fuentes)
            nota_linkedin = ""
            if tarjeta.get('linkedin', ''):
                nota_linkedin = f"\n🔗 LinkedIn: {tarjeta['linkedin']} (verifica que tu perfil esté actualizado)"
            await msg.edit_text(
                f"📄 CV PROFESIONAL\n{'━' * 30}\n\n{cv}\n\n{'━' * 30}\n"
                f"📊 Fuentes utilizadas: {fuentes_txt}{nota_linkedin}\n"
                f"💡 Revisa y personaliza los detalles antes de enviar a reclutadores."
            )
            registrar_servicio_usado(user_id, 'generar_cv')
        else:
            await msg.edit_text("❌ Error generando CV.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


# ==================== NIVEL 2: SIMULADOR ENTREVISTAS (premium) ====================

@requiere_suscripcion
async def entrevista_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /entrevista [cargo] - Simulador de entrevista laboral"""
    user_id = update.effective_user.id
    cargo = ' '.join(context.args) if context.args else ''
    if not cargo:
        await update.message.reply_text(
            "🎯 SIMULADOR DE ENTREVISTA\n\n"
            "Uso: /entrevista [cargo al que postulas]\n\n"
            "Ejemplos:\n"
            "/entrevista Gerente de Logística\n"
            "/entrevista Analista Financiero Senior\n"
            "/entrevista Director de Operaciones"
        )
        return
    if user_id != OWNER_ID:
        precio = get_precio_servicio('entrevista')
        if precio:
            coins = get_coins_balance(user_id)
            if coins['balance'] >= precio['coins']:
                if not gastar_coins(user_id, precio['coins'], 'Servicio: entrevista'):
                    await update.message.reply_text("❌ Error procesando coins.")
                    return
                await update.message.reply_text(f"✅ {precio['coins']} Coins descontados.")
            else:
                faltan = precio['coins'] - coins['balance']
                await update.message.reply_text(
                    f"💎 SERVICIO PREMIUM: Simulador de entrevista\n\n"
                    f"💰 Precio: ${precio['pesos']:,} CLP o {precio['coins']} Coins\n"
                    f"🪙 Tu balance: {coins['balance']} Coins (faltan {faltan})\n\n"
                    f"💡 /mis_coins para ver cómo ganar más."
                )
                return
    msg = await update.message.reply_text(f"🎯 Preparando entrevista para: {cargo}...")
    try:
        prompt = f"""Simula una entrevista laboral profesional para: {cargo}.
Genera 5 preguntas de entrevista (de básica a desafiante). Para cada una incluye:
- La pregunta del entrevistador
- Guía de respuesta ideal
- Un tip práctico
Español, profesional, realista. No uses asteriscos."""
        resp = llamar_groq(prompt, max_tokens=1500, temperature=0.6)
        if resp:
            await msg.edit_text(f"🎯 SIMULADOR DE ENTREVISTA\n📋 Cargo: {cargo}\n{'━' * 30}\n\n{resp}")
            registrar_servicio_usado(user_id, 'entrevista')
        else:
            await msg.edit_text("❌ Error generando entrevista.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


# ==================== NIVEL 2: ANÁLISIS LINKEDIN (premium) ====================

@requiere_suscripcion
async def analisis_linkedin_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /analisis_linkedin - Análisis de perfil profesional con IA"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        precio = get_precio_servicio('analisis_linkedin')
        if precio:
            coins = get_coins_balance(user_id)
            if coins['balance'] >= precio['coins']:
                if not gastar_coins(user_id, precio['coins'], 'Servicio: analisis_linkedin'):
                    await update.message.reply_text("❌ Error procesando coins.")
                    return
                await update.message.reply_text(f"✅ {precio['coins']} Coins descontados.")
            else:
                faltan = precio['coins'] - coins['balance']
                await update.message.reply_text(
                    f"💎 SERVICIO PREMIUM: Análisis LinkedIn\n\n"
                    f"💰 Precio: ${precio['pesos']:,} CLP o {precio['coins']} Coins\n"
                    f"🪙 Tu balance: {coins['balance']} Coins (faltan {faltan})\n\n"
                    f"💡 /mis_coins para ver cómo ganar más."
                )
                return
    conn = get_db_connection()
    tarjeta = {}
    if conn:
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
        t = c.fetchone()
        if t:
            tarjeta = {'nombre': t['nombre_completo'] if DATABASE_URL else t[1],
                       'profesion': t['profesion'] if DATABASE_URL else t[2],
                       'empresa': t['empresa'] if DATABASE_URL else t[3],
                       'servicios': t['servicios'] if DATABASE_URL else t[4],
                       'linkedin': t['linkedin'] if DATABASE_URL else t[8]}
        conn.close()
    if not tarjeta:
        await update.message.reply_text("❌ Crea tu tarjeta con /mi_tarjeta primero.")
        return
    msg = await update.message.reply_text("🔍 Analizando tu perfil profesional...")
    try:
        prompt = f"""Eres experto en LinkedIn y marca personal. Analiza este perfil:
Nombre: {tarjeta.get('nombre','')}, Profesión: {tarjeta.get('profesion','')},
Empresa: {tarjeta.get('empresa','')}, Servicios: {tarjeta.get('servicios','')},
LinkedIn: {tarjeta.get('linkedin','')}
Genera: 1) Fortalezas del perfil, 2) Headline sugerido (120 chars),
3) Resumen/About sugerido (3-4 oraciones), 4) 5 palabras clave a incluir,
5) 3 acciones concretas para mejorar visibilidad. No uses asteriscos."""
        resp = llamar_groq(prompt, max_tokens=1200, temperature=0.6)
        if resp:
            await msg.edit_text(f"🔍 ANÁLISIS DE PERFIL PROFESIONAL\n{'━' * 30}\n\n{resp}")
            registrar_servicio_usado(user_id, 'analisis_linkedin')
        else:
            await msg.edit_text("❌ Error en el análisis.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


# ==================== NIVEL 3: DASHBOARD PERSONAL (premium) ====================

@requiere_suscripcion
async def mi_dashboard_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mi_dashboard - Dashboard personal de networking (GRATIS)"""
    user_id = update.effective_user.id
    msg = await update.message.reply_text("📊 Generando tu dashboard...")
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error de conexión")
            return
        c = conn.cursor()
        data = {}
        if DATABASE_URL:
            c.execute("SELECT COUNT(*) as t FROM mensajes WHERE user_id = %s", (str(user_id),))
            data['mensajes'] = c.fetchone()['t']
            c.execute("SELECT COUNT(*) as t FROM recomendaciones WHERE autor_id = %s", (user_id,))
            data['recs_dadas'] = c.fetchone()['t']
            c.execute("SELECT COUNT(*) as t FROM recomendaciones WHERE destinatario_id = %s", (user_id,))
            data['recs_recibidas'] = c.fetchone()['t']
            c.execute("SELECT COUNT(*) as t FROM respuestas_consultas WHERE user_id = %s", (user_id,))
            data['consultas_resp'] = c.fetchone()['t']
            c.execute("SELECT COUNT(*) as t FROM eventos_asistencia WHERE user_id = %s", (user_id,))
            data['eventos'] = c.fetchone()['t']
            c.execute("SELECT COUNT(*) as t FROM anuncios WHERE user_id = %s", (user_id,))
            data['anuncios'] = c.fetchone()['t']
            c.execute("""SELECT COUNT(*) + 1 as rank FROM
                        (SELECT user_id, COUNT(*) as msgs FROM mensajes GROUP BY user_id
                        HAVING COUNT(*) > (SELECT COUNT(*) FROM mensajes WHERE user_id = %s)) sub""", (str(user_id),))
            data['ranking'] = c.fetchone()['rank']
        else:
            c.execute("SELECT COUNT(*) FROM mensajes WHERE user_id = ?", (str(user_id),))
            data['mensajes'] = c.fetchone()[0]
            data['recs_dadas'] = data['recs_recibidas'] = data['consultas_resp'] = data['eventos'] = data['anuncios'] = 0
            data['ranking'] = '?'
        conn.close()
        trust = calcular_trust_score(user_id)
        coins_info = get_coins_balance(user_id)
        nombre = f"{update.effective_user.first_name or ''} {update.effective_user.last_name or ''}".strip()
        d = f"📊 DASHBOARD DE NETWORKING\n{'━' * 30}\n"
        d += f"👤 {nombre}\n"
        d += f"🏅 Trust Score: {trust['score']}/100 {trust['nivel']}\n"
        d += f"🪙 Cofradía Coins: {coins_info['balance']}\n"
        d += f"🏆 Ranking: #{data['ranking']}\n\n"
        d += f"📈 MÉTRICAS\n"
        d += f"  💬 Mensajes: {data['mensajes']}\n"
        d += f"  ⭐ Recomendaciones dadas: {data['recs_dadas']}\n"
        d += f"  ⭐ Recomendaciones recibidas: {data['recs_recibidas']}\n"
        d += f"  💬 Consultas respondidas: {data['consultas_resp']}\n"
        d += f"  📅 Eventos asistidos: {data['eventos']}\n"
        d += f"  📢 Anuncios publicados: {data['anuncios']}\n\n"
        d += f"🎯 TRUST SCORE DETALLE\n"
        for k, v in trust['detalles'].items():
            d += f"  {k}: {v}/20 pts\n"
        await msg.edit_text(d)
        registrar_servicio_usado(user_id, 'mi_dashboard')
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


# ==================== NIVEL 3: REPORTE MERCADO LABORAL ====================

async def generar_reporte_laboral(context):
    """Job semanal: reporte de mercado laboral (viernes 10AM Chile)"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        profesiones = ciudades = categorias = []
        total_tarjetas = 0
        if DATABASE_URL:
            c.execute("SELECT profesion, COUNT(*) as total FROM tarjetas_profesional WHERE profesion IS NOT NULL GROUP BY profesion ORDER BY total DESC LIMIT 5")
            profesiones = [(r['profesion'], r['total']) for r in c.fetchall()]
            c.execute("SELECT ciudad, COUNT(*) as total FROM tarjetas_profesional WHERE ciudad IS NOT NULL GROUP BY ciudad ORDER BY total DESC LIMIT 5")
            ciudades = [(r['ciudad'], r['total']) for r in c.fetchall()]
            c.execute("SELECT categoria, COUNT(*) as total FROM anuncios WHERE activo = TRUE GROUP BY categoria ORDER BY total DESC")
            categorias = [(r['categoria'], r['total']) for r in c.fetchall()]
            c.execute("SELECT COUNT(*) as t FROM tarjetas_profesional")
            total_tarjetas = c.fetchone()['t']
        conn.close()
        rpt = f"📈 REPORTE LABORAL SEMANAL\n{'━' * 30}\n📅 {datetime.now().strftime('%d/%m/%Y')}\n\n"
        if profesiones:
            rpt += "🏢 PROFESIONES MÁS REPRESENTADAS\n"
            for p, t in profesiones:
                rpt += f"  {p}: {t} cofrades\n"
            rpt += "\n"
        if ciudades:
            rpt += "📍 CIUDADES CON MÁS PROFESIONALES\n"
            for ci, t in ciudades:
                rpt += f"  {ci}: {t}\n"
            rpt += "\n"
        if categorias:
            rpt += "📢 ANUNCIOS ACTIVOS POR CATEGORÍA\n"
            for ca, t in categorias:
                rpt += f"  {ca}: {t}\n"
            rpt += "\n"
        rpt += f"👥 Total directorio: {total_tarjetas} tarjetas\n\n"
        rpt += "💡 /directorio para buscar profesionales"
        if COFRADIA_GROUP_ID:
            await context.bot.send_message(chat_id=COFRADIA_GROUP_ID, text=rpt)
            logger.info("📈 Reporte laboral semanal enviado")
    except Exception as e:
        logger.error(f"Error reporte laboral: {e}")


# ==================== NIVEL 5: MENTORÍA IA (premium) ====================

@requiere_suscripcion
async def mentor_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mentor - Plan de mentoría IA personalizado"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        precio = get_precio_servicio('mentor')
        if precio:
            coins = get_coins_balance(user_id)
            if coins['balance'] >= precio['coins']:
                if not gastar_coins(user_id, precio['coins'], 'Servicio: mentor'):
                    await update.message.reply_text("❌ Error procesando coins.")
                    return
                await update.message.reply_text(f"✅ {precio['coins']} Coins descontados.")
            else:
                faltan = precio['coins'] - coins['balance']
                await update.message.reply_text(
                    f"💎 SERVICIO PREMIUM: Mentoría IA\n\n"
                    f"💰 Precio: ${precio['pesos']:,} CLP o {precio['coins']} Coins\n"
                    f"🪙 Tu balance: {coins['balance']} Coins (faltan {faltan})\n\n"
                    f"💡 /mis_coins para ver cómo ganar más."
                )
                return
    conn = get_db_connection()
    tarjeta = {}
    if conn:
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
        else:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
        t = c.fetchone()
        if t:
            tarjeta = {'nombre': t['nombre_completo'] if DATABASE_URL else t[1],
                       'profesion': t['profesion'] if DATABASE_URL else t[2],
                       'empresa': t['empresa'] if DATABASE_URL else t[3],
                       'servicios': t['servicios'] if DATABASE_URL else t[4]}
        conn.close()
    msg = await update.message.reply_text("🎓 Generando plan de mentoría personalizado...")
    try:
        trust = calcular_trust_score(user_id)
        busq = tarjeta.get('profesion', 'liderazgo desarrollo profesional')
        resultados_rag = busqueda_unificada(busq, limit_historial=0, limit_rag=10)
        contexto_rag = formatear_contexto_unificado(resultados_rag, busq)
        prompt = f"""Eres un mentor ejecutivo de alto nivel. Genera un PLAN DE DESARROLLO PROFESIONAL.
PERFIL: {tarjeta.get('nombre', update.effective_user.first_name)}, {tarjeta.get('profesion','No especificada')},
Empresa: {tarjeta.get('empresa','No especificada')}, Habilidades: {tarjeta.get('servicios','')},
Trust Score: {trust['score']}/100 ({trust['nivel']})
LIBROS DISPONIBLES: {contexto_rag}
Genera: 1) DIAGNÓSTICO breve, 2) 3 metas a 6 meses, 3) 4 tareas concretas esta semana,
4) 3 libros de la biblioteca con razón, 5) 3 acciones de networking en Cofradía.
Concreto, práctico, motivador. No uses asteriscos."""
        resp = llamar_groq(prompt, max_tokens=1500, temperature=0.6)
        if resp:
            await msg.edit_text(f"🎓 PLAN DE MENTORÍA PERSONALIZADO\n{'━' * 30}\n\n{resp}")
            registrar_servicio_usado(user_id, 'mentor')
        else:
            await msg.edit_text("❌ Error generando plan.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


# ==================== NIVEL 5: COFRADÍA COINS COMANDOS ====================

@requiere_suscripcion
async def mis_coins_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mis_coins - Ver balance y servicios canjeables"""
    user_id = update.effective_user.id
    coins = get_coins_balance(user_id)
    servicios = []
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT servicio, precio_coins, precio_pesos, descripcion FROM precios_servicios WHERE activo = TRUE ORDER BY precio_coins")
            else:
                c.execute("SELECT servicio, precio_coins, precio_pesos, descripcion FROM precios_servicios WHERE activo = 1 ORDER BY precio_coins")
            servicios = c.fetchall()
            conn.close()
    except:
        pass
    msg = f"🪙 TUS COFRADÍA COINS\n{'━' * 28}\n\n"
    msg += f"💰 Balance actual: {coins['balance']} Coins\n"
    msg += f"📈 Total ganado: {coins['total_ganado']}\n"
    msg += f"📉 Total gastado: {coins['total_gastado']}\n\n"
    if servicios:
        msg += "🛒 SERVICIOS CANJEABLES\n\n"
        for s in servicios:
            srv = s['servicio'] if DATABASE_URL else s[0]
            precio_c = s['precio_coins'] if DATABASE_URL else s[1]
            precio_p = s['precio_pesos'] if DATABASE_URL else s[2]
            desc = s['descripcion'] if DATABASE_URL else s[3]
            if coins['balance'] >= precio_c:
                msg += f"  ✅ /{srv}: {precio_c} coins\n     {desc}\n\n"
            else:
                faltan = precio_c - coins['balance']
                msg += f"  🔒 /{srv}: {precio_c} coins (faltan {faltan})\n     {desc} (${precio_p:,} CLP)\n\n"
    msg += "💡 CÓMO GANAR COINS\n"
    msg += "  💬 Mensaje en grupo: +1 coin\n"
    msg += "  💡 Responder consulta: +10 coins\n"
    msg += "  ⭐ Recomendar cofrade: +5 coins\n"
    msg += "  📅 Asistir evento: +20 coins\n"
    msg += "  📇 Crear tarjeta: +15 coins\n"
    msg += "  💰 Consulta financiera: +1 coin\n"
    msg += "  🔍 Búsqueda IA: +1 coin\n"
    await enviar_mensaje_largo(update, msg)


# ==================== NIVEL 4: NEWSLETTER EMAIL (placeholder) ====================

async def generar_newsletter_email(context):
    """Job: Newsletter por email (requiere SENDGRID_API_KEY en env vars)"""
    sendgrid_key = os.environ.get('SENDGRID_API_KEY', '')
    if not sendgrid_key:
        return
    # Se activa automáticamente cuando se configure la variable de entorno
    logger.info("📧 Newsletter email: se ejecutará con SendGrid configurado")


# ==================== ADMIN: PRECIOS Y COINS ====================

async def set_precio_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /set_precio [servicio] [pesos] [coins] - Admin: editar precios"""
    if update.effective_user.id != OWNER_ID:
        return
    if not context.args or len(context.args) < 3:
        try:
            conn = get_db_connection()
            servicios_txt = ""
            if conn:
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("SELECT servicio, precio_pesos, precio_coins FROM precios_servicios ORDER BY servicio")
                else:
                    c.execute("SELECT servicio, precio_pesos, precio_coins FROM precios_servicios ORDER BY servicio")
                for s in c.fetchall():
                    srv = s['servicio'] if DATABASE_URL else s[0]
                    pp = s['precio_pesos'] if DATABASE_URL else s[1]
                    pc = s['precio_coins'] if DATABASE_URL else s[2]
                    servicios_txt += f"  {srv}: ${pp:,} / {pc} coins\n"
                conn.close()
        except:
            servicios_txt = ""
        await update.message.reply_text(
            f"💰 EDITAR PRECIOS\n\n"
            f"Formato: /set_precio [servicio] [pesos] [coins]\n\n"
            f"PRECIOS ACTUALES:\n{servicios_txt}\n"
            f"Ejemplo: /set_precio generar_cv 7000 70"
        )
        return
    servicio = context.args[0].lower()
    try:
        pesos = int(context.args[1])
        coins = int(context.args[2])
    except:
        await update.message.reply_text("❌ Formato: /set_precio [servicio] [pesos] [coins]")
        return
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("UPDATE precios_servicios SET precio_pesos = %s, precio_coins = %s WHERE servicio = %s", (pesos, coins, servicio))
            else:
                c.execute("UPDATE precios_servicios SET precio_pesos = ?, precio_coins = ? WHERE servicio = ?", (pesos, coins, servicio))
            if c.rowcount == 0:
                await update.message.reply_text(f"❌ Servicio '{servicio}' no encontrado.")
            else:
                conn.commit()
                await update.message.reply_text(f"✅ Precio actualizado:\n{servicio}: ${pesos:,} CLP / {coins} Coins")
            conn.close()
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def dar_coins_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /dar_coins [user_id] [cantidad] - Admin: regalar coins"""
    if update.effective_user.id != OWNER_ID:
        return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Formato: /dar_coins [user_id] [cantidad]\nEjemplo: /dar_coins 123456789 100")
        return
    try:
        target_id = int(context.args[0])
        cantidad = int(context.args[1])
    except:
        await update.message.reply_text("❌ Formato: /dar_coins [user_id] [cantidad]")
        return
    if otorgar_coins(target_id, cantidad, f'Regalo admin'):
        await update.message.reply_text(f"✅ {cantidad} Coins otorgados a {target_id}")
    else:
        await update.message.reply_text("❌ Error otorgando coins.")


# ==================== SISTEMA DE ONBOARDING ====================

MENSAJE_BIENVENIDA = """⚓ Bienvenido/a <b>{nombre} {apellido}</b>, Generación <b>{generacion}</b>, pasas a formar parte de este selecto grupo de camaradas, donde prima la sana convivencia y la ayuda colectiva en materia laboral. Es importante que cada uno se presente para conocerlos y saber a qué se dedican...

Comparte tus datos de contacto, tu situación laboral y el área en que te desenvuelves laboralmente en la planilla alojada en el siguiente link, donde varios ya ingresamos nuestros datos.

https://docs.google.com/spreadsheets/d/1Py6I68tWZBSBH6koDo1JMf4U5QFvwI31/edit?usp=sharing&ouid=105662138872486212877&rtpof=true&sd=true

Luego, quienes se encuentran en una etapa de "Búsqueda Laboral" (o etapa de Transición) podrán subir su CV en la siguiente carpeta, de manera que el resto del grupo pueda compartirlo entre su red de contactos y su círculo más cercano. Pero además, quienes están actualmente "Con Contrato Laboral" o son "Independientes" también pueden subir sus CV y el de sus cónyuges (si así lo desean), de manera de tener a la vista el área de especialización de cada uno!

https://drive.google.com/drive/folders/1in_JhEy5h19e2F0ShCl3gglx8rnETVHP?usp=sharing

Este chat es sólo de Marinos activos y retirados, cuyo único propósito es formar una red laboral virtuosa, para fomentar el apoyo colectivo entre todos quienes la integran...
Nuestro grupo promueve valores como la amistad, la sana convivencia, respecto hacia los demás y un apoyo genuino y desinteresado para colaborar en el grupo. En consecuencia, no existe espacio para otros temas (como la pornografía, chistes o comentarios políticos) lo que conllevaría a perder nuestro foco central… 😃👍🏻 como nos enseñaron en nuestra querida Escuela Naval (Todos a una!).

<b>DECÁLOGO DE COFRADÍA</b>

1. Somos una Cofradía de Networking y eso nos debe unir siempre
2. Nuestro foco es 100% Laboral y ampliar redes
3. No seamos spam!... No enviemos campañas ni cadenas
4. Respeto y Tolerancia con otras opiniones… No criticar!
5. Si la conversación es para un miembro del grupo, háblalo en privado!
6. NO hablar de política, religión, futbol u otra temática que nos divida
7. Lo que se habla aquí, se queda aquí... ese es nuestro casco de presión!
8. Compartamos buenas prácticas, ideas, consejos y fortalezas
9. Destaquemos los éxitos… Se felicita en público y se critica en privado!
10. Si no compartes tus datos en la Planilla es probable que no necesites ayuda!
11. NO romper estas reglas, debemos cuidarnos y el administrador velará por eso!
12. Si buscas un determinado Perfil Profesional, o indagas en algún Producto o Servicio, que tu búsqueda inicial sea dentro de Cofradía.
13. La Colaboración y la Reciprocidad son dos conceptos fundamentales para alcanzar el éxito! ✨🙌🏻 Porque la Colaboración permite que a todos nos vaya bien, y la Reciprocidad es la valoración y la gratitud a quienes han aportado a nuestro éxito (tal como: Un like en Linkedin; Compartir una publicación; etc.). El Santo padre Juan Pablo II decía: "Nadie es tan pobre que no tenga algo que dar, ni nadie es tan rico que no tenga algo que recibir".

<b>NUESTROS VALORES &amp; PRINCIPIOS</b>

Participamos de un grupo de camaradas, cuyo único propósito es apoyar, colaborar y dar soporte a otros en materia laboral... Nuestra motivación es ver que todos puedan desarrollarse en su campo de acción y logren la anhelada estabilidad. Nuestros principios que nos define son:

1. Consigna: Facilitar la movilidad laboral y contribuir en dar apoyo a otras empresas relacionadas
2. Mística: Nuestros valores y amistad nos sensibilizan con las necesidades de nuestros camaradas
3. Virtud: La colaboración circular nos transforma en un grupo virtuoso, para generar un bien común
4. Redes: La sinergía entre todos contribuye a crear más redes de apoyo, confianza y la cooperación.

🤖 Consulta al Asistente IA de Cofradía: @Cofradia_Premium_Bot"""


async def manejar_solicitud_ingreso(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja solicitudes de ingreso via ChatJoinRequest (fallback si enlace tiene aprobación activa)"""
    join_request = update.chat_join_request
    user = join_request.from_user
    
    logger.info(f"📨 ChatJoinRequest recibido: {user.first_name} {user.last_name or ''} (ID: {user.id})")
    
    # Verificar si ya completó el onboarding
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT estado FROM nuevos_miembros WHERE user_id = %s AND estado = 'aprobado' LIMIT 1", (user.id,))
            else:
                c.execute("SELECT estado FROM nuevos_miembros WHERE user_id = ? AND estado = 'aprobado' LIMIT 1", (user.id,))
            resultado = c.fetchone()
            conn.close()
            
            if resultado:
                # Ya fue aprobado por el bot, aprobar automáticamente
                try:
                    await context.bot.approve_chat_join_request(
                        chat_id=join_request.chat.id,
                        user_id=user.id
                    )
                    logger.info(f"✅ Auto-aprobado (ya verificado): {user.first_name}")
                except Exception as e:
                    logger.warning(f"Error auto-aprobando: {e}")
                return
    except Exception as e:
        logger.warning(f"Error verificando miembro: {e}")
    
    # No fue aprobado aún - notificar al owner
    try:
        await context.bot.send_message(
            chat_id=OWNER_ID,
            text=f"📨 SOLICITUD DIRECTA AL GRUPO\n\n"
                 f"👤 {user.first_name} {user.last_name or ''}\n"
                 f"📱 @{user.username or 'sin_username'}\n"
                 f"🆔 ID: {user.id}\n\n"
                 f"⚠️ Este usuario solicitó ingresar directamente por el enlace.\n"
                 f"No completó las 3 preguntas del bot.\n\n"
                 f"Pídele que primero escriba a @Cofradia_Premium_Bot\n"
                 f"o aprueba manualmente: /aprobar_solicitud {user.id}"
        )
    except:
        pass


async def onboard_nombre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recibe el nombre del solicitante - requiere mínimo 3 palabras"""
    if not context.user_data.get('onboard_activo'):
        return ConversationHandler.END
    
    nombre_completo = update.message.text.strip()
    
    # Validar mínimo 3 palabras (nombre + 2 apellidos)
    partes = nombre_completo.split()
    if len(partes) < 3:
        await update.message.reply_text(
            "❌ Por favor ingresa tu nombre completo con al menos 3 palabras:\n"
            "Nombre + Apellido paterno + Apellido materno\n\n"
            "Ejemplo: Juan Carlos Pérez González\n\n"
            "📝 Pregunta 1 de 6:\n"
            "¿Cuál es tu Nombre y Apellido completo?"
        )
        return ONBOARD_NOMBRE
    
    # Separar nombre y apellidos
    nombre = partes[0]
    apellido = ' '.join(partes[1:])
    
    context.user_data['onboard_nombre'] = nombre
    context.user_data['onboard_apellido'] = apellido
    context.user_data['onboard_nombre_completo'] = nombre_completo
    
    await update.message.reply_text(
        f"✅ Gracias, {nombre}!\n\n"
        f"📝 Pregunta 2 de 6:\n"
        f"¿A qué Generación perteneces? (Año de Guardiamarina, ingresa 4 dígitos)\n\n"
        f"Ejemplo: 1995"
    )
    
    return ONBOARD_GENERACION


async def onboard_generacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recibe la generación del solicitante"""
    texto = update.message.text.strip()
    
    # Validar que sean 4 dígitos
    if not texto.isdigit() or len(texto) != 4:
        await update.message.reply_text(
            "❌ Por favor ingresa un año válido de 4 dígitos.\n"
            "Ejemplo: 1995"
        )
        return ONBOARD_GENERACION
    
    anio = int(texto)
    if anio < 1950 or anio > 2025:
        await update.message.reply_text(
            "❌ El año debe estar entre 1950 y 2025.\n"
            "Ejemplo: 1995"
        )
        return ONBOARD_GENERACION
    
    context.user_data['onboard_generacion'] = texto
    
    await update.message.reply_text(
        f"✅ Generación {texto}!\n\n"
        f"📝 Pregunta 3 de 6:\n"
        f"¿Quién te recomendó el grupo Cofradía?"
    )
    
    return ONBOARD_RECOMENDADO


async def onboard_recomendado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recibe quién recomendó al solicitante — verifica contra BD, no permite autoreferencia"""
    import unicodedata as _ud
    recomendado = update.message.text.strip()
    recomendado_lower = recomendado.lower()
    
    # Palabras/frases prohibidas (respuestas evasivas o genéricas)
    frases_prohibidas = [
        'un amigo', 'un carreta', 'recomendado', 'whatsapp', 'whasapp', 'whats app',
        'no me acuerdo', 'no recuerdo', 'no sé', 'no se', 'no lo sé',
        'un oficial', 'un marino', 'un compañero', 'alguien', 'nadie',
        'no lo recuerdo', 'no tengo idea', 'un conocido', 'un amig',
        'otro marino', 'otro usuario'
    ]
    
    if '+' in recomendado:
        await update.message.reply_text(
            "❌ No se permiten signos especiales (+) en esta respuesta.\n\n"
            "Por favor indica el nombre y apellido de la persona que te recomendó.\n\n"
            "Ejemplo: Pedro González\n\n"
            "📝 Pregunta 3 de 6:\n"
            "¿Quién te recomendó el grupo Cofradía?"
        )
        return ONBOARD_RECOMENDADO
    
    for frase in frases_prohibidas:
        if frase in recomendado_lower:
            await update.message.reply_text(
                f"❌ No se aceptan respuestas genéricas como \"{frase}\".\n\n"
                f"Indica el nombre y apellido real de quien te recomendó.\n\n"
                f"Ejemplo: Pedro González\n\n"
                f"📝 Pregunta 3 de 6:\n"
                f"¿Quién te recomendó el grupo Cofradía?"
            )
            return ONBOARD_RECOMENDADO
    
    partes = recomendado.split()
    if len(partes) < 2:
        await update.message.reply_text(
            "❌ Por favor indica al menos un nombre y un apellido de quien te recomendó.\n\n"
            "Ejemplo: Pedro González\n\n"
            "📝 Pregunta 3 de 6:\n"
            "¿Quién te recomendó el grupo Cofradía?"
        )
        return ONBOARD_RECOMENDADO
    
    # --- VERIFICACIÓN: no autoreferirse ---
    mi_nombre = context.user_data.get('onboard_nombre', '').lower().strip()
    mi_apellido = context.user_data.get('onboard_apellido', '').lower().strip()
    def _qa(s):
        return ''.join(ch for ch in _ud.normalize('NFD', s.lower()) if _ud.category(ch) != 'Mn')
    rec_clean = _qa(recomendado)
    mi_full = f"{mi_nombre} {mi_apellido}".strip()
    mi_clean = _qa(mi_full)
    
    if mi_clean and len(mi_clean) > 3:
        if mi_clean in rec_clean or rec_clean in mi_clean:
            await update.message.reply_text(
                "❌ No puedes referenciarte a ti mismo.\n\n"
                "Debes indicar el nombre de OTRO miembro de Cofradía que te recomendó.\n\n"
                "📝 Pregunta 3 de 6:\n"
                "¿Quién te recomendó el grupo Cofradía?"
            )
            return ONBOARD_RECOMENDADO
    
    # --- BUSCAR coincidencias en la BD ---
    coincidencias = []
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            busqueda = f"%{recomendado_lower[:20]}%"
            if DATABASE_URL:
                c.execute("""SELECT DISTINCT first_name, last_name, user_id FROM suscripciones 
                           WHERE (LOWER(first_name || ' ' || COALESCE(last_name,'')) LIKE %s
                           OR LOWER(COALESCE(last_name,'')) LIKE %s)
                           AND estado = 'activo' LIMIT 8""", (busqueda, busqueda))
            else:
                c.execute("""SELECT DISTINCT first_name, last_name, user_id FROM suscripciones 
                           WHERE (LOWER(first_name || ' ' || COALESCE(last_name,'')) LIKE ?
                           OR LOWER(COALESCE(last_name,'')) LIKE ?)
                           AND estado = 'activo' LIMIT 8""", (busqueda, busqueda))
            for r in c.fetchall():
                fn = (r['first_name'] if DATABASE_URL else r[0]) or ''
                ln = (r['last_name'] if DATABASE_URL else r[1]) or ''
                uid = (r['user_id'] if DATABASE_URL else r[2])
                nombre_c = f"{fn} {ln}".strip()
                if nombre_c and uid != update.effective_user.id:
                    coincidencias.append(nombre_c)
            conn.close()
    except:
        pass
    
    context.user_data['onboard_recomendado'] = recomendado
    nombre = context.user_data.get('onboard_nombre', '')
    
    if coincidencias:
        lista = "\n".join([f"  ✅ {c}" for c in coincidencias[:5]])
        await update.message.reply_text(
            f"👥 Coincidencias encontradas:\n{lista}\n\n"
            f"✅ Registrado: {recomendado}"
        )
    
    # Pregunta 4: verificación naval
    keyboard = [
        [InlineKeyboardButton("a) Preparar legumbres durante la Navegación", callback_data="onboard_p4_a")],
        [InlineKeyboardButton("b) Saltar en paracaídas", callback_data="onboard_p4_b")],
        [InlineKeyboardButton("c) Poesía \"La Campana\"", callback_data="onboard_p4_c")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"✅ Gracias, {nombre}!\n\n"
        f"🔍 Pregunta 4 de 6:\n"
        f"¿Qué aprendiste durante el primer año en la Escuela Naval?\n\n"
        f"Selecciona una alternativa:",
        reply_markup=reply_markup
    )
    
    return ONBOARD_PREGUNTA4


async def onboard_cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancela el proceso de onboarding"""
    context.user_data['onboard_activo'] = False
    await update.message.reply_text("❌ Proceso de registro cancelado.\n\nSi deseas reiniciar, escribe /start")
    return ConversationHandler.END


async def onboard_pregunta4_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para pregunta 4 (La Campana) - respuesta correcta: c"""
    query = update.callback_query
    await query.answer()
    
    respuesta = query.data.replace("onboard_p4_", "")
    context.user_data['onboard_respuesta4'] = respuesta
    
    # Pregunta 5: verificación naval (selección múltiple)
    keyboard = [
        [InlineKeyboardButton("a) Fondo", callback_data="onboard_p5_a")],
        [InlineKeyboardButton("b) Diana", callback_data="onboard_p5_b")],
        [InlineKeyboardButton("c) Rancho", callback_data="onboard_p5_c")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if respuesta == "c":
        texto_feedback = "✅ Correcto!\n\n"
    else:
        texto_feedback = f"📝 Respuesta registrada.\n\n"
    
    await query.edit_message_text(
        f"{texto_feedback}"
        f"🔍 Pregunta 5 de 6:\n"
        f"¿Cuál es la primera formación del día?\n\n"
        f"Selecciona una alternativa:",
        reply_markup=reply_markup
    )
    
    return ONBOARD_PREGUNTA5


async def onboard_pregunta5_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para pregunta 5 (Diana) - respuesta correcta: b → pasa a pregunta 6"""
    query = update.callback_query
    await query.answer()
    
    respuesta = query.data.replace("onboard_p5_", "")
    context.user_data['onboard_respuesta5'] = respuesta
    
    if respuesta == "b":
        texto_feedback = "✅ Correcto!\n\n"
    else:
        texto_feedback = "📝 Respuesta registrada.\n\n"
    
    # Pregunta 6: Desayuno cadetes fines de semana (selección múltiple)
    keyboard = [
        [InlineKeyboardButton("a) Snack de algas + ración de combate 350 Grs.", callback_data="onboard_p6_a")],
        [InlineKeyboardButton("b) Batido energético y frutas", callback_data="onboard_p6_b")],
        [InlineKeyboardButton("c) Porridge y taza de leche", callback_data="onboard_p6_c")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"{texto_feedback}"
        f"🔍 Pregunta 6 de 6:\n"
        f"¿Cuál es el desayuno de los cadetes los fines de semana?\n\n"
        f"Selecciona una alternativa:",
        reply_markup=reply_markup
    )
    
    return ONBOARD_PREGUNTA6


async def onboard_pregunta6_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para pregunta 6 (Porridge) - respuesta correcta: c"""
    query = update.callback_query
    await query.answer()
    
    respuesta = query.data.replace("onboard_p6_", "")
    context.user_data['onboard_respuesta6'] = respuesta
    
    # Recopilar todos los datos
    nombre = context.user_data.get('onboard_nombre', '')
    apellido = context.user_data.get('onboard_apellido', '')
    generacion = context.user_data.get('onboard_generacion', '')
    recomendado = context.user_data.get('onboard_recomendado', '')
    user_id = context.user_data.get('onboard_user_id', query.from_user.id)
    username = context.user_data.get('onboard_username', '')
    resp4 = context.user_data.get('onboard_respuesta4', '')
    resp5 = context.user_data.get('onboard_respuesta5', '')
    resp6 = respuesta
    
    # Evaluar respuestas de verificación
    p4_correcta = (resp4 == "c")  # La Campana
    p5_correcta = (resp5 == "b")  # Diana
    p6_correcta = (resp6 == "c")  # Porridge y taza de leche
    verificacion_ok = p4_correcta and p5_correcta and p6_correcta
    
    # Guardar en base de datos
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("""INSERT INTO nuevos_miembros 
                            (user_id, username, nombre, apellido, generacion, recomendado_por)
                            VALUES (%s, %s, %s, %s, %s, %s)""",
                         (user_id, username, nombre, apellido, generacion, recomendado))
            else:
                c.execute("""INSERT INTO nuevos_miembros 
                            (user_id, username, nombre, apellido, generacion, recomendado_por)
                            VALUES (?, ?, ?, ?, ?, ?)""",
                         (user_id, username, nombre, apellido, generacion, recomendado))
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"Error guardando nuevo miembro: {e}")
    
    # Confirmar al usuario
    if resp6 == "c":
        texto_feedback = "✅ Correcto!\n\n"
    else:
        texto_feedback = "📝 Respuesta registrada.\n\n"
    
    await query.edit_message_text(
        f"{texto_feedback}"
        f"✅ ¡Gracias {nombre}! Tu solicitud ha sido enviada al administrador.\n\n"
        f"📋 Resumen:\n"
        f"👤 Nombre: {nombre} {apellido}\n"
        f"⚓ Generación: {generacion}\n"
        f"👥 Recomendado por: {recomendado}\n\n"
        f"⏳ Recibirás una notificación cuando tu solicitud sea aprobada."
    )
    
    # Mapear letras a textos para el owner
    opciones_p4 = {"a": "Preparar legumbres", "b": "Saltar en paracaídas", "c": "La Campana ✅"}
    opciones_p5 = {"a": "Fondo", "b": "Diana ✅", "c": "Rancho"}
    opciones_p6 = {"a": "Snack algas + ración combate", "b": "Batido energético", "c": "Porridge y leche ✅"}
    
    # Enviar al owner para aprobar
    verificacion_texto = "✅ APROBÓ" if verificacion_ok else "❌ FALLÓ"
    try:
        await context.bot.send_message(
            chat_id=OWNER_ID,
            text=f"📨 NUEVA SOLICITUD DE INGRESO\n"
                 f"{'━' * 30}\n\n"
                 f"👤 Nombre: {nombre} {apellido}\n"
                 f"⚓ Generación: {generacion}\n"
                 f"👥 Recomendado por: {recomendado}\n"
                 f"🆔 User ID: {user_id}\n"
                 f"📱 Username: @{username}\n\n"
                 f"🔍 VERIFICACIÓN NAVAL: {verificacion_texto}\n"
                 f"   P4 (Escuela Naval): {opciones_p4.get(resp4, resp4)} {'✅' if p4_correcta else '❌'}\n"
                 f"   P5 (Primera formación): {opciones_p5.get(resp5, resp5)} {'✅' if p5_correcta else '❌'}\n"
                 f"   P6 (Desayuno cadetes): {opciones_p6.get(resp6, resp6)} {'✅' if p6_correcta else '❌'}\n\n"
                 f"Para aprobar, usa:\n"
                 f"/aprobar_solicitud {user_id}"
        )
    except Exception as e:
        logger.error(f"Error notificando al owner: {e}")
    
    # Limpiar datos de conversación
    context.user_data['onboard_activo'] = False
    
    return ConversationHandler.END


async def detectar_nuevo_miembro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detecta cuando un nuevo miembro ingresa al grupo y envía bienvenida si fue aprobado"""
    if not update.message or not update.message.new_chat_members:
        return
    
    for miembro in update.message.new_chat_members:
        if miembro.is_bot:
            continue
        
        user_id = miembro.id
        logger.info(f"👤 Nuevo miembro detectado: {miembro.first_name} (ID: {user_id})")
        
        # Buscar si fue aprobado por el onboarding
        try:
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("""SELECT nombre, apellido, generacion FROM nuevos_miembros 
                                WHERE user_id = %s AND estado = 'aprobado' 
                                ORDER BY fecha_aprobacion DESC LIMIT 1""", (user_id,))
                    resultado = c.fetchone()
                else:
                    c.execute("""SELECT nombre, apellido, generacion FROM nuevos_miembros 
                                WHERE user_id = ? AND estado = 'aprobado' 
                                ORDER BY fecha_aprobacion DESC LIMIT 1""", (user_id,))
                    resultado = c.fetchone()
                conn.close()
                
                if resultado:
                    if DATABASE_URL:
                        nombre = resultado['nombre']
                        apellido = resultado['apellido']
                        generacion = resultado['generacion']
                    else:
                        nombre = resultado[0]
                        apellido = resultado[1]
                        generacion = resultado[2]
                    
                    # Enviar mensaje de bienvenida al grupo (HTML para negritas)
                    bienvenida = MENSAJE_BIENVENIDA.format(
                        nombre=nombre,
                        apellido=apellido,
                        generacion=generacion
                    )
                    
                    try:
                        await update.message.reply_text(bienvenida, parse_mode='HTML')
                    except Exception:
                        # Fallback sin formato si HTML falla
                        bienvenida_plain = bienvenida.replace('<b>', '').replace('</b>', '').replace('&amp;', '&')
                        await update.message.reply_text(bienvenida_plain)
                    logger.info(f"✅ Bienvenida enviada para {nombre} {apellido}")
                    
                    # Registrar suscripción si no la tiene
                    if not verificar_suscripcion_activa(user_id):
                        registrar_usuario_suscripcion(user_id, nombre, miembro.username or '', 
                                                     es_admin=False, last_name=apellido)
                else:
                    # No fue aprobado por onboarding - pedir que complete el proceso
                    nombre = f"{miembro.first_name or ''} {miembro.last_name or ''}".strip() or "Nuevo miembro"
                    await update.message.reply_text(
                        f"👋 Bienvenido/a {nombre} a Cofradía de Networking!\n\n"
                        f"Para activar tu cuenta y acceder a todos los servicios, "
                        f"completa tu registro en privado:\n\n"
                        f"👉 https://t.me/Cofradia_Premium_Bot?start=registro"
                    )
        except Exception as e:
            logger.error(f"Error en bienvenida nuevo miembro: {e}")


async def aprobar_solicitud_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /aprobar_solicitud [user_id] - Aprobar solicitud de ingreso"""
    if update.effective_user.id != OWNER_ID:
        return
    
    if not context.args:
        # Mostrar solicitudes pendientes
        try:
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("""SELECT user_id, nombre, apellido, generacion, recomendado_por, fecha_solicitud 
                                FROM nuevos_miembros WHERE estado = 'pendiente' 
                                ORDER BY fecha_solicitud DESC LIMIT 10""")
                    pendientes = c.fetchall()
                else:
                    c.execute("""SELECT user_id, nombre, apellido, generacion, recomendado_por, fecha_solicitud 
                                FROM nuevos_miembros WHERE estado = 'pendiente' 
                                ORDER BY fecha_solicitud DESC LIMIT 10""")
                    pendientes = c.fetchall()
                conn.close()
                
                if not pendientes:
                    await update.message.reply_text("✅ No hay solicitudes pendientes.")
                    return
                
                mensaje = "📋 SOLICITUDES PENDIENTES\n\n"
                for p in pendientes:
                    if DATABASE_URL:
                        mensaje += (f"👤 {p['nombre']} {p['apellido']} - Gen {p['generacion']}\n"
                                   f"   Rec: {p['recomendado_por']}\n"
                                   f"   /aprobar_solicitud {p['user_id']}\n\n")
                    else:
                        mensaje += (f"👤 {p[1]} {p[2]} - Gen {p[3]}\n"
                                   f"   Rec: {p[4]}\n"
                                   f"   /aprobar_solicitud {p[0]}\n\n")
                
                await update.message.reply_text(mensaje)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
        return
    
    target_user_id = int(context.args[0])
    
    # Obtener datos del miembro
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de base de datos")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""SELECT nombre, apellido, generacion, recomendado_por 
                        FROM nuevos_miembros WHERE user_id = %s AND estado = 'pendiente'""", (target_user_id,))
        else:
            c.execute("""SELECT nombre, apellido, generacion, recomendado_por 
                        FROM nuevos_miembros WHERE user_id = ? AND estado = 'pendiente'""", (target_user_id,))
        
        miembro = c.fetchone()
        
        if not miembro:
            conn.close()
            await update.message.reply_text("❌ No se encontró solicitud pendiente para ese usuario.")
            return
        
        if DATABASE_URL:
            nombre = miembro['nombre']
            apellido = miembro['apellido']
            generacion = miembro['generacion']
        else:
            nombre = miembro[0]
            apellido = miembro[1]
            generacion = miembro[2]
        
        # Aprobar la solicitud: enviar link del grupo al usuario
        # (Ya no necesitamos approve_chat_join_request porque el flujo es por el bot)
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f"🎉 ¡Felicitaciones {nombre}! Tu solicitud ha sido APROBADA!\n\n"
                     f"⚓ Ya puedes ingresar al grupo Cofradía de Networking:\n\n"
                     f"👉 {COFRADIA_INVITE_LINK}\n\n"
                     f"{'━' * 30}\n"
                     f"🤖 TU ASISTENTE COFRADÍA\n"
                     f"{'━' * 30}\n\n"
                     f"Puedes hacerme consultas de dos formas:\n\n"
                     f"1️⃣ Desde el grupo: escribe @Cofradia_Premium_Bot seguido de tu pregunta\n"
                     f"2️⃣ Desde este chat privado: escríbeme directamente tu consulta\n\n"
                     f"Para ver todos los comandos disponibles escribe /ayuda\n\n"
                     f"💡 Los comandos empiezan con / las palabras no llevan tilde y van unidas por _\n"
                     f"Ejemplo: /buscar_profesional ingenieria\n\n"
                     f"Te esperamos en Cofradía! Recuerda presentarte al grupo."
            )
        except Exception as e:
            logger.warning(f"No se pudo enviar link al usuario: {e}")
            await update.message.reply_text(
                f"⚠️ No se pudo enviar el link al usuario {nombre} {apellido}.\n"
                f"Envíale manualmente el link: {COFRADIA_INVITE_LINK}"
            )
        
        # Si el enlace tiene aprobación activa, aprobar también en Telegram
        try:
            if COFRADIA_GROUP_ID:
                await context.bot.approve_chat_join_request(
                    chat_id=COFRADIA_GROUP_ID,
                    user_id=target_user_id
                )
        except Exception:
            pass  # Puede fallar si no hay solicitud pendiente en Telegram
        
        # Actualizar estado en BD
        if DATABASE_URL:
            c.execute("""UPDATE nuevos_miembros SET estado = 'aprobado', fecha_aprobacion = CURRENT_TIMESTAMP 
                        WHERE user_id = %s""", (target_user_id,))
        else:
            c.execute("""UPDATE nuevos_miembros SET estado = 'aprobado', fecha_aprobacion = CURRENT_TIMESTAMP 
                        WHERE user_id = ?""", (target_user_id,))
        conn.commit()
        conn.close()
        
        # El mensaje de bienvenida se enviará automáticamente cuando el usuario
        # ingrese al grupo (detectado por detectar_nuevo_miembro)
        
        # Registrar al usuario con suscripción de prueba
        try:
            registrar_usuario_suscripcion(
                user_id=target_user_id,
                first_name=nombre,
                username='',
                es_admin=False,
                dias_gratis=DIAS_PRUEBA_GRATIS,
                last_name=apellido
            )
            logger.info(f"✅ Suscripción creada para {nombre} {apellido}")
        except Exception as e:
            logger.warning(f"Error registrando suscripción: {e}")
        
        await update.message.reply_text(
            f"✅ Solicitud de {nombre} {apellido} (Gen {generacion}) APROBADA.\n"
            f"📨 Link del grupo enviado al usuario.\n"
            f"📝 Suscripción de {DIAS_PRUEBA_GRATIS} días activada.\n"
            f"💬 Mensaje de bienvenida se publicará cuando ingrese al grupo."
        )
        
    except Exception as e:
        logger.error(f"Error aprobando solicitud: {e}")
        await update.message.reply_text(f"❌ Error: {e}")


async def editar_usuario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /editar_usuario [user_id] [campo] [valor] - Editar datos de un usuario (solo owner)
    Campos válidos: nombre, apellido, generacion
    Ejemplos:
        /editar_usuario 13031156 nombre Marcelo
        /editar_usuario 13031156 apellido Villegas Soto
        /editar_usuario 13031156 generacion 1995
    """
    if update.effective_user.id != OWNER_ID:
        return
    
    if not context.args or len(context.args) < 3:
        await update.message.reply_text(
            "📝 EDITAR DATOS DE USUARIO\n"
            "━" * 30 + "\n\n"
            "Uso: /editar_usuario [ID] [campo] [valor]\n\n"
            "Campos editables:\n"
            "  nombre - Nombre del usuario\n"
            "  apellido - Apellido(s) del usuario\n"
            "  generacion - Año de generación\n"
            "  antiguedad - Fecha incorporación (DD-MM-YYYY)\n\n"
            "Ejemplos:\n"
            "  /editar_usuario 13031156 nombre Marcelo\n"
            "  /editar_usuario 13031156 apellido Villegas Soto\n"
            "  /editar_usuario 13031156 generacion 1995\n"
            "  /editar_usuario 13031156 antiguedad 15-03-2021"
        )
        return
    
    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ El ID debe ser un número.")
        return
    
    campo = context.args[1].lower()
    valor = ' '.join(context.args[2:])
    
    # Manejar campo especial: antiguedad (fecha_incorporacion)
    if campo == 'antiguedad':
        try:
            # Parsear DD-MM-YYYY
            fecha_inc = datetime.strptime(valor, '%d-%m-%Y')
            fecha_iso = fecha_inc.strftime('%Y-%m-%d')
        except ValueError:
            await update.message.reply_text(
                "❌ Formato de fecha inválido.\n\n"
                "Usa: DD-MM-YYYY\n"
                "Ejemplo: /editar_usuario 13031156 antiguedad 15-03-2021"
            )
            return
        
        try:
            conn = get_db_connection()
            if not conn:
                await update.message.reply_text("❌ Error de conexión a BD")
                return
            c = conn.cursor()
            
            # Obtener fecha_registro original (se mantiene siempre intacta)
            if DATABASE_URL:
                c.execute("SELECT fecha_registro, fecha_incorporacion FROM suscripciones WHERE user_id = %s", (target_user_id,))
            else:
                c.execute("SELECT fecha_registro, fecha_incorporacion FROM suscripciones WHERE user_id = ?", (target_user_id,))
            row = c.fetchone()
            
            if not row:
                await update.message.reply_text(f"⚠️ No se encontró usuario {target_user_id}")
                conn.close()
                return
            
            fecha_reg_original = (row['fecha_registro'] if DATABASE_URL else row[0])
            fecha_inc_anterior = (row['fecha_incorporacion'] if DATABASE_URL else row[1])
            
            # Actualizar solo fecha_incorporacion (fecha_registro queda intacta)
            if DATABASE_URL:
                c.execute("UPDATE suscripciones SET fecha_incorporacion = %s WHERE user_id = %s", (fecha_iso, target_user_id))
            else:
                c.execute("UPDATE suscripciones SET fecha_incorporacion = ? WHERE user_id = ?", (fecha_iso, target_user_id))
            
            conn.commit()
            conn.close()
            
            await update.message.reply_text(
                f"✅ ANTIGÜEDAD EDITADA\n{'━' * 30}\n\n"
                f"🆔 User ID: {target_user_id}\n"
                f"📅 Fecha Activación (original): {str(fecha_reg_original)[:10]}\n"
                f"📅 Fecha Incorporación: {str(fecha_inc_anterior)[:10] if fecha_inc_anterior else 'No tenía'} → {valor}\n\n"
                f"💡 La fecha de Activación original se mantiene intacta."
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        return
    
    campos_validos = {'nombre': 'nombre', 'apellido': 'apellido', 'generacion': 'generacion'}
    
    if campo not in campos_validos:
        await update.message.reply_text(
            f"❌ Campo '{campo}' no válido.\n\n"
            f"Campos editables: nombre, apellido, generacion, antiguedad"
        )
        return
    
    # Validar generación si es el campo
    if campo == 'generacion':
        if not valor.isdigit() or len(valor) != 4:
            await update.message.reply_text("❌ La generación debe ser un año de 4 dígitos.")
            return
        anio = int(valor)
        if anio < 1950 or anio > 2025:
            await update.message.reply_text("❌ El año debe estar entre 1950 y 2025.")
            return
    
    columna_db = campos_validos[campo]
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión a BD")
            return
        
        c = conn.cursor()
        cambios_realizados = []
        
        # 1. Actualizar en nuevos_miembros
        if DATABASE_URL:
            c.execute(f"SELECT nombre, apellido, generacion FROM nuevos_miembros WHERE user_id = %s ORDER BY fecha_solicitud DESC LIMIT 1", (target_user_id,))
        else:
            c.execute(f"SELECT nombre, apellido, generacion FROM nuevos_miembros WHERE user_id = ? ORDER BY fecha_solicitud DESC LIMIT 1", (target_user_id,))
        
        miembro = c.fetchone()
        if miembro:
            if DATABASE_URL:
                valor_anterior = miembro[columna_db]
                c.execute(f"UPDATE nuevos_miembros SET {columna_db} = %s WHERE user_id = %s", (valor, target_user_id))
            else:
                idx = {'nombre': 0, 'apellido': 1, 'generacion': 2}
                valor_anterior = miembro[idx[campo]]
                c.execute(f"UPDATE nuevos_miembros SET {columna_db} = ? WHERE user_id = ?", (valor, target_user_id))
            cambios_realizados.append(f"nuevos_miembros.{columna_db}: '{valor_anterior}' → '{valor}'")
        
        # 2. Actualizar en suscripciones (mapear campos)
        campo_suscripcion = None
        if campo == 'nombre':
            campo_suscripcion = 'first_name'
        elif campo == 'apellido':
            campo_suscripcion = 'last_name'
        
        if campo_suscripcion:
            if DATABASE_URL:
                c.execute(f"SELECT {campo_suscripcion} FROM suscripciones WHERE user_id = %s", (target_user_id,))
                sub = c.fetchone()
                if sub:
                    valor_ant_sub = sub[campo_suscripcion]
                    c.execute(f"UPDATE suscripciones SET {campo_suscripcion} = %s WHERE user_id = %s", (valor, target_user_id))
                    cambios_realizados.append(f"suscripciones.{campo_suscripcion}: '{valor_ant_sub}' → '{valor}'")
            else:
                c.execute(f"SELECT {campo_suscripcion} FROM suscripciones WHERE user_id = ?", (target_user_id,))
                sub = c.fetchone()
                if sub:
                    valor_ant_sub = sub[0]
                    c.execute(f"UPDATE suscripciones SET {campo_suscripcion} = ? WHERE user_id = ?", (valor, target_user_id))
                    cambios_realizados.append(f"suscripciones.{campo_suscripcion}: '{valor_ant_sub}' → '{valor}'")
        
        conn.commit()
        conn.close()
        
        if cambios_realizados:
            detalle = '\n'.join([f"  ✏️ {c}" for c in cambios_realizados])
            await update.message.reply_text(
                f"✅ USUARIO EDITADO\n"
                f"━" * 30 + f"\n\n"
                f"🆔 User ID: {target_user_id}\n"
                f"📝 Cambios realizados:\n{detalle}"
            )
        else:
            await update.message.reply_text(
                f"⚠️ No se encontró el usuario {target_user_id} en la base de datos.\n"
                f"Verifica que el ID sea correcto."
            )
            
    except Exception as e:
        logger.error(f"Error editando usuario: {e}")
        await update.message.reply_text(f"❌ Error: {e}")


async def eliminar_solicitud_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /eliminar_solicitud [user_id] - Eliminar usuario y revocar acceso (solo owner)"""
    if update.effective_user.id != OWNER_ID:
        return
    
    if not context.args:
        # Mostrar últimos usuarios aprobados para referencia
        try:
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("""SELECT user_id, nombre, apellido, generacion, estado, fecha_solicitud 
                               FROM nuevos_miembros ORDER BY fecha_solicitud DESC LIMIT 10""")
                else:
                    c.execute("""SELECT user_id, nombre, apellido, generacion, estado, fecha_solicitud 
                               FROM nuevos_miembros ORDER BY fecha_solicitud DESC LIMIT 10""")
                
                registros = c.fetchall()
                conn.close()
                
                if registros:
                    texto = "🗑️ ELIMINAR SOLICITUD / USUARIO\n"
                    texto += "━" * 30 + "\n\n"
                    texto += "Uso: /eliminar_solicitud [ID]\n\n"
                    texto += "📋 Últimos registros:\n\n"
                    for r in registros:
                        if DATABASE_URL:
                            texto += (f"{'✅' if r['estado'] == 'aprobado' else '⏳'} "
                                     f"{r['nombre']} {r['apellido']} (Gen {r['generacion']})\n"
                                     f"   🆔 {r['user_id']} - Estado: {r['estado']}\n"
                                     f"   /eliminar_solicitud {r['user_id']}\n\n")
                        else:
                            texto += (f"{'✅' if r[4] == 'aprobado' else '⏳'} "
                                     f"{r[1]} {r[2]} (Gen {r[3]})\n"
                                     f"   🆔 {r[0]} - Estado: {r[4]}\n"
                                     f"   /eliminar_solicitud {r[0]}\n\n")
                    await update.message.reply_text(texto)
                else:
                    await update.message.reply_text("No hay registros de miembros.")
                return
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
            return
        return
    
    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ El ID debe ser un número.")
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión a BD")
            return
        
        c = conn.cursor()
        eliminados = []
        
        # Obtener datos del usuario antes de eliminar
        if DATABASE_URL:
            c.execute("SELECT nombre, apellido, generacion, estado FROM nuevos_miembros WHERE user_id = %s ORDER BY fecha_solicitud DESC LIMIT 1", (target_user_id,))
        else:
            c.execute("SELECT nombre, apellido, generacion, estado FROM nuevos_miembros WHERE user_id = ? ORDER BY fecha_solicitud DESC LIMIT 1", (target_user_id,))
        
        miembro = c.fetchone()
        if not miembro:
            await update.message.reply_text(f"⚠️ No se encontró usuario con ID {target_user_id}")
            conn.close()
            return
        
        if DATABASE_URL:
            nombre = miembro['nombre']
            apellido = miembro['apellido']
            generacion = miembro['generacion']
            estado = miembro['estado']
        else:
            nombre = miembro[0]
            apellido = miembro[1]
            generacion = miembro[2]
            estado = miembro[3]
        
        # 1. Eliminar de nuevos_miembros
        if DATABASE_URL:
            c.execute("DELETE FROM nuevos_miembros WHERE user_id = %s", (target_user_id,))
        else:
            c.execute("DELETE FROM nuevos_miembros WHERE user_id = ?", (target_user_id,))
        if c.rowcount > 0:
            eliminados.append(f"nuevos_miembros ({c.rowcount} registros)")
        
        # 2. Eliminar suscripción
        if DATABASE_URL:
            c.execute("DELETE FROM suscripciones WHERE user_id = %s", (target_user_id,))
        else:
            c.execute("DELETE FROM suscripciones WHERE user_id = ?", (target_user_id,))
        if c.rowcount > 0:
            eliminados.append(f"suscripciones ({c.rowcount} registros)")
        
        conn.commit()
        conn.close()
        
        # 3. Intentar banear del grupo (revocar acceso)
        grupo_expulsado = False
        try:
            if COFRADIA_GROUP_ID:
                await context.bot.ban_chat_member(
                    chat_id=COFRADIA_GROUP_ID,
                    user_id=target_user_id
                )
                # Desbanear inmediatamente para permitir re-ingreso futuro si se desea
                await context.bot.unban_chat_member(
                    chat_id=COFRADIA_GROUP_ID,
                    user_id=target_user_id,
                    only_if_banned=True
                )
                grupo_expulsado = True
        except Exception as e:
            logger.warning(f"No se pudo expulsar del grupo: {e}")
        
        # 4. Notificar al usuario
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f"⚠️ Tu acceso a Cofradía de Networking ha sido revocado por el administrador.\n\n"
                     f"Si crees que esto es un error, contacta al administrador."
            )
        except Exception:
            pass
        
        # Respuesta al admin
        detalle = '\n'.join([f"  🗑️ {e}" for e in eliminados])
        await update.message.reply_text(
            f"🗑️ USUARIO ELIMINADO\n"
            f"━" * 30 + f"\n\n"
            f"👤 {nombre} {apellido} (Gen {generacion})\n"
            f"🆔 User ID: {target_user_id}\n"
            f"📋 Estado anterior: {estado}\n\n"
            f"Registros eliminados:\n{detalle}\n\n"
            f"{'✅ Expulsado del grupo' if grupo_expulsado else '⚠️ No se pudo expulsar del grupo (puede que no esté)'}"
        )
        
    except Exception as e:
        logger.error(f"Error eliminando solicitud: {e}")
        await update.message.reply_text(f"❌ Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# SISTEMA DE MEJORA DE INTENCIÓN (Intent Enhancement Engine) — v1.0
# Actúa como middleware invisible: intercepta mensajes en lenguaje natural,
# enriquece el query para RAG/LLM y detecta si corresponde un comando del bot.
# El usuario NUNCA ve este proceso — todo ocurre en el backend antes de responder.
# ═══════════════════════════════════════════════════════════════════════════════

# Catálogo de comandos disponibles para el motor de intención
CATALOGO_COMANDOS_INTENCION = {
    # Búsqueda y consultas
    'buscar_profesional': 'Buscar cofrades por profesión, área o habilidad',
    'buscar_apoyo': 'Buscar cofrades en búsqueda laboral activa',
    'buscar_especialista_sec': 'Buscar especialistas registrados en la SEC',
    'buscar_ia': 'Búsqueda IA en historial y documentos del grupo',
    'rag_consulta': 'Consultar libros, PDFs y documentos indexados',
    'empleo': 'Buscar oportunidades de empleo',
    # Perfil y directorio
    'mi_tarjeta': 'Ver o actualizar tarjeta profesional propia',
    'directorio': 'Ver directorio completo de cofrades',
    'conectar': 'Ver conexiones con otros cofrades',
    'mi_perfil': 'Ver estadísticas y actividad propia',
    'mi_cuenta': 'Ver estado de cuenta y suscripción',
    # Comunidad
    'eventos': 'Ver eventos próximos de la Cofradía',
    'anuncios': 'Ver anuncios del grupo',
    'consultas': 'Ver consultas abiertas de la comunidad',
    'consultar': 'Publicar una consulta al grupo',
    'recomendar': 'Recomendar a un cofrade',
    'publicar': 'Publicar un mensaje en el grupo',
    # Resúmenes y estadísticas
    'resumen': 'Resumen de actividad reciente del día',
    'resumen_semanal': 'Resumen semanal de actividad',
    'resumen_mes': 'Resumen mensual de actividad',
    'estadisticas': 'Estadísticas generales del grupo',
    'graficos': 'Gráficos de actividad y KPIs',
    'top_usuarios': 'Ranking de participación',
    'dotacion': 'Lista de miembros del grupo',
    'cumpleanos_mes': 'Cumpleaños del mes',
    # Desarrollo profesional
    'generar_cv': 'Generar currículum profesional',
    'entrevista': 'Simular entrevista de trabajo',
    'analisis_linkedin': 'Analizar perfil LinkedIn',
    'finanzas': 'Consultor financiero IA',
    'mentor': 'Mentoría personalizada con IA',
    'mi_dashboard': 'Dashboard personal de actividad',
    # Agente inteligente
    'agente': 'Plan de networking personalizado por IA',
    'match': 'Encontrar los mejores matches profesionales',
    'briefing': 'Briefing diario personalizado',
    'indicadores': 'Dashboard indicadores economicos Chile',
    'mi_agenda': 'Ver agenda personal de networking',
    'mis_tareas': 'Ver tareas de networking pendientes',
    'mis_coins': 'Ver saldo de Cofra-Coins',
    'ayuda': 'Ver todos los comandos disponibles',
}


def mejorar_intencion(mensaje_raw: str, user_name: str, user_id: int,
                       canal: str = 'texto') -> dict:
    """
    Motor de mejora de intención invisible.
    
    Recibe el mensaje crudo del usuario y retorna:
    {
      'query_mejorada': str,     — Query enriquecida para búsqueda RAG/LLM
      'comando': str | None,     — Comando del bot a ejecutar (o None)
      'args': str,               — Argumentos del comando
      'tipo': str,               — 'consulta' | 'comando' | 'ambos' | 'saludo'
      'ejecutar_comando': bool,  — Si ejecutar el comando automáticamente
      'razon': str,              — Logging interno (no se muestra al usuario)
    }
    
    Todo ocurre en el backend — el usuario solo ve la respuesta final enriquecida.
    """
    try:
        if not mensaje_raw or not mensaje_raw.strip():
            return _intencion_default(mensaje_raw)
        
        mensaje = mensaje_raw.strip()
        
        # Mensajes muy cortos o saludos simples — no procesar con IA
        if len(mensaje) < 8:
            return _intencion_default(mensaje)
        
        palabras = mensaje.lower().split()
        saludos = {'hola', 'hi', 'hey', 'buenas', 'buenos', 'buen', 'gracias', 'ok', 'perfecto'}
        if len(palabras) <= 3 and all(p in saludos for p in palabras):
            return _intencion_default(mensaje, tipo='saludo')
        
        # Construir catálogo compacto para el prompt
        catalogo_str = "\n".join([f"  /{cmd}: {desc}" for cmd, desc in CATALOGO_COMANDOS_INTENCION.items()])
        
        prompt_intencion = f"""Eres el motor de intención del bot de la Cofradía de Networking. Tu tarea es analizar el mensaje de un usuario y devolver SOLO un JSON (sin markdown, sin texto adicional).

MENSAJE DEL USUARIO ({canal}): "{mensaje}"
NOMBRE DEL USUARIO: {user_name}

COMANDOS DISPONIBLES DEL BOT:
{catalogo_str}

INSTRUCCIONES:
1. "query_mejorada": Reescribe el mensaje en una consulta más específica, técnica y útil para búsqueda semántica. Expande siglas, agrega sinónimos relevantes, clarifica la intención. Mínimo 15 palabras.
2. "comando": Si el mensaje claramente pide algo que un comando puede resolver mejor, pon el nombre del comando (sin /). Si no aplica, pon null.
3. "args": Argumentos para el comando (ej: para /buscar_profesional sería "finanzas" o "ingenieria"). Si no hay args útiles, pon "".
4. "tipo": "consulta" (quiere información/respuesta), "comando" (claramente quiere ejecutar algo), "ambos" (consulta + usar comando), "saludo" (saludo/chat casual).
5. "ejecutar_comando": true solo si el usuario CLARAMENTE pide una acción específica que el comando resuelve. Para consultas generales donde el comando complementa, pon false.
6. "razon": Una frase corta explicando tu razonamiento (solo para logs internos).

CRITERIO PARA ejecutar_comando=true: Solo si el usuario dice explícitamente "muéstrame X", "ver X", "buscar X", "quiero saber X" donde X es exactamente lo que hace el comando.
CRITERIO PARA ejecutar_comando=false: Si el usuario hace una pregunta abierta, consulta general, o quiere una respuesta elaborada (aunque un comando aporte contexto).

RESPONDE SOLO EL JSON, sin bloques de código:
{{"query_mejorada": "...", "comando": "..." o null, "args": "...", "tipo": "...", "ejecutar_comando": true/false, "razon": "..."}}"""

        respuesta_raw = llamar_groq(
            prompt_intencion,
            max_tokens=300,
            temperature=0.2  # Baja temperatura = más determinista y preciso
        )
        
        if not respuesta_raw:
            return _intencion_default(mensaje)
        
        # Parsear JSON de respuesta
        import json as _json
        # Limpiar posibles backticks o texto extra
        resp_limpia = respuesta_raw.strip()
        resp_limpia = resp_limpia.replace('```json', '').replace('```', '').strip()
        
        # Encontrar el JSON dentro de la respuesta
        inicio = resp_limpia.find('{')
        fin = resp_limpia.rfind('}')
        if inicio == -1 or fin == -1:
            logger.debug(f"Intent engine: no JSON encontrado en respuesta: {resp_limpia[:100]}")
            return _intencion_default(mensaje)
        
        json_str = resp_limpia[inicio:fin+1]
        datos = _json.loads(json_str)
        
        # Validar y sanear campos
        query_mejorada = datos.get('query_mejorada', mensaje)
        if not query_mejorada or len(query_mejorada.strip()) < 5:
            query_mejorada = mensaje
        
        comando = datos.get('comando')
        if comando and comando not in CATALOGO_COMANDOS_INTENCION:
            comando = None  # Ignorar comandos inventados
        
        args = str(datos.get('args', '') or '').strip()
        tipo = datos.get('tipo', 'consulta')
        ejecutar = bool(datos.get('ejecutar_comando', False))
        razon = datos.get('razon', '')
        
        # Seguridad: solo ejecutar automáticamente si hay comando válido
        if ejecutar and not comando:
            ejecutar = False
        
        resultado = {
            'query_mejorada': query_mejorada,
            'comando': comando,
            'args': args,
            'tipo': tipo,
            'ejecutar_comando': ejecutar,
            'razon': razon,
            'original': mensaje,
        }
        
        logger.info(f"🧠 IntentEngine [{user_name}] '{mensaje[:50]}' → "
                   f"tipo={tipo}, cmd={comando}, exec={ejecutar} | {razon}")
        
        return resultado
        
    except Exception as e:
        logger.debug(f"IntentEngine error (usando fallback): {e}")
        return _intencion_default(mensaje_raw)


def _intencion_default(mensaje: str, tipo: str = 'consulta') -> dict:
    """Retorna intención por defecto cuando el motor no puede procesar."""
    return {
        'query_mejorada': mensaje,
        'comando': None,
        'args': '',
        'tipo': tipo,
        'ejecutar_comando': False,
        'razon': 'fallback',
        'original': mensaje,
    }


async def ejecutar_comando_desde_intencion(
    comando: str, args: str, update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    """
    Ejecuta un comando detectado por el motor de intención.
    Retorna True si se ejecutó con éxito, False si no.
    El usuario NO ve ningún mensaje previo — el comando simplemente "sucede".
    """
    try:
        # Mapa de comandos a funciones
        MAPA_FUNCIONES = {
            'buscar_profesional': 'buscar_profesional_comando',
            'buscar_apoyo': 'buscar_apoyo_comando',
            'buscar_especialista_sec': 'buscar_especialista_sec_comando',
            'buscar_ia': 'buscar_ia_comando',
            'rag_consulta': 'rag_consulta_comando',
            'empleo': 'empleo_comando',
            'mi_tarjeta': 'mi_tarjeta_comando',
            'directorio': 'directorio_comando',
            'conectar': 'conectar_comando',
            'mi_perfil': 'mi_perfil_comando',
            'mi_cuenta': 'mi_cuenta_comando',
            'eventos': 'eventos_comando',
            'anuncios': 'anuncios_comando',
            'consultas': 'consultas_comando',
            'consultar': 'consultar_comando',
            'recomendar': 'recomendar_comando',
            'publicar': 'publicar_comando',
            'resumen': 'resumen_comando',
            'resumen_semanal': 'resumen_semanal_comando',
            'resumen_mes': 'resumen_mes_comando',
            'estadisticas': 'estadisticas_comando',
            'graficos': 'graficos_comando',
            'top_usuarios': 'top_usuarios_comando',
            'dotacion': 'dotacion_comando',
            'cumpleanos_mes': 'cumpleanos_mes_comando',
            'generar_cv': 'generar_cv_comando',
            'entrevista': 'entrevista_comando',
            'analisis_linkedin': 'analisis_linkedin_comando',
            'finanzas': 'finanzas_comando',
            'mentor': 'mentor_comando',
            'mi_dashboard': 'mi_dashboard_comando',
            'agente': 'agente_networking_comando',
            'match': 'match_networking_comando',
            'briefing': 'briefing_networking_comando',
            'mi_agenda': 'mi_agenda_comando',
            'mis_tareas': 'mis_tareas_comando',
            'mis_coins': 'mis_coins_comando',
            'ayuda': 'ayuda',
        }
        
        nombre_func = MAPA_FUNCIONES.get(comando)
        if not nombre_func:
            return False
        
        # Buscar la función en el scope global
        func = globals().get(nombre_func)
        if not func or not callable(func):
            logger.debug(f"IntentEngine: función '{nombre_func}' no encontrada en globals")
            return False
        
        # Inyectar argumentos y ejecutar
        args_originales = context.args
        context.args = args.split() if args.strip() else []
        
        try:
            await func(update, context)
            logger.info(f"🎯 IntentEngine ejecutó /{comando} {args} automáticamente")
            return True
        finally:
            context.args = args_originales
            
    except Exception as e:
        logger.warning(f"IntentEngine: error ejecutando /{comando}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# ==================== AGENTE INTELIGENTE DE NETWORKING (v5.0) ====================

async def agente_networking_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agente IA que analiza el perfil del usuario y genera un plan de networking personalizado"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Esta función requiere suscripción activa. Escribe /start")
        return
    
    msg = await update.message.reply_text("🤖 Agente de Networking activado...\n🔍 Analizando tu perfil y la comunidad...")
    
    try:
        # Obtener perfil del usuario
        conn = get_db_connection()
        tarjeta = None
        miembro_data = None
        agenda_items = []
        tareas_pend = []
        
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
                tarjeta = c.fetchone()
                c.execute("SELECT nombre, apellido, generacion FROM nuevos_miembros WHERE user_id = %s LIMIT 1", (user_id,))
                miembro_data = c.fetchone()
                c.execute("""SELECT titulo, fecha_evento, tipo FROM agenda_personal 
                             WHERE user_id = %s AND completada = FALSE AND fecha_evento >= NOW() 
                             ORDER BY fecha_evento ASC LIMIT 5""", (user_id,))
                agenda_items = c.fetchall()
                c.execute("""SELECT tarea, prioridad, categoria FROM tareas_networking 
                             WHERE user_id = %s AND completada = FALSE 
                             ORDER BY fecha_creacion DESC LIMIT 5""", (user_id,))
                tareas_pend = c.fetchall()
            else:
                c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
                tarjeta = c.fetchone()
                c.execute("SELECT nombre, apellido, generacion FROM nuevos_miembros WHERE user_id = ? LIMIT 1", (user_id,))
                miembro_data = c.fetchone()
                c.execute("""SELECT titulo, fecha_evento, tipo FROM agenda_personal 
                             WHERE user_id = ? AND completada = 0 AND fecha_evento >= datetime('now') 
                             ORDER BY fecha_evento ASC LIMIT 5""", (user_id,))
                agenda_items = c.fetchall()
                c.execute("""SELECT tarea, prioridad, categoria FROM tareas_networking 
                             WHERE user_id = ? AND completada = 0 
                             ORDER BY fecha_creacion DESC LIMIT 5""", (user_id,))
                tareas_pend = c.fetchall()
            conn.close()
        
        # Construir info del perfil
        perfil_str = ""
        if tarjeta:
            if DATABASE_URL:
                perfil_str = (f"Nombre: {tarjeta['nombre_completo']}\nProfesión: {tarjeta['profesion']}\n"
                             f"Empresa: {tarjeta['empresa']}\nCiudad: {tarjeta['ciudad']}\n"
                             f"Servicios: {tarjeta['servicios']}")
            else:
                perfil_str = f"Nombre: {tarjeta[1]}\nProfesión: {tarjeta[2]}\nEmpresa: {tarjeta[3]}\nCiudad: {tarjeta[7]}\nServicios: {tarjeta[4]}"
        elif miembro_data:
            if DATABASE_URL:
                perfil_str = f"Nombre: {miembro_data['nombre']} {miembro_data['apellido']}\nGeneración: {miembro_data['generacion']}"
            else:
                perfil_str = f"Nombre: {miembro_data[0]} {miembro_data[1]}\nGeneración: {miembro_data[2]}"
        else:
            perfil_str = f"Usuario: {user.first_name} {user.last_name or ''}"
        
        agenda_str = ""
        if agenda_items:
            lines = []
            for item in agenda_items:
                if DATABASE_URL:
                    lines.append(f"• {item['titulo']} ({str(item['fecha_evento'])[:10]})")
                else:
                    lines.append(f"• {item[0]} ({str(item[1])[:10]})")
            agenda_str = "Próximos eventos agendados:\n" + "\n".join(lines)
        
        tareas_str = ""
        if tareas_pend:
            lines = []
            for t in tareas_pend:
                if DATABASE_URL:
                    lines.append(f"• [{t['prioridad'].upper()}] {t['tarea']} ({t['categoria']})")
                else:
                    lines.append(f"• [{t[1].upper()}] {t[0]} ({t[2]})")
            tareas_str = "Tareas pendientes:\n" + "\n".join(lines)
        
        # Búsqueda de contexto comunitario
        resultados_com = busqueda_unificada(
            f"{user.first_name} networking profesional oportunidades", 
            limit_historial=10, limit_rag=20
        )
        contexto_com = formatear_contexto_unificado(resultados_com, "networking")
        
        argumento = " ".join(context.args) if context.args else ""
        
        prompt = f"""Eres el AGENTE DE NETWORKING de la Cofradía, un asistente IA proactivo y estratégico especializado en desarrollo profesional y networking para oficiales navales.

PERFIL DEL COFRADE:
{perfil_str}
{agenda_str}
{tareas_str}

CONTEXTO DE LA COMUNIDAD:
{contexto_com[:2000]}

SOLICITUD DEL USUARIO: {argumento if argumento else 'Análisis general y plan de acción'}

GENERA UN PLAN DE NETWORKING PERSONALIZADO que incluya:
1. 🎯 **Diagnóstico** (2-3 líneas sobre su posición actual en la red)
2. 🤝 **Top 3 acciones de networking** para esta semana (específicas y accionables)
3. 📢 **Oportunidades detectadas** en la comunidad relevantes para su perfil
4. 📅 **Sugerencia de agenda**: qué actividad programar esta semana
5. 💡 **Tip estratégico** de networking para su sector/perfil
6. 🛠️ **Comandos útiles** del bot que debería usar

Sé específico, motivador y con lenguaje profesional pero cercano. Tutéalo como camarada naval."""
        
        await msg.edit_text("🤖 Generando plan estratégico personalizado...")
        respuesta = llamar_groq(prompt, max_tokens=1800, temperature=0.6)
        
        if not respuesta:
            respuesta = llamar_gemini_texto(prompt, max_tokens=1800, temperature=0.6)
        
        if respuesta:
            header = f"🤖 *AGENTE DE NETWORKING - Plan para {user.first_name}*\n{'━'*35}\n\n"
            try:
                await msg.edit_text(header + respuesta, parse_mode='Markdown')
            except Exception:
                await msg.edit_text(f"🤖 AGENTE DE NETWORKING - Plan para {user.first_name}\n\n{respuesta}")
        else:
            await msg.edit_text("❌ No pude generar el plan en este momento. Intenta de nuevo.")
        
        registrar_servicio_usado(user_id, 'agente_networking')
        
    except Exception as e:
        logger.error(f"Error en agente networking: {e}")
        import traceback; logger.error(traceback.format_exc())
        await msg.edit_text(f"❌ Error en el agente: {str(e)[:100]}")


async def agendar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agenda una actividad/reunión con recordatorio automático.
    Uso: /agendar FECHA HORA Título | Descripción | Lugar | Participantes
    Ejemplo: /agendar 2026-03-15 10:00 Reunión con Juan | Explorar alianza comercial | Cafe Caribe | @juan_cofrade
    """
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción. Escribe /start")
        return
    
    if not context.args or len(context.args) < 3:
        await update.message.reply_text(
            "📅 *Agenda una actividad con recordatorio automático*\n\n"
            "*Uso:* `/agendar FECHA HORA Título`\n"
            "*Ejemplo:* `/agendar 2026-03-15 10:00 Reunión de networking con ex-cofrades`\n\n"
            "*Formato completo:* `/agendar FECHA HORA Título | Descripción | Lugar | Participantes`\n\n"
            "💡 El bot te enviará un recordatorio automático 1 hora antes.",
            parse_mode='Markdown'
        )
        return
    
    try:
        # Parsear fecha y hora
        fecha_str = context.args[0]
        hora_str = context.args[1]
        resto = " ".join(context.args[2:])
        
        partes = [p.strip() for p in resto.split("|")]
        titulo = partes[0] if partes else "Actividad de Networking"
        descripcion = partes[1] if len(partes) > 1 else ""
        lugar = partes[2] if len(partes) > 2 else ""
        participantes = partes[3] if len(partes) > 3 else ""
        
        # Parsear datetime
        try:
            fecha_evento = datetime.strptime(f"{fecha_str} {hora_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            await update.message.reply_text(
                "❌ Formato de fecha/hora incorrecto.\n"
                "Usa: `AAAA-MM-DD HH:MM`\nEjemplo: `2026-03-15 10:00`",
                parse_mode='Markdown'
            )
            return
        
        if fecha_evento < datetime.now():
            await update.message.reply_text("⚠️ La fecha ya pasó. Ingresa una fecha futura.")
            return
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión a la base de datos.")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""INSERT INTO agenda_personal 
                        (user_id, titulo, descripcion, fecha_evento, lugar, tipo, participantes)
                        VALUES (%s, %s, %s, %s, %s, 'reunion', %s) RETURNING id""",
                     (user_id, titulo, descripcion, fecha_evento, lugar, participantes))
            agenda_id = c.fetchone()['id']
        else:
            c.execute("""INSERT INTO agenda_personal 
                        (user_id, titulo, descripcion, fecha_evento, lugar, tipo, participantes)
                        VALUES (?,?,?,?,?,'reunion',?)""",
                     (user_id, titulo, descripcion, fecha_evento.strftime('%Y-%m-%d %H:%M:%S'), lugar, participantes))
            agenda_id = c.lastrowid
        conn.commit()
        conn.close()
        
        # Generar resumen con IA
        prompt_resumen = f"""El cofrade {user.first_name} agendó esta actividad de networking:
Título: {titulo}
Descripción: {descripcion}
Lugar: {lugar}
Participantes: {participantes}
Fecha: {fecha_evento.strftime('%d/%m/%Y a las %H:%M')}

En 2-3 oraciones: (1) Confirma el agendamiento con entusiasmo, (2) Da 1 consejo práctico para prepararse para esta reunión/actividad, (3) Sugiere qué llevar o cómo sacar máximo provecho del networking."""
        
        consejo = llamar_groq(prompt_resumen, max_tokens=300, temperature=0.7) or ""
        
        respuesta = (
            f"✅ *¡Actividad agendada exitosamente!* (ID: #{agenda_id})\n\n"
            f"📌 *{titulo}*\n"
            f"📅 {fecha_evento.strftime('%A %d/%m/%Y a las %H:%M')}\n"
        )
        if lugar:
            respuesta += f"📍 {lugar}\n"
        if participantes:
            respuesta += f"👥 {participantes}\n"
        if descripcion:
            respuesta += f"📝 {descripcion}\n"
        respuesta += f"\n🔔 _Recibirás un recordatorio automático 1 hora antes._\n"
        if consejo:
            respuesta += f"\n💡 {consejo}"
        respuesta += f"\n\n📋 Ver agenda: /mi_agenda | ✅ Marcar completa: /completar_{agenda_id}"
        
        try:
            await update.message.reply_text(respuesta, parse_mode='Markdown')
        except Exception:
            await update.message.reply_text(respuesta.replace('*', '').replace('_', ''))
        
        registrar_servicio_usado(user_id, 'agendar')
        
    except Exception as e:
        logger.error(f"Error en /agendar: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def mi_agenda_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra la agenda personal del usuario con próximas actividades"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción. Escribe /start")
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión.")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""SELECT id, titulo, fecha_evento, lugar, tipo, participantes, descripcion, completada 
                        FROM agenda_personal WHERE user_id = %s 
                        ORDER BY completada ASC, fecha_evento ASC LIMIT 20""", (user_id,))
        else:
            c.execute("""SELECT id, titulo, fecha_evento, lugar, tipo, participantes, descripcion, completada 
                        FROM agenda_personal WHERE user_id = ? 
                        ORDER BY completada ASC, fecha_evento ASC LIMIT 20""", (user_id,))
        items = c.fetchall()
        conn.close()
        
        if not items:
            await update.message.reply_text(
                "📅 *Tu agenda está vacía*\n\n"
                "Agenda tu primera actividad con:\n"
                "`/agendar AAAA-MM-DD HH:MM Título | Descripción | Lugar`",
                parse_mode='Markdown'
            )
            return
        
        ahora = datetime.now()
        proximas = []
        pasadas = []
        completadas = []
        
        for item in items:
            if DATABASE_URL:
                item_id, titulo, fecha, lugar, tipo, partic, desc, comp = (
                    item['id'], item['titulo'], item['fecha_evento'], 
                    item['lugar'], item['tipo'], item['participantes'],
                    item['descripcion'], item['completada']
                )
            else:
                item_id, titulo, fecha, lugar, tipo, partic, desc, comp = item
            
            fecha_dt = fecha if hasattr(fecha, 'strftime') else datetime.strptime(str(fecha)[:19], '%Y-%m-%d %H:%M:%S')
            
            if comp:
                completadas.append((item_id, titulo, fecha_dt, lugar))
            elif fecha_dt >= ahora:
                proximas.append((item_id, titulo, fecha_dt, lugar, partic))
            else:
                pasadas.append((item_id, titulo, fecha_dt, lugar))
        
        texto = f"📅 *AGENDA DE {user.first_name.upper()}*\n{'━'*30}\n\n"
        
        if proximas:
            texto += "🔜 *PRÓXIMAS ACTIVIDADES:*\n"
            for item_id, titulo, fecha_dt, lugar, partic in proximas:
                dias_resto = (fecha_dt - ahora).days
                urgencia = "🔴" if dias_resto <= 1 else "🟡" if dias_resto <= 7 else "🟢"
                texto += f"{urgencia} *#{item_id}* {titulo}\n"
                texto += f"   📅 {fecha_dt.strftime('%d/%m/%Y %H:%M')}"
                if lugar:
                    texto += f" | 📍 {lugar}"
                if partic:
                    texto += f"\n   👥 {partic}"
                texto += f"\n   ✅ `/completar_{item_id}`\n\n"
        
        if pasadas:
            texto += "⏰ *PENDIENTES (fecha pasada):*\n"
            for item_id, titulo, fecha_dt, lugar in pasadas[:5]:
                texto += f"⚠️ *#{item_id}* {titulo} ({fecha_dt.strftime('%d/%m')})\n"
                texto += f"   ✅ `/completar_{item_id}` | 🗑 `/eliminar_agenda_{item_id}`\n"
            texto += "\n"
        
        if completadas:
            texto += f"✅ *COMPLETADAS:* {len(completadas)} actividades\n\n"
        
        texto += "➕ *Nueva:* `/agendar FECHA HORA Título`"
        
        try:
            await update.message.reply_text(texto, parse_mode='Markdown')
        except Exception:
            await update.message.reply_text(texto.replace('*', '').replace('_', ''))
        
    except Exception as e:
        logger.error(f"Error en /mi_agenda: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def nueva_tarea_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agrega una tarea de networking pendiente.
    Uso: /tarea [alta|media|baja] categoria texto
    Ejemplo: /tarea alta contacto Llamar a Pedro García sobre proyecto logística
    """
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción. Escribe /start")
        return
    
    if not context.args:
        await update.message.reply_text(
            "📋 *Agregar tarea de networking*\n\n"
            "*Uso:* `/tarea [prioridad] texto`\n\n"
            "*Ejemplos:*\n"
            "`/tarea alta Llamar a Pedro sobre proyecto logística`\n"
            "`/tarea media Actualizar perfil LinkedIn con nuevo cargo`\n"
            "`/tarea baja Revisar ofertas de empleo sector marítimo`\n\n"
            "📋 Ver tareas: /mis_tareas",
            parse_mode='Markdown'
        )
        return
    
    args_texto = " ".join(context.args)
    prioridad = 'normal'
    
    if context.args[0].lower() in ['alta', 'urgente', 'high']:
        prioridad = 'alta'
        args_texto = " ".join(context.args[1:])
    elif context.args[0].lower() in ['media', 'medium', 'normal']:
        prioridad = 'media'
        args_texto = " ".join(context.args[1:])
    elif context.args[0].lower() in ['baja', 'low']:
        prioridad = 'baja'
        args_texto = " ".join(context.args[1:])
    
    if not args_texto.strip():
        await update.message.reply_text("❌ Escribe el texto de la tarea después de la prioridad.")
        return
    
    # Detectar categoría automáticamente con IA
    categoria = 'general'
    cats_map = {
        'contacto': ['llamar', 'contactar', 'escribir', 'mensaje', 'email', 'correo'],
        'perfil': ['linkedin', 'cv', 'perfil', 'actualizar', 'bio'],
        'evento': ['asistir', 'evento', 'reunión', 'reunion', 'conferencia', 'seminario'],
        'empleo': ['oferta', 'empleo', 'trabajo', 'postular', 'aplicar', 'entrevista'],
        'negocio': ['proyecto', 'propuesta', 'contrato', 'alianza', 'socio', 'negocio'],
        'seguimiento': ['seguimiento', 'follow', 'recordar', 'confirmar', 'responder'],
    }
    texto_lower = args_texto.lower()
    for cat, palabras in cats_map.items():
        if any(p in texto_lower for p in palabras):
            categoria = cat
            break
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión.")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""INSERT INTO tareas_networking (user_id, tarea, prioridad, categoria)
                        VALUES (%s, %s, %s, %s) RETURNING id""",
                     (user_id, args_texto, prioridad, categoria))
            tarea_id = c.fetchone()['id']
        else:
            c.execute("""INSERT INTO tareas_networking (user_id, tarea, prioridad, categoria)
                        VALUES (?,?,?,?)""",
                     (user_id, args_texto, prioridad, categoria))
            tarea_id = c.lastrowid
        conn.commit()
        conn.close()
        
        emoji_prior = {'alta': '🔴', 'media': '🟡', 'baja': '🟢', 'normal': '⚪'}.get(prioridad, '⚪')
        
        await update.message.reply_text(
            f"✅ *Tarea #{tarea_id} guardada*\n\n"
            f"{emoji_prior} [{prioridad.upper()}] {args_texto}\n"
            f"🏷️ Categoría: {categoria}\n\n"
            f"📋 Ver todas: /mis_tareas\n"
            f"✅ Completar: `/ok_{tarea_id}`",
            parse_mode='Markdown'
        )
        registrar_servicio_usado(user_id, 'tarea')
        
    except Exception as e:
        logger.error(f"Error en /tarea: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def mis_tareas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra las tareas de networking pendientes"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción.")
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""SELECT id, tarea, prioridad, categoria, completada, fecha_creacion 
                        FROM tareas_networking WHERE user_id = %s 
                        ORDER BY completada ASC, 
                            CASE prioridad WHEN 'alta' THEN 1 WHEN 'media' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END,
                            fecha_creacion DESC LIMIT 25""", (user_id,))
        else:
            c.execute("""SELECT id, tarea, prioridad, categoria, completada, fecha_creacion 
                        FROM tareas_networking WHERE user_id = ? 
                        ORDER BY completada ASC, 
                            CASE prioridad WHEN 'alta' THEN 1 WHEN 'media' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END,
                            fecha_creacion DESC LIMIT 25""", (user_id,))
        tareas = c.fetchall()
        conn.close()
        
        if not tareas:
            await update.message.reply_text(
                "📋 *No tienes tareas pendientes*\n\n"
                "Agrega una:\n`/tarea alta Llamar a Juan García sobre proyecto`",
                parse_mode='Markdown'
            )
            return
        
        pendientes = []
        completadas_list = []
        
        for t in tareas:
            if DATABASE_URL:
                tid, texto, prior, cat, comp, fecha = t['id'], t['tarea'], t['prioridad'], t['categoria'], t['completada'], t['fecha_creacion']
            else:
                tid, texto, prior, cat, comp, fecha = t
            
            if comp:
                completadas_list.append((tid, texto, prior))
            else:
                pendientes.append((tid, texto, prior, cat))
        
        emoji_p = {'alta': '🔴', 'media': '🟡', 'baja': '🟢', 'normal': '⚪'}
        
        texto_resp = f"📋 *TAREAS DE NETWORKING - {user.first_name}*\n{'━'*30}\n\n"
        
        if pendientes:
            texto_resp += "⏳ *PENDIENTES:*\n"
            for tid, texto, prior, cat in pendientes:
                ep = emoji_p.get(prior, '⚪')
                texto_resp += f"{ep} *#{tid}* {texto[:80]}\n"
                texto_resp += f"   🏷️ {cat} | ✅ `/ok_{tid}`\n\n"
        
        if completadas_list:
            texto_resp += f"✅ *Completadas:* {len(completadas_list)}\n\n"
        
        texto_resp += "➕ *Nueva:* `/tarea [alta|media|baja] texto`"
        
        try:
            await update.message.reply_text(texto_resp, parse_mode='Markdown')
        except Exception:
            await update.message.reply_text(texto_resp.replace('*', '').replace('_', ''))
        
    except Exception as e:
        logger.error(f"Error en /mis_tareas: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def match_networking_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agente IA que encuentra los cofrades más compatibles para networking según tu perfil"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción. Escribe /start")
        return
    
    msg = await update.message.reply_text("🔍 Analizando perfiles de la Cofradía para encontrar tus mejores conexiones...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error de conexión.")
            return
        
        c = conn.cursor()
        
        # Obtener perfil propio
        if DATABASE_URL:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
            mi_tarjeta = c.fetchone()
            # Obtener otros cofrades con tarjeta
            c.execute("""SELECT user_id, nombre_completo, profesion, empresa, ciudad, servicios 
                        FROM tarjetas_profesional WHERE user_id != %s LIMIT 40""", (user_id,))
            otros = c.fetchall()
        else:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
            mi_tarjeta = c.fetchone()
            c.execute("""SELECT user_id, nombre_completo, profesion, empresa, ciudad, servicios 
                        FROM tarjetas_profesional WHERE user_id != ? LIMIT 40""", (user_id,))
            otros = c.fetchall()
        conn.close()
        
        if not mi_tarjeta:
            await msg.edit_text(
                "⚠️ No tienes tarjeta profesional creada.\n\n"
                "Crea tu perfil con /mi_tarjeta para que el agente pueda hacer match inteligente."
            )
            return
        
        if DATABASE_URL:
            mi_perfil = f"{mi_tarjeta['nombre_completo']} | {mi_tarjeta['profesion']} | {mi_tarjeta['empresa']} | {mi_tarjeta['ciudad']} | {mi_tarjeta['servicios']}"
        else:
            mi_perfil = f"{mi_tarjeta[1]} | {mi_tarjeta[2]} | {mi_tarjeta[3]} | {mi_tarjeta[7]} | {mi_tarjeta[4]}"
        
        if not otros:
            await msg.edit_text(
                "ℹ️ Aún no hay suficientes perfiles en el directorio para hacer match.\n\n"
                "Invita a otros cofrades a crear su tarjeta con /mi_tarjeta"
            )
            return
        
        otros_str = "\n".join([
            f"- {(o['nombre_completo'] if DATABASE_URL else o[1])} | {(o['profesion'] if DATABASE_URL else o[2])} | {(o['empresa'] if DATABASE_URL else o[3])} | {(o['ciudad'] if DATABASE_URL else o[4])}"
            for o in otros[:30]
        ])
        
        busqueda_extra = " ".join(context.args) if context.args else ""
        
        prompt = f"""Eres el Motor de Match de Networking de la Cofradía. Analiza el perfil del cofrade y encuentra las mejores conexiones estratégicas.

MI PERFIL:
{mi_perfil}
{f"Búsqueda específica: {busqueda_extra}" if busqueda_extra else ""}

COFRADES DISPONIBLES EN EL DIRECTORIO:
{otros_str}

TAREA: Identifica los TOP 5 cofrades más valiosos para hacer networking con {user.first_name}, y para cada uno:
1. Nombre y por qué es una conexión estratégica
2. Qué tienen en común o qué sinergias existen
3. Una propuesta de primer mensaje o tema de conversación concreto
4. Potencial de colaboración (escala 1-10 con razón)

Sé específico y estratégico. Piensa en sinergias de negocio, intercambio de expertise, oportunidades laborales mutuas."""
        
        await msg.edit_text("🤝 Calculando compatibilidades y sinergias...")
        respuesta = llamar_groq(prompt, max_tokens=1600, temperature=0.6)
        
        if not respuesta:
            respuesta = llamar_gemini_texto(prompt, max_tokens=1600, temperature=0.6)
        
        if respuesta:
            header = f"🤝 *MATCH DE NETWORKING para {user.first_name}*\n{'━'*35}\n\n"
            try:
                await msg.edit_text(header + respuesta, parse_mode='Markdown')
            except Exception:
                await msg.edit_text(f"🤝 MATCH para {user.first_name}\n\n{respuesta}")
        else:
            await msg.edit_text("❌ No pude generar el análisis. Intenta de nuevo.")
        
        registrar_servicio_usado(user_id, 'match_networking')
        
    except Exception as e:
        logger.error(f"Error en match_networking: {e}")
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


async def briefing_diario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera un briefing diario de networking: agenda, tareas, oportunidades de la comunidad"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción.")
        return
    
    msg = await update.message.reply_text("☀️ Preparando tu briefing diario de networking...")
    
    try:
        ahora = datetime.now()
        hoy_str = ahora.strftime("%A %d/%m/%Y")
        
        conn = get_db_connection()
        agenda_hoy = []
        tareas_alta = []
        proximos_7 = []
        
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("""SELECT titulo, fecha_evento, lugar FROM agenda_personal 
                             WHERE user_id = %s AND completada = FALSE 
                             AND fecha_evento::date = CURRENT_DATE
                             ORDER BY fecha_evento ASC""", (user_id,))
                agenda_hoy = c.fetchall()
                c.execute("""SELECT tarea, prioridad, categoria FROM tareas_networking 
                             WHERE user_id = %s AND completada = FALSE AND prioridad = 'alta'
                             ORDER BY fecha_creacion ASC LIMIT 5""", (user_id,))
                tareas_alta = c.fetchall()
                c.execute("""SELECT titulo, fecha_evento, lugar FROM agenda_personal 
                             WHERE user_id = %s AND completada = FALSE 
                             AND fecha_evento > CURRENT_TIMESTAMP
                             AND fecha_evento <= CURRENT_TIMESTAMP + INTERVAL '7 days'
                             ORDER BY fecha_evento ASC LIMIT 5""", (user_id,))
                proximos_7 = c.fetchall()
            else:
                c.execute("""SELECT titulo, fecha_evento, lugar FROM agenda_personal 
                             WHERE user_id = ? AND completada = 0 
                             AND date(fecha_evento) = date('now')
                             ORDER BY fecha_evento ASC""", (user_id,))
                agenda_hoy = c.fetchall()
                c.execute("""SELECT tarea, prioridad, categoria FROM tareas_networking 
                             WHERE user_id = ? AND completada = 0 AND prioridad = 'alta'
                             ORDER BY fecha_creacion ASC LIMIT 5""", (user_id,))
                tareas_alta = c.fetchall()
                c.execute("""SELECT titulo, fecha_evento, lugar FROM agenda_personal 
                             WHERE user_id = ? AND completada = 0 
                             AND fecha_evento > datetime('now')
                             AND fecha_evento <= datetime('now', '+7 days')
                             ORDER BY fecha_evento ASC LIMIT 5""", (user_id,))
                proximos_7 = c.fetchall()
            conn.close()
        
        # Actividad reciente de la comunidad
        resultados_com = busqueda_unificada("networking empleo oportunidades noticias", limit_historial=10, limit_rag=15)
        contexto_com = formatear_contexto_unificado(resultados_com, "networking hoy")
        
        agenda_hoy_str = ""
        if agenda_hoy:
            lines = []
            for item in agenda_hoy:
                if DATABASE_URL:
                    hora = str(item['fecha_evento'])[11:16]
                    lines.append(f"⏰ {hora} - {item['titulo']}" + (f" (📍 {item['lugar']})" if item['lugar'] else ""))
                else:
                    hora = str(item[1])[11:16]
                    lines.append(f"⏰ {hora} - {item[0]}" + (f" (📍 {item[2]})" if item[2] else ""))
            agenda_hoy_str = "HOY EN TU AGENDA:\n" + "\n".join(lines)
        
        tareas_str = ""
        if tareas_alta:
            lines = []
            for t in tareas_alta:
                if DATABASE_URL:
                    lines.append(f"🔴 {t['tarea']}")
                else:
                    lines.append(f"🔴 {t[0]}")
            tareas_str = "TAREAS URGENTES:\n" + "\n".join(lines)
        
        proximos_str = ""
        if proximos_7:
            lines = []
            for item in proximos_7:
                if DATABASE_URL:
                    fecha_d = str(item['fecha_evento'])[:10]
                    lines.append(f"• {fecha_d}: {item['titulo']}")
                else:
                    lines.append(f"• {str(item[1])[:10]}: {item[0]}")
            proximos_str = "PRÓXIMOS 7 DÍAS:\n" + "\n".join(lines)
        
        prompt = f"""Eres el asistente de networking de la Cofradía. Genera un BRIEFING DIARIO motivador y estratégico para {user.first_name}.

FECHA: {hoy_str}
{agenda_hoy_str}
{tareas_str}
{proximos_str}

ACTIVIDAD RECIENTE EN LA COMUNIDAD:
{contexto_com[:1500]}

GENERA UN BRIEFING DIARIO que incluya:
🌅 **Buenos días** con frase motivadora naval/profesional
📅 **Agenda de hoy** (si hay items, destácalos; si no, sugiere qué hacer)
🔴 **Urgente** (tareas prioritarias de hoy)  
📡 **Radar de oportunidades** (1-2 oportunidades detectadas en la comunidad hoy)
🎯 **Acción estrella del día** (la 1 cosa más importante que debería hacer hoy para su networking)
💬 **Mensaje para compartir** (algo breve e inspirador para el grupo)

Sé conciso pero impactante. Tono energético, profesional y de camaradería."""
        
        respuesta = llamar_groq(prompt, max_tokens=1200, temperature=0.7)
        
        if not respuesta:
            respuesta = llamar_gemini_texto(prompt, max_tokens=1200, temperature=0.7)
        
        if respuesta:
            try:
                await msg.edit_text(f"☀️ *BRIEFING {hoy_str.upper()}*\n{'━'*30}\n\n{respuesta}", parse_mode='Markdown')
            except Exception:
                await msg.edit_text(f"☀️ BRIEFING {hoy_str}\n\n{respuesta}")
        else:
            await msg.edit_text("❌ No pude generar el briefing. Intenta de nuevo.")
        
        registrar_servicio_usado(user_id, 'briefing_diario')
        
    except Exception as e:
        logger.error(f"Error en briefing_diario: {e}")
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


async def completar_item_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja /completar_ID y /ok_ID para marcar agenda/tareas como completadas"""
    user = update.effective_user
    user_id = user.id
    
    comando_texto = update.message.text.split()[0].lstrip('/')
    
    tipo = None
    item_id = None
    
    if comando_texto.startswith('completar_'):
        tipo = 'agenda'
        try:
            item_id = int(comando_texto.replace('completar_', ''))
        except ValueError:
            pass
    elif comando_texto.startswith('ok_'):
        tipo = 'tarea'
        try:
            item_id = int(comando_texto.replace('ok_', ''))
        except ValueError:
            pass
    elif comando_texto.startswith('eliminar_agenda_'):
        try:
            item_id = int(comando_texto.replace('eliminar_agenda_', ''))
            tipo = 'eliminar_agenda'
        except ValueError:
            pass
    
    if not item_id or not tipo:
        await update.message.reply_text("❌ ID no válido.")
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        
        if tipo == 'agenda':
            if DATABASE_URL:
                c.execute("UPDATE agenda_personal SET completada = TRUE WHERE id = %s AND user_id = %s", (item_id, user_id))
            else:
                c.execute("UPDATE agenda_personal SET completada = 1 WHERE id = ? AND user_id = ?", (item_id, user_id))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"✅ Actividad #{item_id} marcada como completada. ¡Bien hecho! 💪")
        elif tipo == 'tarea':
            if DATABASE_URL:
                c.execute("UPDATE tareas_networking SET completada = TRUE WHERE id = %s AND user_id = %s", (item_id, user_id))
            else:
                c.execute("UPDATE tareas_networking SET completada = 1 WHERE id = ? AND user_id = ?", (item_id, user_id))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"✅ Tarea #{item_id} completada. ¡Excelente! 🎯")
        elif tipo == 'eliminar_agenda':
            if DATABASE_URL:
                c.execute("DELETE FROM agenda_personal WHERE id = %s AND user_id = %s", (item_id, user_id))
            else:
                c.execute("DELETE FROM agenda_personal WHERE id = ? AND user_id = ?", (item_id, user_id))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"🗑️ Actividad #{item_id} eliminada de tu agenda.")
    except Exception as e:
        logger.error(f"Error completando item: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:50]}")


async def recordatorio_agenda_job(context: ContextTypes.DEFAULT_TYPE):
    """Job que envía recordatorios 1 hora antes de actividades agendadas"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("""SELECT a.id, a.user_id, a.titulo, a.fecha_evento, a.lugar, a.participantes
                        FROM agenda_personal a
                        WHERE a.completada = FALSE 
                        AND a.recordatorio_enviado = FALSE
                        AND a.fecha_evento BETWEEN NOW() + INTERVAL '55 minutes' AND NOW() + INTERVAL '65 minutes'""")
        else:
            c.execute("""SELECT id, user_id, titulo, fecha_evento, lugar, participantes
                        FROM agenda_personal
                        WHERE completada = 0 AND recordatorio_enviado = 0
                        AND fecha_evento BETWEEN datetime('now', '+55 minutes') AND datetime('now', '+65 minutes')""")
        
        items = c.fetchall()
        
        for item in items:
            if DATABASE_URL:
                item_id = item['id']
                uid = item['user_id']
                titulo = item['titulo']
                fecha = item['fecha_evento']
                lugar = item['lugar'] or ''
                partic = item['participantes'] or ''
            else:
                item_id, uid, titulo, fecha, lugar, partic = item
                lugar = lugar or ''
                partic = partic or ''
            
            try:
                hora_str = str(fecha)[11:16] if fecha else '??:??'
                mensaje = (
                    f"🔔 *RECORDATORIO - ¡En 1 hora!*\n\n"
                    f"📌 *{titulo}*\n"
                    f"⏰ {hora_str}\n"
                )
                if lugar:
                    mensaje += f"📍 {lugar}\n"
                if partic:
                    mensaje += f"👥 {partic}\n"
                mensaje += f"\n¡Prepárate para tu actividad de networking! 💼\n"
                mensaje += f"✅ `/completar_{item_id}`"
                
                await context.bot.send_message(chat_id=uid, text=mensaje, parse_mode='Markdown')
                
                # Marcar recordatorio como enviado
                if DATABASE_URL:
                    c.execute("UPDATE agenda_personal SET recordatorio_enviado = TRUE WHERE id = %s", (item_id,))
                else:
                    c.execute("UPDATE agenda_personal SET recordatorio_enviado = 1 WHERE id = ?", (item_id,))
                conn.commit()
                
            except Exception as e_msg:
                logger.debug(f"No se pudo enviar recordatorio a {uid}: {e_msg}")
        
        conn.close()
        
    except Exception as e:
        logger.debug(f"Error en job recordatorio_agenda: {e}")





async def ofrezco_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Registra un servicio ofrecido."""
    user = update.effective_user
    if not context.args:
        await update.message.reply_text(
            "*Publicar tu servicio:*\n\n"
            "`/ofrezco [descripcion]`\n\n"
            "Ejemplo:\n`/ofrezco Asesoria en derecho maritimo`",
            parse_mode='Markdown'
        )
        return
    descripcion = " ".join(context.args)[:300]
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            ph = "%s" if DATABASE_URL else "?"
            c.execute(f"UPDATE servicios_cofrades SET activo=0 WHERE user_id={ph} AND tipo='ofrezco'", (user.id,))
            c.execute(f"INSERT INTO servicios_cofrades (user_id, tipo, descripcion) VALUES ({ph},'ofrezco',{ph})", (user.id, descripcion))
            conn.commit()
            conn.close()
        await update.message.reply_text(
            f"Servicio publicado:\n_{descripcion}_\n\nLos cofrades te encontraran con `/busco`.",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error ofrezco: {e}")
        await update.message.reply_text("Error al registrar el servicio.")


async def busco_servicio_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Busca cofrades que ofrecen un servicio."""
    user = update.effective_user
    if not context.args:
        await update.message.reply_text(
            "*Buscar servicio:*\n\n`/busco [lo que necesitas]`\n\nEjemplo:\n`/busco asesoria legal`",
            parse_mode='Markdown'
        )
        return
    busqueda = " ".join(context.args).lower()
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("Error de conexion.")
            return
        c = conn.cursor()
        ph = "%s" if DATABASE_URL else "?"
        if DATABASE_URL:
            c.execute("""SELECT sc.descripcion, s.first_name, s.username
                FROM servicios_cofrades sc
                JOIN suscripciones s ON sc.user_id=s.user_id
                WHERE sc.tipo='ofrezco' AND sc.activo=1 AND LOWER(sc.descripcion) LIKE %s
                LIMIT 5""", (f"%{busqueda}%",))
        else:
            c.execute("""SELECT sc.descripcion, s.first_name, s.username
                FROM servicios_cofrades sc
                JOIN suscripciones s ON sc.user_id=s.user_id
                WHERE sc.tipo='ofrezco' AND sc.activo=1 AND LOWER(sc.descripcion) LIKE ?
                LIMIT 5""", (f"%{busqueda}%",))
        resultados = c.fetchall()
        conn.close()

        if not resultados:
            await update.message.reply_text(
                f"Nadie ofrece *{busqueda}* por ahora.\n\n"
                "Si tu lo ofreces, publicate con `/ofrezco [descripcion]`",
                parse_mode='Markdown'
            )
            return

        texto = f"*Cofrades que ofrecen '{busqueda}':*\n\n"
        for desc, fname, uname in resultados:
            contacto = f"@{uname}" if uname else fname
            texto += f"*{fname}* ({contacto})\n_{desc}_\n\n"
        await update.message.reply_text(texto, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error busco_servicio: {e}")
        await update.message.reply_text("Error al buscar.")


async def mis_servicios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra servicios publicados por el usuario."""
    user = update.effective_user
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("Error de conexion.")
            return
        c = conn.cursor()
        ph = "%s" if DATABASE_URL else "?"
        c.execute(f"SELECT tipo, descripcion FROM servicios_cofrades WHERE user_id={ph} AND activo=1", (user.id,))
        servicios = c.fetchall()
        conn.close()

        if not servicios:
            await update.message.reply_text(
                "No tienes servicios publicados.\n\n"
                "• `/ofrezco [descripcion]` — publica lo que ofreces\n"
                "• `/busco [servicio]` — busca lo que necesitas"
            )
            return

        texto = "*Tus servicios publicados:*\n\n"
        for tipo, desc in servicios:
            icono = "🟢" if tipo == "ofrezco" else "🔵"
            texto += f"{icono} _{desc}_\n\n"
        await update.message.reply_text(texto, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error mis_servicios: {e}")
        await update.message.reply_text("Error al obtener servicios.")


async def recordar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Crea recordatorio. Uso: /recordar 2h Llamar a Juan"""
    user = update.effective_user
    if len(context.args) < 2:
        await update.message.reply_text(
            "*Crear recordatorio:*\n\n"
            "`/recordar [tiempo] [mensaje]`\n\n"
            "*Ejemplos:*\n"
            "`/recordar 30m Revisar propuesta`\n"
            "`/recordar 2h Llamar al cliente`\n"
            "`/recordar 1d Seguimiento reunion`",
            parse_mode='Markdown'
        )
        return

    tiempo_str = context.args[0].lower()
    mensaje = " ".join(context.args[1:])
    from datetime import datetime, timedelta
    ahora = datetime.utcnow()
    try:
        if tiempo_str.endswith('m'):
            delta = timedelta(minutes=int(tiempo_str[:-1]))
        elif tiempo_str.endswith('h'):
            delta = timedelta(hours=int(tiempo_str[:-1]))
        elif tiempo_str.endswith('d'):
            delta = timedelta(days=int(tiempo_str[:-1]))
        else:
            raise ValueError()
        fecha_recordar = ahora + delta
    except Exception:
        await update.message.reply_text("Formato invalido. Usa: 30m, 2h, 1d")
        return

    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            ph = "%s" if DATABASE_URL else "?"
            c.execute(f"INSERT INTO recordatorios (user_id, mensaje, fecha_recordar) VALUES ({ph},{ph},{ph})",
                     (user.id, mensaje, fecha_recordar if DATABASE_URL else fecha_recordar.isoformat()))
            conn.commit()
            conn.close()
        await update.message.reply_text(
            f"Recordatorio creado:\n_{mensaje}_\n\nTe avisare en {tiempo_str}.",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error recordar: {e}")
        await update.message.reply_text("Error al crear recordatorio.")


async def verificar_recordatorios(context):
    """Job: envía recordatorios pendientes cada minuto."""
    from datetime import datetime
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        ahora = datetime.utcnow()
        if DATABASE_URL:
            c.execute("SELECT id, user_id, mensaje FROM recordatorios WHERE enviado=0 AND fecha_recordar <= %s", (ahora,))
        else:
            c.execute("SELECT id, user_id, mensaje FROM recordatorios WHERE enviado=0 AND fecha_recordar <= ?", (ahora.isoformat(),))
        pendientes = c.fetchall()
        for row in pendientes:
            try:
                # Supabase devuelve dict, SQLite devuelve tuple
                if DATABASE_URL:
                    rec_id = row['id']
                    uid    = row['user_id']
                    msg    = row['mensaje']
                else:
                    rec_id, uid, msg = row[0], row[1], row[2]
                await context.bot.send_message(
                    chat_id=uid,
                    text="⏰ *Recordatorio Cofradía*\n\n" + str(msg),
                    parse_mode='Markdown'
                )
                ph = "%s" if DATABASE_URL else "?"
                c.execute(f"UPDATE recordatorios SET enviado=1 WHERE id={ph}", (rec_id,))
                logger.info(f"✅ Recordatorio {rec_id} enviado a {uid}")
            except Exception as e:
                logger.warning(f"Error recordatorio {rec_id}: {e}")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Error verificando recordatorios: {e}")


async def mantenimiento_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: activa/desactiva modo mantenimiento."""
    user = update.effective_user
    if user.id != OWNER_ID:
        await update.message.reply_text("Solo el administrador puede usar este comando.")
        return
    if not context.args:
        estado = get_modo_mantenimiento()
        txt = "ACTIVO" if estado["activo"] else "INACTIVO"
        await update.message.reply_text(
            f"Modo Mantenimiento: *{txt}*\n\n"
            "`/mantenimiento on [mensaje]`\n`/mantenimiento off`",
            parse_mode='Markdown'
        )
        return
    accion = context.args[0].lower()
    if accion == "on":
        msg_c = " ".join(context.args[1:]) if len(context.args) > 1 else "El bot esta en mantenimiento. Volvemos pronto."
        set_modo_mantenimiento(True, msg_c, user.id)
        await update.message.reply_text(f"Modo mantenimiento ACTIVADO\n\n_{msg_c}_", parse_mode='Markdown')
    elif accion == "off":
        set_modo_mantenimiento(False, "", user.id)
        await update.message.reply_text("Modo mantenimiento DESACTIVADO — Bot operativo.")
    else:
        await update.message.reply_text("Usa: `/mantenimiento on` o `/mantenimiento off`", parse_mode='Markdown')


async def velocidad_voz_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ajusta velocidad de la voz del bot."""
    user = update.effective_user
    pref = get_preferencia_voz(user.id)
    actual = pref.get("velocidad", "normal")
    emojis = {"lento": "🐢", "normal": "▶️", "rapido": "🐇"}
    await update.message.reply_text(
        f"Velocidad de voz actual: {emojis.get(actual,'')} *{actual.upper()}*\n\nSelecciona tu preferencia:",
        parse_mode='Markdown'
    )


async def resumen_diario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Activa/desactiva resumen diario personalizado por audio."""
    user = update.effective_user
    pref = get_preferencia_voz(user.id)
    activo = pref.get("resumen_diario", 0)
    try:
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            nuevo = 0 if activo else 1
            if DATABASE_URL:
                c.execute("""INSERT INTO preferencias_usuario (user_id, resumen_diario)
                    VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET resumen_diario=EXCLUDED.resumen_diario""",
                    (user.id, nuevo))
            else:
                c.execute("INSERT OR REPLACE INTO preferencias_usuario (user_id, resumen_diario) VALUES (?, ?)", (user.id, nuevo))
            conn.commit()
            conn.close()
    except Exception as e:
        logger.warning(f"Error resumen_diario: {e}")

    if activo:
        await update.message.reply_text("Resumen diario *desactivado*.", parse_mode='Markdown')
    else:
        await update.message.reply_text(
            "Resumen diario *activado*\n\n"
            "Recibiras un audio cada manana a las 9:00 AM Chile\n"
            "con novedades relevantes para ti.\n\n"
            "Para desactivar, usa `/resumen_diario` de nuevo.",
            parse_mode='Markdown'
        )


async def enviar_resumenes_diarios(context):
    """Job matutino: envia resumen personalizado por audio."""
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        c.execute("""SELECT p.user_id, s.first_name FROM preferencias_usuario p
            JOIN suscripciones s ON p.user_id=s.user_id
            WHERE p.resumen_diario=1 AND s.estado='activo'""")
        usuarios = c.fetchall()
        conn.close()
        for uid, nombre in usuarios:
            try:
                prompt = (
                    f"Crea un resumen ejecutivo breve (4 oraciones maximas) para {nombre}, "
                    "miembro de Cofradia de Networking. Incluye: novedades networking profesional, "
                    "mercado laboral chileno, y algo motivador. Sin listas ni emojis."
                )
                resumen = llamar_groq(prompt, max_tokens=200, temperature=0.7)
                if not resumen:
                    resumen = llamar_gemini_texto(prompt, max_tokens=200, temperature=0.7)
                if resumen:
                    audio_file = await generar_audio_con_velocidad(resumen, uid, f"/tmp/resumen_{uid}.mp3")
                    if audio_file:
                        import os
                        with open(audio_file, 'rb') as f:
                            await context.bot.send_voice(
                                chat_id=uid, voice=f,
                                caption=f"Buenos dias, {nombre}! Tu resumen diario de la Cofradia."
                            )
                        try:
                            os.remove(audio_file)
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"Error resumen a {uid}: {e}")
    except Exception as e:
        logger.warning(f"Error resumenes diarios: {e}")


# ==================== ANÁLISIS DE USUARIOS ====================

async def analizar_usuario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Analiza mensajes recientes de un cofrade. Uso: /analizar @username"""
    user = update.effective_user
    if not context.args:
        await update.message.reply_text(
            "*Analizar a un cofrade:*\n\n"
            "`/analizar @username` o `/analizar Nombre`\n\n"
            "Busca sus mensajes recientes y genera un análisis de su expertise, "
            "temas de interes y aportes destacados.",
            parse_mode="Markdown"
        )
        return

    busqueda = " ".join(context.args).replace("@", "").strip()
    msg = await update.message.reply_text(f"Analizando mensajes recientes de *{busqueda}*...", parse_mode="Markdown")

    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("Error de conexion.")
            return
        c = conn.cursor()

        if DATABASE_URL:
            c.execute("""SELECT user_id, first_name, username FROM suscripciones
                WHERE LOWER(username) = %s OR LOWER(first_name) LIKE %s LIMIT 1""",
                (busqueda.lower(), f"%{busqueda.lower()}%"))
        else:
            c.execute("""SELECT user_id, first_name, username FROM suscripciones
                WHERE LOWER(username) = ? OR LOWER(first_name) LIKE ? LIMIT 1""",
                (busqueda.lower(), f"%{busqueda.lower()}%"))
        cofrade = c.fetchone()

        if not cofrade:
            await msg.edit_text(f"No encontre a *{busqueda}* en la Cofradia.", parse_mode="Markdown")
            conn.close()
            return

        if DATABASE_URL:
            uid_c = cofrade["user_id"]
            nombre_c = cofrade["first_name"]
            uname_c = cofrade.get("username", "") or ""
        else:
            uid_c, nombre_c, uname_c = cofrade[0], cofrade[1], (cofrade[2] or "")

        if DATABASE_URL:
            c.execute("""SELECT contenido FROM mensajes
                WHERE user_id = %s AND contenido IS NOT NULL
                ORDER BY fecha_mensaje DESC LIMIT 30""", (uid_c,))
        else:
            c.execute("""SELECT contenido FROM mensajes
                WHERE user_id = ? AND contenido IS NOT NULL
                ORDER BY fecha_mensaje DESC LIMIT 30""", (uid_c,))
        rows = c.fetchall()
        conn.close()

        if not rows:
            await msg.edit_text(
                f"*{nombre_c}* no tiene mensajes recientes en el sistema.",
                parse_mode="Markdown"
            )
            return

        textos = [r["contenido"] if DATABASE_URL else r[0] for r in rows if (r["contenido"] if DATABASE_URL else r[0])]
        mensajes_str = "\n---\n".join(textos[:20])

        prompt = (
            f"Eres un analista experto en networking profesional chileno.\n"
            f"Analiza los mensajes recientes de {nombre_c} en la Cofradia de Networking:\n\n"
            f"MENSAJES:\n{mensajes_str}\n\n"
            f"Genera un analisis con:\n"
            f"1. Temas de interes y expertise demostrado\n"
            f"2. Recomendaciones o insights destacados que ha compartido\n"
            f"3. Perfil profesional inferido\n"
            f"4. Tu evaluacion y que agregas al respecto\n\n"
            f"Se especifico y menciona ejemplos concretos. Maximo 200 palabras."
        )

        respuesta = llamar_groq(prompt, max_tokens=600, temperature=0.7)
        if not respuesta:
            respuesta = llamar_gemini_texto(prompt, max_tokens=600, temperature=0.7)
        if not respuesta:
            await msg.edit_text("No pude generar el analisis ahora. Intenta en un momento.")
            return

        contacto = f"@{uname_c}" if uname_c else nombre_c
        texto_final = (
            f"Analisis de *{nombre_c}* ({contacto})\n"
            f"Basado en {len(textos)} mensajes recientes\n\n"
            f"{respuesta}"
        )
        await msg.edit_text(texto_final, parse_mode="Markdown")

        # Audio del analisis
        try:
            audio_file = await generar_audio_con_velocidad(respuesta, user.id, f"/tmp/analisis_{uid_c}.mp3")
            if audio_file:
                import os as _os
                with open(audio_file, "rb") as af:
                    await update.message.reply_voice(
                        voice=af,
                        caption=f"Analisis de {nombre_c}"
                    )
                try: _os.remove(audio_file)
                except: pass
        except Exception as e:
            logger.warning(f"Error audio analisis: {e}")

    except Exception as e:
        logger.error(f"Error analizar_usuario: {e}")
        await msg.edit_text("Error al analizar. Intenta de nuevo.")


# ==================== AGENTE INTELIGENTE DE NETWORKING (v5.0) ====================

async def agente_networking_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agente IA que analiza el perfil del usuario y genera un plan de networking personalizado"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Esta función requiere suscripción activa. Escribe /start")
        return
    
    msg = await update.message.reply_text("🤖 Agente de Networking activado...\n🔍 Analizando tu perfil y la comunidad...")
    
    try:
        # Obtener perfil del usuario
        conn = get_db_connection()
        tarjeta = None
        miembro_data = None
        agenda_items = []
        tareas_pend = []
        
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
                tarjeta = c.fetchone()
                c.execute("SELECT nombre, apellido, generacion FROM nuevos_miembros WHERE user_id = %s LIMIT 1", (user_id,))
                miembro_data = c.fetchone()
                c.execute("""SELECT titulo, fecha_evento, tipo FROM agenda_personal 
                             WHERE user_id = %s AND completada = FALSE AND fecha_evento >= NOW() 
                             ORDER BY fecha_evento ASC LIMIT 5""", (user_id,))
                agenda_items = c.fetchall()
                c.execute("""SELECT tarea, prioridad, categoria FROM tareas_networking 
                             WHERE user_id = %s AND completada = FALSE 
                             ORDER BY fecha_creacion DESC LIMIT 5""", (user_id,))
                tareas_pend = c.fetchall()
            else:
                c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
                tarjeta = c.fetchone()
                c.execute("SELECT nombre, apellido, generacion FROM nuevos_miembros WHERE user_id = ? LIMIT 1", (user_id,))
                miembro_data = c.fetchone()
                c.execute("""SELECT titulo, fecha_evento, tipo FROM agenda_personal 
                             WHERE user_id = ? AND completada = 0 AND fecha_evento >= datetime('now') 
                             ORDER BY fecha_evento ASC LIMIT 5""", (user_id,))
                agenda_items = c.fetchall()
                c.execute("""SELECT tarea, prioridad, categoria FROM tareas_networking 
                             WHERE user_id = ? AND completada = 0 
                             ORDER BY fecha_creacion DESC LIMIT 5""", (user_id,))
                tareas_pend = c.fetchall()
            conn.close()
        
        # Construir info del perfil
        perfil_str = ""
        if tarjeta:
            if DATABASE_URL:
                perfil_str = (f"Nombre: {tarjeta['nombre_completo']}\nProfesión: {tarjeta['profesion']}\n"
                             f"Empresa: {tarjeta['empresa']}\nCiudad: {tarjeta['ciudad']}\n"
                             f"Servicios: {tarjeta['servicios']}")
            else:
                perfil_str = f"Nombre: {tarjeta[1]}\nProfesión: {tarjeta[2]}\nEmpresa: {tarjeta[3]}\nCiudad: {tarjeta[7]}\nServicios: {tarjeta[4]}"
        elif miembro_data:
            if DATABASE_URL:
                perfil_str = f"Nombre: {miembro_data['nombre']} {miembro_data['apellido']}\nGeneración: {miembro_data['generacion']}"
            else:
                perfil_str = f"Nombre: {miembro_data[0]} {miembro_data[1]}\nGeneración: {miembro_data[2]}"
        else:
            perfil_str = f"Usuario: {user.first_name} {user.last_name or ''}"
        
        agenda_str = ""
        if agenda_items:
            lines = []
            for item in agenda_items:
                if DATABASE_URL:
                    lines.append(f"• {item['titulo']} ({str(item['fecha_evento'])[:10]})")
                else:
                    lines.append(f"• {item[0]} ({str(item[1])[:10]})")
            agenda_str = "Próximos eventos agendados:\n" + "\n".join(lines)
        
        tareas_str = ""
        if tareas_pend:
            lines = []
            for t in tareas_pend:
                if DATABASE_URL:
                    lines.append(f"• [{t['prioridad'].upper()}] {t['tarea']} ({t['categoria']})")
                else:
                    lines.append(f"• [{t[1].upper()}] {t[0]} ({t[2]})")
            tareas_str = "Tareas pendientes:\n" + "\n".join(lines)
        
        # Búsqueda de contexto comunitario
        resultados_com = busqueda_unificada(
            f"{user.first_name} networking profesional oportunidades", 
            limit_historial=10, limit_rag=20
        )
        contexto_com = formatear_contexto_unificado(resultados_com, "networking")
        
        argumento = " ".join(context.args) if context.args else ""
        
        prompt = f"""Eres el AGENTE DE NETWORKING de la Cofradía, un asistente IA proactivo y estratégico especializado en desarrollo profesional y networking para oficiales navales.

PERFIL DEL COFRADE:
{perfil_str}
{agenda_str}
{tareas_str}

CONTEXTO DE LA COMUNIDAD:
{contexto_com[:2000]}

SOLICITUD DEL USUARIO: {argumento if argumento else 'Análisis general y plan de acción'}

GENERA UN PLAN DE NETWORKING PERSONALIZADO que incluya:
1. 🎯 **Diagnóstico** (2-3 líneas sobre su posición actual en la red)
2. 🤝 **Top 3 acciones de networking** para esta semana (específicas y accionables)
3. 📢 **Oportunidades detectadas** en la comunidad relevantes para su perfil
4. 📅 **Sugerencia de agenda**: qué actividad programar esta semana
5. 💡 **Tip estratégico** de networking para su sector/perfil
6. 🛠️ **Comandos útiles** del bot que debería usar

Sé específico, motivador y con lenguaje profesional pero cercano. Tutéalo como camarada naval."""
        
        await msg.edit_text("🤖 Generando plan estratégico personalizado...")
        respuesta = llamar_groq(prompt, max_tokens=1800, temperature=0.6)
        
        if not respuesta:
            respuesta = llamar_gemini_texto(prompt, max_tokens=1800, temperature=0.6)
        
        if respuesta:
            header = f"🤖 *AGENTE DE NETWORKING - Plan para {user.first_name}*\n{'━'*35}\n\n"
            try:
                await msg.edit_text(header + respuesta, parse_mode='Markdown')
            except Exception:
                await msg.edit_text(f"🤖 AGENTE DE NETWORKING - Plan para {user.first_name}\n\n{respuesta}")
        else:
            await msg.edit_text("❌ No pude generar el plan en este momento. Intenta de nuevo.")
        
        registrar_servicio_usado(user_id, 'agente_networking')
        
    except Exception as e:
        logger.error(f"Error en agente networking: {e}")
        import traceback; logger.error(traceback.format_exc())
        await msg.edit_text(f"❌ Error en el agente: {str(e)[:100]}")


async def agendar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agenda una actividad/reunión con recordatorio automático.
    Uso: /agendar FECHA HORA Título | Descripción | Lugar | Participantes
    Ejemplo: /agendar 2026-03-15 10:00 Reunión con Juan | Explorar alianza comercial | Cafe Caribe | @juan_cofrade
    """
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción. Escribe /start")
        return
    
    if not context.args or len(context.args) < 3:
        await update.message.reply_text(
            "📅 *Agenda una actividad con recordatorio automático*\n\n"
            "*Uso:* `/agendar FECHA HORA Título`\n"
            "*Ejemplo:* `/agendar 2026-03-15 10:00 Reunión de networking con ex-cofrades`\n\n"
            "*Formato completo:* `/agendar FECHA HORA Título | Descripción | Lugar | Participantes`\n\n"
            "💡 El bot te enviará un recordatorio automático 1 hora antes.",
            parse_mode='Markdown'
        )
        return
    
    try:
        # Parsear fecha y hora
        fecha_str = context.args[0]
        hora_str = context.args[1]
        resto = " ".join(context.args[2:])
        
        partes = [p.strip() for p in resto.split("|")]
        titulo = partes[0] if partes else "Actividad de Networking"
        descripcion = partes[1] if len(partes) > 1 else ""
        lugar = partes[2] if len(partes) > 2 else ""
        participantes = partes[3] if len(partes) > 3 else ""
        
        # Parsear datetime
        try:
            fecha_evento = datetime.strptime(f"{fecha_str} {hora_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            await update.message.reply_text(
                "❌ Formato de fecha/hora incorrecto.\n"
                "Usa: `AAAA-MM-DD HH:MM`\nEjemplo: `2026-03-15 10:00`",
                parse_mode='Markdown'
            )
            return
        
        if fecha_evento < datetime.now():
            await update.message.reply_text("⚠️ La fecha ya pasó. Ingresa una fecha futura.")
            return
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión a la base de datos.")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""INSERT INTO agenda_personal 
                        (user_id, titulo, descripcion, fecha_evento, lugar, tipo, participantes)
                        VALUES (%s, %s, %s, %s, %s, 'reunion', %s) RETURNING id""",
                     (user_id, titulo, descripcion, fecha_evento, lugar, participantes))
            agenda_id = c.fetchone()['id']
        else:
            c.execute("""INSERT INTO agenda_personal 
                        (user_id, titulo, descripcion, fecha_evento, lugar, tipo, participantes)
                        VALUES (?,?,?,?,?,'reunion',?)""",
                     (user_id, titulo, descripcion, fecha_evento.strftime('%Y-%m-%d %H:%M:%S'), lugar, participantes))
            agenda_id = c.lastrowid
        conn.commit()
        conn.close()
        
        # Generar resumen con IA
        prompt_resumen = f"""El cofrade {user.first_name} agendó esta actividad de networking:
Título: {titulo}
Descripción: {descripcion}
Lugar: {lugar}
Participantes: {participantes}
Fecha: {fecha_evento.strftime('%d/%m/%Y a las %H:%M')}

En 2-3 oraciones: (1) Confirma el agendamiento con entusiasmo, (2) Da 1 consejo práctico para prepararse para esta reunión/actividad, (3) Sugiere qué llevar o cómo sacar máximo provecho del networking."""
        
        consejo = llamar_groq(prompt_resumen, max_tokens=300, temperature=0.7) or ""
        
        respuesta = (
            f"✅ *¡Actividad agendada exitosamente!* (ID: #{agenda_id})\n\n"
            f"📌 *{titulo}*\n"
            f"📅 {fecha_evento.strftime('%A %d/%m/%Y a las %H:%M')}\n"
        )
        if lugar:
            respuesta += f"📍 {lugar}\n"
        if participantes:
            respuesta += f"👥 {participantes}\n"
        if descripcion:
            respuesta += f"📝 {descripcion}\n"
        respuesta += f"\n🔔 _Recibirás un recordatorio automático 1 hora antes._\n"
        if consejo:
            respuesta += f"\n💡 {consejo}"
        respuesta += f"\n\n📋 Ver agenda: /mi_agenda | ✅ Marcar completa: /completar_{agenda_id}"
        
        try:
            await update.message.reply_text(respuesta, parse_mode='Markdown')
        except Exception:
            await update.message.reply_text(respuesta.replace('*', '').replace('_', ''))
        
        registrar_servicio_usado(user_id, 'agendar')
        
    except Exception as e:
        logger.error(f"Error en /agendar: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def mi_agenda_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra la agenda personal del usuario con próximas actividades"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción. Escribe /start")
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión.")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""SELECT id, titulo, fecha_evento, lugar, tipo, participantes, descripcion, completada 
                        FROM agenda_personal WHERE user_id = %s 
                        ORDER BY completada ASC, fecha_evento ASC LIMIT 20""", (user_id,))
        else:
            c.execute("""SELECT id, titulo, fecha_evento, lugar, tipo, participantes, descripcion, completada 
                        FROM agenda_personal WHERE user_id = ? 
                        ORDER BY completada ASC, fecha_evento ASC LIMIT 20""", (user_id,))
        items = c.fetchall()
        conn.close()
        
        if not items:
            await update.message.reply_text(
                "📅 *Tu agenda está vacía*\n\n"
                "Agenda tu primera actividad con:\n"
                "`/agendar AAAA-MM-DD HH:MM Título | Descripción | Lugar`",
                parse_mode='Markdown'
            )
            return
        
        ahora = datetime.now()
        proximas = []
        pasadas = []
        completadas = []
        
        for item in items:
            if DATABASE_URL:
                item_id, titulo, fecha, lugar, tipo, partic, desc, comp = (
                    item['id'], item['titulo'], item['fecha_evento'], 
                    item['lugar'], item['tipo'], item['participantes'],
                    item['descripcion'], item['completada']
                )
            else:
                item_id, titulo, fecha, lugar, tipo, partic, desc, comp = item
            
            fecha_dt = fecha if hasattr(fecha, 'strftime') else datetime.strptime(str(fecha)[:19], '%Y-%m-%d %H:%M:%S')
            
            if comp:
                completadas.append((item_id, titulo, fecha_dt, lugar))
            elif fecha_dt >= ahora:
                proximas.append((item_id, titulo, fecha_dt, lugar, partic))
            else:
                pasadas.append((item_id, titulo, fecha_dt, lugar))
        
        texto = f"📅 *AGENDA DE {user.first_name.upper()}*\n{'━'*30}\n\n"
        
        if proximas:
            texto += "🔜 *PRÓXIMAS ACTIVIDADES:*\n"
            for item_id, titulo, fecha_dt, lugar, partic in proximas:
                dias_resto = (fecha_dt - ahora).days
                urgencia = "🔴" if dias_resto <= 1 else "🟡" if dias_resto <= 7 else "🟢"
                texto += f"{urgencia} *#{item_id}* {titulo}\n"
                texto += f"   📅 {fecha_dt.strftime('%d/%m/%Y %H:%M')}"
                if lugar:
                    texto += f" | 📍 {lugar}"
                if partic:
                    texto += f"\n   👥 {partic}"
                texto += f"\n   ✅ `/completar_{item_id}`\n\n"
        
        if pasadas:
            texto += "⏰ *PENDIENTES (fecha pasada):*\n"
            for item_id, titulo, fecha_dt, lugar in pasadas[:5]:
                texto += f"⚠️ *#{item_id}* {titulo} ({fecha_dt.strftime('%d/%m')})\n"
                texto += f"   ✅ `/completar_{item_id}` | 🗑 `/eliminar_agenda_{item_id}`\n"
            texto += "\n"
        
        if completadas:
            texto += f"✅ *COMPLETADAS:* {len(completadas)} actividades\n\n"
        
        texto += "➕ *Nueva:* `/agendar FECHA HORA Título`"
        
        try:
            await update.message.reply_text(texto, parse_mode='Markdown')
        except Exception:
            await update.message.reply_text(texto.replace('*', '').replace('_', ''))
        
    except Exception as e:
        logger.error(f"Error en /mi_agenda: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def nueva_tarea_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agrega una tarea de networking pendiente.
    Uso: /tarea [alta|media|baja] categoria texto
    Ejemplo: /tarea alta contacto Llamar a Pedro García sobre proyecto logística
    """
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción. Escribe /start")
        return
    
    if not context.args:
        await update.message.reply_text(
            "📋 *Agregar tarea de networking*\n\n"
            "*Uso:* `/tarea [prioridad] texto`\n\n"
            "*Ejemplos:*\n"
            "`/tarea alta Llamar a Pedro sobre proyecto logística`\n"
            "`/tarea media Actualizar perfil LinkedIn con nuevo cargo`\n"
            "`/tarea baja Revisar ofertas de empleo sector marítimo`\n\n"
            "📋 Ver tareas: /mis_tareas",
            parse_mode='Markdown'
        )
        return
    
    args_texto = " ".join(context.args)
    prioridad = 'normal'
    
    if context.args[0].lower() in ['alta', 'urgente', 'high']:
        prioridad = 'alta'
        args_texto = " ".join(context.args[1:])
    elif context.args[0].lower() in ['media', 'medium', 'normal']:
        prioridad = 'media'
        args_texto = " ".join(context.args[1:])
    elif context.args[0].lower() in ['baja', 'low']:
        prioridad = 'baja'
        args_texto = " ".join(context.args[1:])
    
    if not args_texto.strip():
        await update.message.reply_text("❌ Escribe el texto de la tarea después de la prioridad.")
        return
    
    # Detectar categoría automáticamente con IA
    categoria = 'general'
    cats_map = {
        'contacto': ['llamar', 'contactar', 'escribir', 'mensaje', 'email', 'correo'],
        'perfil': ['linkedin', 'cv', 'perfil', 'actualizar', 'bio'],
        'evento': ['asistir', 'evento', 'reunión', 'reunion', 'conferencia', 'seminario'],
        'empleo': ['oferta', 'empleo', 'trabajo', 'postular', 'aplicar', 'entrevista'],
        'negocio': ['proyecto', 'propuesta', 'contrato', 'alianza', 'socio', 'negocio'],
        'seguimiento': ['seguimiento', 'follow', 'recordar', 'confirmar', 'responder'],
    }
    texto_lower = args_texto.lower()
    for cat, palabras in cats_map.items():
        if any(p in texto_lower for p in palabras):
            categoria = cat
            break
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Error de conexión.")
            return
        
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""INSERT INTO tareas_networking (user_id, tarea, prioridad, categoria)
                        VALUES (%s, %s, %s, %s) RETURNING id""",
                     (user_id, args_texto, prioridad, categoria))
            tarea_id = c.fetchone()['id']
        else:
            c.execute("""INSERT INTO tareas_networking (user_id, tarea, prioridad, categoria)
                        VALUES (?,?,?,?)""",
                     (user_id, args_texto, prioridad, categoria))
            tarea_id = c.lastrowid
        conn.commit()
        conn.close()
        
        emoji_prior = {'alta': '🔴', 'media': '🟡', 'baja': '🟢', 'normal': '⚪'}.get(prioridad, '⚪')
        
        await update.message.reply_text(
            f"✅ *Tarea #{tarea_id} guardada*\n\n"
            f"{emoji_prior} [{prioridad.upper()}] {args_texto}\n"
            f"🏷️ Categoría: {categoria}\n\n"
            f"📋 Ver todas: /mis_tareas\n"
            f"✅ Completar: `/ok_{tarea_id}`",
            parse_mode='Markdown'
        )
        registrar_servicio_usado(user_id, 'tarea')
        
    except Exception as e:
        logger.error(f"Error en /tarea: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def mis_tareas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra las tareas de networking pendientes"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción.")
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        if DATABASE_URL:
            c.execute("""SELECT id, tarea, prioridad, categoria, completada, fecha_creacion 
                        FROM tareas_networking WHERE user_id = %s 
                        ORDER BY completada ASC, 
                            CASE prioridad WHEN 'alta' THEN 1 WHEN 'media' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END,
                            fecha_creacion DESC LIMIT 25""", (user_id,))
        else:
            c.execute("""SELECT id, tarea, prioridad, categoria, completada, fecha_creacion 
                        FROM tareas_networking WHERE user_id = ? 
                        ORDER BY completada ASC, 
                            CASE prioridad WHEN 'alta' THEN 1 WHEN 'media' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END,
                            fecha_creacion DESC LIMIT 25""", (user_id,))
        tareas = c.fetchall()
        conn.close()
        
        if not tareas:
            await update.message.reply_text(
                "📋 *No tienes tareas pendientes*\n\n"
                "Agrega una:\n`/tarea alta Llamar a Juan García sobre proyecto`",
                parse_mode='Markdown'
            )
            return
        
        pendientes = []
        completadas_list = []
        
        for t in tareas:
            if DATABASE_URL:
                tid, texto, prior, cat, comp, fecha = t['id'], t['tarea'], t['prioridad'], t['categoria'], t['completada'], t['fecha_creacion']
            else:
                tid, texto, prior, cat, comp, fecha = t
            
            if comp:
                completadas_list.append((tid, texto, prior))
            else:
                pendientes.append((tid, texto, prior, cat))
        
        emoji_p = {'alta': '🔴', 'media': '🟡', 'baja': '🟢', 'normal': '⚪'}
        
        texto_resp = f"📋 *TAREAS DE NETWORKING - {user.first_name}*\n{'━'*30}\n\n"
        
        if pendientes:
            texto_resp += "⏳ *PENDIENTES:*\n"
            for tid, texto, prior, cat in pendientes:
                ep = emoji_p.get(prior, '⚪')
                texto_resp += f"{ep} *#{tid}* {texto[:80]}\n"
                texto_resp += f"   🏷️ {cat} | ✅ `/ok_{tid}`\n\n"
        
        if completadas_list:
            texto_resp += f"✅ *Completadas:* {len(completadas_list)}\n\n"
        
        texto_resp += "➕ *Nueva:* `/tarea [alta|media|baja] texto`"
        
        try:
            await update.message.reply_text(texto_resp, parse_mode='Markdown')
        except Exception:
            await update.message.reply_text(texto_resp.replace('*', '').replace('_', ''))
        
    except Exception as e:
        logger.error(f"Error en /mis_tareas: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:100]}")


async def match_networking_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Agente IA que encuentra los cofrades más compatibles para networking según tu perfil"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción. Escribe /start")
        return
    
    msg = await update.message.reply_text("🔍 Analizando perfiles de la Cofradía para encontrar tus mejores conexiones...")
    
    try:
        conn = get_db_connection()
        if not conn:
            await msg.edit_text("❌ Error de conexión.")
            return
        
        c = conn.cursor()
        
        # Obtener perfil propio
        if DATABASE_URL:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = %s", (user_id,))
            mi_tarjeta = c.fetchone()
            # Obtener otros cofrades con tarjeta
            c.execute("""SELECT user_id, nombre_completo, profesion, empresa, ciudad, servicios 
                        FROM tarjetas_profesional WHERE user_id != %s LIMIT 40""", (user_id,))
            otros = c.fetchall()
        else:
            c.execute("SELECT * FROM tarjetas_profesional WHERE user_id = ?", (user_id,))
            mi_tarjeta = c.fetchone()
            c.execute("""SELECT user_id, nombre_completo, profesion, empresa, ciudad, servicios 
                        FROM tarjetas_profesional WHERE user_id != ? LIMIT 40""", (user_id,))
            otros = c.fetchall()
        conn.close()
        
        if not mi_tarjeta:
            await msg.edit_text(
                "⚠️ No tienes tarjeta profesional creada.\n\n"
                "Crea tu perfil con /mi_tarjeta para que el agente pueda hacer match inteligente."
            )
            return
        
        if DATABASE_URL:
            mi_perfil = f"{mi_tarjeta['nombre_completo']} | {mi_tarjeta['profesion']} | {mi_tarjeta['empresa']} | {mi_tarjeta['ciudad']} | {mi_tarjeta['servicios']}"
        else:
            mi_perfil = f"{mi_tarjeta[1]} | {mi_tarjeta[2]} | {mi_tarjeta[3]} | {mi_tarjeta[7]} | {mi_tarjeta[4]}"
        
        if not otros:
            await msg.edit_text(
                "ℹ️ Aún no hay suficientes perfiles en el directorio para hacer match.\n\n"
                "Invita a otros cofrades a crear su tarjeta con /mi_tarjeta"
            )
            return
        
        otros_str = "\n".join([
            f"- {(o['nombre_completo'] if DATABASE_URL else o[1])} | {(o['profesion'] if DATABASE_URL else o[2])} | {(o['empresa'] if DATABASE_URL else o[3])} | {(o['ciudad'] if DATABASE_URL else o[4])}"
            for o in otros[:30]
        ])
        
        busqueda_extra = " ".join(context.args) if context.args else ""
        
        prompt = f"""Eres el Motor de Match de Networking de la Cofradía. Analiza el perfil del cofrade y encuentra las mejores conexiones estratégicas.

MI PERFIL:
{mi_perfil}
{f"Búsqueda específica: {busqueda_extra}" if busqueda_extra else ""}

COFRADES DISPONIBLES EN EL DIRECTORIO:
{otros_str}

TAREA: Identifica los TOP 5 cofrades más valiosos para hacer networking con {user.first_name}, y para cada uno:
1. Nombre y por qué es una conexión estratégica
2. Qué tienen en común o qué sinergias existen
3. Una propuesta de primer mensaje o tema de conversación concreto
4. Potencial de colaboración (escala 1-10 con razón)

Sé específico y estratégico. Piensa en sinergias de negocio, intercambio de expertise, oportunidades laborales mutuas."""
        
        await msg.edit_text("🤝 Calculando compatibilidades y sinergias...")
        respuesta = llamar_groq(prompt, max_tokens=1600, temperature=0.6)
        
        if not respuesta:
            respuesta = llamar_gemini_texto(prompt, max_tokens=1600, temperature=0.6)
        
        if respuesta:
            header = f"🤝 *MATCH DE NETWORKING para {user.first_name}*\n{'━'*35}\n\n"
            try:
                await msg.edit_text(header + respuesta, parse_mode='Markdown')
            except Exception:
                await msg.edit_text(f"🤝 MATCH para {user.first_name}\n\n{respuesta}")
        else:
            await msg.edit_text("❌ No pude generar el análisis. Intenta de nuevo.")
        
        registrar_servicio_usado(user_id, 'match_networking')
        
    except Exception as e:
        logger.error(f"Error en match_networking: {e}")
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


async def briefing_diario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera un briefing diario de networking: agenda, tareas, oportunidades de la comunidad"""
    user = update.effective_user
    user_id = user.id
    es_owner = (user_id == OWNER_ID)
    
    if not es_owner and not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("🔒 Requiere suscripción.")
        return
    
    msg = await update.message.reply_text("☀️ Preparando tu briefing diario de networking...")
    
    try:
        ahora = datetime.now()
        hoy_str = ahora.strftime("%A %d/%m/%Y")
        
        conn = get_db_connection()
        agenda_hoy = []
        tareas_alta = []
        proximos_7 = []
        
        if conn:
            c = conn.cursor()
            if DATABASE_URL:
                c.execute("""SELECT titulo, fecha_evento, lugar FROM agenda_personal 
                             WHERE user_id = %s AND completada = FALSE 
                             AND fecha_evento::date = CURRENT_DATE
                             ORDER BY fecha_evento ASC""", (user_id,))
                agenda_hoy = c.fetchall()
                c.execute("""SELECT tarea, prioridad, categoria FROM tareas_networking 
                             WHERE user_id = %s AND completada = FALSE AND prioridad = 'alta'
                             ORDER BY fecha_creacion ASC LIMIT 5""", (user_id,))
                tareas_alta = c.fetchall()
                c.execute("""SELECT titulo, fecha_evento, lugar FROM agenda_personal 
                             WHERE user_id = %s AND completada = FALSE 
                             AND fecha_evento > CURRENT_TIMESTAMP
                             AND fecha_evento <= CURRENT_TIMESTAMP + INTERVAL '7 days'
                             ORDER BY fecha_evento ASC LIMIT 5""", (user_id,))
                proximos_7 = c.fetchall()
            else:
                c.execute("""SELECT titulo, fecha_evento, lugar FROM agenda_personal 
                             WHERE user_id = ? AND completada = 0 
                             AND date(fecha_evento) = date('now')
                             ORDER BY fecha_evento ASC""", (user_id,))
                agenda_hoy = c.fetchall()
                c.execute("""SELECT tarea, prioridad, categoria FROM tareas_networking 
                             WHERE user_id = ? AND completada = 0 AND prioridad = 'alta'
                             ORDER BY fecha_creacion ASC LIMIT 5""", (user_id,))
                tareas_alta = c.fetchall()
                c.execute("""SELECT titulo, fecha_evento, lugar FROM agenda_personal 
                             WHERE user_id = ? AND completada = 0 
                             AND fecha_evento > datetime('now')
                             AND fecha_evento <= datetime('now', '+7 days')
                             ORDER BY fecha_evento ASC LIMIT 5""", (user_id,))
                proximos_7 = c.fetchall()
            conn.close()
        
        # Actividad reciente de la comunidad
        resultados_com = busqueda_unificada("networking empleo oportunidades noticias", limit_historial=10, limit_rag=15)
        contexto_com = formatear_contexto_unificado(resultados_com, "networking hoy")
        
        agenda_hoy_str = ""
        if agenda_hoy:
            lines = []
            for item in agenda_hoy:
                if DATABASE_URL:
                    hora = str(item['fecha_evento'])[11:16]
                    lines.append(f"⏰ {hora} - {item['titulo']}" + (f" (📍 {item['lugar']})" if item['lugar'] else ""))
                else:
                    hora = str(item[1])[11:16]
                    lines.append(f"⏰ {hora} - {item[0]}" + (f" (📍 {item[2]})" if item[2] else ""))
            agenda_hoy_str = "HOY EN TU AGENDA:\n" + "\n".join(lines)
        
        tareas_str = ""
        if tareas_alta:
            lines = []
            for t in tareas_alta:
                if DATABASE_URL:
                    lines.append(f"🔴 {t['tarea']}")
                else:
                    lines.append(f"🔴 {t[0]}")
            tareas_str = "TAREAS URGENTES:\n" + "\n".join(lines)
        
        proximos_str = ""
        if proximos_7:
            lines = []
            for item in proximos_7:
                if DATABASE_URL:
                    fecha_d = str(item['fecha_evento'])[:10]
                    lines.append(f"• {fecha_d}: {item['titulo']}")
                else:
                    lines.append(f"• {str(item[1])[:10]}: {item[0]}")
            proximos_str = "PRÓXIMOS 7 DÍAS:\n" + "\n".join(lines)
        
        prompt = f"""Eres el asistente de networking de la Cofradía. Genera un BRIEFING DIARIO motivador y estratégico para {user.first_name}.

FECHA: {hoy_str}
{agenda_hoy_str}
{tareas_str}
{proximos_str}

ACTIVIDAD RECIENTE EN LA COMUNIDAD:
{contexto_com[:1500]}

GENERA UN BRIEFING DIARIO que incluya:
🌅 **Buenos días** con frase motivadora naval/profesional
📅 **Agenda de hoy** (si hay items, destácalos; si no, sugiere qué hacer)
🔴 **Urgente** (tareas prioritarias de hoy)  
📡 **Radar de oportunidades** (1-2 oportunidades detectadas en la comunidad hoy)
🎯 **Acción estrella del día** (la 1 cosa más importante que debería hacer hoy para su networking)
💬 **Mensaje para compartir** (algo breve e inspirador para el grupo)

Sé conciso pero impactante. Tono energético, profesional y de camaradería."""
        
        respuesta = llamar_groq(prompt, max_tokens=1200, temperature=0.7)
        
        if not respuesta:
            respuesta = llamar_gemini_texto(prompt, max_tokens=1200, temperature=0.7)
        
        if respuesta:
            try:
                await msg.edit_text(f"☀️ *BRIEFING {hoy_str.upper()}*\n{'━'*30}\n\n{respuesta}", parse_mode='Markdown')
            except Exception:
                await msg.edit_text(f"☀️ BRIEFING {hoy_str}\n\n{respuesta}")
        else:
            await msg.edit_text("❌ No pude generar el briefing. Intenta de nuevo.")
        
        registrar_servicio_usado(user_id, 'briefing_diario')
        
    except Exception as e:
        logger.error(f"Error en briefing_diario: {e}")
        await msg.edit_text(f"❌ Error: {str(e)[:100]}")


async def completar_item_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja /completar_ID y /ok_ID para marcar agenda/tareas como completadas"""
    user = update.effective_user
    user_id = user.id
    
    comando_texto = update.message.text.split()[0].lstrip('/')
    
    tipo = None
    item_id = None
    
    if comando_texto.startswith('completar_'):
        tipo = 'agenda'
        try:
            item_id = int(comando_texto.replace('completar_', ''))
        except ValueError:
            pass
    elif comando_texto.startswith('ok_'):
        tipo = 'tarea'
        try:
            item_id = int(comando_texto.replace('ok_', ''))
        except ValueError:
            pass
    elif comando_texto.startswith('eliminar_agenda_'):
        try:
            item_id = int(comando_texto.replace('eliminar_agenda_', ''))
            tipo = 'eliminar_agenda'
        except ValueError:
            pass
    
    if not item_id or not tipo:
        await update.message.reply_text("❌ ID no válido.")
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        
        if tipo == 'agenda':
            if DATABASE_URL:
                c.execute("UPDATE agenda_personal SET completada = TRUE WHERE id = %s AND user_id = %s", (item_id, user_id))
            else:
                c.execute("UPDATE agenda_personal SET completada = 1 WHERE id = ? AND user_id = ?", (item_id, user_id))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"✅ Actividad #{item_id} marcada como completada. ¡Bien hecho! 💪")
        elif tipo == 'tarea':
            if DATABASE_URL:
                c.execute("UPDATE tareas_networking SET completada = TRUE WHERE id = %s AND user_id = %s", (item_id, user_id))
            else:
                c.execute("UPDATE tareas_networking SET completada = 1 WHERE id = ? AND user_id = ?", (item_id, user_id))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"✅ Tarea #{item_id} completada. ¡Excelente! 🎯")
        elif tipo == 'eliminar_agenda':
            if DATABASE_URL:
                c.execute("DELETE FROM agenda_personal WHERE id = %s AND user_id = %s", (item_id, user_id))
            else:
                c.execute("DELETE FROM agenda_personal WHERE id = ? AND user_id = ?", (item_id, user_id))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"🗑️ Actividad #{item_id} eliminada de tu agenda.")
    except Exception as e:
        logger.error(f"Error completando item: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)[:50]}")


async def recordatorio_agenda_job(context: ContextTypes.DEFAULT_TYPE):
    """Job que envía recordatorios 1 hora antes de actividades agendadas"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        c = conn.cursor()
        
        if DATABASE_URL:
            c.execute("""SELECT a.id, a.user_id, a.titulo, a.fecha_evento, a.lugar, a.participantes
                        FROM agenda_personal a
                        WHERE a.completada = FALSE 
                        AND a.recordatorio_enviado = FALSE
                        AND a.fecha_evento BETWEEN NOW() + INTERVAL '55 minutes' AND NOW() + INTERVAL '65 minutes'""")
        else:
            c.execute("""SELECT id, user_id, titulo, fecha_evento, lugar, participantes
                        FROM agenda_personal
                        WHERE completada = 0 AND recordatorio_enviado = 0
                        AND fecha_evento BETWEEN datetime('now', '+55 minutes') AND datetime('now', '+65 minutes')""")
        
        items = c.fetchall()
        
        for item in items:
            if DATABASE_URL:
                item_id = item['id']
                uid = item['user_id']
                titulo = item['titulo']
                fecha = item['fecha_evento']
                lugar = item['lugar'] or ''
                partic = item['participantes'] or ''
            else:
                item_id, uid, titulo, fecha, lugar, partic = item
                lugar = lugar or ''
                partic = partic or ''
            
            try:
                hora_str = str(fecha)[11:16] if fecha else '??:??'
                mensaje = (
                    f"🔔 *RECORDATORIO - ¡En 1 hora!*\n\n"
                    f"📌 *{titulo}*\n"
                    f"⏰ {hora_str}\n"
                )
                if lugar:
                    mensaje += f"📍 {lugar}\n"
                if partic:
                    mensaje += f"👥 {partic}\n"
                mensaje += f"\n¡Prepárate para tu actividad de networking! 💼\n"
                mensaje += f"✅ `/completar_{item_id}`"
                
                await context.bot.send_message(chat_id=uid, text=mensaje, parse_mode='Markdown')
                
                # Marcar recordatorio como enviado
                if DATABASE_URL:
                    c.execute("UPDATE agenda_personal SET recordatorio_enviado = TRUE WHERE id = %s", (item_id,))
                else:
                    c.execute("UPDATE agenda_personal SET recordatorio_enviado = 1 WHERE id = ?", (item_id,))
                conn.commit()
                
            except Exception as e_msg:
                logger.debug(f"No se pudo enviar recordatorio a {uid}: {e_msg}")
        
        conn.close()
        
    except Exception as e:
        logger.debug(f"Error en job recordatorio_agenda: {e}")



# ==================== FERIADOS CHILE ====================

@requiere_suscripcion
async def feriados_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /feriados [año] - Lista feriados legales de Chile con coincidencias de eventos"""
    import re as _re

    # Determinar año consultado
    anio_actual = datetime.now().year
    if context.args:
        try:
            anio = int(context.args[0])
            if anio < 2020 or anio > 2035:
                await update.message.reply_text("❌ Indica un año entre 2020 y 2035.\nEjemplo: /feriados 2025")
                return
        except ValueError:
            await update.message.reply_text("❌ Formato: /feriados 2025")
            return
    else:
        anio = anio_actual

    msg = await update.message.reply_text("🗓 Consultando feriados legales de Chile " + str(anio) + "...")

    feriados_data = []

    # ── Fuente 1: API feriados.cl (JSON oficial) ──
    try:
        import asyncio as _afer
        def _fetch_feriados_api():
            url = "https://apis.digital.gob.cl/fl/feriados/" + str(anio)
            r = requests.get(url, timeout=15, headers={'Accept': 'application/json'})
            if r.status_code == 200:
                return r.json()
            return []
        raw = await _afer.get_event_loop().run_in_executor(None, _fetch_feriados_api)
        if raw and isinstance(raw, list):
            for item in raw:
                fecha_str = item.get('fecha', '')
                nombre    = item.get('nombre', '')
                tipo      = item.get('tipo', 'Legal')
                if fecha_str and nombre:
                    try:
                        fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d')
                        feriados_data.append({
                            'fecha': fecha_dt,
                            'nombre': nombre,
                            'tipo': tipo
                        })
                    except:
                        pass
    except Exception as _e1:
        logger.debug("Feriados API digital.gob.cl: " + str(_e1))

    # ── Fuente 2: Web scraping si la API falla ──
    if not feriados_data:
        try:
            import asyncio as _afer2
            def _scrape_feriados():
                return buscar_en_web(
                    "feriados legales Chile " + str(anio) + " lista completa",
                    extraer_contenido=True)
            wr = await _afer2.get_event_loop().run_in_executor(None, _scrape_feriados)
            if wr.get('resultados') or wr.get('contenido'):
                ctx_web = formatear_contexto_web(wr)
                prompt = (
                    "Extrae TODOS los feriados legales de Chile para el año " + str(anio) + ".\n"
                    "Contexto web:\n" + ctx_web[:3000] + "\n\n"
                    "Responde SOLO con una lista JSON, sin texto adicional, formato:\n"
                    '[{"fecha":"YYYY-MM-DD","nombre":"Nombre del feriado","tipo":"Legal"}]\n'
                    "Incluye TODOS los feriados nacionales de Chile."
                )
                resp_json = llamar_groq(prompt, max_tokens=1200, temperature=0.1)
                if resp_json:
                    import json as _json2
                    clean = resp_json.strip()
                    # Extract JSON array
                    m = _re.search(r'\[.*\]', clean, _re.DOTALL)
                    if m:
                        clean = m.group(0)
                    items = _json2.loads(clean)
                    for item in items:
                        try:
                            fd = datetime.strptime(item['fecha'], '%Y-%m-%d')
                            feriados_data.append({'fecha': fd, 'nombre': item['nombre'], 'tipo': item.get('tipo', 'Legal')})
                        except:
                            pass
        except Exception as _e2:
            logger.debug("Feriados web scraping: " + str(_e2))

    # ── Fuente 3: Hardcoded fallback para años conocidos ──
    if not feriados_data:
        FERIADOS_FIJOS = {
            '01-01': 'Año Nuevo',
            '05-01': 'Día del Trabajo',
            '05-21': 'Día de las Glorias Navales',
            '06-20': 'Día Nacional de los Pueblos Indígenas',
            '06-29': 'San Pedro y San Pablo',
            '07-16': 'Día de la Virgen del Carmen',
            '08-15': 'Asunción de la Virgen',
            '09-18': 'Independencia Nacional',
            '09-19': 'Día de las Glorias del Ejército',
            '10-12': 'Encuentro de Dos Mundos',
            '10-31': 'Día de las Iglesias Evangélicas',
            '11-01': 'Día de Todos los Santos',
            '12-08': 'Inmaculada Concepción',
            '12-25': 'Navidad',
        }
        for dd_mm, nombre in FERIADOS_FIJOS.items():
            try:
                fd = datetime.strptime(str(anio) + '-' + dd_mm, '%Y-%m-%d')
                feriados_data.append({'fecha': fd, 'nombre': nombre, 'tipo': 'Legal'})
            except:
                pass

    if not feriados_data:
        await msg.edit_text("❌ No se pudo obtener los feriados de " + str(anio) + ". Intenta más tarde.")
        return

    # Ordenar por fecha
    feriados_data.sort(key=lambda x: x['fecha'])

    # ── Obtener eventos de la Cofradía para comparar ──
    eventos_cofradia = {}
    try:
        conn_ev = get_db_connection()
        if conn_ev:
            c_ev = conn_ev.cursor()
            if DATABASE_URL:
                c_ev.execute(
                    "SELECT fecha, titulo FROM eventos WHERE EXTRACT(YEAR FROM fecha) = %s",
                    (anio,))
            else:
                c_ev.execute(
                    "SELECT fecha, titulo FROM eventos WHERE strftime('%Y', fecha) = ?",
                    (str(anio),))
            for row in c_ev.fetchall():
                fd = row['fecha'] if DATABASE_URL else row[0]
                tit = row['titulo'] if DATABASE_URL else row[1]
                if fd:
                    try:
                        if isinstance(fd, str):
                            fd = datetime.strptime(fd[:10], '%Y-%m-%d')
                        key = fd.strftime('%Y-%m-%d')
                        eventos_cofradia[key] = tit
                    except:
                        pass
            conn_ev.close()
    except Exception as _ev:
        logger.debug("Feriados: eventos query: " + str(_ev))

    # ── Construir respuesta ──
    DIAS_ES = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves',
               4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
    MESES_ES = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
                5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
                9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}

    lineas = [
        "*🗓 FERIADOS LEGALES DE CHILE — " + str(anio) + "*",
        "━" * 32,
        "_Fuente: Gobierno Digital de Chile_",
        "",
    ]

    mes_actual = 0
    for f in feriados_data:
        fd = f['fecha']
        if fd.month != mes_actual:
            mes_actual = fd.month
            lineas.append("")
            lineas.append("*📅 " + MESES_ES[fd.month].upper() + "*")
        dia_str   = fd.strftime('%d') + " de " + MESES_ES[fd.month]
        dia_sem   = DIAS_ES[fd.weekday()]
        nombre_f  = f['nombre']
        tipo_f    = f.get('tipo', 'Legal')
        clave_ev  = fd.strftime('%Y-%m-%d')
        evento_tag = ""
        if clave_ev in eventos_cofradia:
            evento_tag = "  🎯 _Evento Cofradía: " + eventos_cofradia[clave_ev] + "_"
        icono = "🔴" if tipo_f == 'Legal' else "🟡"
        lineas.append(icono + " *" + dia_str + "* (" + dia_sem + ") — " + nombre_f + evento_tag)

    lineas.append("")
    lineas.append("━" * 32)
    lineas.append("*Total:* " + str(len(feriados_data)) + " feriados  •  🔴 Legal  🟡 Otros")
    if eventos_cofradia:
        lineas.append("🎯 *Eventos Cofradía que coinciden con feriados:* " + str(sum(1 for k in eventos_cofradia if any(f['fecha'].strftime('%Y-%m-%d') == k for f in feriados_data))))

    texto_final = "\n".join(lineas)

    # Telegram tiene límite de 4096 chars — dividir si necesario
    if len(texto_final) <= 4096:
        await msg.edit_text(texto_final, parse_mode='Markdown')
    else:
        await msg.edit_text(texto_final[:4090] + "...", parse_mode='Markdown')

    registrar_servicio_usado(update.effective_user.id, 'feriados')







async def buscar_web_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        motores = "DuckDuckGo"
        if GOOGLE_CSE_KEY: motores += " + Google CSE"
        if BRAVE_API_KEY:  motores += " + Brave"
        motores += " + SearXNG"
        await update.message.reply_text(
            "Uso: /buscar_web [consulta]\n\n"
            "Ejemplos:\n"
            "  /buscar_web noticias economia Chile hoy\n"
            "  /buscar_web precio cobre bolsa\n"
            "  /buscar_web requisitos visa trabajo Australia\n\n"
            "Motores: " + motores + "\n"
            "Resultados guardados en base de conocimientos automaticamente."
        )
        return

    consulta = ' '.join(context.args)
    user_id  = update.effective_user.id
    msg      = await update.message.reply_text("Buscando en Internet...")
    try:
        import asyncio as _aw
        web_result = await _aw.get_event_loop().run_in_executor(
            None, lambda: buscar_en_web(consulta, extraer_contenido=True))

        if not web_result or not web_result.get('resultados'):
            await msg.edit_text("No encontre resultados para: " + consulta)
            return

        desde_cache = web_result.get('desde_cache', False)
        motor       = web_result.get('fuente_motor', 'web')
        resultados  = web_result.get('resultados', [])
        badge       = "(desde cache)" if desde_cache else "(via " + motor + ")"

        if ia_disponible:
            await msg.edit_text("Sintetizando con IA...")
            ctx = formatear_contexto_web(web_result)
            prompt = (
                "Eres el asistente IA de la Cofradia de Networking.\n"
                "El usuario busco en Internet: \"" + consulta + "\"\n\n"
                + ctx +
                "\nSintetiza los resultados de forma clara en espanol chileno. "
                "Cita fuentes por nombre. Maximo 400 palabras."
            )
            import asyncio as _aw2
            def _ia():
                r = llamar_groq(prompt, max_tokens=1000, temperature=0.4)
                return r or llamar_gemini_texto(prompt, max_tokens=1000, temperature=0.4)
            resp_ia = await _aw2.get_event_loop().run_in_executor(None, _ia)
        else:
            resp_ia = None

        await msg.delete()

        if resp_ia:
            texto = "BUSQUEDA WEB: " + consulta + "\n" + badge + "\n" + "-"*28 + "\n\n" + resp_ia + "\n\n"
            for i, it in enumerate(resultados[:3], 1):
                t = it.get('titulo','')[:60]
                u = it.get('url','')
                if t: texto += str(i) + ". " + t + ("\n   " + u if u else "") + "\n"
        else:
            texto = "BUSQUEDA WEB: " + consulta + "\n" + badge + "\n" + "-"*28 + "\n\n"
            for i, it in enumerate(resultados, 1):
                texto += str(i) + ". " + it.get('titulo','Sin titulo') + "\n"
                if it.get('snippet'): texto += it['snippet'][:200] + "\n"
                if it.get('url'):     texto += it['url'] + "\n"
                texto += "\n"

        await enviar_mensaje_largo(update, texto)
        registrar_servicio_usado(user_id, 'buscar_web')
        if not desde_cache and web_result.get('total', 0) > 0:
            await update.message.reply_text("Resultados guardados. Proxima busqueda similar sera instantanea.")
    except Exception as e:
        logger.error("buscar_web_comando: " + str(e), exc_info=True)
        await msg.edit_text("Error en busqueda web: " + str(e)[:100])

# ==================== INDICADORES ECONÓMICOS CHILE v6.0 + IA ====================

def obtener_indicadores_chile():
    """
    Obtiene indicadores desde mindicador.cl (API REST gratuita del Banco Central).
    Retorna dict con:
      - datos_actuales: 11 cards con sparkline 30 días + variación
      - hist_6m: Dólar vs Euro últimos 6 meses (valor inicio de cada mes)
      - hist_10a: series anuales últimos 10 años por indicador
    Usa ThreadPoolExecutor para paralelizar ~80 llamadas históricas.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    BASE   = 'https://mindicador.cl/api'
    AHORA  = datetime.now()
    ANIO_A = AHORA.year

    # ── Indicadores para cards actuales ──────────────────────────────────
    CARDS_CFG = [
        ('uf',             'UF',              'Unidad de Fomento',                   '#c3a55a'),
        ('dolar',          'Dólar USD',       'Tipo de cambio USD/CLP',              '#3478c3'),
        ('euro',           'Euro EUR',        'Tipo de cambio EUR/CLP',              '#2980b9'),
        ('utm',            'UTM',             'Unidad Tributaria Mensual',           '#e67e22'),
        ('ipc',            'IPC',             'Índice de Precios al Consumidor',     '#2ecc71'),
        ('tpm',            'TPM',             'Tasa Política Monetaria (%)',         '#27ae60'),
        ('bitcoin',        'Bitcoin',         'Bitcoin en CLP',                      '#9b59b6'),
        ('tasa_desempleo', 'Desempleo',       'Tasa de Desempleo (%)',               '#e74c3c'),
        ('imacec',         'IMACEC',          'Indicador Mensual Actividad Eco. (%)', '#f39c12'),
        ('libra_cobre',    'Cobre (lb)',      'Precio Libra de Cobre USD',           '#cd7f32'),
        ('ivp',            'IVP',             'Índice Valor Promedio',               '#1abc9c'),
    ]
    # Deduplicar
    seen = set()
    CARDS_CFG_UNIQ = [t for t in CARDS_CFG if t[0] not in seen and not seen.add(t[0])]

    # ── Indicadores para histórico 10 años ────────────────────────────────
    HIST_CFG = [
        ('dolar',          'Dólar USD',       '#3478c3'),
        ('euro',           'Euro EUR',        '#2980b9'),
        ('uf',             'UF',              '#c3a55a'),
        ('utm',            'UTM',             '#e67e22'),
        ('ipc',            'IPC',             '#2ecc71'),
        ('tasa_desempleo', 'Desempleo (%)',   '#e74c3c'),
        ('imacec',         'IMACEC (%)',      '#f39c12'),
        ('libra_cobre',    'Cobre (USD/lb)',  '#cd7f32'),
        ('bitcoin',        'Bitcoin (CLP)',   '#9b59b6'),
    ]

    def fetch(url, timeout=12):
        try:
            r = requests.get(url, timeout=timeout,
                             headers={'Accept': 'application/json'})
            if r.status_code == 200:
                return r.json()
        except Exception as _e:
            logger.debug('mindicador fetch: ' + str(_e))
        return None

    # ── 1. Cards: serie últimos 30 días ───────────────────────────────────
    datos_actuales = {}
    card_tasks = {cod: BASE + '/' + cod for cod, *_ in CARDS_CFG_UNIQ}
    with ThreadPoolExecutor(max_workers=12) as ex:
        fut_map = {ex.submit(fetch, url): cod for cod, url in card_tasks.items()}
        for fut in as_completed(fut_map):
            cod = fut_map[fut]
            j   = fut.result()
            if j:
                serie = j.get('serie', [])
                if serie:
                    cfg = next((c for c in CARDS_CFG_UNIQ if c[0] == cod), None)
                    datos_actuales[cod] = {
                        'nombre':      cfg[1] if cfg else cod,
                        'descripcion': cfg[2] if cfg else '',
                        'color':       cfg[3] if cfg else '#3478c3',
                        'valor':       serie[0].get('valor', 0),
                        'fecha':       serie[0].get('fecha', '')[:10],
                        'unidad':      j.get('unidad_medida', ''),
                        'serie30':     serie[:30],
                    }

    # ── 2. Dólar vs Euro últimos 6 meses (inicio de cada mes) ────────────
    def primer_valor_mes(serie, anio, mes):
        for s in reversed(serie):
            f = s.get('fecha', '')[:7]
            if f == '%04d-%02d' % (anio, mes):
                return s.get('valor', None)
        return None

    hist_6m = {}
    meses_labels   = []
    meses_targets  = []
    for i in range(5, -1, -1):
        mes_offset = AHORA.month - i
        yr_offset  = ANIO_A
        if mes_offset <= 0:
            mes_offset += 12
            yr_offset  -= 1
        meses_targets.append((yr_offset, mes_offset))
        meses_labels.append('%02d/%d' % (mes_offset, yr_offset))

    anios_necesarios = list(set(yr for yr, _ in meses_targets))
    serie_dol = {}
    serie_eur = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {}
        for yr in anios_necesarios:
            futs[ex.submit(fetch, BASE + '/dolar/' + str(yr))] = ('dolar', yr)
            futs[ex.submit(fetch, BASE + '/euro/'  + str(yr))] = ('euro',  yr)
        for fut in as_completed(futs):
            cod, yr = futs[fut]
            j = fut.result()
            serie = j.get('serie', []) if j else []
            if cod == 'dolar': serie_dol[yr] = serie
            else:              serie_eur[yr] = serie

    hist_6m['labels'] = meses_labels
    hist_6m['dolar']  = []
    hist_6m['euro']   = []
    for yr, mes in meses_targets:
        vd = primer_valor_mes(serie_dol.get(yr, []), yr, mes)
        ve = primer_valor_mes(serie_eur.get(yr, []), yr, mes)
        hist_6m['dolar'].append(round(vd, 2) if vd else None)
        hist_6m['euro'].append(round(ve, 2) if ve else None)

    # ── 3. Histórico 10 años (valor representativo por año) ───────────────
    ANIOS_10 = list(range(ANIO_A - 9, ANIO_A + 1))
    hist_10a = {cod: {'nombre': nom, 'color': col, 'anios': ANIOS_10, 'valores': []}
                for cod, nom, col in HIST_CFG}

    tasks_hist = [(cod, yr) for cod, *_ in HIST_CFG for yr in ANIOS_10]
    raw_hist   = {}

    def fetch_year(cod_yr):
        cod, yr = cod_yr
        j = fetch(BASE + '/' + cod + '/' + str(yr), timeout=14)
        return (cod, yr, j.get('serie', []) if j else [])

    with ThreadPoolExecutor(max_workers=30) as ex:
        for cod, yr, serie in ex.map(fetch_year, tasks_hist):
            raw_hist[(cod, yr)] = serie

    PORCENTAJES = {'ipc', 'tasa_desempleo', 'imacec', 'tpm'}
    for cod, nom, col in HIST_CFG:
        valores = []
        for yr in ANIOS_10:
            serie = raw_hist.get((cod, yr), [])
            if not serie:
                valores.append(None)
                continue
            if cod in PORCENTAJES:
                vals = [s.get('valor', 0) for s in serie if s.get('valor') is not None]
                v = round(sum(vals) / len(vals), 3) if vals else None
            else:
                v = round(serie[0].get('valor', 0), 2)
            valores.append(v)
        hist_10a[cod]['valores'] = valores

    return {
        'datos_actuales': datos_actuales,
        'hist_6m':        hist_6m,
        'hist_10a':       hist_10a,
        'generado':       AHORA.strftime('%d/%m/%Y %H:%M'),
    }


def obtener_indicadores_cmf():
    """
    Obtiene 3 indicadores de la CMF (Comisión para el Mercado Financiero):
      - TMC: Tasa de Interés Máxima Convencional, filtrada para 3 plazos representativos
      - TAB UF: Tasa Activa Bancaria en UF a 360 días (publicada diariamente por CMF)
      - TAB Nominal: Tasa Activa Bancaria nominal (endpoint extendido si disponible)
    API: https://api.cmfchile.cl/api-sbifv3/recursos_api/
    """
    AHORA   = datetime.now()
    # Intentar mes actual; si falla (aún sin datos), retroceder al mes anterior
    intentos = [(AHORA.year, AHORA.month)]
    mes_ant = AHORA.month - 1 or 12
    anio_ant = AHORA.year if AHORA.month > 1 else AHORA.year - 1
    intentos.append((anio_ant, mes_ant))

    resultado = {
        'tmc':     [],
        'tab_uf':  [],
        'tab_nom': [],
    }
    HDR = {'Accept': 'application/json', 'User-Agent': 'CofrBot/6.0'}

    def _get(url):
        try:
            r = requests.get(url, headers=HDR, timeout=12)
            if r.status_code == 200:
                return r.json()
        except Exception as _e:
            logger.debug(f'CMF fetch: {_e}')
        return None

    def _val(s):
        try:
            return float(str(s).replace(',', '.').strip())
        except Exception:
            return None

    # ── TMC: Tasa Máxima Convencional ─────────────────────────────────────────
    for yr, mo in intentos:
        url = f'{CMF_BASE_URL}/tmc/{yr}/{mo:02d}?apikey={CMF_API_KEY}&formato=json'
        j = _get(url)
        if not j:
            continue
        tmcs = j.get('TMCs', [])
        if not tmcs:
            continue

        # Estrategia de filtrado por palabras clave en Titulo+SubTitulo
        # Tres plazos representativos para el usuario:
        GRUPOS = [
            ('90 días',  ['menos de 90', 'inferior a 90', 'hasta 90']),
            ('180 días', ['90 días o más', '90 d', 'sobre 90', 'mediano']),
            ('360 días', ['un año o más', '360', 'reajustable', 'largo plazo']),
        ]
        encontrados = {}
        for tmc in tmcs:
            txt = (tmc.get('Titulo', '') + ' ' + tmc.get('SubTitulo', '')).lower()
            v   = _val(tmc.get('Valor'))
            if v is None or v <= 0:
                continue
            for plazo, kws in GRUPOS:
                if plazo not in encontrados and any(kw in txt for kw in kws):
                    encontrados[plazo] = {
                        'plazo':  plazo,
                        'titulo': tmc.get('Titulo', '')[:70],
                        'valor':  v,
                        'fecha':  tmc.get('Fecha', '')[:10],
                    }
        # Si no se detectaron los 3 plazos por palabras clave, tomar los primeros 3 únicos
        if len(encontrados) < 3:
            etiquetas = ['90 días', '180 días', '360 días']
            vistos = set()
            for tmc in tmcs:
                titulo = tmc.get('Titulo', '')
                if titulo in vistos:
                    continue
                v = _val(tmc.get('Valor'))
                if v and v > 0 and len(encontrados) < 3:
                    idx = len(encontrados)
                    label = etiquetas[idx] if idx < 3 else f'Tipo {idx+1}'
                    if label not in encontrados:
                        encontrados[label] = {
                            'plazo':  label,
                            'titulo': titulo[:70],
                            'valor':  v,
                            'fecha':  tmc.get('Fecha', '')[:10],
                        }
                    vistos.add(titulo)

        resultado['tmc'] = list(encontrados.values())[:3]
        break  # datos obtenidos

    # ── TAB UF (Tasa Activa Bancaria en UF a 360 días) ─────────────────────────
    for yr, mo in intentos:
        url = f'{CMF_BASE_URL}/tab/{yr}/{mo:02d}?apikey={CMF_API_KEY}&formato=json'
        j = _get(url)
        if not j:
            continue
        tabs = j.get('TABs', [])
        if tabs:
            tab = tabs[0]  # más reciente
            v   = _val(tab.get('Valor'))
            fecha = tab.get('Fecha', '')[:10]
            # CMF publica TAB solo a 360 días en UF; 90 y 180 días no disponibles vía API
            resultado['tab_uf'] = [
                {'plazo': '90 días',  'valor': None, 'fecha': fecha,
                 'nota': 'No disponible vía API'},
                {'plazo': '180 días', 'valor': None, 'fecha': fecha,
                 'nota': 'No disponible vía API'},
                {'plazo': '360 días', 'valor': v,    'fecha': fecha,
                 'nota': 'Dato oficial CMF'},
            ]
            break

    # ── TAB Nominal (endpoint extendido; si no existe → N/D con fecha referencial) ──
    tab_nom_val = None
    tab_nom_fecha = ''
    for yr, mo in intentos:
        # Intentar endpoint alternativo tabnom
        for endpoint in ['tabnom', 'tab_nom', 'tabnominal']:
            url = f'{CMF_BASE_URL}/{endpoint}/{yr}/{mo:02d}?apikey={CMF_API_KEY}&formato=json'
            j = _get(url)
            if j:
                for key in ['TABNoms', 'TABNominales', 'TABs']:
                    items = j.get(key, [])
                    if items:
                        tab_nom_val   = _val(items[0].get('Valor'))
                        tab_nom_fecha = items[0].get('Fecha', '')[:10]
                        break
            if tab_nom_val:
                break
        if tab_nom_val:
            break
    # Si el TAB UF está disponible, usar como referencia para TAB Nominal 360d
    if tab_nom_val is None and resultado['tab_uf']:
        tab_360 = next((x for x in resultado['tab_uf'] if x['plazo'] == '360 días'), None)
        tab_nom_fecha = tab_360['fecha'] if tab_360 else AHORA.strftime('%Y-%m-%d')
    resultado['tab_nom'] = [
        {'plazo': '90 días',  'valor': None,          'fecha': tab_nom_fecha},
        {'plazo': '180 días', 'valor': None,          'fecha': tab_nom_fecha},
        {'plazo': '360 días', 'valor': tab_nom_val,   'fecha': tab_nom_fecha},
    ]

    return resultado


def obtener_rentabilidad_afp():
    """
    Obtiene variación de rentabilidad de todas las AFPs en los últimos 6 meses.
    API: https://www.quetalmiafp.cl/api/Cuota/ObtenerCuotas
    Cabecera requerida: X-API-Key
    Calcula (cuota_final / cuota_inicial - 1) * 100 por AFP y Fondo.
    """
    AHORA        = datetime.now()
    fecha_fin    = AHORA.strftime('%d/%m/%Y')
    # Inicio = 6 meses atrás (primer día de ese mes)
    mes_ini  = AHORA.month - 6
    anio_ini = AHORA.year
    if mes_ini <= 0:
        mes_ini  += 12
        anio_ini -= 1
    fecha_ini = f'01/{mes_ini:02d}/{anio_ini}'

    resultado = {
        'afps':     [],
        'fondos':   [],
        'tabla':    {},   # {afp: {fondo: {rent_pct, cuota_ini, cuota_fin, fecha_ini, fecha_fin}}}
        'fecha_ini': fecha_ini,
        'fecha_fin': fecha_fin,
        'ok':        False,
    }

    try:
        params = {
            'listaAFPs':    AFP_LISTA,
            'listaFondos':  AFP_FONDOS,
            'fechaInicial': fecha_ini,
            'fechaFinal':   fecha_fin,
        }
        hdrs = {
            'X-API-Key': AFP_API_KEY,
            'Accept':    'application/json',
        }
        r = requests.get(AFP_CUOTAS_URL, params=params, headers=hdrs, timeout=30)
        if r.status_code != 200:
            logger.warning(f'AFP API status: {r.status_code}')
            return resultado

        data = r.json()
        if not isinstance(data, list) or not data:
            logger.warning('AFP API: respuesta vacía o inesperada')
            return resultado

        # Detectar nombres de campos dinámicamente
        sample = data[0]
        keys_lower = {k.lower(): k for k in sample.keys()}

        def _campo(*candidatos):
            for c in candidatos:
                if c in keys_lower:
                    return keys_lower[c]
            return None

        campo_afp   = _campo('afp', 'nombreafp', 'nombre_afp', 'administradora')
        campo_fondo = _campo('fondo', 'tipofondo', 'tipo_fondo', 'letra', 'nombrefondo')
        campo_fecha = _campo('fecha', 'fechacuota', 'fecha_cuota', 'date')
        campo_valor = _campo('valorcuota', 'valor_cuota', 'cuota', 'valor', 'value')

        if not all([campo_afp, campo_fondo, campo_fecha, campo_valor]):
            logger.warning(f'AFP API campos no mapeados. Keys: {list(sample.keys())}')
            return resultado

        # Agrupar serie por (AFP, Fondo)
        series = {}
        for item in data:
            afp   = str(item.get(campo_afp,   '')).upper().strip()
            fondo = str(item.get(campo_fondo, '')).upper().strip()
            fecha = str(item.get(campo_fecha, ''))
            try:
                val = float(str(item.get(campo_valor, '0')).replace(',', '.'))
            except Exception:
                continue
            key = (afp, fondo)
            if key not in series:
                series[key] = []
            series[key].append((fecha, val))

        tabla     = {}
        afps_set  = set()
        fondos_set = set()

        for (afp, fondo), serie in series.items():
            if len(serie) < 2:
                continue
            serie_s   = sorted(serie, key=lambda x: x[0])
            cuota_ini = serie_s[0][1]
            cuota_fin = serie_s[-1][1]
            if cuota_ini and cuota_ini != 0:
                rent_pct = round(((cuota_fin / cuota_ini) - 1) * 100, 2)
            else:
                rent_pct = 0.0
            if afp not in tabla:
                tabla[afp] = {}
            tabla[afp][fondo] = {
                'rent_pct':  rent_pct,
                'cuota_ini': round(cuota_ini, 4),
                'cuota_fin': round(cuota_fin, 4),
                'fecha_ini': serie_s[0][0],
                'fecha_fin': serie_s[-1][0],
            }
            afps_set.add(afp)
            fondos_set.add(fondo)

        ORDEN_FONDOS = ['A', 'B', 'C', 'D', 'E']
        resultado['afps']   = sorted(afps_set)
        resultado['fondos'] = [f for f in ORDEN_FONDOS if f in fondos_set]
        resultado['tabla']  = tabla
        resultado['ok']     = bool(tabla)

    except Exception as e:
        logger.warning(f'AFP API error: {e}')

    return resultado


def scraping_bcentral_noticias():
    """
    Web scraping de noticias y comunicados recientes del Banco Central de Chile.
    Retorna lista de titulares relevantes (máx. 5).
    """
    noticias = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; CofrBot/1.0)'}
        resp = requests.get(
            'https://www.bcentral.cl/web/guest/comunicados-de-prensa',
            headers=headers, timeout=12
        )
        if resp.status_code == 200 and bs4_disponible:
            soup = BeautifulSoup(resp.text, 'html.parser')
            for item in soup.find_all(['h3', 'h4', 'a'], limit=60):
                texto = item.get_text(strip=True)
                if len(texto) > 30 and any(kw in texto.lower() for kw in
                        ['tasa', 'inflación', 'ipc', 'política monetaria',
                         'dólar', 'tipo de cambio', 'pib', 'crecimiento',
                         'uf', 'utm', 'banca', 'desempleo', 'imacec']):
                    noticias.append(texto[:160])
                    if len(noticias) >= 5:
                        break
    except Exception as e:
        logger.warning(f'Scraping BCCH: {e}')
    return noticias


def consultar_rag_economia(query, limit=8):
    """
    Busca en la biblioteca RAG fragmentos sobre el tema económico dado.
    Retorna string con los fragmentos más relevantes.
    """
    try:
        resultados, score = buscar_rag(query, limit=limit)
        if not resultados:
            return ''
        fragmentos = []
        for texto, source in resultados[:4]:
            nombre_libro = source.replace('.pdf', '').replace('_', ' ')
            fragmentos.append(f'[{nombre_libro}]: {texto[:300].strip()}')
        return '\n\n'.join(fragmentos)
    except Exception as e:
        logger.warning(f'RAG economia error: {e}')
        return ''


def generar_explicacion_indicador(codigo, datos, contexto_bcch, fragmento_rag):
    """
    Usa Groq para generar una explicación breve y precisa (2 oraciones)
    del por qué varió el indicador. Fusiona Banco Central + RAG.
    """
    nombre    = datos['nombre']
    val_act   = datos['valor']
    val_ant_list = datos.get('serie30', [])
    val_ant   = val_ant_list[1].get('valor', val_act) if len(val_ant_list) >= 2 else val_act
    if val_ant and val_ant != 0:
        var_pct = ((val_act - val_ant) / val_ant) * 100
    else:
        var_pct = 0.0
    tendencia = 'subió' if var_pct > 0 else ('bajó' if var_pct < 0 else 'se mantuvo')

    contexto_extra = ''
    if contexto_bcch:
        contexto_extra += f'\nContexto Banco Central de Chile:\n{contexto_bcch[:500]}\n'
    if fragmento_rag:
        contexto_extra += f'\nReferencia bibliográfica:\n{fragmento_rag[:350]}\n'

    prompt = (
        f'Eres un economista chileno experto. En MÁXIMO 2 oraciones breves y directas, '
        f'explica POR QUÉ el indicador {nombre} {tendencia} de {val_ant:.2f} a {val_act:.2f} '
        f'({var_pct:+.3f}%). '
        f'Usa el contexto del Banco Central y la teoría económica si corresponde. '
        f'NO uses emojis, asteriscos ni formatos especiales. Solo las 2 oraciones.'
        f'{contexto_extra}'
    )
    resp = llamar_groq(prompt, max_tokens=150, temperature=0.3)
    if not resp:
        if var_pct > 0:
            return f'{nombre} aumentó {abs(var_pct):.3f}% respecto al período anterior.'
        elif var_pct < 0:
            return f'{nombre} disminuyó {abs(var_pct):.3f}% respecto al período anterior.'
        else:
            return f'{nombre} se mantuvo sin variación respecto al período anterior.'
    return resp.strip()


def generar_html_indicadores(all_data, explicaciones):
    """
    Dashboard ECharts COMPLETO — fusión versión v2 + análisis IA:
      Sección 1  — 11 Cards actuales con sparkline 30 días
      Sección 2  — Análisis IA: por qué varió cada indicador (con Groq + RAG)
      Sección 3  — Dólar vs Euro últimos 6 meses (línea doble)
      Sección 4  — Series históricas últimos 10 años (grid 9 gráficos de área)
    """
    import json as _j

    datos    = all_data.get('datos_actuales', {})
    hist_6m  = all_data.get('hist_6m', {})
    hist_10a = all_data.get('hist_10a', {})
    gen      = all_data.get('generado', '')

    PORCENTAJES = {'ipc', 'tpm', 'tasa_desempleo', 'imacec'}
    HIST_ORDER  = ['uf', 'dolar', 'euro', 'utm', 'ipc', 'tasa_desempleo',
                   'imacec', 'libra_cobre', 'bitcoin']

    def fmt(v, cod):
        if v is None: return 'N/D'
        if cod in PORCENTAJES: return '{:.2f}%'.format(v)
        if cod == 'bitcoin':   return '${:,.0f}'.format(v).replace(',', '.')
        if cod == 'libra_cobre': return 'USD {:.4f}'.format(v)
        return '${:,.2f}'.format(v).replace(',', 'X').replace('.', ',').replace('X', '.')

    def variacion_html(serie):
        if len(serie) < 2: return ''
        v0 = serie[0].get('valor', 0)
        v1 = serie[1].get('valor', 1)
        if not v1: return ''
        pct = ((v0 - v1) / v1) * 100
        sig = '▲' if pct >= 0 else '▼'
        col = '#2ecc71' if pct >= 0 else '#e74c3c'
        return '<span style="color:' + col + ';font-size:.68em">' + sig + ' {:.3f}%</span>'.format(abs(pct))

    # ── Cards + sparklines ────────────────────────────────────────────────
    ORDER = ['uf', 'dolar', 'euro', 'utm', 'ipc', 'tpm',
             'bitcoin', 'tasa_desempleo', 'imacec', 'libra_cobre', 'ivp']
    cards_html = ''
    spark_js   = ''
    for cod in ORDER:
        d = datos.get(cod)
        if not d: continue
        col     = d['color']
        vals30  = [s.get('valor', 0) for s in reversed(d['serie30'])]
        spark_d = _j.dumps([round(v, 2) for v in vals30])
        xd      = _j.dumps(list(range(len(vals30))))
        var_h   = variacion_html(d['serie30'])
        val_fmt = fmt(d['valor'], cod)
        cards_html += (
            '<div class="card">'
            '<div class="cn">' + d['nombre'] + '</div>'
            '<div class="cv">' + val_fmt + ' ' + var_h + '</div>'
            '<div class="cd">' + d['descripcion'] + '</div>'
            '<div id="sp_' + cod + '" class="spark"></div>'
            '<div class="cf">Actualizado: ' + d['fecha'] + '</div>'
            '</div>'
        )
        spark_js += (
            '(function(){'
            'var c=echarts.init(document.getElementById("sp_' + cod + '"));'
            'c.setOption({'
            'grid:{top:0,bottom:0,left:0,right:0},'
            'xAxis:{type:"category",show:false,data:' + xd + '},'
            'yAxis:{type:"value",show:false},'
            'series:[{type:"line",data:' + spark_d + ',smooth:true,symbol:"none",'
            'lineStyle:{color:"' + col + '",width:2},'
            'areaStyle:{color:{type:"linear",x:0,y:0,x2:0,y2:1,'
            'colorStops:[{offset:0,color:"' + col + '55"},{offset:1,color:"transparent"}]}}'
            '}]});'
            '})();'
        )

    # ── Cards de análisis IA ─────────────────────────────────────────────
    exp_cards_html = ''
    EXP_ORDER = ['uf', 'dolar', 'euro', 'utm', 'ipc', 'tpm',
                 'tasa_desempleo', 'imacec', 'libra_cobre']
    for cod in EXP_ORDER:
        d   = datos.get(cod)
        exp = explicaciones.get(cod, '')
        if not d or not exp: continue
        serie = d.get('serie30', [])
        v0 = serie[0].get('valor', 0) if serie else 0
        v1 = serie[1].get('valor', v0) if len(serie) >= 2 else v0
        if v1 and v1 != 0:
            pct = ((v0 - v1) / v1) * 100
        else:
            pct = 0.0
        flecha = '▲' if pct >= 0 else '▼'
        col_borde = '#2ecc71' if pct >= 0 else '#e74c3c'
        val_fmt     = fmt(v0, cod)
        val_ant_fmt = fmt(v1, cod)
        exp_cards_html += (
            '<div class="exp-card" style="border-left:4px solid ' + col_borde + '">'
            '<div class="exp-hdr">'
            '<span class="exp-nm">' + d['nombre'] + '</span>'
            '<span class="exp-badge" style="background:' + col_borde + '22;color:' + col_borde + ';border:1px solid ' + col_borde + '55">'
            + flecha + ' {:.3f}%</span>'.format(abs(pct)) +
            '</div>'
            '<div class="exp-vals">'
            '<span class="lbl">Actual:</span>'
            '<span style="font-weight:700;color:' + col_borde + '">' + val_fmt + '</span>'
            '<span class="sep">vs</span>'
            '<span class="lbl">Anterior:</span>'
            '<span style="color:#8899aa">' + val_ant_fmt + '</span>'
            '</div>'
            '<div class="exp-why">&#129302; ¿POR QUÉ?</div>'
            '<div class="exp-txt">' + exp.replace('<', '&lt;').replace('>', '&gt;') + '</div>'
            '<div class="exp-src">Fuentes: Banco Central de Chile &middot; mindicador.cl &middot; RAG Biblioteca Financiera &middot; Groq IA</div>'
            '</div>'
        )

    # ── Dólar vs Euro 6 meses ─────────────────────────────────────────────
    labels_6m = _j.dumps(hist_6m.get('labels', []))
    dolar_6m  = _j.dumps(hist_6m.get('dolar', []))
    euro_6m   = _j.dumps(hist_6m.get('euro', []))

    # ── Histórico 10 años ─────────────────────────────────────────────────
    hist_charts_js   = ''
    hist_cards_html  = ''
    for cod in HIST_ORDER:
        d = hist_10a.get(cod)
        if not d: continue
        col     = d['color']
        anios_j = _j.dumps([str(a) for a in d['anios']])
        vals_j  = _j.dumps([v if v is not None else None for v in d['valores']])
        is_pct  = 'true' if cod in PORCENTAJES else 'false'
        cid     = 'hist_' + cod
        hist_cards_html += (
            '<div class="hcard">'
            '<div class="htitle">' + d['nombre'] + ' — Últimos 10 años</div>'
            '<div id="' + cid + '" class="hchart"></div>'
            '</div>'
        )
        hist_charts_js += (
            '(function(){'
            'var c=echarts.init(document.getElementById("' + cid + '"));'
            'var isPct=' + is_pct + ';'
            'c.setOption({'
            'backgroundColor:"transparent",'
            'tooltip:{trigger:"axis",backgroundColor:"rgba(15,47,89,.95)",borderColor:"' + col + '",'
            'textStyle:{color:"#c0c8d4"},'
            'formatter:function(p){'
            'var v=p[0].value;if(v===null)return p[0].name+": N/D";'
            'return p[0].name+": "+(isPct?v.toFixed(2)+"%":"$"+v.toLocaleString("es-CL"));'
            '}},'
            'grid:{left:"12%",right:"5%",bottom:"18%",top:"8%"},'
            'xAxis:{type:"category",data:' + anios_j + ',axisLabel:{color:"#8899aa",fontSize:10,rotate:30},axisLine:{lineStyle:{color:"#2a4a6a"}}},'
            'yAxis:{type:"value",axisLabel:{color:"#8899aa",fontSize:10,'
            'formatter:function(v){return isPct?v+"%":"$"+v.toLocaleString("es-CL");}},'
            'splitLine:{lineStyle:{color:"rgba(52,120,195,.12)"}}},'
            'series:[{type:"line",data:' + vals_j + ',smooth:true,symbol:"circle",symbolSize:5,'
            'connectNulls:true,'
            'lineStyle:{color:"' + col + '",width:2.5},'
            'itemStyle:{color:"' + col + '"},'
            'areaStyle:{color:{type:"linear",x:0,y:0,x2:0,y2:1,'
            'colorStops:[{offset:0,color:"' + col + '44"},{offset:1,color:"transparent"}]}},'
            'emphasis:{itemStyle:{borderWidth:3,borderColor:"#fff",shadowBlur:8,shadowColor:"' + col + '"}}'
            '}]});'
            '})();'
        )

    # ── HTML completo ──────────────────────────────────────────────────────
    hist_ids_js = ','.join(['"hist_' + cod + '"' for cod in HIST_ORDER if hist_10a.get(cod)])

    # ── CMF: cards HTML ────────────────────────────────────────────────────
    datos_cmf = all_data.get('datos_cmf', {})
    cmf_html  = ''

    def _cmf_card(titulo, emoji, items):
        """Genera una card CMF con tabla de plazos."""
        filas = ''
        ultima_fecha = ''
        for it in items:
            plazo = it.get('plazo', '')
            v     = it.get('valor')
            ultima_fecha = it.get('fecha', '') or ultima_fecha
            if v is not None:
                val_str = f'{v:.2f}%'
                val_cls = 'cmf-val'
            else:
                val_str = 'N/D'
                val_cls = 'cmf-val nd'
            filas += (
                f'<div class="cmf-row data">'
                f'<span class="cmf-plazo">{plazo}</span>'
                f'<span class="{val_cls}">{val_str}</span>'
                f'</div>'
            )
        if not filas:
            filas = '<div class="cmf-row data"><span class="cmf-plazo">Sin datos</span><span class="cmf-val nd">N/D</span></div>'
        fecha_txt = f'Actualizado: {ultima_fecha}' if ultima_fecha else ''
        return (
            f'<div class="cmf-card">'
            f'<div class="cmf-nm">{emoji} {titulo}</div>'
            f'<div class="cmf-row header"><span>Plazo</span><span>Tasa anual</span></div>'
            f'{filas}'
            f'<div class="cmf-fecha">{fecha_txt}</div>'
            f'</div>'
        )

    cmf_html += _cmf_card(
        'Tasa Máxima Convencional (TMC)', '🏦',
        datos_cmf.get('tmc', []) or [
            {'plazo': '90 días',  'valor': None, 'fecha': ''},
            {'plazo': '180 días', 'valor': None, 'fecha': ''},
            {'plazo': '360 días', 'valor': None, 'fecha': ''},
        ]
    )
    cmf_html += _cmf_card(
        'Tasa TAB Nominal', '📋',
        datos_cmf.get('tab_nom', []) or [
            {'plazo': '90 días',  'valor': None, 'fecha': ''},
            {'plazo': '180 días', 'valor': None, 'fecha': ''},
            {'plazo': '360 días', 'valor': None, 'fecha': ''},
        ]
    )
    cmf_html += _cmf_card(
        'Tasa TAB UF', '💹',
        datos_cmf.get('tab_uf', []) or [
            {'plazo': '90 días',  'valor': None, 'fecha': ''},
            {'plazo': '180 días', 'valor': None, 'fecha': ''},
            {'plazo': '360 días', 'valor': None, 'fecha': ''},
        ]
    )

    # ── AFP: tabla de rentabilidad + gráfico ECharts ───────────────────────
    import json as _jafp
    datos_afp   = all_data.get('datos_afp', {})
    afp_lista   = datos_afp.get('afps',   [])
    fondos_list = datos_afp.get('fondos', [])
    tabla_afp   = datos_afp.get('tabla',  {})
    afp_fecha_ini = datos_afp.get('fecha_ini', '')
    afp_fecha_fin = datos_afp.get('fecha_fin', '')
    afp_ok      = datos_afp.get('ok', False)

    # Colores por fondo
    FONDO_COLOR = {
        'A': '#e74c3c', 'B': '#e67e22', 'C': '#f39c12',
        'D': '#2ecc71', 'E': '#3478c3',
    }
    FONDO_DESC = {
        'A': 'Renta Variable', 'B': 'Mixto+',
        'C': 'Mixto',          'D': 'Mixto-',
        'E': 'Renta Fija',
    }

    if afp_ok and afp_lista and fondos_list:
        # Cabecera tabla
        thead_ths = '<th class="afp-nm">AFP</th>'
        for f in fondos_list:
            desc = FONDO_DESC.get(f, '')
            col  = FONDO_COLOR.get(f, '#8899aa')
            thead_ths += (
                f'<th style="color:{col}">Fondo {f}'
                f'<br/><span style="font-size:.65em;font-weight:400;color:#8899aa">{desc}</span>'
                f'</th>'
            )
        afp_rows = ''
        for afp in afp_lista:
            row = f'<tr><td class="afp-nm">{afp}</td>'
            for f in fondos_list:
                cell = tabla_afp.get(afp, {}).get(f)
                if cell is None:
                    row += '<td class="nd">N/D</td>'
                else:
                    rp  = cell['rent_pct']
                    sig = '+' if rp >= 0 else ''
                    cls = 'pos' if rp >= 0 else 'neg'
                    row += f'<td class="{cls}">{sig}{rp:.2f}%</td>'
            row += '</tr>'
            afp_rows += row

        afp_html = (
            f'<div class="afp-wrap">'
            f'<table class="afp-tabla">'
            f'<thead><tr>{thead_ths}</tr></thead>'
            f'<tbody>{afp_rows}</tbody>'
            f'</table>'
            f'</div>'
            f'<div id="chart_afp"></div>'
            f'<div class="afp-meta">Período: {afp_fecha_ini} al {afp_fecha_fin} · '
            f'Rentabilidad = variación del valor cuota · Fuente: AFP</div>'
        )

        # Datos para gráfico de barras agrupadas ECharts (una serie por Fondo)
        afp_chart_series = []
        for f in fondos_list:
            col  = FONDO_COLOR.get(f, '#8899aa')
            vals = []
            for afp in afp_lista:
                cell = tabla_afp.get(afp, {}).get(f)
                vals.append(round(cell['rent_pct'], 2) if cell else None)
            afp_chart_series.append({
                'name':      f'Fondo {f}',
                'type':      'bar',
                'data':      vals,
                'itemStyle': {'color': col},
                'label':     {
                    'show': True, 'position': 'top',
                    'fontSize': 9, 'color': col,
                    'formatter': 'function(p){return p.value===null?"":((p.value>=0?"+":"")+p.value.toFixed(1)+"%")}',
                },
            })
        afp_names_js  = _jafp.dumps(afp_lista)
        afp_series_raw = _jafp.dumps(afp_chart_series)
        # El formatter de label necesita ser función JS, no string; reemplazar aquí
        afp_series_js = afp_series_raw.replace(
            '"formatter": "function(p){return p.value===null?\\"\\":'
            '((p.value>=0?\\"+\\":\\"\\")+p.value.toFixed(1)+\\"%\\")}"',
            '"formatter": function(p){return p.value===null?"":'
            '((p.value>=0?"+":"")+p.value.toFixed(1)+"%")}'
        )
        afp_chart_js = (
            '(function(){'
            'var ca=echarts.init(document.getElementById("chart_afp"));'
            'ca.setOption({'
            'backgroundColor:"transparent",'
            'tooltip:{trigger:"axis",backgroundColor:"rgba(15,47,89,.95)",'
            'borderColor:"#c3a55a",textStyle:{color:"#c0c8d4"},'
            'formatter:function(p){'
            'var s=p[0].name+"<br/>";'
            'p.forEach(function(i){'
            'if(i.value!==null)s+=\'<span style="color:\'+i.color+\'">●</span> \''
            '+i.seriesName+": "+(i.value>=0?"+":"")+i.value.toFixed(2)+"%<br/>";'
            '});return s;}},'
            'legend:{textStyle:{color:"#8899aa"},top:4},'
            'grid:{left:"10%",right:"3%",bottom:"15%",top:"14%"},'
            'xAxis:{type:"category",data:' + _jafp.dumps(afp_lista) + ','
            'axisLabel:{color:"#8899aa",fontSize:10,rotate:30},'
            'axisLine:{lineStyle:{color:"#2a4a6a"}}},'
            'yAxis:{type:"value",'
            'axisLabel:{color:"#8899aa",formatter:function(v){return (v>=0?"+":"")+v+"%";}},'
            'splitLine:{lineStyle:{color:"rgba(52,120,195,.1)"}},'
            'axisLine:{show:true,lineStyle:{color:"#445566"}}},'
            'series:[' +
            ','.join([
                '{'
                'name:"Fondo ' + f + '",'
                'type:"bar",'
                'data:' + _jafp.dumps([
                    round(tabla_afp.get(afp, {}).get(f, {}).get('rent_pct', 0) or 0, 2)
                    if tabla_afp.get(afp, {}).get(f) else None
                    for afp in afp_lista
                ]) + ','
                'itemStyle:{color:"' + FONDO_COLOR.get(f, '#8899aa') + '"},'
                'label:{show:true,position:"top",fontSize:9,color:"' + FONDO_COLOR.get(f, '#8899aa') + '",'
                'formatter:function(p){return p.value===null?"":(p.value>=0?"+":"")+p.value.toFixed(1)+"%";}}'
                '}'
                for f in fondos_list
            ]) +
            ']});'
            'window.addEventListener("resize",function(){ca.resize();});'
            '})();'
        )
    else:
        afp_html = (
            '<div style="background:rgba(15,47,89,.5);border-radius:10px;padding:20px;'
            'text-align:center;color:#445566;font-size:.85em">'
            '&#128274; Datos de AFP no disponibles en este momento. '
            'Verifique conexión con la API QueTalMiAFP.</div>'
        )
        afp_chart_js = ''


    html = (
        '<!DOCTYPE html><html lang="es"><head>'
        '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>Indicadores Económicos Chile — Cofradía de Networking</title>'
        '<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>'
        '<style>'
        '*{margin:0;padding:0;box-sizing:border-box}'
        'body{font-family:"Segoe UI",system-ui,sans-serif;'
        'background:linear-gradient(160deg,#06101e 0%,#0a1628 40%,#0f2f59 100%);'
        'color:#e0e6ed;padding:20px 16px 40px;min-height:100vh}'

        'header{text-align:center;padding:28px 0 18px;'
        'border-bottom:2px solid rgba(195,165,90,.35);margin-bottom:28px}'
        'header h1{font-size:1.9em;color:#c3a55a;letter-spacing:3px;'
        'text-shadow:0 2px 14px rgba(195,165,90,.35)}'
        'header .sub{color:#667788;font-size:.83em;margin-top:5px}'
        'header a{color:#3478c3;text-decoration:none}'

        '.section{margin-bottom:36px}'
        '.section-title{font-size:1.15em;font-weight:700;color:#c3a55a;'
        'border-left:4px solid #c3a55a;padding-left:12px;margin-bottom:16px;letter-spacing:.5px}'
        '.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(185px,1fr));gap:12px}'
        '.card{background:rgba(15,47,89,.65);border:1px solid rgba(195,165,90,.18);'
        'border-radius:11px;padding:14px;transition:transform .2s,box-shadow .2s}'
        '.card:hover{transform:translateY(-4px);box-shadow:0 8px 24px rgba(0,0,0,.4)}'
        '.cn{font-size:.7em;color:#8899aa;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:4px}'
        '.cv{font-size:1.45em;font-weight:800;color:#c3a55a;margin-bottom:2px}'
        '.cd{font-size:.68em;color:#445566;margin-bottom:5px;line-height:1.3}'
        '.spark{width:100%;height:38px;margin:5px 0}'
        '.cf{font-size:.6em;color:#334455}'
        '.exp-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:14px}'
        '.exp-card{background:rgba(13,27,48,.85);border-radius:11px;padding:15px;'
        'box-shadow:0 4px 16px rgba(0,0,0,.3);transition:transform .2s}'
        '.exp-card:hover{transform:translateY(-2px)}'
        '.exp-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:9px}'
        '.exp-nm{font-size:.95em;font-weight:700;color:#e0e6ed}'
        '.exp-badge{font-size:.75em;font-weight:700;padding:3px 9px;border-radius:18px;white-space:nowrap}'
        '.exp-vals{display:flex;align-items:center;gap:7px;font-size:.8em;margin-bottom:10px;'
        'padding:6px 9px;background:rgba(255,255,255,.04);border-radius:7px;flex-wrap:wrap}'
        '.lbl{color:#8899aa}.sep{color:#445566}'
        '.exp-why{font-size:.7em;color:#f39c12;font-weight:800;letter-spacing:1.5px;margin-bottom:6px}'
        '.exp-txt{font-size:.88em;color:#aed6f1;line-height:1.65;margin-bottom:9px}'
        '.exp-src{font-size:.63em;color:#445566;border-top:1px solid rgba(52,120,195,.12);padding-top:7px}'
        '.chart-wide{background:rgba(15,47,89,.65);border:1px solid rgba(52,120,195,.2);'
        'border-radius:12px;padding:16px}'
        '.chart-wide .ctitle{font-size:1em;color:#c3a55a;margin-bottom:10px;font-weight:600}'
        '#chart6m{width:100%;height:340px}'
        '.hist-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:16px}'
        '.hcard{background:rgba(15,47,89,.65);border:1px solid rgba(52,120,195,.18);'
        'border-radius:12px;padding:14px}'
        '.htitle{font-size:.88em;color:#c3a55a;font-weight:700;margin-bottom:8px;'
        'padding-bottom:6px;border-bottom:1px solid rgba(195,165,90,.15)}'
        '.hchart{width:100%;height:220px}'

        'footer{text-align:center;color:#445566;font-size:.72em;margin-top:28px;'
        'padding-top:14px;border-top:1px solid rgba(195,165,90,.12)}'
        'footer a{color:#3478c3;text-decoration:none}'

        '@media(max-width:600px){'
        '.cards,.exp-grid,.hist-grid{grid-template-columns:1fr}'
        '}'

        '.cmf-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px}'
        '.cmf-card{background:rgba(13,27,48,.85);border:1px solid rgba(195,165,90,.25);'
        'border-radius:12px;padding:16px}'
        '.cmf-nm{font-size:.75em;color:#c3a55a;font-weight:800;letter-spacing:1px;'
        'text-transform:uppercase;margin-bottom:10px;padding-bottom:7px;'
        'border-bottom:1px solid rgba(195,165,90,.2)}'
        '.cmf-row{display:flex;justify-content:space-between;align-items:center;'
        'padding:7px 10px;border-radius:7px;margin-bottom:5px}'
        '.cmf-row.header{background:rgba(52,120,195,.12);font-size:.7em;'
        'color:#8899aa;font-weight:700;text-transform:uppercase;letter-spacing:.8px}'
        '.cmf-row.data{background:rgba(15,47,89,.5)}'
        '.cmf-plazo{font-size:.82em;color:#aed6f1;font-weight:600}'
        '.cmf-val{font-size:1.05em;font-weight:800;color:#c3a55a}'
        '.cmf-val.nd{color:#445566;font-size:.82em}'
        '.cmf-fecha{font-size:.6em;color:#334455;margin-top:8px;text-align:right}'

        '.afp-section{margin-bottom:36px}'
        '.afp-wrap{overflow-x:auto}'
        '.afp-tabla{width:100%;border-collapse:collapse;font-size:.82em}'
        '.afp-tabla th{background:rgba(52,120,195,.2);color:#c3a55a;padding:9px 12px;'
        'text-align:center;font-weight:700;letter-spacing:.5px;border:1px solid rgba(52,120,195,.15)}'
        '.afp-tabla th.afp-nm{text-align:left}'
        '.afp-tabla td{padding:8px 12px;text-align:center;border:1px solid rgba(52,120,195,.1);'
        'color:#e0e6ed;transition:background .2s}'
        '.afp-tabla td.afp-nm{text-align:left;font-weight:700;color:#c3a55a;'
        'background:rgba(15,47,89,.5)}'
        '.afp-tabla .pos{color:#2ecc71;font-weight:700}'
        '.afp-tabla .neg{color:#e74c3c;font-weight:700}'
        '.afp-tabla .nd{color:#445566}'
        '.afp-tabla tr:hover td{background:rgba(52,120,195,.08)}'
        '#chart_afp{width:100%;height:320px;margin-top:18px}'
        '.afp-meta{font-size:.7em;color:#445566;margin-top:8px}'

        '</style></head><body>'

        '<header>'
        '<h1>&#9875; INDICADORES ECONÓMICOS DE CHILE</h1>'
        '<div class="sub">Datos en tiempo real · Análisis IA · '
        'Banco Central de Chile · CMF · AFP · ' + gen + '</div>'
        '</header>'

        # Sección 1: Cards actuales (11 indicadores)
        '<div class="section">'
        '<div class="section-title">&#128202; Indicadores del Día — 11 Indicadores con Tendencia 30 días</div>'
        '<div class="cards">' + cards_html + '</div>'
        '</div>'

        # Sección 2: Indicadores CMF (3 indicadores: TMC, TAB UF, TAB Nominal)
        '<div class="section">'
        '<div class="section-title">&#127970; Tasas CMF — Comisión para el Mercado Financiero</div>'
        '<div class="cmf-grid">' + cmf_html + '</div>'
        '</div>'

        # Sección 3: Análisis IA
        '<div class="section">'
        '<div class="section-title">&#129302; Análisis IA — ¿Por qué varió cada indicador?</div>'
        '<div class="exp-grid">' + exp_cards_html + '</div>'
        '</div>'

        # Sección 4: Dólar vs Euro 6 meses
        '<div class="section">'
        '<div class="section-title">&#128178; Dólar vs Euro — Últimos 6 Meses (valor inicio de mes)</div>'
        '<div class="chart-wide"><div id="chart6m"></div></div>'
        '</div>'

        # Sección 5: Histórico 10 años
        '<div class="section">'
        '<div class="section-title">&#128197; Series Históricas — Últimos 10 Años</div>'
        '<div class="hist-grid">' + hist_cards_html + '</div>'
        '</div>'

        # Sección 6: Rentabilidad AFP
        '<div class="section afp-section">'
        '<div class="section-title">&#127963; Rentabilidad AFP — Últimos 6 Meses</div>'
        + afp_html +
        '</div>'

        '<footer>'
        'Banco Central de Chile &nbsp;&middot;&nbsp; CMF &nbsp;&middot;&nbsp; AFP'
        ' &nbsp;&middot;&nbsp; Análisis IA + RAG Biblioteca de Finanzas'
        ' &nbsp;&middot;&nbsp; Cofradía de Networking Bot v6.0'
        '</footer>'

        '<script>'
        + spark_js +

        'var c6m=echarts.init(document.getElementById("chart6m"));'
        'c6m.setOption({'
        'backgroundColor:"transparent",'
        'tooltip:{trigger:"axis",backgroundColor:"rgba(15,47,89,.95)",'
        'borderColor:"#c3a55a",textStyle:{color:"#c0c8d4"},'
        'formatter:function(p){'
        'var s=p[0].name+"<br/>";'
        'p.forEach(function(i){'
        's+=\'<span style="color:\'+i.color+\'">●</span> \'+i.seriesName+\': \';'
        'if(i.value===null)s+="N/D";'
        'else s+="$"+i.value.toLocaleString("es-CL");'
        's+="<br/>";});return s;'
        '}},'
        'legend:{data:["Dólar USD","Euro EUR"],textStyle:{color:"#8899aa"},top:4,right:10},'
        'grid:{left:"10%",right:"5%",bottom:"12%",top:"14%"},'
        'xAxis:{type:"category",data:' + labels_6m + ','
        'axisLabel:{color:"#8899aa",fontSize:12},'
        'axisLine:{lineStyle:{color:"#2a4a6a"}}},'
        'yAxis:{type:"value",'
        'axisLabel:{color:"#8899aa",formatter:function(v){return "$"+v.toLocaleString("es-CL");}},'
        'splitLine:{lineStyle:{color:"rgba(52,120,195,.12)"}}},'
        'series:['
        '{name:"Dólar USD",type:"line",data:' + dolar_6m + ','
        'smooth:true,symbol:"circle",symbolSize:7,'
        'lineStyle:{color:"#3478c3",width:3},itemStyle:{color:"#3478c3"},'
        'label:{show:true,position:"top",color:"#3478c3",fontSize:11,'
        'formatter:function(p){return p.value?("$"+p.value.toLocaleString("es-CL")):"N/D";}},'
        'areaStyle:{color:{type:"linear",x:0,y:0,x2:0,y2:1,'
        'colorStops:[{offset:0,color:"#3478c344"},{offset:1,color:"transparent"}]}}},'
        '{name:"Euro EUR",type:"line",data:' + euro_6m + ','
        'smooth:true,symbol:"circle",symbolSize:7,'
        'lineStyle:{color:"#2ecc71",width:3},itemStyle:{color:"#2ecc71"},'
        'label:{show:true,position:"bottom",color:"#2ecc71",fontSize:11,'
        'formatter:function(p){return p.value?("$"+p.value.toLocaleString("es-CL")):"N/D";}},'
        'areaStyle:{color:{type:"linear",x:0,y:0,x2:0,y2:1,'
        'colorStops:[{offset:0,color:"#2ecc7133"},{offset:1,color:"transparent"}]}}}]'
        '});'

        + hist_charts_js +

        'window.addEventListener("resize",function(){'
        'if(c6m)c6m.resize();'
        '[' + hist_ids_js + '].forEach(function(id){'
        'var el=document.getElementById(id);'
        'if(el){var inst=echarts.getInstanceByDom(el);if(inst)inst.resize();}'
        '});});'

        + afp_chart_js +

        '</script>'
        '</body></html>'
    )
    return html


@requiere_suscripcion
async def indicadores_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /indicadores — Dashboard económico completo de Chile:
      • 11 indicadores del día con sparkline 30 días (Banco Central)
      • 3 tasas CMF: TMC · TAB Nominal · TAB UF (90/180/360 días)
      • Análisis IA del por qué varió cada indicador (Groq + RAG + BCCH)
      • Dólar vs Euro últimos 6 meses
      • Histórico 10 años: UF, Dólar, Euro, UTM, IPC, Desempleo, IMACEC, Cobre, Bitcoin
      • Rentabilidad AFP últimos 6 meses (todos los fondos)
    Fuentes: Banco Central · CMF · AFP · RAG Biblioteca Finanzas
    """
    msg = await update.message.reply_text(
        "📈 Consultando indicadores económicos...\n"
        "⏳ Descargando histórico 10 años y analizando con IA (~20 s)"
    )
    try:
        loop = asyncio.get_running_loop()

        # 1. Datos completos (histórico + actuales) — fetch paralelo ~80 peticiones
        all_data = await loop.run_in_executor(None, obtener_indicadores_chile)
        datos = all_data.get('datos_actuales', {})
        if not datos:
            await msg.edit_text("❌ Sin datos de indicadores. Intenta en unos minutos.")
            return

        await msg.edit_text(
            f"✅ {len(datos)} indicadores obtenidos.\n"
            "🔍 Web scraping del Banco Central...\n"
            "📚 Consultando biblioteca financiera RAG...\n"
            "🏦 Consultando tasas CMF..."
        )

        # 2. Web scraping BCCH
        noticias_bcch = await loop.run_in_executor(None, scraping_bcentral_noticias)
        contexto_bcch = "\n".join(noticias_bcch) if noticias_bcch else ""

        # 2b. Indicadores CMF (TMC, TAB UF, TAB Nominal)
        datos_cmf = await loop.run_in_executor(None, obtener_indicadores_cmf)
        all_data['datos_cmf'] = datos_cmf

        # 2c. Rentabilidad AFP (últimos 6 meses)
        await msg.edit_text(
            f"✅ Indicadores + CMF obtenidos.\n"
            "🏛️ Consultando rentabilidad AFP...\n"
            "📚 Analizando con IA..."
        )
        datos_afp = await loop.run_in_executor(None, obtener_rentabilidad_afp)
        all_data['datos_afp'] = datos_afp

        # 3. RAG por indicador
        queries_rag = {
            'uf':            'unidad de fomento inflacion reajuste Chile',
            'dolar':         'tipo de cambio dolar peso chileno politica cambiaria',
            'euro':          'tipo de cambio euro moneda extranjera',
            'utm':           'unidad tributaria mensual impuestos Chile',
            'ipc':           'indice de precios al consumidor inflacion causas efectos',
            'tpm':           'tasa de politica monetaria banco central tasas de interes',
            'bitcoin':       'bitcoin criptomoneda volatilidad mercado digital',
            'tasa_desempleo':'desempleo mercado laboral Chile causas empleo',
            'imacec':        'IMACEC actividad economica Chile indicador mensual PIB',
            'libra_cobre':   'precio cobre Chile exportaciones materias primas',
            'ivp':           'indice valor promedio creditos hipotecarios Chile',
        }
        fragmentos_rag = {}
        for cod in datos:
            query = queries_rag.get(cod, f'indicador economico {cod} Chile')
            frag  = await loop.run_in_executor(None, consultar_rag_economia, query)
            fragmentos_rag[cod] = frag

        # 4. Explicaciones IA
        await msg.edit_text(
            f"🤖 Generando análisis IA para {len(datos)} indicadores...\n"
            "📊 Construyendo dashboard interactivo..."
        )
        explicaciones = {}
        for cod, d in datos.items():
            exp = await loop.run_in_executor(
                None, generar_explicacion_indicador,
                cod, d, contexto_bcch, fragmentos_rag.get(cod, '')
            )
            explicaciones[cod] = exp

        # 5. Mensaje de texto resumido en Telegram
        sep = "━" * 30
        PORCENTAJES = {'ipc', 'tpm', 'tasa_desempleo', 'imacec'}
        ORDER_MSG = ['uf', 'dolar', 'euro', 'utm', 'ipc', 'tpm',
                     'bitcoin', 'tasa_desempleo', 'imacec', 'libra_cobre', 'ivp']
        lineas = [
            "📈 INDICADORES ECONÓMICOS DE CHILE",
            sep,
            "Fuente: Banco Central de Chile · CMF · AFP",
            "",
        ]
        for cod in ORDER_MSG:
            d = datos.get(cod)
            if not d: continue
            v = d['valor']
            if cod in PORCENTAJES:
                vf = "{:.2f}%".format(v)
            elif cod == 'bitcoin':
                vf = "${:,.0f} CLP".format(v).replace(',', '.')
            elif cod == 'libra_cobre':
                vf = "USD {:.4f}".format(v)
            else:
                vf = "${:,.2f} CLP".format(v).replace(',', 'X').replace('.', ',').replace('X', '.')
            serie = d.get('serie30', [])
            tend  = ''
            if len(serie) >= 2:
                v0 = serie[0].get('valor', 0)
                v1 = serie[1].get('valor', 1)
                if v1:
                    pct  = ((v0 - v1) / v1) * 100
                    tend = '  ' + ('▲' if pct >= 0 else '▼') + ' {:.3f}%'.format(abs(pct))
            lineas.append(d['nombre'] + ": " + vf + tend + "  (" + d['fecha'] + ")")

        hist_6m = all_data.get('hist_6m', {})
        d6l = hist_6m.get('dolar', [])
        e6l = hist_6m.get('euro',  [])
        if d6l and e6l:
            lineas += ["", sep, "Dólar vs Euro — últimos 6 meses:", ""]
            for label, vd, ve in zip(hist_6m.get('labels', []), d6l, e6l):
                vds = "${:,.0f}".format(vd).replace(',', '.') if vd else 'N/D'
                ves = "${:,.0f}".format(ve).replace(',', '.') if ve else 'N/D'
                lineas.append("  " + label + " — USD " + vds + "  |  EUR " + ves)

        lineas += ["", sep, "Se adjunta dashboard interactivo completo con análisis IA."]

        # Agregar resumen CMF al mensaje de texto
        tmc_list = datos_cmf.get('tmc', [])
        if tmc_list:
            lineas += ["", sep, "🏦 Tasas CMF (Comisión para el Mercado Financiero):"]
            for item in tmc_list:
                v = item.get('valor')
                vf = f"{v:.2f}%" if v is not None else "N/D"
                lineas.append(f"  TMC {item.get('plazo','')}: {vf}")
        tab_uf_360 = next((x for x in datos_cmf.get('tab_uf', []) if x.get('plazo') == '360 días' and x.get('valor')), None)
        if tab_uf_360:
            lineas.append(f"  TAB UF 360 días: {tab_uf_360['valor']:.2f}%")

        # Agregar resumen AFP al mensaje de texto
        if datos_afp.get('ok') and datos_afp.get('tabla'):
            lineas += ["", sep, f"🏛️ Rentabilidad AFP — últimos 6 meses ({datos_afp.get('fecha_ini','')}):",  ""]
            for afp in datos_afp.get('afps', []):
                row_txt = f"  {afp}:"
                for f in datos_afp.get('fondos', []):
                    cell = datos_afp['tabla'].get(afp, {}).get(f)
                    if cell:
                        rp = cell['rent_pct']
                        row_txt += f"  F{f}: {'+'if rp>=0 else ''}{rp:.1f}%"
                lineas.append(row_txt)

        await update.message.reply_text("\n".join(lineas))

        # 6. Generar y enviar HTML
        await msg.edit_text("📊 Generando dashboard HTML interactivo...")
        html_content = await loop.run_in_executor(
            None, generar_html_indicadores, all_data, explicaciones
        )
        html_path = f"/tmp/ind_{update.effective_user.id}.html"
        with open(html_path, 'w', encoding='utf-8') as fh:
            fh.write(html_content)
        with open(html_path, 'rb') as fh:
            await update.message.reply_document(
                document=fh,
                filename="indicadores_chile_" + datetime.now().strftime('%Y%m%d') + ".html",
                caption=(
                    "📊 Dashboard Indicadores Económicos Chile\n\n"
                    "• 11 indicadores del día con sparklines 30 días\n"
                    "• 🏦 Tasas CMF: TMC · TAB UF · TAB Nominal (90/180/360 días)\n"
                    "• 🤖 Análisis IA del por qué de cada variación\n"
                    "• 💱 Dólar vs Euro últimos 6 meses\n"
                    "• 📅 Histórico 10 años: UF · Dólar · Euro · UTM · IPC\n"
                    "       Desempleo · IMACEC · Cobre · Bitcoin\n"
                    "• 🏛️ Rentabilidad AFP últimos 6 meses (Fondos A–E)\n\n"
                    "Abre en tu navegador para los gráficos interactivos."
                )
            )
        try:
            os.remove(html_path)
        except Exception:
            pass
        await msg.delete()
        registrar_servicio_usado(update.effective_user.id, 'indicadores')

    except Exception as e:
        import traceback as _tb
        logger.error("indicadores_comando: " + str(e) + "\n" + _tb.format_exc())
        try:
            await msg.edit_text("❌ Error indicadores: " + str(e)[:120])
        except Exception:
            pass




async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Captura TODOS los errores no manejados del bot y los loguea correctamente."""
    import traceback as _tb

    error = context.error
    tb_str = ''.join(_tb.format_exception(type(error), error, error.__traceback__))

    # Errores esperados que no necesitan alerta (red, Telegram timeouts)
    ignorar = (
        'Message is not modified',
        'Query is too old',
        'Bad Gateway',
        'Timed out',
        'Connection reset',
        'Network',
        'httpx',
        'ReadTimeout',
        'ConnectTimeout',
        'RetryAfter',
    )
    for patron in ignorar:
        if patron.lower() in str(error).lower():
            logger.debug(f"Error esperado ignorado: {error}")
            return

    logger.error(f"Exception no manejada:\n{tb_str}")

    # Intentar notificar al usuario si hay update activo
    if update:
        try:
            mensaje_error = "❌ Ocurrió un error inesperado. Por favor intenta de nuevo."
            if hasattr(update, 'effective_message') and update.effective_message:
                await update.effective_message.reply_text(mensaje_error)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.answer(mensaje_error, show_alert=True)
        except Exception:
            pass  # No propagar errores desde el error handler


def main():
    """Función principal"""
    logger.info("🚀 Iniciando Bot Cofradía Premium...")
    logger.info(f"📊 Groq IA: {'✅' if GROQ_API_KEY else '❌'} | DeepSeek: {'✅' if deepseek_disponible else '❌'} | IA Global: {'✅' if ia_disponible else '❌'}")
    logger.info(f"📷 Gemini OCR: {'✅' if gemini_disponible else '❌'}")
    logger.info(f"💼 JSearch (empleos reales): {'✅' if jsearch_disponible else '❌'}")
    logger.info(f"🗄️ Base de datos: {'Supabase' if DATABASE_URL else 'SQLite local'}")
    
    # Inicializar BD
    if not init_db():
        logger.error("❌ No se pudo inicializar la base de datos")
        return
    
    # Keep-alive SIEMPRE activo — servidor HTTP en PORT(10000) para Render
    keepalive_thread = threading.Thread(target=run_keepalive_server, daemon=True)
    keepalive_thread.start()
    # Auto-ping cada 4min — Render NUNCA duerme el bot
    ping_thread = threading.Thread(target=auto_ping, daemon=True)
    ping_thread.start()
    logger.info("🏓 Keep-alive 24/7 activado — bot siempre despierto")
    
    if not TOKEN_BOT:
        logger.error("❌ TOKEN_BOT no configurado")
        return
    
    # Crear aplicación
    async def post_init(app):
        """Eliminar webhook anterior + configurar comandos del menú + limpiar nombres vacíos"""
        # PASO 1: Limpiar webhook para evitar Conflict en Render
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            logger.info("🧹 Webhook anterior eliminado - sin conflictos")
        except Exception as e:
            logger.warning(f"Nota al limpiar webhook: {e}")
        
        # PASO 2: Esperar un momento para que Telegram procese la eliminación
        await asyncio.sleep(2)
        
        # PASO 2.5: Limpiar registros con nombres vacíos o inválidos en la BD
        try:
            conn = get_db_connection()
            if conn:
                c = conn.cursor()
                nombres_invalidos = ['', 'Sin nombre', 'Sin Nombre', 'no name', 'No Name', 'None', 'null']
                for nombre_malo in nombres_invalidos:
                    if DATABASE_URL:
                        c.execute("""UPDATE suscripciones 
                                    SET first_name = COALESCE(NULLIF(username, ''), CONCAT('ID_', CAST(user_id AS TEXT)))
                                    WHERE first_name = %s OR first_name IS NULL""", (nombre_malo,))
                        c.execute("""UPDATE mensajes 
                                    SET first_name = COALESCE(NULLIF(username, ''), CONCAT('ID_', CAST(user_id AS TEXT)))
                                    WHERE first_name = %s OR first_name IS NULL""", (nombre_malo,))
                    else:
                        c.execute("""UPDATE suscripciones 
                                    SET first_name = COALESCE(NULLIF(username, ''), 'ID_' || CAST(user_id AS TEXT))
                                    WHERE first_name = ? OR first_name IS NULL""", (nombre_malo,))
                        c.execute("""UPDATE mensajes 
                                    SET first_name = COALESCE(NULLIF(username, ''), 'ID_' || CAST(user_id AS TEXT))
                                    WHERE first_name = ? OR first_name IS NULL""", (nombre_malo,))
                
                # Fix: corregir registros que fueron incorrectamente asignados como "Germán" pero NO son el owner
                if DATABASE_URL:
                    c.execute("""UPDATE suscripciones 
                                SET first_name = COALESCE(NULLIF(username, ''), CONCAT('ID_', CAST(user_id AS TEXT)))
                                WHERE first_name = 'Germán' AND user_id != %s""", (OWNER_ID,))
                    c.execute("""UPDATE mensajes 
                                SET first_name = COALESCE(NULLIF(username, ''), CONCAT('ID_', CAST(user_id AS TEXT)))
                                WHERE first_name = 'Germán' AND user_id != %s""", (OWNER_ID,))
                else:
                    c.execute("""UPDATE suscripciones 
                                SET first_name = COALESCE(NULLIF(username, ''), 'ID_' || CAST(user_id AS TEXT))
                                WHERE first_name = 'Germán' AND user_id != ?""", (OWNER_ID,))
                    c.execute("""UPDATE mensajes 
                                SET first_name = COALESCE(NULLIF(username, ''), 'ID_' || CAST(user_id AS TEXT))
                                WHERE first_name = 'Germán' AND user_id != ?""", (OWNER_ID,))
                
                conn.commit()
                conn.close()
                logger.info("🧹 Nombres vacíos/inválidos limpiados en BD")
        except Exception as e:
            logger.warning(f"Error limpiando nombres: {e}")
        
        # PASO 3: Configurar comandos del menú
        commands = [
            BotCommand("start", "Iniciar bot"),
            BotCommand("ayuda", "Ver todos los comandos"),
            BotCommand("buscar", "Buscar en historial del grupo"),
            BotCommand("buscar_ia", "Busqueda inteligente con IA"),
            BotCommand("rag_consulta", "Consultar documentos y libros"),
            BotCommand("buscar_profesional", "Buscar profesionales en Cofradia"),
            BotCommand("buscar_apoyo", "Buscar cofrades en busqueda laboral"),
            BotCommand("empleo", "Buscar ofertas de empleo"),
            BotCommand("mi_tarjeta", "Tu tarjeta profesional"),
            BotCommand("directorio", "Directorio de profesionales"),
            BotCommand("conectar", "Conexiones inteligentes"),
            BotCommand("alertas", "Alertas de palabras clave"),
            BotCommand("publicar", "Publicar anuncio"),
            BotCommand("anuncios", "Ver tablon de anuncios"),
            BotCommand("eventos", "Ver proximos eventos"),
            BotCommand("consultar", "Consulta profesional"),
            BotCommand("consultas", "Ver consultas abiertas"),
            BotCommand("recomendar", "Recomendar a un cofrade"),
            BotCommand("graficos", "Ver graficos de actividad"),
            BotCommand("estadisticas", "Estadisticas del grupo"),
            BotCommand("top_usuarios", "Ranking de participacion"),
            BotCommand("mi_perfil", "Tu perfil de actividad"),
            BotCommand("mi_cuenta", "Estado de tu suscripcion"),
            BotCommand("cumpleanos_mes", "Cumpleanos del mes"),
            BotCommand("resumen", "Resumen del dia"),
            BotCommand("resumen_semanal", "Resumen de 7 dias"),
            BotCommand("dotacion", "Total de integrantes"),
            # === AGENTE INTELIGENTE v5.0 ===
            BotCommand("agente", "Agente IA: plan de networking personalizado"),
            BotCommand("match", "Match inteligente con cofrades compatibles"),
            BotCommand("agendar", "Agendar actividad con recordatorio automatico"),
            BotCommand("mi_agenda", "Ver tu agenda personal"),
            BotCommand("tarea", "Agregar tarea de networking"),
            BotCommand("mis_tareas", "Ver tus tareas pendientes"),
            BotCommand("briefing", "Briefing diario de networking"),
            BotCommand("indicadores", "Indicadores economicos Chile con analisis IA"),
        ]
        try:
            await app.bot.set_my_commands(commands)
            
            if COFRADIA_GROUP_ID:
                from telegram import BotCommandScopeChat
                comandos_grupo = [
                    BotCommand("buscar", "Buscar en historial"),
                    BotCommand("buscar_ia", "Busqueda con IA"),
                    BotCommand("rag_consulta", "Consultar documentos y libros"),
                    BotCommand("buscar_profesional", "Buscar profesionales"),
                    BotCommand("buscar_apoyo", "Cofrades en busqueda laboral"),
                    BotCommand("empleo", "Buscar empleos"),
                    BotCommand("directorio", "Directorio profesional"),
                    BotCommand("anuncios", "Tablon de anuncios"),
                    BotCommand("eventos", "Proximos eventos"),
                    BotCommand("consultas", "Consultas abiertas"),
                    BotCommand("graficos", "Graficos de actividad"),
                    BotCommand("estadisticas", "Estadisticas del grupo"),
                    BotCommand("indicadores", "Indicadores economicos Chile"),
                    BotCommand("top_usuarios", "Ranking de participacion"),
                    BotCommand("mi_perfil", "Tu perfil de actividad"),
                    BotCommand("dotacion", "Total de integrantes"),
                    BotCommand("ayuda", "Ver todos los comandos"),
                ]
                try:
                    await app.bot.set_my_commands(comandos_grupo, scope=BotCommandScopeChat(chat_id=COFRADIA_GROUP_ID))
                    await app.bot.set_chat_menu_button(chat_id=COFRADIA_GROUP_ID, menu_button=MenuButtonCommands())
                except Exception as e:
                    logger.warning(f"No se pudo configurar menú en grupo: {e}")
            
            logger.info("✅ Comandos configurados")
        except Exception as e:
            logger.warning(f"Error configurando comandos: {e}")
    
    application = Application.builder().token(TOKEN_BOT).post_init(post_init).build()
    
    # Handlers básicos (NOTA: /start se maneja en el ConversationHandler de onboarding más abajo)
    application.add_handler(CommandHandler("ayuda", ayuda))
    # ── Comandos v6.0 ──
    application.add_handler(CommandHandler("analizar", analizar_usuario_comando))
    application.add_handler(CommandHandler("ofrezco", ofrezco_comando))
    application.add_handler(CommandHandler("busco_servicio", busco_servicio_comando))
    application.add_handler(CommandHandler("mis_servicios", mis_servicios_comando))
    application.add_handler(CommandHandler("recordar", recordar_comando))
    application.add_handler(CommandHandler("mantenimiento", mantenimiento_comando))
    application.add_handler(CommandHandler("velocidad_voz", velocidad_voz_comando))
    application.add_handler(CommandHandler("resumen_diario", resumen_diario_comando))
    application.add_handler(CommandHandler("buscar_web", buscar_web_comando))
    application.add_handler(CommandHandler("feriados", feriados_comando))
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
    application.add_handler(CommandHandler("indicadores", indicadores_comando))
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
    application.add_handler(CommandHandler("precios", precios_comando))
    application.add_handler(CommandHandler("set_precios", set_precios_comando))
    application.add_handler(CommandHandler("pagos_pendientes", pagos_pendientes_comando))
    application.add_handler(CommandHandler("vencimientos", vencimientos_comando))
    application.add_handler(CommandHandler("vencimientos_mes", vencimientos_mes_comando))
    application.add_handler(CommandHandler("ingresos", ingresos_comando))
    application.add_handler(CommandHandler("ingreso", ingresos_comando))  # alias
    application.add_handler(CommandHandler("crecimiento_mes", crecimiento_mes_comando))
    application.add_handler(CommandHandler("crecimiento_anual", crecimiento_anual_comando))
    application.add_handler(CommandHandler("resumen_usuario", resumen_usuario_comando))
    application.add_handler(CommandHandler("ver_topics", ver_topics_comando))
    application.add_handler(CommandHandler("set_topic", set_topic_comando))
    application.add_handler(CommandHandler("set_topic_emoji", set_topic_emoji_comando))
    application.add_handler(CommandHandler("generar_codigo", generar_codigo_comando))
    
    # Handlers RAG PDF
    application.add_handler(CommandHandler("subir_pdf", subir_pdf_comando))
    application.add_handler(CommandHandler("rag_status", rag_status_comando))
    application.add_handler(CommandHandler("rag_debug", rag_debug_comando))
    application.add_handler(CommandHandler("rag_consulta", rag_consulta_comando))
    application.add_handler(CommandHandler("rag_reindexar", rag_reindexar_comando))
    application.add_handler(CommandHandler("rag_enriquecer", rag_enriquecer_keywords_comando))
    application.add_handler(CommandHandler("rag_backup", rag_backup_comando))
    application.add_handler(CommandHandler("eliminar_pdf", eliminar_pdf_comando))
    
    # Handlers v3.0: Directorio, Alertas, Anuncios, Eventos, Consultas
    application.add_handler(CommandHandler("mi_tarjeta", mi_tarjeta_comando))
    application.add_handler(CommandHandler("directorio", directorio_comando))
    application.add_handler(CommandHandler("alertas", alertas_comando))
    application.add_handler(CommandHandler("publicar", publicar_comando))
    application.add_handler(CommandHandler("anuncios", anuncios_comando))
    application.add_handler(CommandHandler("conectar", conectar_comando))
    application.add_handler(CommandHandler("cumpleanos_mes", cumpleanos_mes_comando))
    application.add_handler(CommandHandler("encuesta", encuesta_comando))
    application.add_handler(CommandHandler("nuevo_evento", nuevo_evento_comando))
    application.add_handler(CommandHandler("eventos", eventos_comando))
    application.add_handler(CommandHandler("asistir", asistir_comando))
    application.add_handler(CommandHandler("recomendar", recomendar_comando))
    application.add_handler(CommandHandler("mis_recomendaciones", mis_recomendaciones_comando))
    application.add_handler(CommandHandler("consultar", consultar_comando))
    application.add_handler(CommandHandler("consultas", consultas_comando))
    application.add_handler(CommandHandler("responder", responder_consulta_comando))
    application.add_handler(CommandHandler("ver_consulta", ver_consulta_comando))
    
    # Onboarding: Aprobar solicitudes
    application.add_handler(CommandHandler("aprobar_solicitud", aprobar_solicitud_comando))
    application.add_handler(CommandHandler("editar_usuario", editar_usuario_comando))
    application.add_handler(CommandHandler("eliminar_solicitud", eliminar_solicitud_comando))
    application.add_handler(CommandHandler("buscar_usuario", buscar_usuario_comando))
    
    # v4.0 handlers: Coins, Premium, Trust
    application.add_handler(CommandHandler("finanzas", finanzas_comando))
    application.add_handler(CommandHandler("generar_cv", generar_cv_comando))
    application.add_handler(CommandHandler("entrevista", entrevista_comando))
    application.add_handler(CommandHandler("analisis_linkedin", analisis_linkedin_comando))
    application.add_handler(CommandHandler("mi_dashboard", mi_dashboard_comando))
    application.add_handler(CommandHandler("mentor", mentor_comando))
    application.add_handler(CommandHandler("mis_coins", mis_coins_comando))
    application.add_handler(CommandHandler("set_precio", set_precio_comando))
    application.add_handler(CommandHandler("dar_coins", dar_coins_comando))
    
    # === AGENTE INTELIGENTE DE NETWORKING (v5.0) ===
    application.add_handler(CommandHandler("agente", agente_networking_comando))
    application.add_handler(CommandHandler("match", match_networking_comando))
    application.add_handler(CommandHandler("agendar", agendar_comando))
    application.add_handler(CommandHandler("mi_agenda", mi_agenda_comando))
    application.add_handler(CommandHandler("tarea", nueva_tarea_comando))
    application.add_handler(CommandHandler("mis_tareas", mis_tareas_comando))
    application.add_handler(CommandHandler("briefing", briefing_diario_comando))
    # Handlers dinámicos para /completar_ID, /ok_ID, /eliminar_agenda_ID
    application.add_handler(MessageHandler(
        filters.Regex(r'^/(completar_|ok_|eliminar_agenda_)\d+'),
        completar_item_comando
    ))
    
    # Onboarding: ConversationHandler para preguntas de ingreso
    # /start es el entry point - detecta si es usuario nuevo o registrado
    onboarding_conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
        ],
        states={
            ONBOARD_NOMBRE: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, onboard_nombre)],
            ONBOARD_GENERACION: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, onboard_generacion)],
            ONBOARD_RECOMENDADO: [MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, onboard_recomendado)],
            ONBOARD_PREGUNTA4: [CallbackQueryHandler(onboard_pregunta4_callback, pattern='^onboard_p4_')],
            ONBOARD_PREGUNTA5: [CallbackQueryHandler(onboard_pregunta5_callback, pattern='^onboard_p5_')],
            ONBOARD_PREGUNTA6: [CallbackQueryHandler(onboard_pregunta6_callback, pattern='^onboard_p6_')],
        },
        fallbacks=[
            CommandHandler("cancelar", onboard_cancelar),
            CommandHandler("start", start),  # Permite reiniciar con /start
        ],
        per_user=True,
        per_chat=True,
    )
    application.add_handler(onboarding_conv)
    
    # ChatJoinRequest handler (fallback si alguien solicita por link con aprobación)
    application.add_handler(ChatJoinRequestHandler(manejar_solicitud_ingreso))
    
    # Detectar nuevos miembros que ingresan al grupo (para bienvenida)
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, detectar_nuevo_miembro))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(callback_plan, pattern='^plan_'))
    application.add_handler(CallbackQueryHandler(callback_generar_codigo, pattern='^gencodigo_'))
    application.add_handler(CallbackQueryHandler(callback_aprobar_rechazar, pattern='^(aprobar|rechazar)_'))
    application.add_handler(CallbackQueryHandler(callback_ayuda_ejemplos, pattern='^ayuda_ej_'))
    
    # Mensajes y documentos
    application.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, recibir_comprobante))
    application.add_handler(MessageHandler(filters.Document.PDF & filters.ChatType.PRIVATE, recibir_documento_pdf))
    
    # Handler de mensajes de voz (privado y grupo)
    application.add_handler(MessageHandler(filters.VOICE | filters.AUDIO, manejar_mensaje_voz))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'@'), responder_mencion))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS & ~filters.COMMAND, guardar_mensaje_grupo))
    
    # PENDIENTE 3: Handler para chat privado (usuarios registrados pueden escribir directo)
    async def responder_chat_privado(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Responde mensajes en chat privado de usuarios registrados"""
        if not update.message or not update.message.text:
            return
        user_id = update.effective_user.id
        
        # No interferir con onboarding activo
        if context.user_data.get('onboard_activo'):
            return
        
        # Solo responder a owner y usuarios registrados
        es_owner = (user_id == OWNER_ID)
        if not es_owner and not verificar_suscripcion_activa(user_id):
            return  # El catch-all redirigirá a /start
        
        mensaje = update.message.text.strip()
        user_name = update.effective_user.first_name
        
        # SEGURIDAD: No aceptar instrucciones para modificar datos de usuarios
        patrones_peligrosos = [
            r'(?:su |el |la )?(?:apellido|nombre|generaci[oó]n)\s+(?:es|ser[ií]a|debe ser|cambia)',
            r'(?:registr|cambi|modific|actualiz)\w+\s+(?:el nombre|su nombre|apellido|datos)',
            r'(?:nombre completo|se llama|llamarse)\s+(?:es|ser[ií]a)',
        ]
        for patron in patrones_peligrosos:
            if re.search(patron, mensaje.lower()):
                # Si es el owner, guiarlo al comando correcto
                if es_owner:
                    await update.message.reply_text(
                        "🔒 Por seguridad, no modifico datos de usuarios a través del chat.\n\n"
                        "Como administrador, usa estos comandos:\n\n"
                        "📝 Editar datos:\n"
                        "  /editar_usuario [ID] nombre [nuevo nombre]\n"
                        "  /editar_usuario [ID] apellido [nuevo apellido]\n"
                        "  /editar_usuario [ID] generacion [año]\n\n"
                        "🗑️ Eliminar usuario:\n"
                        "  /eliminar_solicitud [ID]\n\n"
                        "Ejemplo: /editar_usuario 13031156 apellido Villegas"
                    )
                else:
                    await update.message.reply_text(
                        "🔒 Por seguridad, no puedo modificar datos de usuarios a través del chat.\n\n"
                        "Los datos personales solo se registran durante el proceso de onboarding "
                        "(las 5 preguntas de ingreso) y son proporcionados directamente por cada usuario.\n\n"
                        "Si necesitas corregir datos, contacta al administrador."
                    )
                return
        
        # ── FILTRO CONVERSACIONAL ─────────────────────────────────────────────
        # Detectar mensajes cortos de cortesía/chat ANTES de activar el RAG completo
        msg_lower = mensaje.lower().strip()
        palabras = msg_lower.split()
        
        RESPUESTAS_CONVERSACIONALES = {
            # Agradecimientos
            frozenset(['gracias']): f"¡De nada, {user_name}! Siempre a tu disposición. 👍",
            frozenset(['gracias', 'muchas']): f"¡Con mucho gusto, {user_name}! Para eso estoy. 🤝",
            frozenset(['gracias', 'muy']): f"¡De nada, {user_name}! Cualquier cosa me avisas.",
            frozenset(['gracias', 'igual']): f"¡Igualmente, {user_name}! 👋",
            frozenset(['gracias', 'todo']): f"¡De nada por todo, {user_name}! Fue un placer ayudarte. 🙌",
            # Saludos
            frozenset(['hola']): f"¡Hola, {user_name}! ¿En qué puedo ayudarte?",
            frozenset(['buenas']): f"¡Buenas, {user_name}! ¿Qué necesitas?",
            frozenset(['buenos', 'días']): f"¡Buenos días, {user_name}! ¿En qué te puedo ayudar hoy?",
            frozenset(['buenos', 'dias']): f"¡Buenos días, {user_name}! ¿En qué te puedo ayudar?",
            frozenset(['buenas', 'tardes']): f"¡Buenas tardes, {user_name}! ¿Qué necesitas?",
            frozenset(['buenas', 'noches']): f"¡Buenas noches, {user_name}! ¿En qué te puedo ayudar?",
            frozenset(['hey']): f"¡Hey, {user_name}! ¿Qué necesitas?",
            # Confirmaciones / cierre
            frozenset(['ok']): f"¡Entendido, {user_name}! ✅",
            frozenset(['listo']): f"¡Perfecto, {user_name}! ✅",
            frozenset(['perfecto']): f"¡Excelente, {user_name}! ¿Algo más en que pueda ayudarte?",
            frozenset(['excelente']): f"Me alegra que haya sido útil, {user_name}! 😊",
            frozenset(['genial']): f"¡Me alegra, {user_name}! ¿Algo más?",
            frozenset(['entendido']): f"¡Perfecto, {user_name}! Cualquier cosa aquí estoy.",
            frozenset(['dale']): f"¡De acuerdo, {user_name}! Aquí estoy cuando necesites. 👍",
            frozenset(['claro']): f"¡Claro que sí, {user_name}! ¿En qué más te ayudo?",
            frozenset(['si']): f"¡Entendido, {user_name}! ¿Algo más?",
            frozenset(['sí']): f"¡Entendido, {user_name}! ¿Algo más?",
            # Despedidas
            frozenset(['bye']): f"¡Hasta luego, {user_name}! 👋",
            frozenset(['chao']): f"¡Chao, {user_name}! Cuando quieras volver, aquí estoy. 👋",
            frozenset(['adios']): f"¡Adiós, {user_name}! Un gusto ayudarte. 👋",
            frozenset(['adiós']): f"¡Adiós, {user_name}! Hasta pronto. 👋",
            frozenset(['hasta', 'luego']): f"¡Hasta luego, {user_name}! 👋",
        }
        
        # Buscar match exacto por set de palabras (máximo 3 palabras)
        if len(palabras) <= 3:
            palabras_set = frozenset(palabras)
            respuesta_rapida = RESPUESTAS_CONVERSACIONALES.get(palabras_set)
            if not respuesta_rapida:
                # Intentar con subset (ej: "muchas gracias de verdad" → match "muchas gracias")
                for clave, resp in RESPUESTAS_CONVERSACIONALES.items():
                    if clave.issubset(palabras_set):
                        respuesta_rapida = resp
                        break
            
            if respuesta_rapida:
                await update.message.reply_text(respuesta_rapida)
                return
        # ── FIN FILTRO CONVERSACIONAL ─────────────────────────────────────────
        
        # PENDIENTE 5: Interpretar preguntas naturales sobre comandos
        
        # Detectar consultas sobre búsqueda de profesionales
        if any(p in msg_lower for p in ['buscar persona', 'buscar profesional', 'buscar cofrade', 
                'quien se dedica', 'quién se dedica', 'alguien que sea', 'alguien que trabaje',
                'necesito un', 'necesito una', 'busco un', 'busco una', 'conocen a alguien',
                'hay alguien que', 'datos de alguien', 'contacto de alguien']):
            await update.message.reply_text(
                f"💡 {user_name}, para buscar profesionales dentro de Cofradía tienes estos comandos:\n\n"
                f"🔹 /buscar_profesional [area]\n"
                f"   Busca en la base de datos de Cofradía por profesión o industria.\n"
                f"   Ejemplo: /buscar_profesional ingenieria\n"
                f"   Ejemplo: /buscar_profesional finanzas\n\n"
                f"🔹 /buscar_apoyo [area]\n"
                f"   Busca cofrades que están en búsqueda laboral activa.\n"
                f"   Ejemplo: /buscar_apoyo logistica\n\n"
                f"🔹 /buscar_especialista_sec [especialidad], [ciudad]\n"
                f"   Busca especialistas registrados en la SEC.\n"
                f"   Ejemplo: /buscar_especialista_sec electricidad, santiago\n\n"
                f"💡 Recuerda: los comandos empiezan con / las palabras no llevan tilde y van unidas por _"
            )
            return
        
        # Detectar consultas sobre búsqueda de información
        if any(p in msg_lower for p in ['buscar mensaje', 'buscar informaci', 'buscar en el grupo',
                'alguien habló de', 'alguien hablo de', 'se habló de', 'se hablo de',
                'información sobre', 'informacion sobre', 'qué se dijo', 'que se dijo']):
            await update.message.reply_text(
                f"💡 {user_name}, para buscar información en el historial:\n\n"
                f"🔹 /buscar [texto]\n"
                f"   Busca palabras exactas en los mensajes del grupo.\n"
                f"   Ejemplo: /buscar combustible\n\n"
                f"🔹 /buscar_ia [consulta]\n"
                f"   Búsqueda inteligente con IA que entiende contexto.\n"
                f"   Ejemplo: /buscar_ia quien vende paneles solares\n\n"
                f"💡 Recuerda: los comandos empiezan con / las palabras no llevan tilde y van unidas por _"
            )
            return
        
        # Detectar consultas sobre empleos
        if any(p in msg_lower for p in ['buscar empleo', 'buscar trabajo', 'ofertas de trabajo',
                'ofertas laboral', 'vacante', 'postular']):
            await update.message.reply_text(
                f"💡 {user_name}, para buscar empleos:\n\n"
                f"🔹 /empleo [cargo]\n"
                f"   Busca ofertas laborales compartidas en el grupo.\n"
                f"   Ejemplo: /empleo gerente\n"
                f"   Ejemplo: /empleo ingeniero\n\n"
                f"💡 Recuerda: los comandos empiezan con / las palabras no llevan tilde y van unidas por _"
            )
            return
        
        # Detectar consultas admin (solo owner)
        if es_owner and any(p in msg_lower for p in ['editar usuario', 'editar datos', 'corregir nombre',
                'corregir datos', 'cambiar nombre', 'eliminar usuario', 'eliminar solicitud',
                'borrar usuario', 'como edito', 'cómo edito', 'como elimino', 'cómo elimino']):
            await update.message.reply_text(
                f"👑 {user_name}, como administrador tienes estos comandos:\n\n"
                f"📝 EDITAR DATOS DE USUARIO:\n"
                f"  /editar_usuario [ID] nombre [valor]\n"
                f"  /editar_usuario [ID] apellido [valor]\n"
                f"  /editar_usuario [ID] generacion [año]\n\n"
                f"  Ejemplo: /editar_usuario 13031156 nombre Marcelo\n"
                f"  Ejemplo: /editar_usuario 13031156 apellido Villegas Soto\n\n"
                f"🗑️ ELIMINAR USUARIO:\n"
                f"  /eliminar_solicitud [ID]\n"
                f"  (Elimina registros y revoca acceso al grupo)\n\n"
                f"📋 VER SOLICITUDES:\n"
                f"  /ver_solicitudes - Ver pendientes\n"
                f"  /eliminar_solicitud - Ver últimos registros\n\n"
                f"💡 Los comandos empiezan con / las palabras no llevan tilde y van unidas por _"
            )
            return
        
        # Detectar consultas sobre estadísticas
        if any(p in msg_lower for p in ['estadistica', 'estadística', 'cuantos miembros', 'cuántos miembros',
                'grafico', 'gráfico', 'ranking']):
            await update.message.reply_text(
                f"💡 {user_name}, para ver estadísticas:\n\n"
                f"🔹 /graficos - Gráficos de actividad y KPIs\n"
                f"🔹 /estadisticas - Estadísticas generales\n"
                f"🔹 /top_usuarios - Ranking de participación\n"
                f"🔹 /mi_perfil - Tu perfil de actividad\n"
                f"🔹 /categorias - Categorías de mensajes\n\n"
                f"💡 Recuerda: los comandos empiezan con / las palabras no llevan tilde y van unidas por _"
            )
            return
        
        # Si no es una consulta de comandos, procesar como pregunta al bot con búsqueda exhaustiva
        msg = await update.message.reply_text("🧠 Buscando en toda la base de conocimientos...")
        
        try:
            # ===== MOTOR DE INTENCIÓN: Mejora silenciosa del mensaje =====
            intencion = mejorar_intencion(mensaje, user_name, user_id, canal='texto')
            query_para_busqueda = intencion['query_mejorada']
            
            # Si el motor detectó un comando claro → ejecutarlo directamente
            if intencion['ejecutar_comando'] and intencion['comando']:
                await msg.delete()
                ejecutado = await ejecutar_comando_desde_intencion(
                    intencion['comando'], intencion['args'], update, context
                )
                if ejecutado:
                    return  # El comando fue ejecutado, no necesitamos respuesta de IA
                # Si falló la ejecución, continuar con flujo normal
            
            # ===== BÚSQUEDA EXHAUSTIVA EN TODAS LAS FUENTES =====
            # Usar query mejorada para búsqueda más precisa
            # 1. Búsqueda unificada: historial + RAG (30-60 chunks)
            resultados_busq = busqueda_unificada(query_para_busqueda, limit_historial=20, limit_rag=40)
            contexto_completo = formatear_contexto_unificado(resultados_busq, query_para_busqueda)
            fuentes_usadas = resultados_busq.get('fuentes_usadas', [])
            
            # 2. Buscar también en tarjetas profesionales (directorio de cofrades)
            contexto_tarjetas = ""
            try:
                conn_dir = get_db_connection()
                if conn_dir:
                    c_dir = conn_dir.cursor()
                    palabras_dir = [p for p in mensaje.lower().split() if len(p) > 3][:5]
                    if palabras_dir:
                        cond_dir = []
                        params_dir = []
                        for p in palabras_dir:
                            if DATABASE_URL:
                                cond_dir.append("(LOWER(nombre_completo) LIKE %s OR LOWER(profesion) LIKE %s OR LOWER(empresa) LIKE %s OR LOWER(servicios) LIKE %s OR LOWER(ciudad) LIKE %s)")
                                params_dir.extend([f'%{p}%']*5)
                            else:
                                cond_dir.append("(LOWER(nombre_completo) LIKE ? OR LOWER(profesion) LIKE ? OR LOWER(empresa) LIKE ? OR LOWER(servicios) LIKE ? OR LOWER(ciudad) LIKE ?)")
                                params_dir.extend([f'%{p}%']*5)
                        if cond_dir:
                            wh_dir = " OR ".join(cond_dir)
                            if DATABASE_URL:
                                c_dir.execute(f"SELECT nombre_completo, profesion, empresa, ciudad, servicios, email, telefono FROM tarjetas_profesional WHERE {wh_dir} LIMIT 10", params_dir)
                            else:
                                c_dir.execute(f"SELECT nombre_completo, profesion, empresa, ciudad, servicios, email, telefono FROM tarjetas_profesional WHERE {wh_dir} LIMIT 10", params_dir)
                            tarjetas_encontradas = c_dir.fetchall()
                            if tarjetas_encontradas:
                                lineas_tar = []
                                for t in tarjetas_encontradas:
                                    if DATABASE_URL:
                                        lineas_tar.append(f"• {t['nombre_completo']} | {t['profesion']} | {t['empresa']} | {t['ciudad']} | {t['servicios']}")
                                    else:
                                        lineas_tar.append(f"• {t[0]} | {t[1]} | {t[2]} | {t[3]} | {t[4]}")
                                contexto_tarjetas = "\n\n=== DIRECTORIO PROFESIONAL COFRADES (Tarjetas) ===\n" + "\n".join(lineas_tar)
                                fuentes_usadas.append(f"Directorio ({len(tarjetas_encontradas)} cofrades)")
                    conn_dir.close()
            except Exception as e_dir:
                logger.debug(f"Error buscando directorio privado: {e_dir}")
            
            # 3. Buscar en eventos y actividades del grupo
            contexto_eventos = ""
            try:
                conn_ev = get_db_connection()
                if conn_ev and DATABASE_URL:
                    c_ev = conn_ev.cursor()
                    c_ev.execute("""SELECT titulo, descripcion, fecha_evento, lugar 
                                   FROM eventos WHERE activo = TRUE 
                                   AND fecha_evento >= CURRENT_TIMESTAMP 
                                   ORDER BY fecha_evento ASC LIMIT 5""")
                    eventos_prox = c_ev.fetchall()
                    conn_ev.close()
                    if eventos_prox:
                        lineas_ev = [f"• {e['titulo']} | {str(e['fecha_evento'])[:16]} | {e['lugar']}" for e in eventos_prox]
                        contexto_eventos = "\n\n=== PRÓXIMOS EVENTOS COFRADÍA ===\n" + "\n".join(lineas_ev)
                        fuentes_usadas.append(f"Eventos ({len(eventos_prox)})")
            except Exception as e_ev:
                logger.debug(f"Error buscando eventos privado: {e_ev}")

            # Construir prompt con modo inteligente según relevancia temática
            fuentes_str = " | ".join(fuentes_usadas) if fuentes_usadas else "conocimiento propio"
            total_rag = len(resultados_busq.get('rag', []))
            rag_confianza = resultados_busq.get('rag_confianza', 'ninguna')
            rag_tematica = resultados_busq.get('rag_tematica', 'desconocida')
            rag_score = resultados_busq.get('rag_score_max', 0.0)
            
            # Intent hint
            intent_hint = ""
            if query_para_busqueda != mensaje:
                intent_hint = f"\n(búsqueda enriquecida: \"{query_para_busqueda}\")"
            
            # Sugerencia de comando complementario
            cmd_sugerido = intencion.get('comando')
            sugerencia_cmd = ""
            if cmd_sugerido and not intencion.get('ejecutar_comando'):
                desc_cmd = CATALOGO_COMANDOS_INTENCION.get(cmd_sugerido, '')
                sugerencia_cmd = f"\nComando relacionado disponible: /{cmd_sugerido} — {desc_cmd}"
            
            # ── DECISIÓN DE MODO ─────────────────────────────────────────────
            docs_son_relevantes = (
                rag_tematica in ('relevante', 'posible') and
                rag_confianza in ('alta', 'media') and
                bool(resultados_busq.get('rag'))
            )
            
            if docs_son_relevantes:
                # MODO A: documentos del RAG son sobre el tema → sintetizar
                instruccion = (
                    f"El sistema encontró {total_rag} fragmentos de documentos relevantes "
                    f"para esta consulta. LÉELOS y sintetiza una respuesta completa basándote en ellos."
                )
                ctx_rag = contexto_completo[:7000]
                fuentes_str_final = fuentes_str
                logger.info(f"💬 Modo RAG para '{mensaje[:40]}'")
            else:
                # MODO B: docs no son sobre el tema → responder desde conocimiento LLM
                instruccion = (
                    "La base de documentos indexados no contiene información sobre este tema específico. "
                    "RESPONDE directamente desde tu conocimiento como LLM experto (LLaMA 3.3 70B, hasta 2024). "
                    "No menciones documentos, no pidas más contexto. Solo responde lo que sabes."
                )
                ctx_rag = ""  # No pasar docs irrelevantes al LLM
                fuentes_str_final = "Conocimiento propio del modelo"
                logger.info(f"💬 Modo LLM para '{mensaje[:40]}' (rag_tematica={rag_tematica})")
            # ── FIN DECISIÓN ─────────────────────────────────────────────────
            
            prompt = f"""Eres el asistente IA de la Cofradía de Networking, comunidad de oficiales navales chilenos (LLaMA 3.3 70B, conocimiento hasta 2024).

Responde a {user_name} empezando con "{user_name}," de forma directa y natural.{intent_hint}

INSTRUCCIÓN: {instruccion}

REGLAS:
- Nunca digas "no tengo información" sobre temas que conoces hasta 2024
- Nunca inventes personas de la Cofradía
- Responde completo, útil, en español, sin asteriscos

{ctx_rag}
{contexto_tarjetas[:600] if contexto_tarjetas else ''}
{contexto_eventos[:200] if contexto_eventos else ''}

PREGUNTA: {mensaje}{sugerencia_cmd}"""
            
            fuentes_str = fuentes_str_final

            await msg.edit_text("🧠 Generando respuesta completa...")
            respuesta = llamar_groq(prompt, max_tokens=1800, temperature=0.7)
            
            if not respuesta:
                respuesta = llamar_gemini_texto(prompt, max_tokens=1800, temperature=0.7)
            
            if respuesta:
                # Agregar footer con fuentes consultadas
                footer = f"\n\n─────────────────\n🔍 *Fuentes consultadas:* {fuentes_str}"
                respuesta_final = respuesta + footer
                try:
                    await msg.edit_text(respuesta_final, parse_mode='Markdown')
                except Exception:
                    await msg.edit_text(respuesta + f"\n\n─────────────────\n🔍 Fuentes: {fuentes_str}")
            else:
                await msg.edit_text("Lo siento, no pude procesar tu consulta en este momento. Intenta de nuevo.")
                
        except Exception as e:
            logger.error(f"Error en chat privado: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await msg.edit_text(f"❌ Error procesando consulta: {str(e)[:100]}")
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, responder_chat_privado))
    
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
        
        # RAG indexación cada 6 horas (Excel only, preserva PDFs)
        job_queue.run_repeating(
            indexar_rag_job,
            interval=21600,  # 6 horas en segundos
            first=300,  # Primera ejecución después de 5 minutos (no inmediato)
            name='rag_indexacion'
        )
        logger.info("🧠 Tarea de indexación RAG programada cada 6 horas (primera en 5 min)")
        
        # Recordatorios de agenda personal (cada 10 minutos)
        job_queue.run_repeating(
            recordatorio_agenda_job,
            interval=600,  # 10 minutos
            first=120,     # Primera ejecución 2 minutos después del inicio
            name='recordatorio_agenda'
        )
        logger.info("🔔 Job de recordatorios de agenda programado (cada 10 min)")
        
        # Newsletter semanal: lunes a las 9:00 AM Chile
        try:
            job_queue.run_daily(
                generar_newsletter_semanal,
                time=dt_time(hour=12, minute=0),  # 12:00 UTC = 9:00 Chile
                days=(0,),  # Solo lunes
                name='newsletter_semanal'
            )
            logger.info("📰 Newsletter semanal programada: lunes 9:00 AM Chile")
        except Exception as e:
            logger.warning(f"No se pudo programar newsletter: {e}")
        
        # Recordatorio de eventos: diario a las 10:00 AM Chile
        async def recordar_eventos_proximos(context):
            """Envía recordatorio de eventos que ocurren mañana"""
            try:
                conn = get_db_connection()
                if not conn:
                    return
                c = conn.cursor()
                if DATABASE_URL:
                    c.execute("""SELECT e.id, e.titulo, e.fecha_evento, e.lugar,
                                array_agg(ea.user_id) as asistentes_ids
                                FROM eventos e
                                LEFT JOIN eventos_asistencia ea ON ea.evento_id = e.id
                                WHERE e.activo = TRUE 
                                AND e.fecha_evento::date = (CURRENT_DATE + INTERVAL '1 day')::date
                                GROUP BY e.id, e.titulo, e.fecha_evento, e.lugar""")
                    eventos_manana = c.fetchall()
                    conn.close()
                    
                    for ev in eventos_manana:
                        titulo = ev['titulo']
                        fecha = str(ev['fecha_evento'])[:16]
                        lugar = ev['lugar']
                        ids = ev['asistentes_ids'] or []
                        
                        for uid in ids:
                            if uid:
                                try:
                                    await context.bot.send_message(
                                        chat_id=uid,
                                        text=f"📅 RECORDATORIO: Mañana tienes un evento!\n\n"
                                             f"📌 {titulo}\n📆 {fecha}\n📍 {lugar}\n\n"
                                             f"Ver detalles: /eventos"
                                    )
                                except:
                                    pass
                        
                        # Notificar al owner también
                        if OWNER_ID:
                            asistentes_count = len([x for x in ids if x])
                            try:
                                await context.bot.send_message(
                                    chat_id=OWNER_ID,
                                    text=f"📅 Mañana: {titulo}\n📍 {lugar}\n👥 {asistentes_count} confirmados"
                                )
                            except:
                                pass
            except Exception as e:
                logger.debug(f"Error recordando eventos: {e}")
        
        try:
            job_queue.run_daily(
                recordar_eventos_proximos,
                time=dt_time(hour=13, minute=0),  # 13:00 UTC = 10:00 Chile
                name='recordatorio_eventos'
            )
            logger.info("📅 Recordatorio de eventos programado: diario 10:00 AM Chile")
        except Exception as e:
            logger.warning(f"No se pudo programar recordatorio eventos: {e}")
    
        # Job: Reporte laboral semanal (viernes 10:00 AM Chile = 13:00 UTC)
        try:
            job_queue.run_daily(
                generar_reporte_laboral,
                time=dt_time(hour=13, minute=30),
                days=(4,),  # Viernes
                name='reporte_laboral'
            )
            logger.info("📈 Reporte laboral programado: viernes 10:30 AM Chile")
        except Exception as e:
            logger.warning(f"No se pudo programar reporte laboral: {e}")
        
        # Job: Newsletter email (lunes 10:00 AM Chile = 13:00 UTC)
        try:
            job_queue.run_daily(
                generar_newsletter_email,
                time=dt_time(hour=13, minute=15),
                days=(0,),  # Lunes
                name='newsletter_email'
            )
            logger.info("📧 Newsletter email programado: lunes 10:15 AM Chile")
        except Exception as e:
            logger.warning(f"No se pudo programar newsletter email: {e}")
    
    logger.info("✅ Bot iniciado!")
    
    # POLLING — keep-alive server en PORT(10000) mantiene Render despierto
    logger.info("🔄 Modo POLLING activo — keep-alive en PORT mantiene bot despierto 24/7")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )


if __name__ == '__main__':
    main()
