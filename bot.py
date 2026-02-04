import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes, CallbackQueryHandler
import google.generativeai as genai
import requests
import PIL.Image
import base64
import sqlite3
from datetime import datetime, timedelta, time
from collections import Counter
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import secrets
import string

# Configuración de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
genai.configure(api_key=GEMINI_API_KEY)

# Modelo principal para texto
model = genai.GenerativeModel('gemini-1.5-flash-latest')

# Modelo para visión (OCR, análisis de imágenes)
vision_model = genai.GenerativeModel('gemini-1.5-flash-latest')

# ID del dueño del bot (se configura en variables de entorno)
OWNER_ID = int(os.environ.get('OWNER_TELEGRAM_ID', '0'))

# Datos bancarios para pagos
DATOS_BANCARIOS = """
💳 **DATOS PARA TRANSFERENCIA**

**Titular:** Destak E.I.R.L.
**RUT:** 76.698.480-0
**Banco:** Banco Santander
**Cuenta Corriente:** 69104312

📸 Envía el comprobante como imagen después de transferir.
"""

# Estilo de gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

# ==================== BASE DE DATOS ====================

def init_db():
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    # Tabla de mensajes
    c.execute('''CREATE TABLE IF NOT EXISTS mensajes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  username TEXT,
                  first_name TEXT,
                  message TEXT,
                  topic_id INTEGER,
                  fecha TEXT,
                  embedding TEXT,
                  categoria TEXT)''')
    
    # Tabla de resúmenes
    c.execute('''CREATE TABLE IF NOT EXISTS resumenes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  fecha TEXT,
                  tipo TEXT,
                  resumen TEXT,
                  mensajes_count INTEGER)''')
    
    # Tabla de suscripciones
    c.execute('''CREATE TABLE IF NOT EXISTS suscripciones
                 (user_id INTEGER PRIMARY KEY,
                  first_name TEXT,
                  username TEXT,
                  es_admin INTEGER DEFAULT 0,
                  fecha_registro TEXT,
                  fecha_expiracion TEXT,
                  estado TEXT DEFAULT 'activo',
                  mensajes_engagement INTEGER DEFAULT 0,
                  ultimo_mensaje_engagement TEXT,
                  servicios_usados TEXT DEFAULT '[]')''')
    
    # Tabla de códigos de activación
    c.execute('''CREATE TABLE IF NOT EXISTS codigos_activacion
                 (codigo TEXT PRIMARY KEY,
                  dias_validez INTEGER,
                  precio INTEGER,
                  fecha_creacion TEXT,
                  fecha_expiracion TEXT,
                  usado INTEGER DEFAULT 0,
                  usado_por INTEGER,
                  fecha_uso TEXT)''')
    
    # Tabla de pagos pendientes
    c.execute('''CREATE TABLE IF NOT EXISTS pagos_pendientes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  first_name TEXT,
                  dias_plan INTEGER,
                  precio INTEGER,
                  comprobante_file_id TEXT,
                  fecha_envio TEXT,
                  estado TEXT DEFAULT 'pendiente')''')
    
    # Tabla de precios
    c.execute('''CREATE TABLE IF NOT EXISTS precios_planes
                 (dias INTEGER PRIMARY KEY,
                  precio INTEGER,
                  nombre_plan TEXT)''')
    
    # Insertar precios por defecto
    c.execute("SELECT COUNT(*) FROM precios_planes")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO precios_planes VALUES (30, 2000, 'Mensual')")
        c.execute("INSERT INTO precios_planes VALUES (180, 10500, 'Semestral')")
        c.execute("INSERT INTO precios_planes VALUES (365, 20000, 'Anual')")
    
    conn.commit()
    conn.close()

# ==================== FUNCIONES DE SUSCRIPCIÓN ====================

def registrar_usuario_suscripcion(user_id, first_name, username, es_admin=False):
    """Registra usuario con 3 meses gratis"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    fecha_registro = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fecha_expiracion = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")
    
    c.execute("""INSERT OR REPLACE INTO suscripciones 
                 (user_id, first_name, username, es_admin, fecha_registro, fecha_expiracion, estado, mensajes_engagement, ultimo_mensaje_engagement, servicios_usados) 
                 VALUES (?, ?, ?, ?, ?, ?, 'activo', 0, ?, '[]')""",
              (user_id, first_name, username, 1 if es_admin else 0, fecha_registro, fecha_expiracion, fecha_registro))
    conn.commit()
    conn.close()

def verificar_suscripcion_activa(user_id):
    """Verifica si el usuario tiene suscripción activa"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute("SELECT fecha_expiracion, estado FROM suscripciones WHERE user_id = ?", (user_id,))
    resultado = c.fetchone()
    conn.close()
    
    if not resultado:
        return False
    
    fecha_exp, estado = resultado
    
    if estado != 'activo':
        return False
    
    fecha_expiracion = datetime.strptime(fecha_exp, "%Y-%m-%d %H:%M:%S")
    
    return datetime.now() < fecha_expiracion

def obtener_dias_restantes(user_id):
    """Obtiene días restantes de suscripción"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute("SELECT fecha_expiracion FROM suscripciones WHERE user_id = ?", (user_id,))
    resultado = c.fetchone()
    conn.close()
    
    if not resultado:
        return 0
    
    fecha_exp = datetime.strptime(resultado[0], "%Y-%m-%d %H:%M:%S")
    dias = (fecha_exp - datetime.now()).days
    
    return max(0, dias)

def generar_codigo_activacion(dias, precio):
    """Genera código único de activación"""
    caracteres = string.ascii_uppercase + string.digits
    codigo = ''.join(secrets.choice(caracteres) for _ in range(12))
    codigo = f"COF-{codigo[:4]}-{codigo[4:8]}-{codigo[8:]}"
    
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    fecha_creacion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fecha_expiracion = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    
    c.execute("""INSERT INTO codigos_activacion 
                 (codigo, dias_validez, precio, fecha_creacion, fecha_expiracion, usado, usado_por, fecha_uso) 
                 VALUES (?, ?, ?, ?, ?, 0, NULL, NULL)""",
              (codigo, dias, precio, fecha_creacion, fecha_expiracion))
    conn.commit()
    conn.close()
    
    return codigo

def validar_y_usar_codigo(user_id, codigo):
    """Valida y aplica código de activación"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
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
    
    fecha_exp = datetime.strptime(fecha_exp_codigo, "%Y-%m-%d %H:%M:%S")
    if datetime.now() > fecha_exp:
        conn.close()
        return False, "❌ Código expirado."
    
    c.execute("UPDATE codigos_activacion SET usado = 1, usado_por = ?, fecha_uso = ? WHERE codigo = ?",
              (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), codigo))
    
    c.execute("SELECT fecha_expiracion FROM suscripciones WHERE user_id = ?", (user_id,))
    resultado_user = c.fetchone()
    
    if resultado_user:
        fecha_exp_actual = datetime.strptime(resultado_user[0], "%Y-%m-%d %H:%M:%S")
        if fecha_exp_actual < datetime.now():
            nueva_fecha = datetime.now() + timedelta(days=dias_validez)
        else:
            nueva_fecha = fecha_exp_actual + timedelta(days=dias_validez)
        
        c.execute("UPDATE suscripciones SET fecha_expiracion = ?, estado = 'activo' WHERE user_id = ?",
                  (nueva_fecha.strftime("%Y-%m-%d %H:%M:%S"), user_id))
    
    conn.commit()
    conn.close()
    
    return True, f"✅ ¡Código activado! Tu suscripción se extendió por {dias_validez} días."

def registrar_servicio_usado(user_id, servicio):
    """Registra qué servicios ha usado el usuario"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute("SELECT servicios_usados FROM suscripciones WHERE user_id = ?", (user_id,))
    resultado = c.fetchone()
    
    if resultado:
        servicios = json.loads(resultado[0])
        if servicio not in servicios:
            servicios.append(servicio)
            c.execute("UPDATE suscripciones SET servicios_usados = ? WHERE user_id = ?",
                      (json.dumps(servicios), user_id))
            conn.commit()
    
    conn.close()

def obtener_precios():
    """Obtiene los precios configurados"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT dias, precio, nombre_plan FROM precios_planes ORDER BY dias")
    precios = c.fetchall()
    conn.close()
    return precios

def actualizar_precio(dias, nuevo_precio):
    """Actualiza precio de un plan"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("UPDATE precios_planes SET precio = ? WHERE dias = ?", (nuevo_precio, dias))
    conn.commit()
    conn.close()

# ==================== FUNCIONES DE IA Y ANÁLISIS ====================

def categorizar_mensaje(mensaje):
    try:
        prompt = f"""Clasifica en UNA categoría:
Categorías: Networking, Negocios, Tecnología, Marketing, Eventos, Emprendimiento, Consultas, Recursos, Empleos, Social, Otros
Mensaje: "{mensaje[:200]}"
Responde SOLO la categoría."""
        response = model.generate_content(prompt)
        categoria = response.text.strip()
        categorias_validas = ['Networking', 'Negocios', 'Tecnología', 'Marketing', 'Eventos', 
                             'Emprendimiento', 'Consultas', 'Recursos', 'Empleos', 'Social', 'Otros']
        return categoria if categoria in categorias_validas else 'Otros'
    except:
        return 'Otros'

def generar_embedding(texto):
    try:
        result = genai.embed_content(model="models/embedding-001", content=texto, task_type="retrieval_document")
        return json.dumps(result['embedding'])
    except:
        return None

def guardar_mensaje(user_id, username, first_name, message, topic_id=None):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    embedding = generar_embedding(message)
    categoria = categorizar_mensaje(message)
    c.execute("""INSERT INTO mensajes (user_id, username, first_name, message, topic_id, fecha, embedding, categoria) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (user_id, username, first_name, message, topic_id, fecha, embedding, categoria))
    conn.commit()
    conn.close()

def buscar_semantica(query, topic_id=None, limit=5):
    try:
        query_result = genai.embed_content(model="models/embedding-001", content=query, task_type="retrieval_query")
        query_embedding = query_result['embedding']
        conn = sqlite3.connect('mensajes.db', check_same_thread=False)
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
                emb = json.loads(emb_str)
                similitud = sum(a * b for a, b in zip(query_embedding, emb))
                similitudes.append((similitud, nombre, mensaje, fecha))
        similitudes.sort(reverse=True)
        return [(n, m, f) for _, n, m, f in similitudes[:limit]]
    except:
        return []

def buscar_en_historial(query, topic_id=None, limit=10):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    if topic_id:
        c.execute("""SELECT first_name, message, fecha FROM mensajes 
                     WHERE (message LIKE ? OR message LIKE ?) AND topic_id = ?
                     ORDER BY fecha DESC LIMIT ?""",
                  (f'%{query}%', f'%{query.lower()}%', topic_id, limit))
    else:
        c.execute("""SELECT first_name, message, fecha FROM mensajes 
                     WHERE message LIKE ? OR message LIKE ?
                     ORDER BY fecha DESC LIMIT ?""",
                  (f'%{query}%', f'%{query.lower()}%', limit))
    resultados = c.fetchall()
    conn.close()
    return resultados

async def buscar_empleos_web(cargo=None, industria=None, area=None, ubicacion=None, rango_renta=None):
    try:
        partes = []
        if cargo: partes.append(f"cargo: {cargo}")
        if industria: partes.append(f"industria: {industria}")
        if area: partes.append(f"área: {area}")
        if ubicacion: partes.append(f"ubicación: {ubicacion}")
        if rango_renta: partes.append(f"renta: {rango_renta}")
        consulta = ", ".join(partes) if partes else "empleos"
        
        prompt = f"""Busca ofertas en LinkedIn, Indeed y Laborum para: {consulta}
Proporciona 5-8 opciones con: título, empresa, ubicación, salario, link, descripción breve.
Formatea profesionalmente en español."""
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"❌ Error: {str(e)}"

def obtener_estadisticas_graficos(dias=7):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    c.execute("SELECT DATE(fecha), COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY DATE(fecha) ORDER BY fecha", (fecha_inicio,))
    mensajes_por_dia = c.fetchall()
    c.execute("SELECT first_name, COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY user_id, first_name ORDER BY COUNT(*) DESC LIMIT 10", (fecha_inicio,))
    usuarios_activos = c.fetchall()
    c.execute("SELECT categoria, COUNT(*) FROM mensajes WHERE fecha >= ? AND categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC", (fecha_inicio,))
    por_categoria = c.fetchall()
    c.execute("SELECT CAST(strftime('%H', fecha) AS INTEGER), COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY strftime('%H', fecha) ORDER BY strftime('%H', fecha)", (fecha_inicio,))
    por_hora = c.fetchall()
    conn.close()
    return {'mensajes_por_dia': mensajes_por_dia, 'usuarios_activos': usuarios_activos, 'por_categoria': por_categoria, 'por_hora': por_hora}

def generar_grafico_visual(stats):
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('📊 Análisis - Cofradía de Networking', fontsize=18, fontweight='bold', y=0.98)
    
    if stats['mensajes_por_dia']:
        dias = [d[0] for d in stats['mensajes_por_dia']]
        valores = [d[1] for d in stats['mensajes_por_dia']]
        ax1.plot(dias, valores, marker='o', linewidth=3, color='#1f77b4', markersize=8)
        ax1.fill_between(range(len(dias)), valores, alpha=0.3, color='#1f77b4')
        ax1.set_title('📅 Actividad Diaria', fontsize=14, fontweight='bold', pad=15)
        ax1.set_xlabel('Fecha', fontsize=11)
        ax1.set_ylabel('Mensajes', fontsize=11)
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
        for i, v in enumerate(valores):
            ax1.text(i, v + max(valores)*0.02, str(v), ha='center', va='bottom', fontweight='bold')
    
    if stats['usuarios_activos']:
        usuarios = [u[0][:15] for u in stats['usuarios_activos'][:8]]
        mensajes = [u[1] for u in stats['usuarios_activos'][:8]]
        colores = plt.cm.viridis(range(len(usuarios)))
        bars = ax2.barh(usuarios, mensajes, color=colores, edgecolor='black', linewidth=1.5)
        ax2.set_title('👥 Usuarios Más Activos', fontsize=14, fontweight='bold', pad=15)
        ax2.set_xlabel('Mensajes', fontsize=11)
        ax2.invert_yaxis()
        for bar in bars:
            width = bar.get_width()
            ax2.text(width + max(mensajes)*0.01, bar.get_y() + bar.get_height()/2, f'{int(width)}', ha='left', va='center', fontweight='bold')
    
    if stats['por_categoria']:
        categorias = [c[0] for c in stats['por_categoria']]
        valores_cat = [c[1] for c in stats['por_categoria']]
        colores_pastel = plt.cm.Set3(range(len(categorias)))
        wedges, texts, autotexts = ax3.pie(valores_cat, labels=categorias, autopct='%1.1f%%', colors=colores_pastel, startangle=90, textprops={'fontsize': 10, 'fontweight': 'bold'})
        ax3.set_title('🏷️ Distribución por Categorías', fontsize=14, fontweight='bold', pad=15)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(10)
            autotext.set_fontweight('bold')
    
    if stats['por_hora']:
        horas = list(range(24))
        valores_hora = [0] * 24
        for hora, count in stats['por_hora']:
            valores_hora[hora] = count
        colores_hora = plt.cm.YlOrRd([(v / max(valores_hora) if max(valores_hora) > 0 else 0) for v in valores_hora])
        bars = ax4.bar(horas, valores_hora, color=colores_hora, edgecolor='black', linewidth=1)
        ax4.set_title('🕐 Actividad por Hora', fontsize=14, fontweight='bold', pad=15)
        ax4.set_xlabel('Hora', fontsize=11)
        ax4.set_ylabel('Mensajes', fontsize=11)
        ax4.set_xticks(range(0, 24, 2))
        ax4.grid(True, alpha=0.3, axis='y')
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax4.text(bar.get_x() + bar.get_width()/2., height, f'{int(height)}', ha='center', va='bottom', fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
    buffer.seek(0)
    plt.close()
    return buffer

def analizar_participacion_usuarios(dias=7):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    c.execute("""SELECT user_id, first_name, COUNT(*), COUNT(DISTINCT DATE(fecha)), COUNT(DISTINCT categoria)
                 FROM mensajes WHERE fecha >= ? GROUP BY user_id, first_name ORDER BY COUNT(*) DESC""", (fecha_inicio,))
    usuarios = c.fetchall()
    conn.close()
    
    analisis = []
    for user_id, nombre, total_msg, dias_act, categorias in usuarios:
        promedio_diario = total_msg / max(dias_act, 1)
        if total_msg >= 50 and dias_act >= 5:
            nivel = "🌟 DESTACADO"
            sugerencia = "Candidato ideal para moderador."
        elif total_msg >= 30:
            nivel = "⭐ MUY ACTIVO"
            sugerencia = "Podría liderar discusiones."
        elif total_msg >= 15:
            nivel = "✨ ACTIVO"
            sugerencia = "Motivar a compartir más."
        elif total_msg >= 5:
            nivel = "👤 PARTICIPANTE"
            sugerencia = "Invitar a eventos."
        else:
            nivel = "💤 INACTIVO"
            sugerencia = "Mensajes personalizados."
        analisis.append({'nombre': nombre, 'total_mensajes': total_msg, 'dias_activos': dias_act, 'promedio_diario': round(promedio_diario, 1), 'categorias_variadas': categorias, 'nivel': nivel, 'sugerencia': sugerencia})
    return analisis

# ==================== BÚSQUEDA DE PROFESIONALES EN GOOGLE DRIVE ====================

# ==================== BÚSQUEDA DE PROFESIONALES EN GOOGLE DRIVE ====================

def buscar_archivo_excel_drive():
    """Busca el archivo más reciente de BD Grupo Laboral en Google Drive usando SOLO requests"""
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        import io
        
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDS')
        if not creds_json:
            logger.error("GOOGLE_DRIVE_CREDS no configurado")
            return None
        
        # Configurar credenciales
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        
        # Obtener token de acceso
        access_token = creds.get_access_token().access_token
        headers = {'Authorization': f'Bearer {access_token}'}
        
        # PASO 1: Buscar carpeta INBESTU usando requests directo
        search_url = "https://www.googleapis.com/drive/v3/files"
        params_carpeta = {
            'q': "name='INBESTU' and mimeType='application/vnd.google-apps.folder'",
            'fields': 'files(id, name)'
        }
        
        response_carpeta = requests.get(search_url, headers=headers, params=params_carpeta)
        
        if response_carpeta.status_code != 200:
            logger.error(f"Error buscando carpeta: {response_carpeta.status_code}")
            return None
        
        carpetas = response_carpeta.json().get('files', [])
        
        if not carpetas:
            logger.error("Carpeta INBESTU no encontrada")
            return None
        
        carpeta_id = carpetas[0]['id']
        logger.info(f"Carpeta encontrada: {carpetas[0]['name']}")
        
        # PASO 2: Buscar archivos Excel en la carpeta usando requests directo
        params_archivos = {
            'q': f"name contains 'BD Grupo Laboral' and '{carpeta_id}' in parents and trashed=false",
            'fields': 'files(id, name)',
            'orderBy': 'name desc'
        }
        
        response_archivos = requests.get(search_url, headers=headers, params=params_archivos)
        
        if response_archivos.status_code != 200:
            logger.error(f"Error buscando archivos: {response_archivos.status_code}")
            return None
        
        archivos = response_archivos.json().get('files', [])
        
        if not archivos:
            logger.error("No se encontró archivo BD Grupo Laboral")
            return None
        
        # Tomar el archivo más reciente
        archivo_mas_reciente = archivos[0]
        logger.info(f"Archivo encontrado: {archivo_mas_reciente['name']}")
        
        # PASO 3: Descargar archivo usando requests directo
        file_id = archivo_mas_reciente['id']
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        
        response_download = requests.get(download_url, headers=headers)
        
        if response_download.status_code == 200:
            logger.info(f"Archivo descargado exitosamente: {len(response_download.content)} bytes")
            return io.BytesIO(response_download.content)
        else:
            logger.error(f"Error descargando archivo: {response_download.status_code}")
            return None
        
    except Exception as e:
        logger.error(f"Error buscando archivo en Drive: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def buscar_profesionales(query):
    """Busca profesionales en el Excel usando IA semántica avanzada"""
    try:
        import pandas as pd
        
        archivo = buscar_archivo_excel_drive()
        
        if not archivo:
            return "❌ No se pudo acceder a la base de datos de profesionales.\n\n💡 **Posibles causas:**\n• La carpeta INBESTU no está compartida con el bot\n• No existe el archivo 'BD Grupo Laboral' en la carpeta\n• Error de permisos en Google Drive\n\nContacta al administrador."
        
        df = pd.read_excel(archivo, engine='openpyxl')
        df.columns = df.columns.str.strip().str.lower()
        
        profesionales_lista = []
        
        for idx, row in df.iterrows():
            nombre = str(row.get('nombre completo', row.get('nombre', 'N/A'))).strip()
            profesion = str(row.get('profesión', row.get('profesion', row.get('área', row.get('area', 'N/A'))))).strip()
            expertise = str(row.get('expertise', row.get('experiencia', row.get('especialidad', 'N/A')))).strip()
            email = str(row.get('email', row.get('correo', row.get('e-mail', 'N/A')))).strip()
            telefono = str(row.get('teléfono', row.get('telefono', row.get('celular', row.get('fono', 'N/A'))))).strip()
            estado = str(row.get('estado', row.get('situación', row.get('situacion', row.get('disponibilidad', 'N/A'))))).strip()
            trabajos = str(row.get('trabajos', row.get('descripción', row.get('descripcion', row.get('experiencia laboral', 'N/A'))))).strip()
            
            if nombre == 'N/A' or nombre == 'nan' or not nombre or nombre == '':
                continue
            
            profesional = {
                'id': idx + 1,
                'nombre': nombre,
                'profesion': profesion,
                'expertise': expertise,
                'email': email,
                'telefono': telefono,
                'estado': estado,
                'trabajos': trabajos
            }
            
            profesionales_lista.append(profesional)
        
        if not profesionales_lista:
            return "❌ No se encontraron profesionales en la base de datos.\n\nPor favor, verifica que el archivo Excel contenga datos válidos."
        
        profesionales_texto = ""
        for prof in profesionales_lista:
            profesionales_texto += f"""
ID: {prof['id']}
Nombre: {prof['nombre']}
Profesión/Área: {prof['profesion']}
Expertise: {prof['expertise']}
Estado: {prof['estado']}
Email: {prof['email']}
Teléfono: {prof['telefono']}
Trabajos: {prof['trabajos']}
---
"""
        
        prompt = f"""Eres un asistente experto en búsqueda semántica de profesionales en la comunidad Cofradía.

CONSULTA DEL USUARIO: "{query}"

BASE DE DATOS DE PROFESIONALES (Total: {len(profesionales_lista)} profesionales):
{profesionales_texto[:12000]}

INSTRUCCIONES DE BÚSQUEDA SEMÁNTICA:

1. PRIORIDAD DE COINCIDENCIAS:
   - EXACTA: Coincidencia directa (Score: 10/10)
   - ALTA: Profesión relacionada (Score: 7-9/10)
   - MEDIA: Experiencia tangencial (Score: 5-6/10)
   - BAJA: Habilidades complementarias (Score: 3-4/10)

2. CANTIDAD: Selecciona hasta 10 profesionales máximo

3. FORMATO DE RESPUESTA:
Determina el encabezado según coincidencias:
- 5+ EXACTAS/ALTAS: "✅ PROFESIONALES QUE COINCIDEN CON TU BÚSQUEDA:"
- Principalmente MEDIAS: "🔍 LOS PROFESIONALES DE COFRADÍA QUE MEJOR SE AJUSTAN A TU BÚSQUEDA SON LOS SIGUIENTES:"
- Solo BAJAS: "💡 PROFESIONALES RELACIONADOS QUE PODRÍAN AYUDARTE:"

Lista profesionales (máximo 10):

**[Número]. [Nombre]**
🎯 Área: [profesión]
💼 Expertise: [expertise - 1 línea]
📊 Estado: [Contratado/Independiente/Cesante]
📧 Email: [email]
📱 Teléfono: [teléfono]
💡 Experiencia: [trabajos - 2 líneas máximo]
⭐ Relevancia: [EXACTA/ALTA/MEDIA/BAJA] - [justificación breve]

---

Al final: "💬 Para más información, contacta directamente a los profesionales."

SI NO HAY COINCIDENCIAS:
"❌ No se encontraron profesionales en Cofradía que coincidan con: {query}

💡 Intenta términos más generales."

Responde en español, claro y profesional."""

        response = model.generate_content(prompt)
        resultado = response.text
        
        if "contacta directamente" not in resultado.lower():
            resultado += "\n\n💬 *Para más información, contacta directamente a los profesionales.*"
        
        return resultado
        
    except Exception as e:
        logger.error(f"Error buscando profesionales: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return f"❌ Error al buscar profesionales: {str(e)}\n\n**Detalles técnicos:** {type(e).__name__}\n\nPor favor, intenta de nuevo o contacta al administrador."

def generar_resumen_usuarios(dias=1):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    fecha_inicio = (datetime.now() - timedelta(dias=dias)).strftime("%Y-%m-%d")
    c.execute("SELECT first_name, message, categoria FROM mensajes WHERE fecha >= ? ORDER BY fecha", (fecha_inicio,))
    mensajes = c.fetchall()
    if not mensajes:
        conn.close()
        return None
    por_categoria = {}
    for nombre, msg, cat in mensajes:
        if cat not in por_categoria:
            por_categoria[cat] = []
        por_categoria[cat].append(f"{nombre}: {msg}")
    contexto = ""
    for cat, msgs in por_categoria.items():
        contexto += f"\n[{cat}]\n" + "\n".join(msgs[:5]) + "\n"
    prompt = f"""Resumen profesional de conversaciones:
{contexto[:6000]}
Estructura:
📊 RESUMEN {'DIARIO' if dias == 1 else 'SEMANAL'} - {datetime.now().strftime('%d/%m/%Y')}
**📌 Temas Principales** (4-5 bullets)
**💡 Insights** (3-4 bullets)
**🎯 Destacados**
**📚 Próximos Pasos**
Total: {len(mensajes)} mensajes
Máximo 350 palabras."""
    try:
        response = model.generate_content(prompt)
        resumen = response.text
        fecha_actual = datetime.now().strftime("%Y-%m-%d")
        c.execute("INSERT INTO resumenes (fecha, tipo, resumen, mensajes_count) VALUES (?, ?, ?, ?)", (fecha_actual, 'usuario', resumen, len(mensajes)))
        conn.commit()
        conn.close()
        return resumen
    except:
        conn.close()
        return None

def generar_resumen_admins(dias=1):
    resumen_base = generar_resumen_usuarios(dias)
    if not resumen_base:
        return None
    analisis = analizar_participacion_usuarios(dias)
    seccion_admin = "\n\n" + "="*50 + "\n👑 **SECCIÓN ADMINISTRADORES**\n" + "="*50 + "\n\n**📊 MÉTRICAS**\n\n"
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    c.execute("SELECT COUNT(*) FROM mensajes WHERE fecha >= ?", (fecha_inicio,))
    total_msgs = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes WHERE fecha >= ?", (fecha_inicio,))
    usuarios_activos = c.fetchone()[0]
    conn.close()
    seccion_admin += f"• Total: {total_msgs}\n• Usuarios: {usuarios_activos}\n\n**🌟 DESTACADOS**\n\n"
    for user in analisis[:10]:
        seccion_admin += f"{user['nivel']} **{user['nombre']}**\n   • {user['total_mensajes']} mensajes\n   • 💡 {user['sugerencia']}\n\n"
    return resumen_base + seccion_admin

# ==================== RECORDATORIOS Y ENGAGEMENT (MEJORADOS) ====================

async def enviar_recordatorios(context: ContextTypes.DEFAULT_TYPE):
    """Envía recordatorios de renovación persuasivos"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute("""SELECT user_id, first_name, fecha_expiracion, servicios_usados 
                 FROM suscripciones 
                 WHERE estado = 'activo'""")
    usuarios = c.fetchall()
    conn.close()
    
    precios = obtener_precios()
    precio_mensual = next((p[1] for p in precios if p[0] == 30), 2000)
    
    for user_id, nombre, fecha_exp_str, servicios_str in usuarios:
        fecha_exp = datetime.strptime(fecha_exp_str, "%Y-%m-%d %H:%M:%S")
        dias_restantes = (fecha_exp - datetime.now()).days
        
        servicios_usados = json.loads(servicios_str)
        todos_servicios = ['búsqueda', 'búsqueda_ia', 'buscar_profesional', 'empleos', 'gráficos', 'resumen']
        no_usados = [s for s in todos_servicios if s not in servicios_usados]
        
        mensaje = ""
        
        if dias_restantes == 5:
            mensaje = f"""
🔔 **Hola {nombre}!**

Te escribo para recordarte que en **5 días** vence tu acceso al Bot Cofradía.

💡 **¿Por qué renovar?**

Este bot es un **servicio opcional y voluntario** que hemos creado para la comunidad. Tu suscripción no solo te da acceso a herramientas poderosas, sino que nos permite:

✅ Mantener servidores activos 24/7
✅ Pagar el servicio de IA (Gemini API)
✅ Desarrollar nuevas funcionalidades
✅ Ofrecer soporte técnico continuo
✅ Mejorar constantemente la experiencia

**Tu aporte hace posible que Cofradía siga creciendo.** 🌱

Si decides renovar, estarás invirtiendo en una herramienta que te ahorra tiempo y te mantiene conectado con la comunidad.

💳 Usa /renovar cuando estés listo. ¡Sin presiones!

Gracias por ser parte de Cofradía. 🙏
"""
        
        elif dias_restantes == 3:
            mensaje = f"""
⭐ **{nombre}, quedan 3 días!**

Quiero recordarte el **valor real** que el Bot Cofradía te ofrece:

🔍 **Búsqueda inteligente con IA** - Encuentra info en segundos
🧠 **Búsqueda semántica** - Por significado, no solo palabras
💼 **Búsqueda de empleos** - LinkedIn, Indeed, Laborum integrados
👥 **Búsqueda de profesionales** - Encuentra expertos en Cofradía
📊 **Análisis visuales** - Gráficos profesionales estilo Google
📝 **Resúmenes automáticos** - Mantente al día sin esfuerzo

**¿Cuánto vale tu tiempo?**

Si el bot te ahorra **30 minutos al día** = **15 horas al mes**
A $10.000/hora = **$150.000 de valor**
Tu inversión: Solo **${precio_mensual:,}/mes**

**💰 ROI: 7,500% de retorno**

Tu aporte permite:
• Pagar servidores ($15 USD/mes)
• Licencia de IA Gemini ($20 USD/mes)
• Almacenamiento ($10 USD/mes)
• Actualizaciones constantes

**Es voluntario, pero es valioso.** 🎯

Usa /renovar para continuar.
"""
        
        elif dias_restantes == 1:
            servicios_usados_texto = ", ".join(servicios_usados) if servicios_usados else "ninguno aún"
            no_usados_texto = ", ".join(no_usados) if no_usados else "todos"
            
            mensaje = f"""
⚠️ **{nombre}, ¡MAÑANA vence tu acceso!**

Quiero ser **totalmente transparente** contigo:

**Este bot es 100% opcional y voluntario.** No estás obligado a renovar.

**PERO...**

Si has encontrado valor en usar el bot, tu renovación hace posible que sigamos mejorándolo para TODA la comunidad Cofradía.

**Tu historial:**
✅ **Servicios usados:** {servicios_usados_texto}
⏳ **Te faltan por probar:** {no_usados_texto}

**¿Qué financia tu suscripción?**

Cada mes invertimos en:
• **$15 USD** - Servidor Render (24/7)
• **$20 USD** - API de Gemini (IA avanzada)
• **$10 USD** - Almacenamiento y bases de datos
• **Horas** - Desarrollo y soporte

**Total: ~$45 USD/mes de costos reales**

Con 23 usuarios pagando = Cubrimos costos básicos
Más usuarios = Más mejoras para todos

**Tu aporte sí importa.** 💪

**Beneficios de renovar HOY:**
✅ Sin interrupciones en el servicio
✅ Mantienes tu historial de búsquedas
✅ Acceso inmediato a nuevas funciones
✅ Apoyas el crecimiento de Cofradía

**Precio:** ${precio_mensual:,}/mes
**Valor que recibes:** Incalculable

⏰ **Renueva ahora:** /renovar

Si decides no renovar, está bien. Seguirás siendo parte de Cofradía, solo sin acceso al bot.

Pero si lo renuevas, estarás invirtiendo en:
1. **Tu productividad** personal
2. **Tu tiempo** valioso
3. **Tu comunidad** profesional

**¿Qué eliges?** La decisión es tuya. 🤝

Gracias por considerarlo. 🙏
"""
        
        if mensaje:
            try:
                await context.bot.send_message(chat_id=user_id, text=mensaje, parse_mode='Markdown')
                logger.info(f"Recordatorio enviado a {nombre} ({dias_restantes} días)")
            except Exception as e:
                logger.error(f"Error enviando recordatorio a {nombre}: {e}")

async def enviar_mensajes_engagement(context: ContextTypes.DEFAULT_TYPE):
    """Envía mensajes semanales durante periodo gratuito"""
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute("""SELECT user_id, first_name, fecha_registro, mensajes_engagement, ultimo_mensaje_engagement, servicios_usados
                 FROM suscripciones 
                 WHERE estado = 'activo' AND mensajes_engagement < 12""")
    usuarios = c.fetchall()
    
    for user_id, nombre, fecha_reg_str, num_msg, ultimo_msg_str, servicios_str in usuarios:
        fecha_reg = datetime.strptime(fecha_reg_str, "%Y-%m-%d %H:%M:%S")
        dias_desde_registro = (datetime.now() - fecha_reg).days
        
        if dias_desde_registro > 90:
            continue
        
        if ultimo_msg_str:
            ultimo_msg = datetime.strptime(ultimo_msg_str, "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - ultimo_msg).days < 7:
                continue
        
        servicios_usados = json.loads(servicios_str)
        
        mensajes_engagement = [
            f"👋 **Hola {nombre}!**\n\n¿Sabías que puedes usar /buscar_ia para encontrar conversaciones por significado?\n\nPruébalo! 🧠",
            f"💼 **{nombre}, ¿buscas empleo?**\n\nUsa /empleo o /buscar_profesional para encontrar oportunidades! 🚀",
            f"📊 **{nombre}, usa /graficos** para ver análisis visuales del grupo! 📈",
            f"⏰ **Tip:** Usa /resumen para mantenerte al día en 2 minutos! ⚡",
            f"🎯 **{nombre}:** Servicios usados: {', '.join(servicios_usados) if servicios_usados else 'Ninguno'}. Usa /ayuda! 💡"
        ]
        
        mensaje = mensajes_engagement[num_msg % len(mensajes_engagement)]
        
        try:
            await context.bot.send_message(chat_id=user_id, text=mensaje, parse_mode='Markdown')
            
            c.execute("""UPDATE suscripciones 
                         SET mensajes_engagement = ?, ultimo_mensaje_engagement = ? 
                         WHERE user_id = ?""",
                      (num_msg + 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
            conn.commit()
            
            logger.info(f"Mensaje engagement #{num_msg + 1} enviado a {nombre}")
            
        except Exception as e:
            logger.error(f"Error enviando engagement a {nombre}: {e}")
    
    conn.close()

# ==================== DECORADOR DE SUSCRIPCIÓN ====================

def requiere_suscripcion(func):
    """Decorador para verificar suscripción activa"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        
        if not verificar_suscripcion_activa(user_id):
            dias_restantes = obtener_dias_restantes(user_id)
            if dias_restantes > 0:
                await update.message.reply_text(
                    f"⏰ Tu suscripción vence en **{dias_restantes} días**.\n\nUsa /renovar para extenderla.",
                    parse_mode='Markdown'
                )
            else:
                await update.message.reply_text(
                    "❌ **Tu suscripción ha expirado.**\n\nPara seguir usando el bot, renueva con /renovar",
                    parse_mode='Markdown'
                )
            return
        
        return await func(update, context, *args, **kwargs)
    
    return wrapper

# ==================== COMANDOS BÁSICOS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    mensaje_bienvenida = f"""
👋 **¡Bienvenido {user.first_name} a Bot Cofradía Premium!**

Soy tu asistente inteligente para la comunidad.

📍 **¿DÓNDE USAR LOS COMANDOS?**

🔓 **EN EL GRUPO COFRADÍA:**
   • `/buscar [palabra]` - Buscar en historial
   • `/buscar_ia [frase]` - Búsqueda semántica IA
   • `/empleo cargo:X ubicacion:Y` - Buscar empleos
   • `/graficos` - Estadísticas visuales del grupo
   • `/resumen` - Resumen del día
   • `/estadisticas` - Números del grupo
   • `@bot [pregunta]` - Consultar a la IA
   
🔒 **EN PRIVADO (solo tú y yo):**
   • `/registrarse` - Activar tu cuenta (primera vez)
   • `/renovar` - Renovar suscripción
   • `/mi_cuenta` - Ver estado de tu cuenta
   • **Enviar comprobante** - Foto de transferencia
   
💡 **EJEMPLOS DE USO:**
   ✓ `/buscar networking`
   ✓ `/buscar_ia cómo conseguir financiamiento`
   ✓ `/empleo cargo:Gerente ubicacion:Santiago`
   ✓ `@bot ¿qué es blockchain?`
   
📖 Usa `/ayuda` para ver todos los comandos disponibles.

¡Comienza registrándote con `/registrarse` en el grupo! ✨
"""
    
    await update.message.reply_text(mensaje_bienvenida, parse_mode='Markdown')

async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto_ayuda = """
🤖 **Bot Cofradía - Guía Completa**

**🔍 Búsqueda:**
/buscar [palabra] - Búsqueda tradicional
/buscar_ia [frase] - Búsqueda semántica IA

**💼 Empleos y Profesionales:**
/empleo cargo:[...] ubicacion:[...] - Buscar empleos
/buscar_profesional [área/expertise] - Buscar profesionales

**📊 Análisis:**
/graficos - Gráficos profesionales
/estadisticas - Números del grupo
/categorias - Distribución

**📝 Resúmenes:**
/resumen - Resumen del día
/resumen_semanal - Resumen semanal

**💳 Suscripción:**
/registrarse - Activar cuenta
/renovar - Renovar suscripción
/activar [código] - Usar código
/mi_cuenta - Ver estado

**👑 Admin (solo dueño):**
/generar_codigo - Crear códigos
/precios - Configurar precios
/pagos_pendientes - Revisar pagos

**💬 IA:**
Menciona @bot [pregunta]
"""
    await update.message.reply_text(texto_ayuda, parse_mode='Markdown')

async def registrarse_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    
    if update.effective_chat.type == 'private':
        await update.message.reply_text("❌ Usa este comando en el grupo Cofradía.")
        return
    
    if verificar_suscripcion_activa(user.id):
        dias = obtener_dias_restantes(user.id)
        await update.message.reply_text(
            f"✅ Ya estás registrado. Tu suscripción vence en **{dias} días**.",
            parse_mode='Markdown'
        )
        return
    
    chat_member = await context.bot.get_chat_member(update.effective_chat.id, user.id)
    es_admin = chat_member.status in ['creator', 'administrator']
    
    registrar_usuario_suscripcion(user.id, user.first_name, user.username or "sin_username", es_admin)
    
    mensaje_grupo = f"✅ **{user.first_name}** registrado! Inicia conversación conmigo en privado (/start) para activar todas las funciones."
    
    await update.message.reply_text(mensaje_grupo, parse_mode='Markdown')
    
    try:
        mensaje_privado = f"""
👋 **¡Bienvenido {user.first_name}!**

Has activado tu cuenta en el Bot Cofradía. 🎉

**Ahora puedes:**
🔍 Buscar información con IA
👥 Buscar profesionales en la comunidad
💼 Encontrar empleos
📊 Ver análisis del grupo
📝 Recibir resúmenes diarios

Usa /ayuda para ver todos los comandos.

¡Empieza a explorar! 🚀
"""
        await context.bot.send_message(chat_id=user.id, text=mensaje_privado, parse_mode='Markdown')
    except:
        pass

async def renovar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    precios = obtener_precios()
    
    keyboard = []
    for dias, precio, nombre in precios:
        keyboard.append([InlineKeyboardButton(
            f"{nombre} ({dias} días) - ${precio:,}",
            callback_data=f"plan_{dias}"
        )])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    mensaje = f"""
💳 **RENOVACIÓN DE SUSCRIPCIÓN**

Selecciona tu plan:
"""
    
    for dias, precio, nombre in precios:
        ahorro = ""
        if dias == 180:
            precio_normal = next((p[1] for p in precios if p[0] == 30), 2000)
            ahorro = f" (Ahorras ${int((precio_normal * 6) - precio):,})"
        elif dias == 365:
            precio_normal = next((p[1] for p in precios if p[0] == 30), 2000)
            ahorro = f" (Ahorras ${int((precio_normal * 12) - precio):,})"
        
        mensaje += f"\n💎 **{nombre}** - ${precio:,}{ahorro}"
    
    await update.message.reply_text(mensaje, reply_markup=reply_markup, parse_mode='Markdown')

async def callback_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    dias = int(query.data.split('_')[1])
    precios = obtener_precios()
    precio = next((p[1] for p in precios if p[0] == dias), 0)
    nombre_plan = next((p[2] for p in precios if p[0] == dias), "Plan")
    
    mensaje = f"""
✅ **Plan seleccionado:** {nombre_plan}
💰 **Precio:** ${precio:,}
⏳ **Duración:** {dias} días

{DATOS_BANCARIOS}

Después de transferir, envíame el comprobante como **imagen**.
"""
    
    await query.edit_message_text(mensaje, parse_mode='Markdown')
    
    context.user_data['plan_seleccionado'] = dias
    context.user_data['precio'] = precio

async def recibir_comprobante(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recibe comprobante de pago y lo analiza con OCR"""
    user = update.message.from_user
    
    if 'plan_seleccionado' not in context.user_data:
        await update.message.reply_text(
            "❌ Primero selecciona un plan con /renovar",
            parse_mode='Markdown'
        )
        return
    
    dias = context.user_data['plan_seleccionado']
    precio = context.user_data['precio']
    
    msg_procesando = await update.message.reply_text("🔍 Analizando comprobante con IA...")
    
    photo = update.message.photo[-1]
    file = await context.bot.get_file(photo.file_id)
    
    image_bytes = requests.get(file.file_path).content
    
    try:
        prompt_ocr = f"""Analiza este comprobante de transferencia bancaria.

DATOS ESPERADOS:
- Monto: ${precio:,} CLP
- Cuenta: 69104312
- Banco: Santander
- Titular: Destak E.I.R.L.

FORMATO JSON:
{{
  "monto_detectado": "2000",
  "monto_correcto": true,
  "fecha_detectada": "03/02/2026",
  "fecha_valida": true,
  "cuenta_detectada": "69104312",
  "cuenta_correcta": true,
  "banco_detectado": "Banco Santander",
  "calidad_imagen": "buena",
  "legible": true,
  "observaciones": ""
}}

Responde SOLO JSON."""

        vision_model = genai.GenerativeModel('gemini-1.5-flash')
        
        from io import BytesIO
        image = PIL.Image.open(BytesIO(image_bytes))
        
        response = vision_model.generate_content([prompt_ocr, image])
        
        import re
        response_text = response.text.strip()
        response_text = re.sub(r'```json\s*|\s*```', '', response_text)
        
        try:
            datos_ocr = json.loads(response_text)
        except:
            datos_ocr = {"legible": False, "calidad_imagen": "mala"}
        
        if not datos_ocr.get("legible", False) or datos_ocr.get("calidad_imagen") == "mala":
            await msg_procesando.delete()
            await update.message.reply_text(
                "❌ **Imagen no clara**\n\nEnvía una foto más nítida. 📸",
                parse_mode='Markdown'
            )
            return
        
        analisis = "🤖 **ANÁLISIS AUTOMÁTICO**\n\n"
        
        if datos_ocr.get("monto_correcto"):
            analisis += f"✅ **Monto:** ${datos_ocr.get('monto_detectado', 'N/A')} (Correcto)\n"
        else:
            analisis += f"⚠️ **Monto:** ${datos_ocr.get('monto_detectado', 'N/A')} (Esperado: ${precio:,})\n"
        
        if datos_ocr.get("cuenta_correcta"):
            analisis += f"✅ **Cuenta:** {datos_ocr.get('cuenta_detectada', 'N/A')} (Correcta)\n"
        else:
            analisis += f"⚠️ **Cuenta:** {datos_ocr.get('cuenta_detectada', 'N/A')}\n"
        
        await msg_procesando.delete()
        await update.message.reply_text(
            f"{analisis}\n\n⏳ Tu comprobante está siendo revisado.\nRecibirás tu código pronto. 🙏",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Error en OCR: {e}")
        await msg_procesando.delete()
        analisis = "⚠️ **Revisión manual**\n\nEl administrador revisará tu comprobante."
        await update.message.reply_text(analisis, parse_mode='Markdown')
        datos_ocr = {"observaciones": f"Error: {str(e)}"}
    
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    file_id = photo.file_id
    fecha_envio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    c.execute("""INSERT INTO pagos_pendientes 
                 (user_id, first_name, dias_plan, precio, comprobante_file_id, fecha_envio, estado)
                 VALUES (?, ?, ?, ?, ?, ?, 'pendiente')""",
              (user.id, user.first_name, dias, precio, file_id, fecha_envio))
    
    pago_id = c.lastrowid
    conn.commit()
    conn.close()
    
    nombre_plan = dict([(p[0], p[2]) for p in obtener_precios()])[dias]
    
    keyboard = [
        [InlineKeyboardButton("✅ Aprobar", callback_data=f"aprobar_{pago_id}")],
        [InlineKeyboardButton("❌ Rechazar", callback_data=f"rechazar_{pago_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    caption_dueño = f"""
💳 **PAGO #{pago_id}**

👤 {user.first_name} (@{user.username or 'sin_username'})
📱 ID: `{user.id}`
💎 Plan: {nombre_plan} ({dias} días)
💰 Precio: ${precio:,}

{analisis}

¿Aprobar?
"""
    
    try:
        await context.bot.send_photo(
            chat_id=OWNER_ID,
            photo=file_id,
            caption=caption_dueño,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error notificando al dueño: {e}")

async def callback_aprobar_rechazar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja aprobación/rechazo de pagos"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != OWNER_ID:
        await query.answer("❌ Solo el dueño puede hacer esto.", show_alert=True)
        return
    
    accion, pago_id = query.data.split('_')
    pago_id = int(pago_id)
    
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
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
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✅ **¡PAGO APROBADO!**\n\nCódigo: `{codigo}`\n\nActívalo: /activar {codigo}\n\n¡Gracias! 🎉",
                parse_mode='Markdown'
            )
            
            await query.edit_message_caption(
                f"{query.message.caption}\n\n✅ **APROBADO**\nCódigo: `{codigo}`",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await query.edit_message_caption(f"❌ Error: {e}")
    
    else:
        c.execute("UPDATE pagos_pendientes SET estado = 'rechazado' WHERE id = ?", (pago_id,))
        conn.commit()
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="❌ Tu pago no pudo ser verificado. Contacta al administrador.",
                parse_mode='Markdown'
            )
            
            await query.edit_message_caption(f"{query.message.caption}\n\n❌ **RECHAZADO**", parse_mode='Markdown')
        except:
            pass
    
    conn.close()

async def activar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Activa un código de suscripción"""
    user = update.message.from_user
    
    if not context.args:
        await update.message.reply_text(
            "❌ Uso: /activar [código]\n\nEjemplo: `/activar COF-ABCD-1234-EFGH`",
            parse_mode='Markdown'
        )
        return
    
    codigo = context.args[0].upper()
    
    exito, mensaje = validar_y_usar_codigo(user.id, codigo)
    
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def mi_cuenta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra info de la cuenta"""
    user = update.message.from_user
    
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute("""SELECT fecha_registro, fecha_expiracion, estado, es_admin, servicios_usados 
                 FROM suscripciones WHERE user_id = ?""", (user.id,))
    resultado = c.fetchone()
    conn.close()
    
    if not resultado:
        await update.message.reply_text("❌ No estás registrado. Usa /registrarse en el grupo.", parse_mode='Markdown')
        return
    
    fecha_reg, fecha_exp, estado, es_admin, servicios_str = resultado
    
    fecha_exp_dt = datetime.strptime(fecha_exp, "%Y-%m-%d %H:%M:%S")
    dias_restantes = (fecha_exp_dt - datetime.now()).days
    
    servicios = json.loads(servicios_str)
    
    estado_emoji = "✅" if estado == 'activo' and dias_restantes > 0 else "❌"
    
    mensaje = f"""
👤 **MI CUENTA**

{estado_emoji} Estado: {'Activo' if estado == 'activo' and dias_restantes > 0 else 'Expirado'}
{'👑 Administrador' if es_admin else ''}

⏳ Días restantes: **{max(0, dias_restantes)}**
📅 Vence: {fecha_exp_dt.strftime('%d/%m/%Y')}

**Servicios usados:**
{', '.join(servicios) if servicios else 'Ninguno aún'}

Usa /renovar para extender.
"""
    
    await update.message.reply_text(mensaje, parse_mode='Markdown')

# ==================== COMANDOS ADMIN ====================

async def generar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera código (solo dueño)"""
    user = update.message.from_user
    
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño puede generar códigos.")
        return
    
    precios = obtener_precios()
    
    keyboard = []
    for dias, precio, nombre in precios:
        keyboard.append([InlineKeyboardButton(
            f"{nombre} ({dias} días) - ${precio:,}",
            callback_data=f"gencodigo_{dias}"
        )])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text("👑 **GENERAR CÓDIGO**\n\nSelecciona:", reply_markup=reply_markup, parse_mode='Markdown')

async def callback_generar_codigo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback generar código"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != OWNER_ID:
        return
    
    dias = int(query.data.split('_')[1])
    precios = obtener_precios()
    precio = next((p[1] for p in precios if p[0] == dias), 0)
    
    codigo = generar_codigo_activacion(dias, precio)
    
    await query.edit_message_text(
        f"✅ **CÓDIGO GENERADO**\n\n`{codigo}`\n\n📋 Duración: {dias} días\n💰 Precio: ${precio:,}\n⏰ Válido: 30 días",
        parse_mode='Markdown'
    )

async def precios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra precios (solo dueño)"""
    user = update.message.from_user
    
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño.")
        return
    
    precios = obtener_precios()
    
    mensaje = "💰 **PRECIOS**\n\n"
    for dias, precio, nombre in precios:
        mensaje += f"• {nombre} ({dias} días): ${precio:,}\n"
    
    mensaje += "\n📝 /set_precio [dias] [precio]"
    
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def set_precio_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Actualiza precio (solo dueño)"""
    user = update.message.from_user
    
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño.")
        return
    
    if len(context.args) != 2:
        await update.message.reply_text("❌ Uso: /set_precio [dias] [precio]", parse_mode='Markdown')
        return
    
    try:
        dias = int(context.args[0])
        precio = int(context.args[1])
        
        actualizar_precio(dias, precio)
        
        await update.message.reply_text(f"✅ Precio actualizado: {dias} días = ${precio:,}", parse_mode='Markdown')
    except:
        await update.message.reply_text("❌ Error.")

async def pagos_pendientes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista pagos (solo dueño)"""
    user = update.message.from_user
    
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño.")
        return
    
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute("""SELECT id, first_name, dias_plan, precio, fecha_envio, estado 
                 FROM pagos_pendientes 
                 ORDER BY fecha_envio DESC 
                 LIMIT 20""")
    pagos = c.fetchall()
    conn.close()
    
    if not pagos:
        await update.message.reply_text("✅ No hay pagos.")
        return
    
    mensaje = "💳 **PAGOS RECIENTES**\n\n"
    
    for pago_id, nombre, dias, precio, fecha, estado in pagos:
        emoji = "⏳" if estado == 'pendiente' else ("✅" if estado == 'aprobado' else "❌")
        mensaje += f"{emoji} #{pago_id} - {nombre}\n   {dias} días - ${precio:,} - {estado}\n\n"
    
    await update.message.reply_text(mensaje, parse_mode='Markdown')

# ==================== COMANDOS CON SUSCRIPCIÓN ====================

@requiere_suscripcion
async def buscar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'búsqueda')
    if not context.args:
        await update.message.reply_text("❌ Uso: /buscar [palabra]")
        return
    query = ' '.join(context.args)
    topic_id = update.message.message_thread_id if update.message.is_topic_message else None
    resultados = buscar_en_historial(query, topic_id, limit=5)
    if not resultados:
        await update.message.reply_text(f"❌ No encontré: *{query}*", parse_mode='Markdown')
        return
    respuesta = f"🔍 **Búsqueda:** {query}\n\n"
    for nombre, mensaje, fecha in resultados:
        mensaje_corto = mensaje[:100] + "..." if len(mensaje) > 100 else mensaje
        respuesta += f"👤 **{nombre}** ({fecha}):\n{mensaje_corto}\n\n"
    await update.message.reply_text(respuesta, parse_mode='Markdown')

@requiere_suscripcion
async def buscar_semantica_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'búsqueda_ia')
    if not context.args:
        await update.message.reply_text("❌ Uso: /buscar_ia [frase]")
        return
    query = ' '.join(context.args)
    topic_id = update.message.message_thread_id if update.message.is_topic_message else None
    await update.message.reply_text("🧠 Buscando con IA...")
    resultados = buscar_semantica(query, topic_id, limit=5)
    if not resultados:
        await update.message.reply_text("❌ Sin resultados", parse_mode='Markdown')
        return
    respuesta = f"🧠 **Búsqueda IA:** {query}\n\n"
    for nombre, mensaje, fecha in resultados:
        mensaje_corto = mensaje[:100] + "..." if len(mensaje) > 100 else mensaje
        respuesta += f"👤 **{nombre}** ({fecha}):\n{mensaje_corto}\n\n"
    await update.message.reply_text(respuesta, parse_mode='Markdown')

@requiere_suscripcion
async def buscar_empleo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'empleos')
    if not context.args:
        await update.message.reply_text("❌ Uso: /empleo cargo:[...] ubicacion:[...]")
        return
    texto = ' '.join(context.args)
    cargo = industria = area = ubicacion = rango_renta = None
    if 'cargo:' in texto:
        cargo = ' '.join(texto.split('cargo:')[1].split()[0:3])
    if 'industria:' in texto:
        industria = ' '.join(texto.split('industria:')[1].split()[0:2])
    if 'ubicacion:' in texto:
        ubicacion = ' '.join(texto.split('ubicacion:')[1].split()[0:2])
    if 'renta:' in texto:
        rango_renta = texto.split('renta:')[1].split()[0]
    await update.message.reply_text("🔍 Buscando empleos...")
    resultados = await buscar_empleos_web(cargo, industria, area, ubicacion, rango_renta)
    await update.message.reply_text(resultados, parse_mode='Markdown')

@requiere_suscripcion
async def buscar_profesional_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Busca profesionales en Cofradía"""
    registrar_servicio_usado(update.effective_user.id, 'buscar_profesional')
    
    if not context.args:
        await update.message.reply_text(
            "❌ **Uso:** /buscar_profesional [área]\n\n**Ejemplos:**\n• diseñador gráfico\n• contador\n• abogado laboral",
            parse_mode='Markdown'
        )
        return
    
    query = ' '.join(context.args)
    
    await update.message.reply_text("🔍 Buscando profesionales...")
    
    resultados = buscar_profesionales(query)
    
    if len(resultados) > 4000:
        partes = [resultados[i:i+4000] for i in range(0, len(resultados), 4000)]
        for parte in partes:
            await update.message.reply_text(parte, parse_mode='Markdown')
    else:
        await update.message.reply_text(resultados, parse_mode='Markdown')

@requiere_suscripcion
async def graficos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'gráficos')
    await update.message.reply_text("📊 Generando...")
    stats = obtener_estadisticas_graficos(dias=7)
    imagen_buffer = generar_grafico_visual(stats)
    await update.message.reply_photo(photo=imagen_buffer, caption="📊 **Análisis Visual**")

@requiere_suscripcion
async def resumen_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'resumen')
    await update.message.reply_text("📝 Generando...")
    resumen = generar_resumen_usuarios(dias=1)
    if not resumen:
        await update.message.reply_text("❌ No hay mensajes hoy")
        return
    await update.message.reply_text(resumen, parse_mode='Markdown')

@requiere_suscripcion
async def resumen_semanal_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'resumen')
    await update.message.reply_text("📝 Generando...")
    resumen = generar_resumen_usuarios(dias=7)
    if not resumen:
        await update.message.reply_text("❌ No hay mensajes")
        return
    await update.message.reply_text(resumen, parse_mode='Markdown')

@requiere_suscripcion
async def estadisticas_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM mensajes")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT user_id) FROM mensajes")
    usuarios = c.fetchone()[0]
    hoy = datetime.now().strftime("%Y-%m-%d")
    c.execute("SELECT COUNT(*) FROM mensajes WHERE DATE(fecha) = ?", (hoy,))
    hoy_count = c.fetchone()[0]
    conn.close()
    respuesta = f"""
📊 **ESTADÍSTICAS**

📝 Total: {total:,}
👥 Usuarios: {usuarios}
🕐 Hoy: {hoy_count}
"""
    await update.message.reply_text(respuesta, parse_mode='Markdown')

@requiere_suscripcion
async def categorias_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("""SELECT categoria, COUNT(*) as total
                 FROM mensajes
                 WHERE categoria IS NOT NULL
                 GROUP BY categoria
                 ORDER BY total DESC""")
    categorias = c.fetchall()
    conn.close()
    if not categorias:
        await update.message.reply_text("❌ No hay datos")
        return
    respuesta = "🏷️ **CATEGORÍAS**\n\n"
    total = sum([c[1] for c in categorias])
    for cat, count in categorias:
        porcentaje = (count / total) * 100
        barra = '█' * int(porcentaje / 5)
        respuesta += f"**{cat}:** {barra} {count} ({porcentaje:.1f}%)\n"
    await update.message.reply_text(respuesta, parse_mode='Markdown')

# ==================== HANDLERS AUXILIARES ====================

async def guardar_mensaje_grupo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        user = update.message.from_user
        topic_id = update.message.message_thread_id if update.message.is_topic_message else None
        guardar_mensaje(user.id, user.username or "sin_username", user.first_name or "Anónimo", update.message.text, topic_id)

async def responder_con_ia(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mensaje = update.message.text
    user_id = update.effective_user.id
    
    if not context.bot.username or f"@{context.bot.username}" not in mensaje:
        return
    
    if not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("❌ Tu suscripción expiró. Usa /renovar")
        return
    
    pregunta = mensaje.replace(f"@{context.bot.username}", "").strip()
    if not pregunta:
        await update.message.reply_text("¿En qué puedo ayudarte? 😊")
        return
    
    topic_id = update.message.message_thread_id if update.message.is_topic_message else None
    resultados = buscar_semantica(pregunta, topic_id, limit=5)
    contexto = ""
    if resultados:
        contexto = "\n\nCONTEXTO:\n"
        for nombre, msg, fecha in resultados:
            contexto += f"- {nombre}: {msg}\n"
    prompt = f"""Asistente de Cofradía de Networking. Responde amigable.
PREGUNTA: {pregunta}
{contexto}
Responde en español, máximo 3 párrafos."""
    try:
        response = model.generate_content(prompt)
        await update.message.reply_text(response.text)
    except:
        await update.message.reply_text("❌ Error. Intenta de nuevo.")

async def resumen_automatico(context: ContextTypes.DEFAULT_TYPE):
    logger.info("⏰ Ejecutando resumen automático...")
    resumen_usuarios = generar_resumen_usuarios(dias=1)
    resumen_admins = generar_resumen_admins(dias=1)
    if not resumen_usuarios:
        logger.info("No hay mensajes hoy")
        return
    
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT user_id, first_name, es_admin FROM suscripciones WHERE estado = 'activo'")
    usuarios = c.fetchall()
    conn.close()
    
    for user_id, nombre, es_admin in usuarios:
        if not verificar_suscripcion_activa(user_id):
            continue
        try:
            if es_admin:
                mensaje = f"👑 **RESUMEN DIARIO - ADMIN**\n\n{resumen_admins}"
            else:
                mensaje = f"📧 **RESUMEN DIARIO**\n\n{resumen_usuarios}"
            if len(mensaje) > 4000:
                partes = [mensaje[i:i+4000] for i in range(0, len(mensaje), 4000)]
                for parte in partes:
                    await context.bot.send_message(chat_id=user_id, text=parte, parse_mode='Markdown')
            else:
                await context.bot.send_message(chat_id=user_id, text=mensaje, parse_mode='Markdown')
            logger.info(f"Resumen enviado a {nombre}")
        except Exception as e:
            logger.error(f"Error enviando a {nombre}: {e}")

# ==================== MAIN ====================



async def post_init(application):
    """Configura los comandos del bot para que aparezcan con /"""
    from telegram import BotCommand
    
    commands = [
        BotCommand("start", "🚀 Iniciar bot"),
        BotCommand("ayuda", "📖 Ver todos los comandos"),
        BotCommand("registrarse", "✅ Activar cuenta (90 días gratis)"),
        BotCommand("buscar", "🔍 Buscar en historial del grupo"),
        BotCommand("buscar_ia", "🤖 Búsqueda semántica con IA"),
        BotCommand("empleo", "💼 Buscar ofertas de empleo"),
        BotCommand("graficos", "📊 Ver gráficos y estadísticas"),
        BotCommand("estadisticas", "📈 Ver números del grupo"),
        BotCommand("categorias", "📂 Ver distribución por categorías"),
        BotCommand("resumen", "📝 Resumen del día"),
        BotCommand("resumen_semanal", "📅 Resumen de la semana"),
        BotCommand("mi_cuenta", "👤 Ver estado de tu cuenta"),
        BotCommand("renovar", "🔄 Renovar suscripción"),
        BotCommand("activar", "🎟️ Usar código de activación"),
    ]
    
    await application.bot.set_my_commands(commands)
    logger.info("✅ Comandos del bot configurados")


async def keep_alive(context: ContextTypes.DEFAULT_TYPE):
    """Mantiene el bot activo enviando pings cada 10 minutos"""
    logger.info("💓 Keep-alive ping - bot activo")

def main():
    init_db()
    TOKEN = os.environ.get('TOKEN_BOT')
    if not TOKEN:
        logger.error("❌ TOKEN_BOT no configurado")
        return
    
    application = Application.builder().token(TOKEN).post_init(post_init).build()
    
    job_queue = application.job_queue
    job_queue.run_daily(resumen_automatico, time=time(hour=20, minute=0), name='resumen_diario')
    
    # Mantener bot activo (cada 10 minutos)
    job_queue.run_repeating(keep_alive, interval=600, first=10)
    logger.info('✅ Keep-alive activado')
    job_queue.run_daily(enviar_recordatorios, time=time(hour=10, minute=0), name='recordatorios')
    job_queue.run_daily(enviar_mensajes_engagement, time=time(hour=15, minute=0), name='engagement')
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ayuda", ayuda))
    application.add_handler(CommandHandler("registrarse", registrarse_comando))
    application.add_handler(CommandHandler("renovar", renovar_comando))
    application.add_handler(CommandHandler("activar", activar_codigo_comando))
    application.add_handler(CommandHandler("mi_cuenta", mi_cuenta_comando))
    
    application.add_handler(CommandHandler("buscar", buscar_comando))
    application.add_handler(CommandHandler("buscar_ia", buscar_semantica_comando))
    application.add_handler(CommandHandler("empleo", buscar_empleo_comando))
    application.add_handler(CommandHandler("buscar_profesional", buscar_profesional_comando))
    application.add_handler(CommandHandler("graficos", graficos_comando))
    application.add_handler(CommandHandler("resumen", resumen_comando))
    application.add_handler(CommandHandler("resumen_semanal", resumen_semanal_comando))
    application.add_handler(CommandHandler("estadisticas", estadisticas_comando))
    application.add_handler(CommandHandler("categorias", categorias_comando))
    
    application.add_handler(CommandHandler("generar_codigo", generar_codigo_comando))
    application.add_handler(CommandHandler("precios", precios_comando))
    application.add_handler(CommandHandler("set_precio", set_precio_comando))
    application.add_handler(CommandHandler("pagos_pendientes", pagos_pendientes_comando))
    
    application.add_handler(CallbackQueryHandler(callback_plan, pattern='^plan_'))
    application.add_handler(CallbackQueryHandler(callback_generar_codigo, pattern='^gencodigo_'))
    application.add_handler(CallbackQueryHandler(callback_aprobar_rechazar, pattern='^(aprobar|rechazar)_'))
    
    application.add_handler(MessageHandler(filters.PHOTO, recibir_comprobante))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'@'), responder_con_ia))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, guardar_mensaje_grupo))
    
    logger.info("🚀 Bot Cofradía PRO iniciado!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
