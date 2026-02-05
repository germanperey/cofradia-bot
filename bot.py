import os
import re
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes, CallbackQueryHandler
import google.generativeai as genai
import requests
import PIL.Image
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from io import BytesIO
import base64
import sqlite3
from datetime import datetime, timedelta, time
from collections import Counter
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import secrets
import string


# Función helper para formato CLP
def formato_clp(monto):
    """Formatea montos en pesos chilenos con separador de miles usando punto"""
    return f"${monto:,}".replace(",", ".")

# Configuración de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

# ID del dueño del bot
OWNER_ID = int(os.environ.get('OWNER_TELEGRAM_ID', '0'))

# Datos bancarios
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

def init_db():
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
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
                  estado TEXT DEFAULT 'pendiente')''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS precios_planes
                 (dias INTEGER PRIMARY KEY, precio INTEGER, nombre_plan TEXT)''')
    
    c.execute("SELECT COUNT(*) FROM precios_planes")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO precios_planes VALUES (30, 2000, 'Mensual')")
        c.execute("INSERT INTO precios_planes VALUES (180, 10500, 'Semestral')")
        c.execute("INSERT INTO precios_planes VALUES (365, 20000, 'Anual')")
    
    conn.commit()
    conn.close()

# ==================== FUNCIONES DE SUSCRIPCIÓN ====================

def registrar_usuario_suscripcion(user_id, first_name, username, es_admin=False, dias_gratis=90):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    fecha_registro = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fecha_expiracion = (datetime.now() + timedelta(days=dias_gratis)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT OR REPLACE INTO suscripciones 
                 (user_id, first_name, username, es_admin, fecha_registro, fecha_expiracion, estado, mensajes_engagement, ultimo_mensaje_engagement, servicios_usados) 
                 VALUES (?, ?, ?, ?, ?, ?, 'activo', 0, ?, '[]')""",
              (user_id, first_name, username, 1 if es_admin else 0, fecha_registro, fecha_expiracion, fecha_registro))
    conn.commit()
    conn.close()

def verificar_suscripcion_activa(user_id):
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
    caracteres = string.ascii_uppercase + string.digits
    codigo = ''.join(secrets.choice(caracteres) for _ in range(12))
    codigo = f"COF-{codigo[:4]}-{codigo[4:8]}-{codigo[8:]}"
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    fecha_creacion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fecha_expiracion = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT INTO codigos_activacion VALUES (?, ?, ?, ?, ?, 0, NULL, NULL)""",
              (codigo, dias, precio, fecha_creacion, fecha_expiracion))
    conn.commit()
    conn.close()
    return codigo

def validar_y_usar_codigo(user_id, codigo):
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
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT dias, precio, nombre_plan FROM precios_planes ORDER BY dias")
    precios = c.fetchall()
    conn.close()
    return precios

def actualizar_precio(dias, nuevo_precio):
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
    c.execute("""SELECT DATE(fecha) as dia, COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY dia ORDER BY dia""", (fecha_inicio,))
    por_dia = c.fetchall()
    c.execute("""SELECT first_name, COUNT(*) as total FROM mensajes WHERE fecha >= ? GROUP BY user_id, first_name ORDER BY total DESC LIMIT 10""", (fecha_inicio,))
    usuarios_activos = c.fetchall()
    c.execute("""SELECT categoria, COUNT(*) FROM mensajes WHERE fecha >= ? AND categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC""", (fecha_inicio,))
    por_categoria = c.fetchall()
    c.execute("""SELECT CAST(strftime('%H', fecha) AS INTEGER) as hora, COUNT(*) FROM mensajes WHERE fecha >= ? GROUP BY hora ORDER BY hora""", (fecha_inicio,))
    por_hora = c.fetchall()
    conn.close()
    return {'por_dia': por_dia, 'usuarios_activos': usuarios_activos, 'por_categoria': por_categoria, 'por_hora': por_hora}

def generar_grafico_visual(stats):
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('📊 ANÁLISIS VISUAL - COFRADÍA', fontsize=16, fontweight='bold', y=0.98)
    
    if stats['por_dia']:
        dias = [d[0][-5:] for d in stats['por_dia']]
        valores = [d[1] for d in stats['por_dia']]
        ax1.fill_between(range(len(dias)), valores, alpha=0.3, color='#2E86AB')
        ax1.plot(range(len(dias)), valores, marker='o', linewidth=2, color='#2E86AB', markersize=8)
        ax1.set_title('📈 Mensajes por Día', fontsize=14, fontweight='bold', pad=15)
        ax1.set_xticks(range(len(dias)))
        ax1.set_xticklabels(dias, rotation=45)
        ax1.grid(True, alpha=0.3)
    
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

def buscar_archivo_excel_drive():
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        import io
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDS')
        if not creds_json:
            logger.error("GOOGLE_DRIVE_CREDS no configurado")
            return None
        scope = ['https://www.googleapis.com/auth/drive.readonly']
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        access_token = creds.get_access_token().access_token
        headers = {'Authorization': f'Bearer {access_token}'}
        search_url = "https://www.googleapis.com/drive/v3/files"
        params_carpeta = {'q': "name='INBESTU' and mimeType='application/vnd.google-apps.folder'", 'fields': 'files(id, name)'}
        response_carpeta = requests.get(search_url, headers=headers, params=params_carpeta)
        if response_carpeta.status_code != 200:
            return None
        carpetas = response_carpeta.json().get('files', [])
        if not carpetas:
            return None
        carpeta_id = carpetas[0]['id']
        params_archivos = {'q': f"name contains 'BD Grupo Laboral' and '{carpeta_id}' in parents and trashed=false", 'fields': 'files(id, name)', 'orderBy': 'name desc'}
        response_archivos = requests.get(search_url, headers=headers, params=params_archivos)
        if response_archivos.status_code != 200:
            return None
        archivos = response_archivos.json().get('files', [])
        if not archivos:
            return None
        file_id = archivos[0]['id']
        download_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        response_download = requests.get(download_url, headers=headers)
        if response_download.status_code == 200:
            return io.BytesIO(response_download.content)
        return None
    except Exception as e:
        logger.error(f"Error buscando archivo en Drive: {e}")
        return None

def buscar_profesionales(query):
    try:
        archivo = buscar_archivo_excel_drive()
        if not archivo:
            return "❌ No se pudo acceder a la base de datos de profesionales.\n\nContacta al administrador."
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
            if nombre == 'N/A' or nombre == 'nan' or not nombre:
                continue
            profesionales_lista.append({'id': idx + 1, 'nombre': nombre, 'profesion': profesion, 'expertise': expertise, 'email': email, 'telefono': telefono, 'estado': estado, 'trabajos': trabajos})
        if not profesionales_lista:
            return "❌ No se encontraron profesionales."
        profesionales_texto = ""
        for prof in profesionales_lista:
            profesionales_texto += f"\nID: {prof['id']}\nNombre: {prof['nombre']}\nProfesión: {prof['profesion']}\nExpertise: {prof['expertise']}\nEstado: {prof['estado']}\nEmail: {prof['email']}\nTeléfono: {prof['telefono']}\n---\n"
        prompt = f"""Busca profesionales para: "{query}"
BASE DE DATOS ({len(profesionales_lista)} profesionales):
{profesionales_texto[:12000]}
Lista máximo 10 profesionales relevantes con formato:
**[Número]. [Nombre]**
🎯 Área: [profesión]
📧 Email: [email]
📱 Teléfono: [teléfono]
⭐ Relevancia: [justificación]
---
Al final: "💬 Contacta directamente a los profesionales."
"""
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        logger.error(f"Error buscando profesionales: {e}")
        return f"❌ Error: {str(e)}"

def generar_resumen_usuarios(dias=1):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    fecha_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
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
    prompt = f"""Resumen profesional:
{contexto[:6000]}
📊 RESUMEN {'DIARIO' if dias == 1 else 'SEMANAL'} - {datetime.now().strftime('%d/%m/%Y')}
**📌 Temas** (4-5 bullets)
**💡 Insights** (3-4 bullets)
**🎯 Destacados**
Total: {len(mensajes)} mensajes. Máximo 350 palabras."""
    try:
        response = model.generate_content(prompt)
        resumen = response.text
        c.execute("INSERT INTO resumenes (fecha, tipo, resumen, mensajes_count) VALUES (?, ?, ?, ?)", 
                  (datetime.now().strftime("%Y-%m-%d"), 'usuario', resumen, len(mensajes)))
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
    seccion_admin = "\n\n" + "="*50 + "\n👑 **SECCIÓN ADMIN**\n" + "="*50 + "\n\n"
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
        seccion_admin += f"{user['nivel']} **{user['nombre']}**: {user['total_mensajes']} msgs\n"
    return resumen_base + seccion_admin

# ==================== DECORADOR DE SUSCRIPCIÓN ====================

def requiere_suscripcion(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        if not verificar_suscripcion_activa(user_id):
            dias_restantes = obtener_dias_restantes(user_id)
            if dias_restantes > 0:
                await update.message.reply_text(f"⏰ Tu suscripción vence en **{dias_restantes} días**.\n\nUsa /renovar", parse_mode='Markdown')
            else:
                await update.message.reply_text("❌ **Tu suscripción ha expirado.**\n\nRenueva con /renovar", parse_mode='Markdown')
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

# ==================== COMANDOS BÁSICOS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if verificar_suscripcion_activa(user.id):
        dias = obtener_dias_restantes(user.id)
        await update.message.reply_text(
            f"👋 **¡Hola de nuevo, {user.first_name}!**\n\n"
            f"✅ Tu suscripción está activa ({dias} días restantes)\n\n"
            f"📋 Usa /ayuda para ver comandos\n"
            f"💬 Mencióname: @Cofradia_Premium_Bot ¿pregunta?",
            parse_mode='Markdown')
        return
    mensaje = f"""
🎉 **¡Bienvenido/a {user.first_name} al Bot Cofradía Premium!**

━━━━━━━━━━━━━━━━━━━━
📌 **¿CÓMO EMPEZAR?**
━━━━━━━━━━━━━━━━━━━━

**PASO 1️⃣** → Ve al grupo Cofradía
**PASO 2️⃣** → Escribe: /registrarse
**PASO 3️⃣** → ¡Listo! Ya puedes usar el bot

━━━━━━━━━━━━━━━━━━━━
🛠️ **¿QUÉ PUEDO HACER?**
━━━━━━━━━━━━━━━━━━━━

🔍 Buscar información → /buscar o /buscar_ia
👥 Encontrar profesionales → /buscar_profesional
💼 Buscar empleos → /empleo
📊 Ver estadísticas → /graficos
📝 Resúmenes diarios → /resumen
🤖 Preguntarme → @Cofradia_Premium_Bot + pregunta

Escribe /ayuda para todos los comandos.
🚀 **¡Regístrate en el grupo para comenzar!**
"""
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = """
🤖 **BOT COFRADÍA - GUÍA**

🔍 **BÚSQUEDA**
/buscar [palabra] - Búsqueda
/buscar_ia [frase] - Búsqueda IA

💼 **EMPLEOS/PROFESIONALES**
/empleo cargo:[X] - Buscar empleos
/buscar_profesional [área] - Buscar expertos

📊 **ANÁLISIS**
/graficos - Gráficos
/estadisticas - Números
/categorias - Distribución
/top_usuarios - Ranking
/mi_perfil - Tu perfil

📝 **RESÚMENES**
/resumen - Resumen del día
/resumen_semanal - 7 días
/resumen_mes - Mensual
/resumen_usuario @nombre - Usuario

💳 **SUSCRIPCIÓN**
/registrarse - Activar cuenta
/renovar - Renovar plan
/activar [código] - Usar código
/mi_cuenta - Ver estado

💬 **IA**: @Cofradia_Premium_Bot + pregunta
"""
    await update.message.reply_text(texto, parse_mode='Markdown')

async def registrarse_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if update.effective_chat.type == 'private':
        await update.message.reply_text("❌ Usa este comando en el grupo Cofradía", parse_mode='Markdown')
        return
    if verificar_suscripcion_activa(user.id):
        dias = obtener_dias_restantes(user.id)
        await update.message.reply_text(f"✅ Ya estás registrado. {dias} días restantes.\n\nUsa /ayuda", parse_mode='Markdown')
        return
    chat_member = await context.bot.get_chat_member(update.effective_chat.id, user.id)
    es_admin = chat_member.status in ['creator', 'administrator']
    registrar_usuario_suscripcion(user.id, user.first_name, user.username or "sin_username", es_admin)
    await update.message.reply_text(f"✅ **¡{user.first_name} registrado!**\n\n🚀 Ya puedes usar el bot.\n📱 Inicia chat privado conmigo.\n💡 Usa /ayuda", parse_mode='Markdown')
    try:
        await context.bot.send_message(chat_id=user.id, text=f"🎉 **¡Bienvenido/a {user.first_name}!**\n\nTu cuenta está activa.\n\nUsa /ayuda para ver comandos.", parse_mode='Markdown')
    except:
        pass

async def renovar_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    precios = obtener_precios()
    keyboard = [[InlineKeyboardButton(f"{nombre} ({dias}d) - {formato_clp(precio)}", callback_data=f"plan_{dias}")] for dias, precio, nombre in precios]
    reply_markup = InlineKeyboardMarkup(keyboard)
    mensaje = "💳 **RENOVAR SUSCRIPCIÓN**\n\nSelecciona tu plan:"
    for dias, precio, nombre in precios:
        mensaje += f"\n💎 {nombre} - {formato_clp(precio)}"
    await update.message.reply_text(mensaje, reply_markup=reply_markup, parse_mode='Markdown')

async def callback_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    dias = int(query.data.split('_')[1])
    precios = obtener_precios()
    precio = next((p[1] for p in precios if p[0] == dias), 0)
    nombre_plan = next((p[2] for p in precios if p[0] == dias), "Plan")
    mensaje = f"✅ **Plan:** {nombre_plan}\n💰 **Precio:** {formato_clp(precio)}\n⏳ **Duración:** {dias} días\n\n{DATOS_BANCARIOS}\n\nEnvía el comprobante como **imagen**."
    await query.edit_message_text(mensaje, parse_mode='Markdown')
    context.user_data['plan_seleccionado'] = dias
    context.user_data['precio'] = precio

async def recibir_comprobante(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if 'plan_seleccionado' not in context.user_data:
        await update.message.reply_text("❌ Primero selecciona un plan con /renovar")
        return
    dias = context.user_data['plan_seleccionado']
    precio = context.user_data['precio']
    msg = await update.message.reply_text("🔍 Analizando comprobante...")
    photo = update.message.photo[-1]
    file = await context.bot.get_file(photo.file_id)
    image_bytes = requests.get(file.file_path).content
    try:
        vision_model = genai.GenerativeModel('gemini-1.5-flash')
        image = PIL.Image.open(BytesIO(image_bytes))
        prompt_ocr = f"Analiza este comprobante. Monto esperado: ${precio:,}. Cuenta: 69104312. Responde JSON: {{\"monto_correcto\": true/false, \"legible\": true/false}}"
        response = vision_model.generate_content([prompt_ocr, image])
        response_text = re.sub(r'```json\s*|\s*```', '', response.text.strip())
        try:
            datos_ocr = json.loads(response_text)
        except:
            datos_ocr = {"legible": True}
        await msg.delete()
        await update.message.reply_text("🤖 **Comprobante recibido**\n\n⏳ En revisión.", parse_mode='Markdown')
    except Exception as e:
        await msg.delete()
        await update.message.reply_text("⚠️ El administrador revisará tu comprobante.", parse_mode='Markdown')
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("INSERT INTO pagos_pendientes (user_id, first_name, dias_plan, precio, comprobante_file_id, fecha_envio, estado) VALUES (?, ?, ?, ?, ?, ?, 'pendiente')",
              (user.id, user.first_name, dias, precio, photo.file_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    pago_id = c.lastrowid
    conn.commit()
    conn.close()
    nombre_plan = dict([(p[0], p[2]) for p in obtener_precios()])[dias]
    keyboard = [[InlineKeyboardButton("✅ Aprobar", callback_data=f"aprobar_{pago_id}")], [InlineKeyboardButton("❌ Rechazar", callback_data=f"rechazar_{pago_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await context.bot.send_photo(chat_id=OWNER_ID, photo=photo.file_id, caption=f"💳 **PAGO #{pago_id}**\n\n👤 {user.first_name}\n💎 {nombre_plan} ({dias}d)\n💰 {formato_clp(precio)}", reply_markup=reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error notificando: {e}")

async def callback_aprobar_rechazar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != OWNER_ID:
        await query.answer("❌ Solo el dueño", show_alert=True)
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
            await context.bot.send_message(chat_id=user_id, text=f"✅ **¡PAGO APROBADO!**\n\nCódigo: `{codigo}`\n\nActívalo: /activar {codigo}", parse_mode='Markdown')
            await query.edit_message_caption(f"{query.message.caption}\n\n✅ APROBADO\nCódigo: `{codigo}`", parse_mode='Markdown')
        except Exception as e:
            await query.edit_message_caption(f"❌ Error: {e}")
    else:
        c.execute("UPDATE pagos_pendientes SET estado = 'rechazado' WHERE id = ?", (pago_id,))
        conn.commit()
        try:
            await context.bot.send_message(chat_id=user_id, text="❌ Pago no verificado. Contacta al administrador.")
            await query.edit_message_caption(f"{query.message.caption}\n\n❌ RECHAZADO", parse_mode='Markdown')
        except:
            pass
    conn.close()

async def activar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if not context.args:
        await update.message.reply_text("❌ Uso: /activar [código]\nEjemplo: `/activar COF-ABCD-1234-EFGH`", parse_mode='Markdown')
        return
    exito, mensaje = validar_y_usar_codigo(user.id, context.args[0].upper())
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def mi_cuenta_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT fecha_registro, fecha_expiracion, estado, es_admin, servicios_usados FROM suscripciones WHERE user_id = ?", (user.id,))
    resultado = c.fetchone()
    conn.close()
    if not resultado:
        await update.message.reply_text("❌ No estás registrado. Usa /registrarse en el grupo.")
        return
    fecha_reg, fecha_exp, estado, es_admin, servicios_str = resultado
    fecha_exp_dt = datetime.strptime(fecha_exp, "%Y-%m-%d %H:%M:%S")
    dias_restantes = (fecha_exp_dt - datetime.now()).days
    servicios = json.loads(servicios_str)
    estado_emoji = "✅" if estado == 'activo' and dias_restantes > 0 else "❌"
    mensaje = f"👤 **MI CUENTA**\n\n{estado_emoji} Estado: {'Activo' if estado == 'activo' and dias_restantes > 0 else 'Expirado'}\n{'👑 Admin' if es_admin else ''}\n\n⏳ Días restantes: **{max(0, dias_restantes)}**\n📅 Vence: {fecha_exp_dt.strftime('%d/%m/%Y')}\n\n**Servicios usados:** {', '.join(servicios) if servicios else 'Ninguno'}\n\nUsa /renovar para extender."
    await update.message.reply_text(mensaje, parse_mode='Markdown')

# ==================== COMANDOS ADMIN ====================

async def generar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño")
        return
    precios = obtener_precios()
    keyboard = [[InlineKeyboardButton(f"{nombre} ({dias}d) - {formato_clp(precio)}", callback_data=f"gencodigo_{dias}")] for dias, precio, nombre in precios]
    await update.message.reply_text("👑 **GENERAR CÓDIGO**\n\nSelecciona:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

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

async def precios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño")
        return
    precios = obtener_precios()
    mensaje = "💰 **PRECIOS**\n\n"
    for dias, precio, nombre in precios:
        mensaje += f"• {nombre} ({dias}d): {formato_clp(precio)}\n"
    mensaje += "\n📝 /set_precio [dias] [precio]"
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def set_precio_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño")
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
        await update.message.reply_text("❌ Solo el dueño")
        return
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT id, first_name, dias_plan, precio, fecha_envio, estado FROM pagos_pendientes ORDER BY fecha_envio DESC LIMIT 20")
    pagos = c.fetchall()
    conn.close()
    if not pagos:
        await update.message.reply_text("✅ No hay pagos")
        return
    mensaje = "💳 **PAGOS RECIENTES**\n\n"
    for pago_id, nombre, dias, precio, fecha, estado in pagos:
        emoji = "⏳" if estado == 'pendiente' else ("✅" if estado == 'aprobado' else "❌")
        mensaje += f"{emoji} #{pago_id} {nombre} - {dias}d - {formato_clp(precio)} - {estado}\n"
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
        msg_corto = mensaje[:100] + "..." if len(mensaje) > 100 else mensaje
        respuesta += f"👤 **{nombre}** ({fecha}):\n{msg_corto}\n\n"
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
        await update.message.reply_text("❌ Sin resultados")
        return
    respuesta = f"🧠 **Búsqueda IA:** {query}\n\n"
    for nombre, mensaje, fecha in resultados:
        msg_corto = mensaje[:100] + "..." if len(mensaje) > 100 else mensaje
        respuesta += f"👤 **{nombre}** ({fecha}):\n{msg_corto}\n\n"
    await update.message.reply_text(respuesta, parse_mode='Markdown')

@requiere_suscripcion
async def buscar_empleo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'empleos')
    if not context.args:
        await update.message.reply_text("❌ Uso: /empleo cargo:[...] ubicacion:[...]")
        return
    texto = ' '.join(context.args)
    cargo = industria = ubicacion = rango_renta = None
    if 'cargo:' in texto: cargo = ' '.join(texto.split('cargo:')[1].split()[0:3])
    if 'industria:' in texto: industria = ' '.join(texto.split('industria:')[1].split()[0:2])
    if 'ubicacion:' in texto: ubicacion = ' '.join(texto.split('ubicacion:')[1].split()[0:2])
    if 'renta:' in texto: rango_renta = texto.split('renta:')[1].split()[0]
    await update.message.reply_text("🔍 Buscando empleos...")
    resultados = await buscar_empleos_web(cargo, industria, None, ubicacion, rango_renta)
    await update.message.reply_text(resultados, parse_mode='Markdown')

@requiere_suscripcion
async def buscar_profesional_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'buscar_profesional')
    if not context.args:
        await update.message.reply_text("❌ **Uso:** /buscar_profesional [área]\n\nEjemplos: contador, abogado, diseñador", parse_mode='Markdown')
        return
    query = ' '.join(context.args)
    await update.message.reply_text("🔍 Buscando profesionales...")
    resultados = buscar_profesionales(query)
    if len(resultados) > 4000:
        for i in range(0, len(resultados), 4000):
            await update.message.reply_text(resultados[i:i+4000], parse_mode='Markdown')
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
async def resumen_mes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'resumen_mes')
    await update.message.reply_text("📊 Generando resumen mensual...")
    resumen = generar_resumen_usuarios(dias=30)
    if not resumen:
        await update.message.reply_text("📭 Aún no hay suficientes datos del mes")
        return
    if len(resumen) > 4000:
        for i in range(0, len(resumen), 4000):
            await update.message.reply_text(resumen[i:i+4000])
    else:
        await update.message.reply_text(resumen)

@requiere_suscripcion
async def resumen_semestre_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'resumen_semestre')
    await update.message.reply_text("📊 Generando resumen semestral...")
    resumen = generar_resumen_usuarios(dias=180)
    if not resumen:
        await update.message.reply_text("📭 Aún no hay suficientes datos")
        return
    if len(resumen) > 4000:
        for i in range(0, len(resumen), 4000):
            await update.message.reply_text(resumen[i:i+4000])
    else:
        await update.message.reply_text(resumen)

@requiere_suscripcion
async def resumen_usuario_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Uso: /resumen_usuario @nombre")
        return
    username = context.args[0].replace('@', '').lower()
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
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
    c.execute("SELECT COUNT(DISTINCT DATE(fecha)) FROM mensajes WHERE user_id = ?", (user_id,))
    dias_activos = c.fetchone()[0]
    conn.close()
    primera_fecha = datetime.strptime(primera, "%Y-%m-%d %H:%M:%S")
    ultima_fecha = datetime.strptime(ultima, "%Y-%m-%d %H:%M:%S")
    promedio = total / max(dias_activos, 1)
    if total >= 100: nivel = "🌟 LÍDER"
    elif total >= 50: nivel = "⭐ MUY ACTIVO"
    elif total >= 20: nivel = "✨ ACTIVO"
    elif total >= 5: nivel = "👤 PARTICIPANTE"
    else: nivel = "💤 OCASIONAL"
    respuesta = f"👤 **PERFIL DE {nombre.upper()}**\n\n🏷️ **Nivel:** {nivel}\n📊 Mensajes: **{total}**\n📅 Días activos: **{dias_activos}**\n📈 Promedio: **{promedio:.1f}** msgs/día\n\n🏷️ **CATEGORÍAS:**\n"
    if categorias_top:
        for cat, count in categorias_top:
            respuesta += f"• {cat}: {count} msgs\n"
    else:
        respuesta += "Sin categorizar\n"
    await update.message.reply_text(respuesta)
    registrar_servicio_usado(update.effective_user.id, 'resumen_usuario')

@requiere_suscripcion
async def top_usuarios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT first_name, COUNT(*) as total, COUNT(DISTINCT DATE(fecha)) as dias FROM mensajes GROUP BY user_id, first_name ORDER BY total DESC LIMIT 15")
    top_users = c.fetchall()
    conn.close()
    if not top_users:
        await update.message.reply_text("📭 Aún no hay datos")
        return
    respuesta = "🏆 **TOP USUARIOS**\n\n"
    medallas = ["🥇", "🥈", "🥉"]
    for i, (nombre, total, dias) in enumerate(top_users, 1):
        emoji = medallas[i-1] if i <= 3 else f"**{i}.**"
        promedio = total / max(dias, 1)
        respuesta += f"{emoji} **{nombre}**: {total} msgs ({promedio:.1f}/día)\n"
    respuesta += "\n💡 ¡Sigue participando!"
    await update.message.reply_text(respuesta)
    registrar_servicio_usado(update.effective_user.id, 'top_usuarios')

@requiere_suscripcion
async def mi_perfil_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM mensajes WHERE user_id = ?", (user.id,))
    resultado = c.fetchone()
    if not resultado or resultado[0] == 0:
        conn.close()
        await update.message.reply_text("📭 Aún no tienes actividad registrada")
        return
    total, primera, ultima = resultado
    c.execute("SELECT categoria, COUNT(*) FROM mensajes WHERE user_id = ? AND categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC", (user.id,))
    categorias = c.fetchall()
    c.execute("SELECT COUNT(*) + 1 FROM (SELECT user_id, COUNT(*) as total FROM mensajes GROUP BY user_id HAVING total > ?)", (total,))
    posicion = c.fetchone()[0]
    conn.close()
    primera_fecha = datetime.strptime(primera, "%Y-%m-%d %H:%M:%S")
    dias_activo = (datetime.now() - primera_fecha).days + 1
    if total >= 100: motivacion = "🌟 ¡Eres un líder destacado!"
    elif total >= 50: motivacion = "⭐ ¡Excelente participación!"
    elif total >= 20: motivacion = "✨ ¡Sigue así!"
    else: motivacion = "💪 ¡Participa más!"
    respuesta = f"👤 **TU PERFIL - {user.first_name}**\n\n📊 Mensajes: **{total}**\n🏆 Ranking: **#{posicion}**\n📅 Miembro desde: {primera_fecha.strftime('%d/%m/%Y')}\n📈 Promedio: **{total/max(dias_activo,1):.1f}** msgs/día\n\n🏷️ **TUS CATEGORÍAS:**\n"
    if categorias:
        for cat, count in categorias[:3]:
            respuesta += f"• {cat}: {count} msgs\n"
    else:
        respuesta += "Sin categorizar\n"
    respuesta += f"\n{motivacion}"
    await update.message.reply_text(respuesta)
    registrar_servicio_usado(user.id, 'mi_perfil')

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
    respuesta = f"📊 **ESTADÍSTICAS**\n\n📝 Total: {total:,}\n👥 Usuarios: {usuarios}\n🕐 Hoy: {hoy_count}"
    await update.message.reply_text(respuesta, parse_mode='Markdown')

@requiere_suscripcion
async def categorias_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT categoria, COUNT(*) FROM mensajes WHERE categoria IS NOT NULL GROUP BY categoria ORDER BY COUNT(*) DESC")
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

# ==================== HANDLER DE MENCIONES ====================

async def responder_mencion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mensaje = update.message.text
    user_id = update.effective_user.id
    bot_username = context.bot.username
    menciones_validas = [f"@{bot_username}".lower(), "@bot", "@cofradia_premium_bot"]
    tiene_mencion = any(m in mensaje.lower() for m in menciones_validas)
    if not tiene_mencion:
        return
    if not verificar_suscripcion_activa(user_id):
        await update.message.reply_text("❌ Necesitas suscripción activa.\nUsa /registrarse en el grupo.")
        return
    pregunta = re.sub(r'@\w+', '', mensaje).strip()
    if not pregunta:
        await update.message.reply_text(f"💡 Mencióname con tu pregunta:\n@{bot_username} ¿Qué es networking?", parse_mode='Markdown')
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
        prompt = f"Asistente de Cofradía de Networking. Responde amigable y profesional.\nPREGUNTA: {pregunta}\n{contexto}\nMáximo 3 párrafos. Español."
        response = model.generate_content(prompt)
        respuesta = response.text
        await msg.delete()
        if len(respuesta) > 4000:
            for i in range(0, len(respuesta), 4000):
                await update.message.reply_text(respuesta[i:i+4000])
        else:
            await update.message.reply_text(respuesta)
        registrar_servicio_usado(user_id, 'ia_mencion')
    except Exception as e:
        logger.error(f"Error en mencion: {e}")
        await msg.delete()
        await update.message.reply_text("❌ Error. Intenta de nuevo.")

# ==================== HANDLERS AUXILIARES ====================

async def guardar_mensaje_grupo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        user = update.message.from_user
        topic_id = update.message.message_thread_id if update.message.is_topic_message else None
        guardar_mensaje(user.id, user.username or "sin_username", user.first_name or "Anónimo", update.message.text, topic_id)

async def resumen_automatico(context: ContextTypes.DEFAULT_TYPE):
    logger.info("⏰ Ejecutando resumen automático...")
    resumen_usuarios = generar_resumen_usuarios(dias=1)
    resumen_admins = generar_resumen_admins(dias=1)
    if not resumen_usuarios:
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
            mensaje = f"👑 **RESUMEN DIARIO - ADMIN**\n\n{resumen_admins}" if es_admin else f"📧 **RESUMEN DIARIO**\n\n{resumen_usuarios}"
            if len(mensaje) > 4000:
                for i in range(0, len(mensaje), 4000):
                    await context.bot.send_message(chat_id=user_id, text=mensaje[i:i+4000], parse_mode='Markdown')
            else:
                await context.bot.send_message(chat_id=user_id, text=mensaje, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error enviando a {nombre}: {e}")

async def enviar_recordatorios(context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT user_id, first_name, fecha_expiracion FROM suscripciones WHERE estado = 'activo'")
    usuarios = c.fetchall()
    conn.close()
    for user_id, nombre, fecha_exp_str in usuarios:
        fecha_exp = datetime.strptime(fecha_exp_str, "%Y-%m-%d %H:%M:%S")
        dias_restantes = (fecha_exp - datetime.now()).days
        mensaje = ""
        if dias_restantes == 5:
            mensaje = f"🔔 **Hola {nombre}!**\n\nTu suscripción vence en **5 días**.\n\n💳 /renovar cuando estés listo."
        elif dias_restantes == 3:
            mensaje = f"⭐ **{nombre}**, quedan **3 días**!\n\nRenueva con /renovar"
        elif dias_restantes == 1:
            mensaje = f"⚠️ **{nombre}**, ¡MAÑANA vence tu acceso!\n\n⏰ /renovar ahora"
        if mensaje:
            try:
                await context.bot.send_message(chat_id=user_id, text=mensaje, parse_mode='Markdown')
            except:
                pass

async def enviar_mensajes_engagement(context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT user_id, first_name, mensajes_engagement, ultimo_mensaje_engagement FROM suscripciones WHERE estado = 'activo' AND mensajes_engagement < 12")
    usuarios = c.fetchall()
    for user_id, nombre, num_msg, ultimo_msg_str in usuarios:
        if ultimo_msg_str:
            ultimo_msg = datetime.strptime(ultimo_msg_str, "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - ultimo_msg).days < 7:
                continue
        mensajes = [f"👋 **Hola {nombre}!** Prueba /buscar_ia 🧠", f"💼 **{nombre}**, usa /empleo 🚀", f"📊 **{nombre}**, usa /graficos 📈"]
        try:
            await context.bot.send_message(chat_id=user_id, text=mensajes[num_msg % len(mensajes)], parse_mode='Markdown')
            c.execute("UPDATE suscripciones SET mensajes_engagement = ?, ultimo_mensaje_engagement = ? WHERE user_id = ?", (num_msg + 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
            conn.commit()
        except:
            pass
    conn.close()

async def enviar_recordatorios(context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT user_id, first_name, fecha_expiracion FROM suscripciones WHERE estado = 'activo'")
    usuarios = c.fetchall()
    conn.close()
    for user_id, nombre, fecha_exp_str in usuarios:
        fecha_exp = datetime.strptime(fecha_exp_str, "%Y-%m-%d %H:%M:%S")
        dias_restantes = (fecha_exp - datetime.now()).days
        mensaje = ""
        if dias_restantes == 5:
            mensaje = f"🔔 **Hola {nombre}!**\n\nTu suscripción vence en **5 días**.\n\n💳 /renovar cuando estés listo."
        elif dias_restantes == 3:
            mensaje = f"⭐ **{nombre}**, quedan **3 días**!\n\nRenueva con /renovar"
        elif dias_restantes == 1:
            mensaje = f"⚠️ **{nombre}**, ¡MAÑANA vence tu acceso!\n\n⏰ /renovar ahora"
        if mensaje:
            try:
                await context.bot.send_message(chat_id=user_id, text=mensaje, parse_mode='Markdown')
            except:
                pass

async def enviar_mensajes_engagement(context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT user_id, first_name, mensajes_engagement, ultimo_mensaje_engagement FROM suscripciones WHERE estado = 'activo' AND mensajes_engagement < 12")
    usuarios = c.fetchall()
    for user_id, nombre, num_msg, ultimo_msg_str in usuarios:
        if ultimo_msg_str:
            ultimo_msg = datetime.strptime(ultimo_msg_str, "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - ultimo_msg).days < 7:
                continue
        mensajes = [
            f"👋 **Hola {nombre}!** Prueba /buscar_ia 🧠",
            f"💼 **{nombre}**, usa /empleo o /buscar_profesional 🚀",
            f"📊 **{nombre}**, usa /graficos 📈",
            f"⏰ Tip: /resumen para mantenerte al día ⚡",
        ]
        try:
            await context.bot.send_message(chat_id=user_id, text=mensajes[num_msg % len(mensajes)], parse_mode='Markdown')
            c.execute("UPDATE suscripciones SET mensajes_engagement = ?, ultimo_mensaje_engagement = ? WHERE user_id = ?",
                      (num_msg + 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
            conn.commit()
        except:
            pass
    conn.close()

# ==================== MAIN ====================

def main():
    init_db()
    
    # Iniciar keep-alive
    keepalive_thread = threading.Thread(target=run_keepalive_server, daemon=True)
    keepalive_thread.start()
    
    TOKEN = os.environ.get('TOKEN_BOT')
    if not TOKEN:
        logger.error("❌ TOKEN_BOT no configurado")
        return
    
    application = Application.builder().token(TOKEN).build()
    
    # Configurar menú de comandos
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
            BotCommand("resumen", "Resumen del día"),
            BotCommand("estadisticas", "Ver números"),
            BotCommand("mi_cuenta", "Mi suscripción"),
            BotCommand("renovar", "Renovar plan"),
        ]
        await app.bot.set_my_commands(commands)
    
    application.post_init = set_commands
    
    # Jobs programados
    job_queue = application.job_queue
    job_queue.run_daily(resumen_automatico, time=time(hour=20, minute=0), name='resumen_diario')
    job_queue.run_daily(enviar_recordatorios, time=time(hour=10, minute=0), name='recordatorios')
    job_queue.run_daily(enviar_mensajes_engagement, time=time(hour=15, minute=0), name='engagement')
    
    # Handlers
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
    application.add_handler(CommandHandler("resumen_mes", resumen_mes_comando))
    application.add_handler(CommandHandler("resumen_semestre", resumen_semestre_comando))
    application.add_handler(CommandHandler("resumen_usuario", resumen_usuario_comando))
    application.add_handler(CommandHandler("top_usuarios", top_usuarios_comando))
    application.add_handler(CommandHandler("mi_perfil", mi_perfil_comando))
    application.add_handler(CommandHandler("categorias", categorias_comando))
    
    application.add_handler(CommandHandler("generar_codigo", generar_codigo_comando))
    application.add_handler(CommandHandler("precios", precios_comando))
    application.add_handler(CommandHandler("set_precio", set_precio_comando))
    application.add_handler(CommandHandler("pagos_pendientes", pagos_pendientes_comando))
    
    application.add_handler(CallbackQueryHandler(callback_plan, pattern='^plan_'))
    application.add_handler(CallbackQueryHandler(callback_generar_codigo, pattern='^gencodigo_'))
    application.add_handler(CallbackQueryHandler(callback_aprobar_rechazar, pattern='^(aprobar|rechazar)_'))
    
    application.add_handler(MessageHandler(filters.PHOTO, recibir_comprobante))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'@'), responder_mencion))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, guardar_mensaje_grupo))
    
    logger.info("🚀 Bot Cofradía PRO iniciado!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
