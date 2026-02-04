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
model = genai.GenerativeModel('gemini-1.5-flash')

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
        return False, "❌ Código expirado. Debes renovar tu suscripción con un código vigente."
    
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

# ==================== RECORDATORIOS Y ENGAGEMENT ====================

async def enviar_recordatorios(context: ContextTypes.DEFAULT_TYPE):
    """Envía recordatorios de renovación (5, 3, 1 día antes)"""
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
        todos_servicios = ['búsqueda', 'búsqueda_ia', 'empleos', 'gráficos', 'resumen', 'exportar']
        no_usados = [s for s in todos_servicios if s not in servicios_usados]
        
        mensaje = ""
        
        if dias_restantes == 5:
            mensaje = f"""
🔔 **Hola {nombre}!**

Te recuerdo que en **5 días** vence tu acceso al Bot Cofradía.

Para seguir disfrutando de todas las funcionalidades que te ayudan día a día en el grupo, no olvides renovar tu suscripción.

¡Seguimos conectados! 🚀
"""
        
        elif dias_restantes == 3:
            mensaje = f"""
⭐ **{nombre}, quedan solo 3 días!**

El Bot Cofradía te ha estado ayudando con:

🔍 **Búsqueda inteligente** - Encuentra info al instante
🧠 **IA semántica** - Búsquedas por significado
💼 **Empleos** - Ofertas de LinkedIn, Indeed, Laborum
📊 **Análisis visual** - Gráficos profesionales
📝 **Resúmenes diarios** - Mantente al día sin esfuerzo

¿Imaginas el grupo sin estas herramientas? 

💰 Renueva por solo **${precio_mensual:,}** mensuales y sigue optimizando tu tiempo.

Usa /renovar para ver las opciones.
"""
        
        elif dias_restantes == 1:
            servicios_usados_texto = ", ".join(servicios_usados) if servicios_usados else "ninguno aún"
            no_usados_texto = ", ".join(no_usados) if no_usados else "todos"
            
            mensaje = f"""
⚠️ **{nombre}, ¡ÚLTIMO DÍA!**

Tu suscripción vence **MAÑANA**.

**Servicios que has usado:**
{servicios_usados_texto}

**Aún te faltan por probar:**
{no_usados_texto}

**Beneficios de renovar:**
✅ Acceso ilimitado a todas las funciones
✅ Resúmenes diarios automáticos
✅ Búsqueda inteligente sin límites
✅ Soporte prioritario

💳 **Precio:** ${precio_mensual:,}/mes (¡menos que un café diario!)

Usa /renovar AHORA y no pierdas el acceso.
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
            f"""
👋 **Hola {nombre}!**

¿Sabías que puedes usar /buscar_ia para encontrar conversaciones por significado y no solo por palabras exactas?

Por ejemplo: `/buscar_ia consejos para emprendedores` encuentra todas las conversaciones relacionadas, ¡aunque no usen esas palabras exactas!

Pruébalo y descubre todo el conocimiento del grupo. 🧠
""",
            f"""
💼 **{nombre}, ¿buscas empleo?**

El Bot Cofradía puede buscar ofertas en LinkedIn, Indeed y Laborum por ti.

Usa: `/empleo cargo:desarrollador ubicacion:Santiago renta:1.5-2M`

¡Encuentra tu próxima oportunidad sin salir del grupo! 🚀
""",
            f"""
📊 **{nombre}, ¿quieres ver cómo está el grupo?**

Usa /graficos para ver análisis visuales súper profesionales:
- Actividad diaria
- Usuarios más activos
- Temas más discutidos
- Horarios de mayor actividad

¡Es como tener un dashboard de Google! 📈
""",
            f"""
⏰ **Tip para {nombre}:**

¿No puedes leer todo el grupo cada día?

Usa /resumen y recibirás un resumen completo con:
- Temas principales
- Decisiones importantes
- Próximos pasos

¡Mantente al día en 2 minutos! ⚡
""",
            f"""
🎯 **{nombre}, maximiza tu experiencia:**

Servicios que **SÍ has usado:** {', '.join(servicios_usados) if servicios_usados else 'Ninguno aún'}

Servicios que te **FALTAN probar:** {', '.join([s for s in ['búsqueda', 'búsqueda_ia', 'empleos', 'gráficos', 'resumen'] if s not in servicios_usados])}

Usa /ayuda para ver todo lo que puedes hacer. 💡
"""
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
                    f"⏰ Tu suscripción vence en **{dias_restantes} días**.\n\n"
                    f"Usa /renovar para extenderla.",
                    parse_mode='Markdown'
                )
            else:
                await update.message.reply_text(
                    "❌ **Tu suscripción ha expirado.**\n\n"
                    "Para seguir usando el bot, renueva tu suscripción con /renovar",
                    parse_mode='Markdown'
                )
            return
        
        return await func(update, context, *args, **kwargs)
    
    return wrapper

# ==================== COMANDOS BÁSICOS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    await update.message.reply_text(
        f"👋 **¡Bienvenido {user.first_name}!**\n\n"
        f"Soy el Bot Cofradía, tu asistente inteligente.\n\n"
        f"Para empezar, usa /registrarse en el grupo.\n\n"
        f"Luego podrás usar todas las funciones disponibles. ✨",
        parse_mode='Markdown'
    )

async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto_ayuda = """
🤖 **Bot Cofradía - Guía Completa**

**🔍 Búsqueda:**
/buscar [palabra] - Búsqueda tradicional
/buscar_ia [frase] - Búsqueda semántica IA

**💼 Empleos:**
/empleo cargo:[...] ubicacion:[...] renta:[...]

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
    
    # Mensaje privado de bienvenida
    try:
        mensaje_privado = f"""
👋 **¡Bienvenido {user.first_name}!**

Has activado tu cuenta en el Bot Cofradía. 🎉

**Ahora puedes:**
🔍 Buscar información con IA
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
    
    # Notificar que está procesando
    msg_procesando = await update.message.reply_text("🔍 Analizando comprobante con IA...")
    
    # Obtener la imagen
    photo = update.message.photo[-1]
    file = await context.bot.get_file(photo.file_id)
    
    # Descargar imagen temporalmente
    import requests
    image_bytes = requests.get(file.file_path).content
    
    # Analizar con Gemini Vision (OCR)
    try:
        # Convertir bytes a base64
        import base64
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
        
        # Crear prompt para análisis OCR
        prompt_ocr = f"""Analiza este comprobante de transferencia bancaria y extrae los siguientes datos:

DATOS A EXTRAER:
1. Monto transferido (busca "Monto", "Total", "$", "CLP", etc.)
2. Fecha de la transacción (formato DD/MM/YYYY o similar)
3. Cuenta de destino (número de cuenta)
4. Banco destino (nombre del banco)
5. Titular de la cuenta destino
6. RUT (si aparece)

DATOS ESPERADOS:
- Monto esperado: ${precio:,} CLP
- Cuenta esperada: 69104312
- Banco esperado: Banco Santander
- Titular esperado: Destak E.I.R.L.
- RUT esperado: 76.698.480-0

IMPORTANTE:
- Si no puedes leer algún dato claramente, indica "NO DETECTADO"
- Verifica si el monto coincide con el esperado
- Verifica si la cuenta coincide
- Verifica si la fecha es reciente (últimos 7 días)

FORMATO DE RESPUESTA (JSON):
{{
  "monto_detectado": "2000",
  "monto_correcto": true/false,
  "fecha_detectada": "03/02/2026",
  "fecha_valida": true/false,
  "cuenta_detectada": "69104312",
  "cuenta_correcta": true/false,
  "banco_detectado": "Banco Santander",
  "titular_detectado": "Destak E.I.R.L.",
  "rut_detectado": "76.698.480-0",
  "calidad_imagen": "buena/regular/mala",
  "legible": true/false,
  "observaciones": "cualquier comentario relevante"
}}

RESPONDE SOLO CON EL JSON, sin explicaciones adicionales."""

        # Llamar a Gemini Vision
        vision_model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Preparar la imagen para Gemini
        import PIL.Image
        from io import BytesIO
        image = PIL.Image.open(BytesIO(image_bytes))
        
        response = vision_model.generate_content([prompt_ocr, image])
        
        # Parsear respuesta JSON
        import re
        response_text = response.text.strip()
        # Eliminar ```json si existe
        response_text = re.sub(r'```json\s*|\s*```', '', response_text)
        
        try:
            datos_ocr = json.loads(response_text)
        except:
            # Si falla el parsing, intentar extraer manualmente
            datos_ocr = {
                "legible": False,
                "calidad_imagen": "mala",
                "observaciones": "No se pudo procesar correctamente"
            }
        
        # Verificar si la imagen es legible
        if not datos_ocr.get("legible", False) or datos_ocr.get("calidad_imagen") == "mala":
            await msg_procesando.delete()
            await update.message.reply_text(
                "❌ **La imagen no es suficientemente clara**\n\n"
                "Por favor, envía una nueva foto con:\n"
                "✅ Mejor iluminación\n"
                "✅ Imagen más nítida\n"
                "✅ Todos los datos visibles\n"
                "✅ Sin brillos o sombras\n\n"
                "Vuelve a enviar el comprobante cuando tengas una mejor foto. 📸",
                parse_mode='Markdown'
            )
            return
        
        # Crear resumen del análisis
        analisis = "🤖 **ANÁLISIS AUTOMÁTICO DEL COMPROBANTE**\n\n"
        
        # Monto
        if datos_ocr.get("monto_correcto"):
            analisis += f"✅ **Monto:** ${datos_ocr.get('monto_detectado', 'N/A')} (Correcto)\n"
        else:
            analisis += f"⚠️ **Monto:** ${datos_ocr.get('monto_detectado', 'N/A')} (Esperado: ${precio:,})\n"
        
        # Fecha
        if datos_ocr.get("fecha_valida"):
            analisis += f"✅ **Fecha:** {datos_ocr.get('fecha_detectada', 'N/A')} (Válida)\n"
        else:
            analisis += f"⚠️ **Fecha:** {datos_ocr.get('fecha_detectada', 'N/A')} (Verificar si es reciente)\n"
        
        # Cuenta
        if datos_ocr.get("cuenta_correcta"):
            analisis += f"✅ **Cuenta:** {datos_ocr.get('cuenta_detectada', 'N/A')} (Correcta)\n"
        else:
            analisis += f"⚠️ **Cuenta:** {datos_ocr.get('cuenta_detectada', 'N/A')} (Esperada: 69104312)\n"
        
        # Banco
        banco = datos_ocr.get('banco_detectado', 'NO DETECTADO')
        if banco != 'NO DETECTADO':
            analisis += f"✅ **Banco:** {banco}\n"
        else:
            analisis += f"⚠️ **Banco:** No detectado claramente\n"
        
        # Titular
        titular = datos_ocr.get('titular_detectado', 'NO DETECTADO')
        if titular != 'NO DETECTADO':
            analisis += f"✅ **Titular:** {titular}\n"
        else:
            analisis += f"⚠️ **Titular:** No detectado\n"
        
        # RUT
        rut = datos_ocr.get('rut_detectado', 'NO DETECTADO')
        if rut != 'NO DETECTADO':
            analisis += f"✅ **RUT:** {rut}\n"
        else:
            analisis += f"⚠️ **RUT:** No detectado\n"
        
        # Observaciones
        if datos_ocr.get('observaciones'):
            analisis += f"\n💡 **Observaciones:** {datos_ocr['observaciones']}\n"
        
        # Recomendación automática
        todos_correctos = (
            datos_ocr.get("monto_correcto", False) and 
            datos_ocr.get("fecha_valida", False) and 
            datos_ocr.get("cuenta_correcta", False)
        )
        
        if todos_correctos:
            analisis += "\n✅ **Recomendación:** Todos los datos coinciden. Parece válido."
        else:
            analisis += "\n⚠️ **Recomendación:** Algunos datos no coinciden. Revisar manualmente."
        
        # Enviar análisis al usuario
        await msg_procesando.delete()
        await update.message.reply_text(
            f"{analisis}\n\n⏳ Tu comprobante está siendo revisado por el administrador.\n"
            "Recibirás tu código pronto. 🙏",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Error en OCR: {e}")
        await msg_procesando.delete()
        analisis = "⚠️ **No se pudo analizar automáticamente**\n\nEl comprobante será revisado manualmente por el administrador."
        await update.message.reply_text(analisis, parse_mode='Markdown')
        datos_ocr = {"observaciones": f"Error en OCR: {str(e)}"}
    
    # Guardar en base de datos
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
    
    # Notificar al dueño CON análisis OCR
    nombre_plan = dict([(p[0], p[2]) for p in obtener_precios()])[dias]
    
    keyboard = [
        [InlineKeyboardButton("✅ Aprobar", callback_data=f"aprobar_{pago_id}")],
        [InlineKeyboardButton("❌ Rechazar", callback_data=f"rechazar_{pago_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Crear caption con análisis
    caption_dueño = f"""
💳 **NUEVO PAGO RECIBIDO** #{pago_id}

👤 **Usuario:** {user.first_name} (@{user.username or 'sin_username'})
📱 **ID:** `{user.id}`
💎 **Plan:** {nombre_plan} ({dias} días)
💰 **Precio esperado:** ${precio:,}
📅 **Fecha envío:** {fecha_envio}

{analisis}

¿Aprobar pago?
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
                text=f"""
✅ **¡PAGO APROBADO!**

Tu código de activación es:

`{codigo}`

Para activarlo, usa:
/activar {codigo}

¡Gracias por renovar! 🎉
""",
                parse_mode='Markdown'
            )
            
            await query.edit_message_caption(
                f"{query.message.caption}\n\n✅ **APROBADO**\nCódigo enviado: `{codigo}`",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await query.edit_message_caption(f"❌ Error enviando código: {e}")
    
    else:
        c.execute("UPDATE pagos_pendientes SET estado = 'rechazado' WHERE id = ?", (pago_id,))
        conn.commit()
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="❌ Tu pago no pudo ser verificado. Por favor contacta al administrador.",
                parse_mode='Markdown'
            )
            
            await query.edit_message_caption(
                f"{query.message.caption}\n\n❌ **RECHAZADO**",
                parse_mode='Markdown'
            )
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
    """Muestra info de la cuenta del usuario"""
    user = update.message.from_user
    
    conn = sqlite3.connect('mensajes.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute("""SELECT fecha_registro, fecha_expiracion, estado, es_admin, servicios_usados 
                 FROM suscripciones WHERE user_id = ?""", (user.id,))
    resultado = c.fetchone()
    conn.close()
    
    if not resultado:
        await update.message.reply_text(
            "❌ No estás registrado. Usa /registrarse en el grupo.",
            parse_mode='Markdown'
        )
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

Usa /renovar para extender tu suscripción.
"""
    
    await update.message.reply_text(mensaje, parse_mode='Markdown')

# ==================== COMANDOS ADMIN (SOLO DUEÑO) ====================

async def generar_codigo_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera código de activación (solo dueño)"""
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
    
    await update.message.reply_text(
        "👑 **GENERAR CÓDIGO**\n\nSelecciona el plan:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def callback_generar_codigo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback para generar código"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != OWNER_ID:
        return
    
    dias = int(query.data.split('_')[1])
    precios = obtener_precios()
    precio = next((p[1] for p in precios if p[0] == dias), 0)
    
    codigo = generar_codigo_activacion(dias, precio)
    
    await query.edit_message_text(
        f"""
✅ **CÓDIGO GENERADO**

`{codigo}`

📋 **Detalles:**
- Duración: {dias} días
- Precio: ${precio:,}
- Válido por: 30 días desde hoy

Comparte este código con el usuario.
""",
        parse_mode='Markdown'
    )

async def precios_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Configura precios (solo dueño)"""
    user = update.message.from_user
    
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño puede configurar precios.")
        return
    
    precios = obtener_precios()
    
    mensaje = "💰 **PRECIOS ACTUALES**\n\n"
    for dias, precio, nombre in precios:
        mensaje += f"• {nombre} ({dias} días): ${precio:,}\n"
    
    mensaje += "\n📝 Para cambiar un precio:\n`/set_precio [dias] [nuevo_precio]`\n\nEjemplo: `/set_precio 30 2500`"
    
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def set_precio_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Actualiza precio (solo dueño)"""
    user = update.message.from_user
    
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño puede hacer esto.")
        return
    
    if len(context.args) != 2:
        await update.message.reply_text(
            "❌ Uso: /set_precio [dias] [precio]\n\nEjemplo: `/set_precio 30 2500`",
            parse_mode='Markdown'
        )
        return
    
    try:
        dias = int(context.args[0])
        precio = int(context.args[1])
        
        actualizar_precio(dias, precio)
        
        await update.message.reply_text(
            f"✅ Precio actualizado:\n\nPlan de {dias} días: ${precio:,}",
            parse_mode='Markdown'
        )
    except:
        await update.message.reply_text("❌ Error. Verifica los valores.")

async def pagos_pendientes_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista pagos pendientes (solo dueño)"""
    user = update.message.from_user
    
    if user.id != OWNER_ID:
        await update.message.reply_text("❌ Solo el dueño puede ver esto.")
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
        await update.message.reply_text("✅ No hay pagos pendientes.")
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
async def graficos_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'gráficos')
    await update.message.reply_text("📊 Generando...")
    stats = obtener_estadisticas_graficos(dias=7)
    imagen_buffer = generar_grafico_visual(stats)
    await update.message.reply_photo(photo=imagen_buffer, caption="📊 **Análisis Visual** (7 días)")

@requiere_suscripcion
async def resumen_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'resumen')
    await update.message.reply_text("📝 Generando resumen...")
    resumen = generar_resumen_usuarios(dias=1)
    if not resumen:
        await update.message.reply_text("❌ No hay mensajes hoy")
        return
    await update.message.reply_text(resumen, parse_mode='Markdown')

@requiere_suscripcion
async def resumen_semanal_comando(update: Update, context: ContextTypes.DEFAULT_TYPE):
    registrar_servicio_usado(update.effective_user.id, 'resumen')
    await update.message.reply_text("📝 Generando resumen semanal...")
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
    prompt = f"""Asistente de "Cofradía de Networking". Responde amigable y útil.
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
                mensaje = f"👑 **RESUMEN DIARIO - ADMINISTRADOR**\n\n{resumen_admins}"
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

def main():
    init_db()
    TOKEN = os.environ.get('TOKEN_BOT')
    if not TOKEN:
        logger.error("❌ TOKEN_BOT no configurado")
        return
    
    application = Application.builder().token(TOKEN).build()
    
    # Jobs programados
    job_queue = application.job_queue
    job_queue.run_daily(resumen_automatico, time=time(hour=20, minute=0), name='resumen_diario')
    job_queue.run_daily(enviar_recordatorios, time=time(hour=10, minute=0), name='recordatorios')
    job_queue.run_daily(enviar_mensajes_engagement, time=time(hour=15, minute=0), name='engagement')
    
    # Comandos públicos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ayuda", ayuda))
    application.add_handler(CommandHandler("registrarse", registrarse_comando))
    application.add_handler(CommandHandler("renovar", renovar_comando))
    application.add_handler(CommandHandler("activar", activar_codigo_comando))
    application.add_handler(CommandHandler("mi_cuenta", mi_cuenta_comando))
    
    # Comandos con suscripción
    application.add_handler(CommandHandler("buscar", buscar_comando))
    application.add_handler(CommandHandler("buscar_ia", buscar_semantica_comando))
    application.add_handler(CommandHandler("empleo", buscar_empleo_comando))
    application.add_handler(CommandHandler("graficos", graficos_comando))
    application.add_handler(CommandHandler("resumen", resumen_comando))
    application.add_handler(CommandHandler("resumen_semanal", resumen_semanal_comando))
    application.add_handler(CommandHandler("estadisticas", estadisticas_comando))
    application.add_handler(CommandHandler("categorias", categorias_comando))
    
    # Comandos admin
    application.add_handler(CommandHandler("generar_codigo", generar_codigo_comando))
    application.add_handler(CommandHandler("precios", precios_comando))
    application.add_handler(CommandHandler("set_precio", set_precio_comando))
    application.add_handler(CommandHandler("pagos_pendientes", pagos_pendientes_comando))
    
    # Callbacks
    application.add_handler(CallbackQueryHandler(callback_plan, pattern='^plan_'))
    application.add_handler(CallbackQueryHandler(callback_generar_codigo, pattern='^gencodigo_'))
    application.add_handler(CallbackQueryHandler(callback_aprobar_rechazar, pattern='^(aprobar|rechazar)_'))
    
    # Mensajes
    application.add_handler(MessageHandler(filters.PHOTO, recibir_comprobante))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'@'), responder_con_ia))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, guardar_mensaje_grupo))
    
    logger.info("🚀 Bot Cofradía PRO con Suscripciones iniciado!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()


