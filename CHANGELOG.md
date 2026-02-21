# CHANGELOG — Bot Cofradía Premium

## v4.3.2 (21-02-2026) — 3 Correcciones Críticas

### 🔧 FIX 1: Tarjeta — Stats reales (años, trofeo/referidos, QR verificación)

**Causa raíz identificada:** `fecha_incorporacion` del owner no se persistía porque el UPDATE en `init_db()` corría ANTES de que el registro existiera en suscripciones. Además, el fallback `SELECT generacion FROM suscripciones` fallaba silenciosamente porque esa columna NO EXISTE en la tabla.

**Correcciones:**
- **registrar_usuario_suscripcion():** Ahora detecta `user_id == OWNER_ID` y FUERZA `fecha_incorporacion='2020-09-22'`, `fecha_expiracion='2099-12-31'`, estado activo — se ejecuta cada vez que el owner interactúa
- **Eliminado** fallback roto `SELECT generacion FROM suscripciones` (columna inexistente)
- **Logging mejorado:** Todos los `except: pass` silenciosos en stats reemplazados por `logger.warning()` con mensajes descriptivos
- **Referidos:** Búsqueda en TODOS los registros (no solo 'aprobado'), 3 niveles de matching Unicode NFD
- **QR Verificación:** El handler deep link `/start verificar_ID` ya funcionaba correctamente — el problema era que `obtener_stats_tarjeta()` retornaba valores vacíos por los errores silenciosos

### 📄 FIX 2: CV estrictamente verídico (no inventa datos)

**Causa raíz:** El prompt le decía a la IA que "genere posiciones anteriores coherentes" e "incluya certificaciones típicas del sector" — instrucciones que generaban datos inventados.

**Correcciones:**
- **Prompt reescrito desde cero:** Regla absoluta "PROHIBIDO INVENTAR: universidades, títulos, empresas, certificaciones, nombres de proyectos, cifras exactas"
- **Columnas Drive corregidas:** Profesión = col Y (iloc[24]), Situación Laboral = col I (iloc[8]), Industrias = K/L/M/N/O/P (10-15)
- **Secciones opcionales:** Si no hay datos de formación civil, SOLO muestra Escuela Naval. Si no hay certificaciones, OMITE la sección
- **temperature=0.4** (antes 0.5) para mayor precisión y menor "creatividad"
- **LinkedIn:** Ya no se reporta como "fuente" porque el bot no puede acceder a LinkedIn realmente
- **Logros genéricos:** En vez de inventar "incrementé ventas en 47%", usa frases como "Optimicé procesos logrando mejoras significativas"

### 📊 FIX 3: Gráficos — Columnas Drive corregidas

**Causa raíz:** Los índices de columnas estaban equivocados.

**Correcciones:**
- "Top Profesiones/Cargos": `iloc[5]` → `iloc[24]` (Columna Y del Excel)
- "Situación Laboral": `iloc[6]` → `iloc[8]` (Columna I del Excel)
- Generaciones ya usaba `iloc[1]` (Columna B) correctamente ✓
- Ciudades ya usaba `iloc[7]` (Columna H) correctamente ✓
