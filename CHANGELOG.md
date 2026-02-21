# CHANGELOG — Bot Cofradía Premium

## v4.3.1 (21-02-2026) — 4 Correcciones Críticas

### 🔧 FIX 1: Tarjeta — Stats reales + QR verificación
- **Owner fecha_incorporacion FORZADA** a 22-09-2020 (sin condición IS NULL)
- **Owner nuevos_miembros** creado automáticamente con generacion='2000'
- **Generación fallback**: busca en nuevos_miembros → suscripciones
- **Referidos mejorados**: busca en TODOS los registros (no solo aprobados), 3 niveles de matching:
  1. Nombre completo (con/sin acentos via Unicode NFD)
  2. Nombre + apellido por separado
  3. Solo apellido si es largo y único
- Debug logging explícito para referidos y stats
- QR verificación: deep link funcional con nombre, generación, estado, fecha incorporación

### 📄 FIX 2: CV Profesional — Multi-fuente, sin placeholders
- **Recopilación de 4 fuentes**: Tarjeta + Google Drive Excel + Stats + Recomendaciones textuales
- Busca datos del usuario en BD Excel Drive (formación, universidad, postgrado, certificaciones, idiomas)
- Integra generación naval, antigüedad, recomendaciones recibidas
- **Prompt rediseñado**: genera CV COMPLETO sin corchetes [] ni placeholders
- Si faltan datos, infiere razonablemente según cargo y empresa (no deja vacíos)
- max_tokens=3000, temperature=0.5 para mayor completitud y coherencia
- Muestra fuentes utilizadas al entregar el CV

### 🔤 FIX 3: Título "COFRADÍA DE NETWORKING" agrandado
- Font size 14 → **22 bold** (56% más grande)
- Posición Y ajustada (38→30) para mejor centrado visual

### 📊 FIX 4: Gráficos — Generaciones columna B + 2 charts nuevos
- **Generaciones**: ahora lee columna B (iloc[1]) del Excel Drive (antes columna D)
- Extracción inteligente: filtra solo años 4 dígitos entre 1950-2025
- **Nuevo chart: Top Profesiones/Cargos** (horizontal bar azul)
- **Nuevo chart: Situación Laboral** (pie chart con colores semáforo)
- **Nuevo KPI**: "BD Excel Drive" con total de registros
- Total: 8 charts ECharts (4 actividad + 4 Drive)
- Resize responsive para los 8 charts
