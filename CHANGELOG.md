# CHANGELOG — Bot Cofradía Premium

## v4.3 (20-02-2026) — ECharts + Tarjeta Épica + Anti-Fraude

### 🎴 1. Tarjeta de Presentación — Rediseño Completo
- **QR verificación** reubicado → **inferior derecha** (separado del QR principal)
- **QR verificación funcional**: nombre, generación, estado, fecha incorporación
- **NRO_KDT-GENERACIÓN** en esquina superior derecha (ej: "322-2000")
- **3 iconos dorados corregidos** con valores reales + trofeo rediseñado
- Línea horizontal eliminada, fuentes agrandadas, H=620px
- Header: "Red Profesional de Ex-cadetes y Oficiales"

### 📄 2. CV — No Inventa Datos
- PROHIBIDO inventar universidades/certificaciones, usa placeholders []
- Incluye Escuela Naval "Arturo Prat" por defecto

### 🛡️ 3. Anti Auto-Referencia
- P3: no puede escribir su propio nombre + busca coincidencias BD
- /recomendar: búsqueda por nombre + bloqueo auto-recomendación

### 📊 5. ECharts — Dashboards Interactivos
- /graficos: 6 charts ECharts (line, bar, pie, rose) + preview PNG
- /estadisticas: 3 gauges + KPIs expandidos
- Tema navy+dorado, responsive, tooltips, animaciones
