# CHANGELOG - Bot Cofradía Premium

## v2.1 - Correcciones y Mejoras (2026-02-09)

### Correcciones Críticas
- Fix "Anónimo" en /top_usuarios: consultas SQL agrupan por user_id
- Fix nombre Owner: registros "Group" se corrigen a "Germán Perey"
- Fix Conflict error en Render: post_init elimina webhook anterior
- Fix guardar_mensaje_grupo: detecta nombres tipo "Group"

### Gráficos Mejorados (/graficos)
- Layout 4x2 (8 gráficos) con datos de Drive
- Colores uniformes azul #4472C4 (consistente con Excel)
- Nuevo gráfico "Año de Egreso" con auto-detección de columna
- Nuevo panel "Resumen BD Profesionales"

### SEC Scraper Mejorado
- URLs reales del buscador SEC (wlhttp.sec.cl)
- Mapeo de 25+ ciudades a códigos de región
- Intento de scraping real con fallback a links directos

### Timezone Fix
- Usa zoneinfo America/Santiago para scheduling preciso
- Cumpleaños 8:00 AM y Resumen 20:00 hora Chile exacta

## v2.0 - Funcionalidades Nuevas (2026-02-09)
1. Sistema RAG - Memoria con Google Drive + pgvector
2. SEC Scraper - /buscar_especialista_sec
3. Nombres completos - first_name + last_name
4. Owner siempre activo - /mi_cuenta sin restricciones
5. Fix /registrarse - Sin parse_mode
6. Gráficos mejorados - 8 charts con KPIs de Google Drive
7. Continuidad de datos - Migraciones automáticas
8. Comando /buscar_apoyo - Buscar en búsqueda laboral
9. Mensajes automáticos - Cumpleaños 8AM + Resumen 8PM
