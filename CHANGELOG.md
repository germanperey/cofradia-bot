# CHANGELOG - Bot Cofradía Premium

## v2.2 - Sistema RAG con PDFs (2026-02-09)

### Sistema RAG PDF Completo
- **Subida de PDFs**: Envía un PDF al bot en chat privado y se sube automáticamente a Google Drive (INBESTU/RAG_PDF)
- **Extracción de texto**: PyPDF2 extrae texto del PDF automáticamente
- **Indexación inteligente**: Texto dividido en chunks con keywords para búsqueda semántica
- **Re-indexación automática**: Cada 6 horas se re-indexan Excel + todos los PDFs
- **Consultas RAG**: `/rag_consulta [pregunta]` busca en todos los documentos indexados y genera respuesta con IA

### Nuevos Comandos
- `/subir_pdf` - Instrucciones para subir PDFs
- `/rag_status` - Ver estado del sistema RAG (chunks, PDFs, espacio Drive)
- `/rag_consulta [pregunta]` - Consultar todos los documentos indexados
- `/rag_reindexar` - Re-indexar todo manualmente (solo admin)
- `/eliminar_pdf [nombre]` - Eliminar PDF del sistema (solo admin)

### Funciones Técnicas Nuevas
- `obtener_drive_auth_headers()` - Auth centralizada Google Drive
- `obtener_o_crear_carpeta_drive()` - Gestión de carpetas Drive
- `subir_pdf_a_drive()` - Upload con verificación de espacio
- `extraer_texto_pdf()` - Extracción con PyPDF2
- `crear_chunks_texto()` - Chunking inteligente con overlap
- `generar_keywords_chunk()` - Keywords automáticas (sin stopwords)
- `indexar_todos_pdfs_rag()` - Indexación masiva
- `verificar_espacio_drive()` - Monitor de espacio (15 GB gratis)
- Handler automático para documentos PDF en chat privado

## v2.1 - Correcciones (2026-02-09)
- Fix "Anónimo" en /top_usuarios (GROUP BY user_id)
- Fix Conflict error en Render (post_init webhook cleanup)
- Gráficos mejorados 4x2 con Año de Egreso
- Colores uniformes azul #4472C4
- SEC scraper con URLs reales
- Timezone Chile exacto (zoneinfo)

## v2.0 - Funcionalidades Nuevas (2026-02-09)
- Sistema RAG base (Excel)
- SEC Scraper, Nombres completos, Owner activo
- Gráficos con KPIs Drive, /buscar_apoyo
- Mensajes automáticos (cumpleaños + resumen)
