# ✅ CHECKLIST DE DEPLOYMENT - BOT COFRADÍA

## ANTES DE SUBIR A GITHUB:

- [ ] Descargaste `bot.py`
- [ ] Descargaste `requirements.txt`
- [ ] Descargaste `.python-version` (si no lo tienes)
- [ ] Tienes acceso a tu repositorio de GitHub

---

## SUBIR A GITHUB:

- [ ] Reemplazaste `bot.py` en tu repo local
- [ ] Reemplazaste `requirements.txt` en tu repo local
- [ ] Verificaste que `.python-version` existe
- [ ] Hiciste `git add .`
- [ ] Hiciste `git commit -m "Bot v2.0 - Correcciones completas"`
- [ ] Hiciste `git push`

---

## CONFIGURAR EN RENDER:

### Variables de Entorno:
- [ ] `TOKEN_BOT` está configurado
- [ ] `GEMINI_API_KEY` está configurado
- [ ] **`OWNER_TELEGRAM_ID` está configurado** ⚠️ CRÍTICO

### Cómo configurar OWNER_TELEGRAM_ID:
1. [ ] Abriste Telegram
2. [ ] Buscaste `@userinfobot`
3. [ ] Enviaste `/start`
4. [ ] Copiaste tu ID
5. [ ] Fuiste a Render → Environment
6. [ ] Agregaste `OWNER_TELEGRAM_ID` = TU_ID
7. [ ] Guardaste cambios

### Plan Starter:
- [ ] Fuiste a Settings → Instance Type
- [ ] Seleccionaste "Starter" ($7/mes)
- [ ] Guardaste
- [ ] Entiendes que se activará en el siguiente deploy

---

## ESPERAR DEPLOY:

- [ ] Render detectó el push automáticamente
- [ ] Deploy está en progreso (3-5 minutos)
- [ ] Deploy terminó exitosamente
- [ ] Bot está corriendo (ves logs activos)

---

## PROBAR EN TELEGRAM:

### En Chat Privado con el Bot:
- [ ] `/start` - Mensaje de bienvenida mejorado aparece
- [ ] `/ayuda` - Lista completa de comandos aparece
- [ ] Escribes `/` - Aparece lista de comandos
- [ ] `/mi_cuenta` - Te dice que no estás registrado

### En el Grupo Cofradía:
- [ ] `/registrarse` - Te registra con 90 días gratis
- [ ] `/mi_cuenta` - Muestra tu suscripción (90 días)
- [ ] Espera 5 minutos y verifica que sigue activo
- [ ] `/estadisticas` - Muestra estadísticas
- [ ] `/graficos` - Genera gráficos

### Como Admin (Solo si eres Owner):
- [ ] `/generar_codigo` - Genera código de activación
- [ ] `/precios` - Muestra precios actuales
- [ ] `/pagos_pendientes` - Lista de pagos (vacía al inicio)

---

## VERIFICACIONES FINALES:

- [ ] Bot responde en menos de 2 segundos
- [ ] Suscripción dura 90 días (NO 1-2 minutos)
- [ ] Comandos aparecen al escribir `/`
- [ ] Keep-alive está activo (check logs)
- [ ] Plan Starter está activo
- [ ] Bot NO se duerme después de 5 minutos

---

## SI ALGO FALLA:

### Bot no responde:
1. [ ] Revisa Render → Logs
2. [ ] Busca errores en rojo
3. [ ] Verifica que el deploy fue exitoso

### No te reconoce como admin:
1. [ ] Verifica OWNER_TELEGRAM_ID en Render
2. [ ] Confirma que es TU ID correcto de Telegram
3. [ ] Redeploy si cambiaste la variable

### Suscripción expira inmediatamente:
1. [ ] Elimina tu registro: DELETE FROM suscripciones WHERE user_id=TU_ID
2. [ ] Regístrate de nuevo
3. [ ] Verifica que dure 90 días

### Bot se duerme:
1. [ ] Verifica que Plan Starter esté activo
2. [ ] Revisa logs para confirmar keep-alive
3. [ ] Puede tomar hasta 1 deploy para activarse

---

## 🎉 ¡SUCCESS!

Si todos los checks están ✅, tu bot está funcionando perfectamente.

**Próximo paso:** ¡Empieza a monetizar! 💰

---

**Fecha de deployment:** _______________
**Hora:** _______________
**Deploy exitoso:** [ ] SÍ [ ] NO
**Notas:** _____________________________
________________________________________
________________________________________
