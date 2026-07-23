"""
IntelliBot Memory & Learning Service (v1.0)
============================================
Standalone memory + learning layer for chat bots (Telegram / WhatsApp).
Designed for Supabase Postgres + pgvector on the FREE tier. Zero new
dependencies: uses psycopg2 and requests, already present in the stack.

The bot NEVER needs internal changes to its logic: it just calls
    svc.log_message(...)   after each user/bot message
    svc.get_context(...)   before building its LLM prompt
    svc.persona_block()    once inside its system prompt

Identity model (replicable across channels):
    user_key = 'tg:<telegram_id>'  or  'wa:<E164 phone>'

Privacy: every query filters by user_key (defense in depth) AND each
transaction sets `SET LOCAL app.user_key` so Supabase RLS policies apply.

Graceful degradation:
    - No GEMINI_API_KEY  -> semantic search disabled, recency + keyword only.
    - No pgvector column -> same fallback, never crashes.

CLI (admin review flow, no extra UI/infra):
    python memory_service.py pending
    python memory_service.py approve <id>
    python memory_service.py reject <id>
    python memory_service.py suggest        # mine FAQs -> pending KB entries
    python memory_service.py report
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import unicodedata
from collections import Counter
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests

logger = logging.getLogger("memory_service")

# ─────────────────────────────────────────────────────────────────────────
# Persona: single source of truth for IntelliBot's voice.
# Inject persona_block() into the bot's system prompt for consistency.
# ─────────────────────────────────────────────────────────────────────────
PERSONA = {
    "name": "IntelliBot",
    "voice": (
        "Eres IntelliBot, el asistente de inteligencia artificial de la "
        "Cofradía de Networking, comunidad profesional chilena vinculada a "
        "oficiales de la Armada. Tu tono es FORMAL PERO CERCANO: profesional, "
        "respetuoso y directo, con calidez chilena y sin caer en jerga "
        "informal. Tratas a cada persona por su nombre (y su grado naval si "
        "lo conoces), respetas la jerarquía y la cultura naval, y jamás "
        "revelas información de un miembro a otro. Respondes en español de "
        "Chile, sin asteriscos ni Markdown, en párrafos breves y claros. "
        "Si no sabes algo con certeza, lo dices con honestidad y ofreces el "
        "mejor camino para averiguarlo."
    ),
}

GEMINI_EMBED_URL = (
    "https://generativelanguage.googleapis.com/v1beta/"
    "models/text-embedding-004:embedContent"
)
# FASE 31.54: Google retiró text-embedding-004 (HTTP 404 visto en producción
# 2026-07-23). Cascada de 4 combos igual que bot.py FASE 31.40; el ganador se
# cachea en la instancia. gemini-embedding-001 entrega 3072 dims por defecto →
# se pide outputDimensionality=768 y se corta [:768] (la tabla es vector(768)).
_EMB_COMBOS = (
    ("v1beta", "text-embedding-004"),
    ("v1",     "text-embedding-004"),
    ("v1beta", "gemini-embedding-001"),
    ("v1",     "gemini-embedding-001"),
)

_UNANSWERED_PATTERNS = (
    "no tengo informaci", "no se encuentra", "no cuento con",
    "no dispongo", "no pude encontrar", "no hay informaci",
    "no puedo proporcionar", "reformula tu pregunta",
)

_STOPWORDS = {
    "para", "como", "sobre", "este", "esta", "pero", "porque", "cuando",
    "donde", "que", "los", "las", "del", "por", "con", "una", "uno", "mas",
    "sus", "hay", "muy", "sin", "son", "the", "and", "qué", "cuál", "cómo",
    "puedo", "puede", "quiero", "quiere", "tiene", "tengo", "hacer", "saber",
    "mejor", "ahora", "hola", "gracias", "favor", "necesito", "quisiera",
    "podria", "puedes", "dime", "cuentame", "existe", "estan", "sirve",
}


def _normalize(text: str) -> str:
    text = (text or "").lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9ñ ]+", " ", text)


def _question_hash(question: str) -> str:
    return hashlib.md5(_normalize(question).encode()).hexdigest()


class MemoryService:
    """Facade the bot consumes. One instance per process is enough."""

    def __init__(self, db_url: str | None = None, gemini_api_key: str | None = None):
        self.db_url = db_url or os.environ.get("DATABASE_URL", "")
        self.gemini_key = gemini_api_key or os.environ.get("GEMINI_API_KEY", "")
        if not self.db_url:
            raise ValueError("DATABASE_URL is required for MemoryService")

    # ── connection helpers ────────────────────────────────────────────────
    def _conn(self, user_key: str | None = None, admin: bool = False):
        conn = psycopg2.connect(self.db_url, cursor_factory=psycopg2.extras.RealDictCursor)
        cur = conn.cursor()
        if admin:
            cur.execute("SET app.role = 'admin'")
        if user_key:
            cur.execute("SET app.user_key = %s", (user_key,))
        return conn, cur

    # ── embeddings (free: Gemini text-embedding-004, 768 dims) ───────────
    def _embed(self, text: str) -> list | None:
        # FASE 31.54: cascada anti-404 (espejo de bot.py 31.40) + cache del combo.
        if not self.gemini_key or not text:
            return None
        combos = ((getattr(self, "_emb_combo_ok", None),) if getattr(self, "_emb_combo_ok", None) else _EMB_COMBOS)
        for combo in combos:
            api_ver, modelo = combo
            try:
                r = requests.post(
                    f"https://generativelanguage.googleapis.com/{api_ver}/"
                    f"models/{modelo}:embedContent?key={self.gemini_key}",
                    json={
                        "model": f"models/{modelo}",
                        "content": {"parts": [{"text": text[:8000]}]},
                        "outputDimensionality": 768,
                    },
                    timeout=(5, 15),
                )
                if r.status_code == 200:
                    vals = r.json().get("embedding", {}).get("values") or []
                    if vals:
                        if getattr(self, "_emb_combo_ok", None) != combo:
                            self._emb_combo_ok = combo
                            logger.info("🧬 FASE 31.54 memoria: embeddings vía %s/%s", api_ver, modelo)
                        return vals[:768]
                logger.warning("Embedding failed: HTTP %s (%s/%s)", r.status_code, api_ver, modelo)
            except Exception as exc:  # noqa: BLE001 — never break the bot for memory
                logger.debug("Embedding error (%s/%s): %r", api_ver, modelo, exc)
        return None

    @staticmethod
    def _vec_literal(vec: list) -> str:
        return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"

    # ─────────────────────────────────────────────────────────────────────
    # 1. WRITE PATH — call after every message (user and bot turns)
    # ─────────────────────────────────────────────────────────────────────
    def log_message(self, user_key: str, role: str, message: str,
                    channel: str = "telegram", display_name: str | None = None) -> None:
        """Store one turn; update the lightweight profile counters.

        Also auto-records an 'unanswered' metric when a bot reply matches
        known no-answer patterns, feeding the improvement report.
        """
        if not message:
            return
        emb = self._embed(message) if role == "user" else None
        conn, cur = self._conn(user_key)
        try:
            if emb:
                cur.execute(
                    "INSERT INTO mem_conversations (user_key, channel, role, message, embedding)"
                    " VALUES (%s, %s, %s, %s, %s::vector)",
                    (user_key, channel, role, message[:4000], self._vec_literal(emb)),
                )
            else:
                cur.execute(
                    "INSERT INTO mem_conversations (user_key, channel, role, message)"
                    " VALUES (%s, %s, %s, %s)",
                    (user_key, channel, role, message[:4000]),
                )

            if role == "user":
                topics = self._extract_topics(message)
                cur.execute(
                    """
                    INSERT INTO mem_profiles (user_key, channel, display_name, interactions,
                                              recurring_topics, updated_at)
                    VALUES (%s, %s, %s, 1, %s::jsonb, now())
                    ON CONFLICT (user_key) DO UPDATE SET
                        interactions     = mem_profiles.interactions + 1,
                        display_name     = COALESCE(EXCLUDED.display_name, mem_profiles.display_name),
                        recurring_topics = (
                            SELECT COALESCE(jsonb_object_agg(k, v), '{}'::jsonb) FROM (
                                SELECT k,
                                       SUM((v)::int) AS v
                                FROM (
                                    SELECT key AS k, value::text AS v
                                    FROM jsonb_each(mem_profiles.recurring_topics)
                                    UNION ALL
                                    SELECT key AS k, value::text AS v
                                    FROM jsonb_each(EXCLUDED.recurring_topics)
                                ) merged
                                GROUP BY k
                            ) agg
                        ),
                        updated_at = now()
                    """,
                    (user_key, channel, display_name,
                     json.dumps({t: 1 for t in topics})),
                )
                for t in topics[:3]:
                    cur.execute(
                        "INSERT INTO mem_metrics (kind, detail, user_key) VALUES ('topic', %s, %s)",
                        (t, user_key),
                    )

            if role == "bot":
                low = message.lower()
                if any(p in low for p in _UNANSWERED_PATTERNS):
                    cur.execute(
                        "SELECT message FROM mem_conversations WHERE user_key = %s"
                        " AND role = 'user' ORDER BY created_at DESC LIMIT 1",
                        (user_key,),
                    )
                    row = cur.fetchone()
                    question = (row or {}).get("message", "")[:400]
                    cur.execute(
                        "INSERT INTO mem_metrics (kind, detail, user_key) VALUES ('unanswered', %s, %s)",
                        (question or message[:400], user_key),
                    )
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            logger.warning("log_message failed: %s", exc)
        finally:
            conn.close()

    @staticmethod
    def _extract_topics(message: str, max_topics: int = 3) -> list:
        words = [w for w in _normalize(message).split()
                 if len(w) >= 5 and w not in _STOPWORDS]
        return [w for w, _ in Counter(words).most_common(max_topics)]

    # ─────────────────────────────────────────────────────────────────────
    # 2. READ PATH — call before building the LLM prompt
    # ─────────────────────────────────────────────────────────────────────
    def get_context(self, user_key: str, query: str,
                    max_recent: int = 6, max_semantic: int = 4,
                    max_kb: int = 3) -> dict:
        """Return profile + relevant history + approved KB for this user."""
        out = {"profile": None, "recent": [], "semantic": [], "kb": []}
        conn, cur = self._conn(user_key)
        try:
            cur.execute("SELECT * FROM mem_profiles WHERE user_key = %s", (user_key,))
            out["profile"] = cur.fetchone()

            cur.execute(
                "SELECT role, message, created_at FROM mem_conversations"
                " WHERE user_key = %s ORDER BY created_at DESC LIMIT %s",
                (user_key, max_recent),
            )
            out["recent"] = list(reversed(cur.fetchall()))

            emb = self._embed(query)
            if emb:
                vec = self._vec_literal(emb)
                cur.execute(
                    "SELECT message, created_at, 1 - (embedding <=> %s::vector) AS sim"
                    " FROM mem_conversations WHERE user_key = %s AND role = 'user'"
                    " AND embedding IS NOT NULL"
                    " ORDER BY embedding <=> %s::vector LIMIT %s",
                    (vec, user_key, vec, max_semantic),
                )
                out["semantic"] = [r for r in cur.fetchall() if (r.get("sim") or 0) >= 0.55]

                cur.execute(
                    "SELECT id, question, answer, 1 - (embedding <=> %s::vector) AS sim"
                    " FROM kb_entries WHERE status = 'approved' AND embedding IS NOT NULL"
                    " ORDER BY embedding <=> %s::vector LIMIT %s",
                    (vec, vec, max_kb),
                )
                out["kb"] = [r for r in cur.fetchall() if (r.get("sim") or 0) >= 0.60]
            else:
                # Keyword fallback (no embeddings available)
                terms = self._extract_topics(query, 2)
                if terms:
                    like = f"%{terms[0]}%"
                    cur.execute(
                        "SELECT id, question, answer, 0.0 AS sim FROM kb_entries"
                        " WHERE status = 'approved' AND (question ILIKE %s OR answer ILIKE %s)"
                        " LIMIT %s",
                        (like, like, max_kb),
                    )
                    out["kb"] = cur.fetchall()

            if out["kb"]:
                cur.execute(
                    "UPDATE kb_entries SET times_used = times_used + 1 WHERE id = ANY(%s)",
                    ([r["id"] for r in out["kb"]],),
                )
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            logger.warning("get_context failed: %s", exc)
        finally:
            conn.close()
        return out

    def build_prompt_block(self, user_key: str, query: str) -> str:
        """Ready-to-inject Spanish context block for the bot's prompt."""
        ctx = self.get_context(user_key, query)
        lines: list[str] = []
        p = ctx.get("profile")
        if p:
            lines.append("MEMORIA DEL USUARIO (privada, solo para personalizar tu respuesta):")
            if p.get("display_name"):
                lines.append(f"- Nombre: {p['display_name']}")
            if p.get("summary"):
                lines.append(f"- Perfil: {p['summary']}")
            topics = p.get("recurring_topics") or {}
            if topics:
                top = sorted(topics.items(), key=lambda kv: -int(kv[1]))[:5]
                lines.append("- Temas recurrentes: " + ", ".join(k for k, _ in top))
            if p.get("preferred_tone"):
                lines.append(f"- Tono preferido: {p['preferred_tone']}")
            facts = p.get("relevant_facts") or []
            if facts:
                lines.append("- Datos relevantes: " + "; ".join(str(f) for f in facts[:5]))
            lines.append(f"- Interacciones previas: {p.get('interactions', 0)}")
        if ctx.get("recent"):
            lines.append("ÚLTIMOS MENSAJES DE ESTA CONVERSACIÓN:")
            for r in ctx["recent"]:
                who = "Usuario" if r["role"] == "user" else "IntelliBot"
                lines.append(f"  {who}: {r['message'][:220]}")
        if ctx.get("semantic"):
            lines.append("RECUERDOS RELEVANTES DE CONVERSACIONES PASADAS DE ESTE USUARIO:")
            for r in ctx["semantic"]:
                lines.append(f"  - {r['message'][:220]}")
        if ctx.get("kb"):
            lines.append("BASE DE CONOCIMIENTO APROBADA (úsala como fuente confiable):")
            for r in ctx["kb"]:
                lines.append(f"  P: {r['question'][:180]}")
                lines.append(f"  R: {r['answer'][:400]}")
        return "\n".join(lines)

    @staticmethod
    def persona_block() -> str:
        return PERSONA["voice"]

    # ─────────────────────────────────────────────────────────────────────
    # 3. PROFILE CONSOLIDATION — run periodically (e.g. daily job)
    # ─────────────────────────────────────────────────────────────────────
    def consolidate_profile(self, user_key: str, llm_fn=None) -> str | None:
        """Distill the user's recent messages into a short profile summary.

        llm_fn: optional callable (prompt: str) -> str provided by the BOT
        (its own free LLM cascade). Keeping the LLM outside this module
        means zero coupling and zero new API costs here. Without llm_fn a
        heuristic summary (top topics) is written instead.
        """
        conn, cur = self._conn(user_key)
        try:
            cur.execute(
                "SELECT message FROM mem_conversations WHERE user_key = %s"
                " AND role = 'user' ORDER BY created_at DESC LIMIT 40",
                (user_key,),
            )
            msgs = [r["message"] for r in cur.fetchall()]
            if not msgs:
                return None
            summary = None
            if llm_fn:
                prompt = (
                    "Resume en 3-4 líneas el perfil de este usuario para "
                    "personalizar futuras respuestas: intereses, tono con que "
                    "escribe, y datos personales/profesionales que él mismo "
                    "haya mencionado. Sin inventar nada. Mensajes:\n- "
                    + "\n- ".join(m[:200] for m in msgs[:25])
                )
                try:
                    summary = (llm_fn(prompt) or "").strip()[:800] or None
                except Exception as exc:  # noqa: BLE001
                    logger.debug("llm_fn failed in consolidate_profile: %s", exc)
            if not summary:
                topics = Counter()
                for m in msgs:
                    topics.update(self._extract_topics(m))
                top = ", ".join(t for t, _ in topics.most_common(6))
                summary = f"Usuario con interés recurrente en: {top}." if top else None
            if summary:
                cur.execute(
                    "UPDATE mem_profiles SET summary = %s, updated_at = now()"
                    " WHERE user_key = %s",
                    (summary, user_key),
                )
                conn.commit()
            return summary
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            logger.warning("consolidate_profile failed: %s", exc)
            return None
        finally:
            conn.close()

    # ─────────────────────────────────────────────────────────────────────
    # 4. LEARNING — mine FAQs into PENDING KB entries (never auto-approved)
    # ─────────────────────────────────────────────────────────────────────
    def suggest_kb_from_conversations(self, min_freq: int = 3, days: int = 30) -> int:
        """Find questions asked by >= min_freq users; pair each with the most
        recent bot answer; insert as status='pending' for human review."""
        conn, cur = self._conn(admin=True)
        created = 0
        try:
            cur.execute(
                """
                SELECT message FROM mem_conversations
                WHERE role = 'user' AND created_at > now() - (%s || ' days')::interval
                """,
                (days,),
            )
            counts: Counter = Counter()
            originals: dict = {}
            for r in cur.fetchall():
                norm = _normalize(r["message"])[:200]
                if len(norm.split()) < 3 or "?" not in r["message"] and not norm.startswith(("que", "como", "cual", "cuando", "donde", "quien", "por que")):
                    continue
                counts[norm] += 1
                originals.setdefault(norm, r["message"])
            for norm, freq in counts.items():
                if freq < min_freq:
                    continue
                question = originals[norm][:400]
                qh = _question_hash(question)
                cur.execute("SELECT 1 FROM kb_entries WHERE question_hash = %s", (qh,))
                if cur.fetchone():
                    continue
                cur.execute(
                    """
                    SELECT c2.message FROM mem_conversations c1
                    JOIN mem_conversations c2
                      ON c2.user_key = c1.user_key AND c2.role = 'bot'
                     AND c2.created_at > c1.created_at
                     AND c2.created_at < c1.created_at + interval '3 minutes'
                    WHERE c1.role = 'user' AND c1.message = %s
                    ORDER BY c2.created_at DESC LIMIT 1
                    """,
                    (originals[norm],),
                )
                row = cur.fetchone()
                if not row:
                    continue
                answer = row["message"][:2000]
                if any(p in answer.lower() for p in _UNANSWERED_PATTERNS):
                    continue  # never learn from non-answers
                emb = self._embed(question)
                if emb:
                    cur.execute(
                        "INSERT INTO kb_entries (question, answer, source, status, question_hash, embedding)"
                        " VALUES (%s, %s, 'auto', 'pending', %s, %s::vector)"
                        " ON CONFLICT (question_hash) DO NOTHING",
                        (question, answer, qh, self._vec_literal(emb)),
                    )
                else:
                    cur.execute(
                        "INSERT INTO kb_entries (question, answer, source, status, question_hash)"
                        " VALUES (%s, %s, 'auto', 'pending', %s)"
                        " ON CONFLICT (question_hash) DO NOTHING",
                        (question, answer, qh),
                    )
                created += cur.rowcount
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            logger.warning("suggest_kb failed: %s", exc)
        finally:
            conn.close()
        return created

    # ── human-in-the-loop review ─────────────────────────────────────────
    def list_pending(self) -> list:
        conn, cur = self._conn(admin=True)
        try:
            cur.execute("SELECT id, question, answer, created_at FROM kb_entries"
                        " WHERE status = 'pending' ORDER BY created_at")
            return cur.fetchall()
        finally:
            conn.close()

    def review(self, kb_id: int, approve: bool, admin: str = "owner") -> bool:
        conn, cur = self._conn(admin=True)
        try:
            cur.execute(
                "UPDATE kb_entries SET status = %s, reviewed_by = %s, reviewed_at = now()"
                " WHERE id = %s AND status = 'pending'",
                ("approved" if approve else "rejected", admin, kb_id),
            )
            ok = cur.rowcount > 0
            conn.commit()
            return ok
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            logger.warning("review failed: %s", exc)
            return False
        finally:
            conn.close()

    def add_manual_kb(self, question: str, answer: str, topic: str | None = None) -> int | None:
        """Admin shortcut: insert curated knowledge directly as approved."""
        conn, cur = self._conn(admin=True)
        try:
            emb = self._embed(question)
            qh = _question_hash(question)
            if emb:
                cur.execute(
                    "INSERT INTO kb_entries (question, answer, topic, source, status, question_hash, embedding)"
                    " VALUES (%s, %s, %s, 'manual', 'approved', %s, %s::vector)"
                    " ON CONFLICT (question_hash) DO NOTHING RETURNING id",
                    (question, answer, topic, qh, self._vec_literal(emb)),
                )
            else:
                cur.execute(
                    "INSERT INTO kb_entries (question, answer, topic, source, status, question_hash)"
                    " VALUES (%s, %s, %s, 'manual', 'approved', %s)"
                    " ON CONFLICT (question_hash) DO NOTHING RETURNING id",
                    (question, answer, topic, qh),
                )
            row = cur.fetchone()
            conn.commit()
            return row["id"] if row else None
        finally:
            conn.close()

    def active_user_keys(self, hours: int = 24, limit: int = 100) -> list:
        """User keys with activity in the last N hours (for periodic jobs)."""
        conn, cur = self._conn(admin=True)
        try:
            cur.execute(
                "SELECT user_key, max(created_at) AS last_seen FROM mem_conversations"
                " WHERE created_at > now() - (%s || ' hours')::interval"
                " GROUP BY user_key ORDER BY last_seen DESC LIMIT %s",
                (hours, limit),
            )
            return [r["user_key"] for r in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001
            logger.debug("active_user_keys failed: %s", exc)
            return []
        finally:
            conn.close()

    # ─────────────────────────────────────────────────────────────────────
    # 5. METRICS / WEEKLY REPORT
    # ─────────────────────────────────────────────────────────────────────
    def weekly_report(self) -> str:
        conn, cur = self._conn(admin=True)
        try:
            cur.execute("SELECT * FROM v_weekly_report")
            g = cur.fetchone() or {}
            cur.execute(
                "SELECT detail, count(*) AS n FROM mem_metrics"
                " WHERE kind = 'topic' AND created_at > now() - interval '7 days'"
                " GROUP BY detail ORDER BY n DESC LIMIT 8")
            topics = cur.fetchall()
            cur.execute(
                "SELECT detail, count(*) AS n FROM mem_metrics"
                " WHERE kind = 'unanswered' AND created_at > now() - interval '7 days'"
                " GROUP BY detail ORDER BY n DESC LIMIT 10")
            unanswered = cur.fetchall()
            lines = [
                "REPORTE SEMANAL — IntelliBot Memoria & Aprendizaje",
                "=" * 50,
                f"Mensajes (7 días): {g.get('messages_7d', 0)}",
                f"Usuarios activos: {g.get('active_users_7d', 0)}",
                f"Preguntas sin respuesta: {g.get('unanswered_7d', 0)}",
                f"KB pendientes de revisión: {g.get('kb_pending', 0)} | aprobadas: {g.get('kb_approved', 0)}",
                "",
                "TEMAS MÁS CONSULTADOS:",
            ]
            lines += [f"  {t['detail']}: {t['n']}" for t in topics] or ["  (sin datos)"]
            lines += ["", "PREGUNTAS QUE EL BOT NO SUPO RESPONDER (candidatas a KB):"]
            lines += [f"  [{u['n']}x] {u['detail'][:90]}" for u in unanswered] or ["  (ninguna, excelente)"]
            return "\n".join(lines)
        finally:
            conn.close()


# ─────────────────────────────────────────────────────────────────────────
# CLI — the "simple review flow" (no new infra): run from any machine with
# DATABASE_URL set (e.g. locally or a Render shell).
# ─────────────────────────────────────────────────────────────────────────
def _cli() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    svc = MemoryService()
    cmd = sys.argv[1].lower()
    if cmd == "pending":
        rows = svc.list_pending()
        if not rows:
            print("No hay entradas pendientes. ✔")
        for r in rows:
            print(f"\n[#{r['id']}] {r['created_at']:%d-%m-%Y}")
            print(f"  P: {r['question']}")
            print(f"  R: {r['answer'][:300]}")
        print(f"\nTotal pendientes: {len(rows)}")
        print("Aprueba con: python memory_service.py approve <id>")
    elif cmd in ("approve", "reject") and len(sys.argv) >= 3:
        ok = svc.review(int(sys.argv[2]), approve=(cmd == "approve"))
        print("✔ Hecho" if ok else "✖ No se encontró la entrada pendiente")
    elif cmd == "suggest":
        n = svc.suggest_kb_from_conversations()
        print(f"✔ {n} nuevas entradas de KB creadas en estado 'pending'")
    elif cmd == "report":
        print(svc.weekly_report())
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
