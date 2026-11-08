import html
import json
import os
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, field_validator

from .db import create_pool, init_db

# optional SSRF guard: comma-separated hosts that reply_url may point to
ALLOWED_REPLY_HOSTS = {h for h in os.getenv("ALLOWED_REPLY_HOSTS", "").split(",") if h}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool()
    await init_db(app.state.pool)
    yield
    await app.state.pool.close()


app = FastAPI(title="Helpdesk AI assistant", lifespan=lifespan)


class HelpdeskEvent(BaseModel):
    event_id: str
    ticket_id: str
    customer_email: str
    message: str
    reply_url: str

    @field_validator("reply_url")
    @classmethod
    def check_reply_url(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("reply_url must be an http(s) URL")
        if ALLOWED_REPLY_HOSTS and parsed.hostname not in ALLOWED_REPLY_HOSTS:
            raise ValueError("reply_url host is not allowed")
        return v


@app.post("/webhooks/helpdesk")
async def helpdesk_webhook(event: HelpdeskEvent):
    """Persist and ack. Must stay far below the helpdesk's 2s timeout, so no LLM here.

    The event row is the dedup barrier: a redelivery (or two concurrent deliveries)
    of the same event_id hits ON CONFLICT and does nothing. One transaction means we
    never ack an event we did not store.
    """
    async with app.state.pool.acquire() as conn:
        async with conn.transaction():
            inserted = await conn.fetchval(
                "INSERT INTO events (event_id, ticket_id) VALUES ($1, $2) "
                "ON CONFLICT (event_id) DO NOTHING RETURNING 1",
                event.event_id,
                event.ticket_id,
            )
            if inserted:
                # a second event for an existing ticket does not restart its pipeline
                await conn.execute(
                    "INSERT INTO tickets (ticket_id, first_event_id, customer_email, message, reply_url) "
                    "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (ticket_id) DO NOTHING",
                    event.ticket_id,
                    event.event_id,
                    event.customer_email,
                    event.message,
                    event.reply_url,
                )
    return {"status": "accepted" if inserted else "duplicate"}


@app.get("/healthz")
async def healthz():
    await app.state.pool.fetchval("SELECT 1")
    return {"ok": True}


# ---------- operator UI ----------

STATUS_ORDER = ["draft_ready", "queued", "generating", "approved", "sending", "sent", "rejected", "failed", "send_failed"]
STATUS_LABEL = {
    "draft_ready": "Очікує підтвердження",
    "queued": "У черзі на генерацію",
    "generating": "Генерується",
    "approved": "Підтверджено, у черзі на відправку",
    "sending": "Надсилається",
    "sent": "Надіслано",
    "rejected": "Відхилено",
    "failed": "Помилка генерації",
    "send_failed": "Помилка відправки",
}

PAGE = """<!doctype html><html lang="uk"><meta charset="utf-8"><title>Звернення</title>
<style>
body{{font:15px/1.4 system-ui,sans-serif;max-width:980px;margin:24px auto;padding:0 12px;color:#222}}
.t{{border:1px solid #ccc;border-radius:6px;padding:12px;margin:12px 0}}
.meta{{color:#666;font-size:13px}} textarea{{width:100%;min-height:110px;box-sizing:border-box}}
.tag{{display:inline-block;padding:1px 8px;border-radius:10px;background:#eee;font-size:12px}}
nav a{{margin-right:10px}} button{{padding:6px 14px;margin-right:6px}}
</style>
<h1>Звернення в підтримку</h1>
<nav>{nav}</nav>
{body}
</html>"""


def _nav(counts: dict[str, int], current: str | None) -> str:
    parts = [f'<a href="/">Усі ({sum(counts.values())})</a>']
    for s in STATUS_ORDER:
        if counts.get(s):
            mark = "<b>" if s == current else ""
            end = "</b>" if s == current else ""
            parts.append(f'<a href="/?status={s}">{mark}{STATUS_LABEL[s]} ({counts[s]}){end}</a>')
    parts.append('<a href="/kb">База знань</a>')
    return " ".join(parts)


def _sources_html(sources: list[dict]) -> str:
    if not sources:
        return ""
    links = ", ".join(f'<a href="/kb/{s["id"]}">{html.escape(s["title"])}</a>' for s in sources)
    return f'<div class="meta">Джерела: {links}</div>'


def _ticket_html(t) -> str:
    tid = html.escape(t["ticket_id"])
    status = t["status"]
    head = (
        f'<b>{tid}</b> <span class="tag">{STATUS_LABEL.get(status, status)}</span> '
        f'<span class="meta">{html.escape(t["customer_email"])}</span>'
    )
    msg = f'<p>{html.escape(t["message"])}</p>'
    sources = _sources_html(json.loads(t["sources"]) if isinstance(t["sources"], str) else t["sources"])
    err = f'<div class="meta">Помилка: {html.escape(t["last_error"])}</div>' if t["last_error"] else ""
    action = ""
    if status == "draft_ready":
        action = (
            f'<form method="post" action="/tickets/{tid}/approve">'
            f'<textarea name="reply">{html.escape(t["draft"] or "")}</textarea><br>'
            f'<button>Підтвердити й надіслати</button>'
            f'<button formaction="/tickets/{tid}/reject" formnovalidate>Відхилити</button></form>'
        )
    elif status in ("failed", "send_failed"):
        action = f'<form method="post" action="/tickets/{tid}/retry"><button>Повторити</button></form>'
    elif t["final_reply"] or t["draft"]:
        text = t["final_reply"] or t["draft"]
        action = f'<div class="meta">Відповідь:</div><p>{html.escape(text)}</p>'
    return f'<div class="t">{head}{msg}{sources}{err}{action}</div>'


@app.get("/", response_class=HTMLResponse)
async def index(status: str | None = None):
    pool = app.state.pool
    counts = {r["status"]: r["n"] for r in await pool.fetch("SELECT status, count(*) AS n FROM tickets GROUP BY 1")}
    rows = await pool.fetch(
        """
        SELECT * FROM tickets
        WHERE ($1::text IS NULL OR status = $1)
        ORDER BY (status = 'draft_ready') DESC, created_at DESC
        LIMIT 200
        """,
        status,
    )
    body = "".join(_ticket_html(r) for r in rows) or "<p>Звернень немає.</p>"
    return PAGE.format(nav=_nav(counts, status), body=body)


@app.post("/tickets/{ticket_id}/approve")
async def approve(ticket_id: str, reply: str = Form(...)):
    reply = reply.strip()
    if not reply:
        raise HTTPException(400, "reply is empty")
    # the status guard makes a double click / double submit a no-op
    await app.state.pool.execute(
        "UPDATE tickets SET status='approved', final_reply=$2, next_attempt_at=now(), updated_at=now() "
        "WHERE ticket_id=$1 AND status='draft_ready'",
        ticket_id,
        reply,
    )
    return RedirectResponse("/", status_code=303)


@app.post("/tickets/{ticket_id}/reject")
async def reject(ticket_id: str):
    await app.state.pool.execute(
        "UPDATE tickets SET status='rejected', updated_at=now() WHERE ticket_id=$1 AND status='draft_ready'",
        ticket_id,
    )
    return RedirectResponse("/", status_code=303)


@app.post("/tickets/{ticket_id}/retry")
async def retry(ticket_id: str):
    await app.state.pool.execute(
        """
        UPDATE tickets SET
          status = CASE status WHEN 'failed' THEN 'queued' ELSE 'approved' END,
          gen_attempts = CASE status WHEN 'failed' THEN 0 ELSE gen_attempts END,
          send_attempts = CASE status WHEN 'send_failed' THEN 0 ELSE send_attempts END,
          next_attempt_at = now(), last_error = NULL, updated_at = now()
        WHERE ticket_id = $1 AND status IN ('failed', 'send_failed')
        """,
        ticket_id,
    )
    return RedirectResponse("/", status_code=303)


@app.get("/kb", response_class=HTMLResponse)
async def kb_index():
    rows = await app.state.pool.fetch("SELECT id, title FROM articles ORDER BY id")
    items = "".join(f'<li><a href="/kb/{r["id"]}">{html.escape(r["title"])}</a></li>' for r in rows)
    return PAGE.format(nav='<a href="/">← Звернення</a>', body=f"<h2>База знань</h2><ul>{items}</ul>")


@app.get("/kb/{article_id}", response_class=HTMLResponse)
async def kb_article(article_id: int):
    row = await app.state.pool.fetchrow("SELECT title, body FROM articles WHERE id=$1", article_id)
    if not row:
        raise HTTPException(404)
    body = f'<h2>{html.escape(row["title"])}</h2><p>{html.escape(row["body"])}</p>'
    return PAGE.format(nav='<a href="/">← Звернення</a> <a href="/kb">База знань</a>', body=body)


# ---------- small JSON API for scripts / the load test ----------

@app.get("/api/tickets")
async def api_tickets(status: str | None = None, limit: int = 1000):
    rows = await app.state.pool.fetch(
        "SELECT ticket_id, status FROM tickets WHERE ($1::text IS NULL OR status=$1) ORDER BY created_at LIMIT $2",
        status,
        limit,
    )
    return JSONResponse([dict(r) for r in rows])


@app.get("/api/stats")
async def api_stats():
    rows = await app.state.pool.fetch("SELECT status, count(*) AS n FROM tickets GROUP BY 1")
    events = await app.state.pool.fetchval("SELECT count(*) FROM events")
    return {"events": events, "tickets": {r["status"]: r["n"] for r in rows}}
