"""Fake helpdesk that accepts replies and misbehaves on purpose.

FAIL_RATE: share of requests answered with 500 without storing anything.
SLOW_RATE: share of requests that store the reply but answer only after SLOW_SECONDS
           (longer than the client timeout) - the nasty "lost response" case that makes
           a naive retry post the same reply twice.
Replies are deduplicated by the Idempotency-Key header; /stats exposes duplicates so a
test can prove none slipped through.
"""
import asyncio
import os
import random

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

FAIL_RATE = float(os.getenv("FAIL_RATE", "0.2"))
SLOW_RATE = float(os.getenv("SLOW_RATE", "0.1"))
SLOW_SECONDS = float(os.getenv("SLOW_SECONDS", "12"))

app = FastAPI(title="Mock helpdesk")
replies: dict[str, dict] = {}  # ticket_id -> stored reply
stats = {"requests": 0, "stored": 0, "idempotent_hits": 0, "duplicate_replies": 0, "injected_500": 0, "injected_slow": 0}


@app.post("/tickets/{ticket_id}/replies")
async def post_reply(ticket_id: str, request: Request, idempotency_key: str | None = Header(default=None)):
    stats["requests"] += 1
    if random.random() < FAIL_RATE:
        stats["injected_500"] += 1
        return JSONResponse({"error": "temporary failure"}, status_code=500)

    body = await request.json()
    if idempotency_key and ticket_id in replies:
        stats["idempotent_hits"] += 1
    else:
        if ticket_id in replies:
            stats["duplicate_replies"] += 1  # the client delivered the same reply twice
        replies.setdefault(ticket_id, body)
        stats["stored"] += 1

    if random.random() < SLOW_RATE:
        stats["injected_slow"] += 1
        await asyncio.sleep(SLOW_SECONDS)
    return JSONResponse({"ticket_id": ticket_id, "status": "created"}, status_code=201)


@app.get("/stats")
async def get_stats():
    return {**stats, "tickets_with_reply": len(replies)}


@app.get("/replies")
async def get_replies():
    return replies
