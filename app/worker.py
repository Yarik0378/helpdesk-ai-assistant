"""Background worker: LLM drafting and reply delivery.

Both stages pull from the `tickets` table with FOR UPDATE SKIP LOCKED, so any number of
worker processes can run side by side, and a crash only leaves rows with an expired
`locked_until` that the recovery step puts back in the queue.
"""
import asyncio
import json
import logging
import os

import asyncpg
import httpx

from . import kb, llm, ratelimit
from .db import create_pool, init_db

log = logging.getLogger("worker")

GEN_CONCURRENCY = int(os.getenv("GEN_CONCURRENCY", "40"))  # >= 60/min * 30s latency / 60
SEND_CONCURRENCY = int(os.getenv("SEND_CONCURRENCY", "20"))
MAX_GEN_ATTEMPTS = int(os.getenv("MAX_GEN_ATTEMPTS", "5"))
MAX_SEND_ATTEMPTS = int(os.getenv("MAX_SEND_ATTEMPTS", "8"))
REPLY_TIMEOUT = float(os.getenv("REPLY_TIMEOUT", "10"))


def backoff(attempts: int) -> int:
    return min(300, 5 * 2 ** max(0, attempts - 1))


async def recover_stale(pool: asyncpg.Pool) -> None:
    await pool.execute(
        "UPDATE tickets SET status='queued', locked_until=NULL "
        "WHERE status='generating' AND locked_until < now()"
    )
    # safe to re-send: delivery carries an Idempotency-Key
    await pool.execute(
        "UPDATE tickets SET status='approved', locked_until=NULL "
        "WHERE status='sending' AND locked_until < now()"
    )


# ---------- generation ----------

async def claim_for_generation(pool: asyncpg.Pool, limit: int):
    return await pool.fetch(
        """
        UPDATE tickets SET status='generating', locked_until = now() + interval '3 minutes',
               gen_attempts = gen_attempts + 1, updated_at = now()
        WHERE ticket_id IN (
            SELECT ticket_id FROM tickets
            WHERE status='queued' AND next_attempt_at <= now()
            ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED)
        RETURNING ticket_id, message, gen_attempts
        """,
        limit,
    )


async def generate(pool: asyncpg.Pool, t: asyncpg.Record) -> None:
    tid = t["ticket_id"]
    try:
        sources = await kb.search(pool, t["message"])
        await ratelimit.acquire(pool)
        text = await llm.draft(t["message"], sources)
        await pool.execute(
            "UPDATE tickets SET status='draft_ready', draft=$2, sources=$3::jsonb, "
            "locked_until=NULL, last_error=NULL, updated_at=now() "
            "WHERE ticket_id=$1 AND status='generating'",
            tid,
            text,
            json.dumps([{"id": s["id"], "title": s["title"]} for s in sources], ensure_ascii=False),
        )
        log.info("draft ready %s", tid)
    except Exception as exc:  # noqa: BLE001 - any failure must release the ticket
        retryable = isinstance(exc, llm.RetryableLLMError)
        give_up = (not retryable) or t["gen_attempts"] >= MAX_GEN_ATTEMPTS
        log.warning("generation failed %s (attempt %s): %r", tid, t["gen_attempts"], exc)
        await pool.execute(
            "UPDATE tickets SET status=$2, last_error=$3, locked_until=NULL, "
            "next_attempt_at = now() + make_interval(secs => $4), updated_at=now() "
            "WHERE ticket_id=$1 AND status='generating'",
            tid,
            "failed" if give_up else "queued",
            repr(exc)[:500],
            float(backoff(t["gen_attempts"])),
        )


# ---------- delivery ----------

async def claim_for_sending(pool: asyncpg.Pool, limit: int):
    return await pool.fetch(
        """
        UPDATE tickets SET status='sending', locked_until = now() + interval '60 seconds',
               send_attempts = send_attempts + 1, updated_at = now()
        WHERE ticket_id IN (
            SELECT ticket_id FROM tickets
            WHERE status='approved' AND next_attempt_at <= now()
            ORDER BY updated_at LIMIT $1 FOR UPDATE SKIP LOCKED)
        RETURNING ticket_id, reply_url, final_reply, send_attempts
        """,
        limit,
    )


async def send_reply(pool: asyncpg.Pool, client: httpx.AsyncClient, t: asyncpg.Record) -> None:
    tid = t["ticket_id"]
    error = None
    permanent = False
    try:
        resp = await client.post(
            t["reply_url"],
            json={"ticket_id": tid, "message": t["final_reply"]},
            headers={"Idempotency-Key": tid},  # a retry after a lost response must not double-post
            timeout=REPLY_TIMEOUT,
        )
        if resp.status_code in (200, 201, 409):  # 409 = helpdesk already has this reply
            await pool.execute(
                "UPDATE tickets SET status='sent', sent_at=now(), locked_until=NULL, last_error=NULL, "
                "updated_at=now() WHERE ticket_id=$1",
                tid,
            )
            log.info("reply sent %s", tid)
            return
        error = f"helpdesk returned {resp.status_code}"
        permanent = 400 <= resp.status_code < 500 and resp.status_code not in (408, 425, 429)
    except httpx.HTTPError as exc:
        error = f"network error: {exc!r}"

    give_up = permanent or t["send_attempts"] >= MAX_SEND_ATTEMPTS
    log.warning("send failed %s (attempt %s): %s", tid, t["send_attempts"], error)
    await pool.execute(
        "UPDATE tickets SET status=$2, last_error=$3, locked_until=NULL, "
        "next_attempt_at = now() + make_interval(secs => $4), updated_at=now() WHERE ticket_id=$1",
        tid,
        "send_failed" if give_up else "approved",
        error,
        float(backoff(t["send_attempts"])),
    )


# ---------- main loop ----------

async def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    pool = await create_pool()
    await init_db(pool)
    inflight = {"gen": 0, "send": 0}
    tasks: set[asyncio.Task] = set()

    def spawn(kind: str, coro) -> None:
        inflight[kind] += 1
        task = asyncio.create_task(coro)
        tasks.add(task)

        def done(d: asyncio.Task) -> None:
            tasks.discard(d)
            inflight[kind] -= 1

        task.add_done_callback(done)

    async with httpx.AsyncClient() as client:
        tick = 0
        while True:
            if tick % 10 == 0:
                await recover_stale(pool)
                await ratelimit.prune(pool)
            tick += 1

            if (free := GEN_CONCURRENCY - inflight["gen"]) > 0:
                for t in await claim_for_generation(pool, free):
                    spawn("gen", generate(pool, t))
            if (free := SEND_CONCURRENCY - inflight["send"]) > 0:
                for t in await claim_for_sending(pool, free):
                    spawn("send", send_reply(pool, client, t))

            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(run())
