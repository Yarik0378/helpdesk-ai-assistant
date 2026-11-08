import asyncio
import os

import asyncpg

from .kb_seed import ARTICLES

DSN = os.getenv("DATABASE_URL", "postgresql://app:app@localhost:5432/app")

SCHEMA = """
-- every webhook delivery is recorded by event_id: this is the dedup barrier
CREATE TABLE IF NOT EXISTS events (
    event_id    text PRIMARY KEY,
    ticket_id   text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

-- tickets double as the durable job queue (status + next_attempt_at)
-- status: queued -> generating -> draft_ready -> approved -> sending -> sent
--         side exits: rejected, failed (LLM gave up), send_failed (helpdesk gave up)
CREATE TABLE IF NOT EXISTS tickets (
    ticket_id        text PRIMARY KEY,
    first_event_id   text NOT NULL,
    customer_email   text NOT NULL,
    message          text NOT NULL,
    reply_url        text NOT NULL,
    status           text NOT NULL DEFAULT 'queued',
    draft            text,
    sources          jsonb NOT NULL DEFAULT '[]',
    final_reply      text,
    gen_attempts     int NOT NULL DEFAULT 0,
    send_attempts    int NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    locked_until     timestamptz,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    sent_at          timestamptz
);
CREATE INDEX IF NOT EXISTS tickets_queue_idx ON tickets (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS articles (
    id    serial PRIMARY KEY,
    title text NOT NULL,
    body  text NOT NULL
);

-- sliding-window log used to enforce the global LLM rate limit across workers
CREATE TABLE IF NOT EXISTS llm_calls (
    id        bigserial PRIMARY KEY,
    called_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS llm_calls_at_idx ON llm_calls (called_at);
"""


async def create_pool() -> asyncpg.Pool:
    last = None
    for _ in range(60):
        try:
            return await asyncpg.create_pool(
                DSN, min_size=2, max_size=int(os.getenv("DB_POOL_MAX", "30"))
            )
        except (OSError, asyncpg.PostgresError) as exc:
            last = exc
            await asyncio.sleep(1)
    raise RuntimeError(f"database is not reachable: {last}")


async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        # api and worker start together; the advisory lock serialises schema setup
        await conn.execute("SELECT pg_advisory_lock(4242)")
        try:
            await conn.execute(SCHEMA)
            if await conn.fetchval("SELECT count(*) FROM articles") == 0:
                await conn.executemany(
                    "INSERT INTO articles (title, body) VALUES ($1, $2)", ARTICLES
                )
        finally:
            await conn.execute("SELECT pg_advisory_unlock(4242)")
