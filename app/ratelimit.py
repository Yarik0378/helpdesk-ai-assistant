import asyncio
import os
import random

import asyncpg

LLM_RPM = int(os.getenv("LLM_RPM", "60"))
WINDOW_SECONDS = 60


async def acquire(pool: asyncpg.Pool) -> None:
    """Block until the global sliding window (LLM_RPM calls / 60s) has room, then take a slot.

    State lives in Postgres, so the limit holds across any number of worker processes
    and survives restarts. A transaction-level advisory lock makes check+insert atomic.
    """
    while True:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(1001)")
                row = await conn.fetchrow(
                    """
                    SELECT count(*) AS n,
                           COALESCE(extract(epoch FROM (min(called_at) + make_interval(secs => $1) - now())), 0) AS wait
                    FROM llm_calls WHERE called_at > now() - make_interval(secs => $1)
                    """,
                    float(WINDOW_SECONDS),
                )
                if row["n"] < LLM_RPM:
                    await conn.execute("INSERT INTO llm_calls DEFAULT VALUES")
                    return
                wait = float(row["wait"])
        # connection is released while sleeping; jitter avoids a thundering herd
        await asyncio.sleep(max(0.2, wait) + random.uniform(0, 0.5))


async def prune(pool: asyncpg.Pool) -> None:
    await pool.execute("DELETE FROM llm_calls WHERE called_at < now() - interval '10 minutes'")
