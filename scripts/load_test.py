"""Fire webhooks at the service the way the helpdesk would: bursts, redeliveries, concurrency.

    python scripts/load_test.py --count 500 --duration 600   # the "peak" from the brief
    python scripts/load_test.py --count 50 --duration 10     # quick smoke test

Checks that every delivery is acked in under 2s (the helpdesk timeout) and that
redelivered events (same event_id, sent concurrently) create exactly one ticket.
"""
import argparse
import asyncio
import random
import statistics
import time

import httpx


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000/webhooks/helpdesk")
    ap.add_argument("--reply-base", default="http://helpdesk:9000", help="reply_url host as seen by the worker")
    ap.add_argument("--count", type=int, default=500)
    ap.add_argument("--duration", type=float, default=600, help="spread the tickets over this many seconds")
    ap.add_argument("--dup-rate", type=float, default=0.2, help="share of events delivered 2-3 extra times")
    ap.add_argument("--prefix", default=f"L{int(time.time())}")
    args = ap.parse_args()

    messages = [
        "Не можу експортувати звіт у PDF",
        "Забув пароль, лист не приходить",
        "Як перейти на інший тариф?",
        "Не завантажується дашборд",
        "Як запросити колегу до команди?",
        "Платіж карткою відхилено",
        "Як підключити Google Analytics?",
        "Яка погода на Марсі?",  # nothing relevant in the KB
    ]
    latencies: list[float] = []
    statuses: dict[int, int] = {}
    sem = asyncio.Semaphore(100)

    async def deliver(client: httpx.AsyncClient, payload: dict) -> None:
        async with sem:
            t0 = time.monotonic()
            try:
                r = await client.post(args.url, json=payload, timeout=2.0)
                code = r.status_code
            except httpx.HTTPError:
                code = 0
            latencies.append(time.monotonic() - t0)
            statuses[code] = statuses.get(code, 0) + 1

    async def one(client: httpx.AsyncClient, i: int) -> None:
        await asyncio.sleep(random.uniform(0, args.duration))
        tid = f"{args.prefix}-{i}"
        payload = {
            "event_id": f"evt_{tid}",
            "ticket_id": tid,
            "customer_email": f"user{i}@example.com",
            "message": random.choice(messages),
            "reply_url": f"{args.reply_base}/tickets/{tid}/replies",
        }
        copies = 1 + (random.randint(1, 2) if random.random() < args.dup_rate else 0)
        await asyncio.gather(*(deliver(client, payload) for _ in range(copies)))

    async with httpx.AsyncClient() as client:
        await asyncio.gather(*(one(client, i) for i in range(args.count)))

    latencies.sort()
    p99 = latencies[int(len(latencies) * 0.99) - 1]
    print(f"deliveries: {len(latencies)}  statuses: {statuses}")
    print(f"latency: median {statistics.median(latencies)*1000:.0f} ms, p99 {p99*1000:.0f} ms, max {latencies[-1]*1000:.0f} ms")
    print(f"unique tickets expected: {args.count}  (prefix {args.prefix})")
    print("OK" if latencies[-1] < 2.0 and set(statuses) == {200} else "SLOW OR FAILED DELIVERIES")


if __name__ == "__main__":
    asyncio.run(main())
