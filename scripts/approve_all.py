"""Approve every ready draft as-is (stands in for the operator during load tests).

    python scripts/approve_all.py
"""
import asyncio
import sys

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


async def main() -> None:
    async with httpx.AsyncClient(base_url=BASE, timeout=10, follow_redirects=False) as client:
        tickets = (await client.get("/api/tickets", params={"status": "draft_ready"})).json()
        for t in tickets:
            await client.post(f"/tickets/{t['ticket_id']}/approve", data={"reply": "Дякуємо за звернення! Деталі у базі знань."})
        print(f"approved {len(tickets)} tickets")


if __name__ == "__main__":
    asyncio.run(main())
