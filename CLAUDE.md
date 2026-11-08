# Helpdesk AI assistant: notes for coding agents

Built with Claude Code (Sonnet 5.5) from the test brief in `README.md`. Read the README first.

## Layout
- `app/main.py` — webhook (`/webhooks/helpdesk`), operator UI, small JSON API.
- `app/worker.py` — generation + delivery loops. `app/llm.py`, `app/ratelimit.py`, `app/kb.py` are its parts.
- `app/db.py` — schema (plain SQL, `CREATE ... IF NOT EXISTS`) and KB seeding. `app/kb_seed.py` holds the articles.
- `mock_helpdesk/` — fake helpdesk with injected 500s and lost responses. `scripts/` — load test and bulk approve.

## Invariants (do not break)
- The webhook must never call the LLM or do slow work (helpdesk timeout is 2s). It only writes `events` + `tickets` in one transaction.
- `events.event_id` PRIMARY KEY is the dedup barrier. Every state change in `tickets` is guarded by `WHERE status = ...`.
- The LLM rate limit lives in Postgres (`llm_calls` + advisory lock), not in process memory, so it holds across workers.
- Delivery always sends `Idempotency-Key: <ticket_id>`; `sent` is set only after 200/201/409.

## Run
`docker compose up --build`; or locally: Postgres + `DATABASE_URL`, then `uvicorn app.main:app`, `python -m app.worker`, `uvicorn mock_helpdesk.main:app --port 9000`.
Use `STUB_DELAY_MIN/MAX` to shorten the no-API-key latency while testing.
