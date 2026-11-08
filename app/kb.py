import re

import asyncpg

STOP = {"для", "при", "щоб", "який", "яка", "які", "можу", "можна", "мені", "його", "вона", "якщо", "this", "that"}


def build_tsquery(text: str) -> str | None:
    """OR-query over word prefixes: 'експортувати' and 'експорт' both become 'експор:*'.

    The 'simple' config has no Ukrainian stemmer, so prefix matching is a cheap stand-in.
    Tokens are \\w-only, so interpolating them into to_tsquery is safe.
    """
    words = re.findall(r"\w{3,}", text.lower())
    stems = sorted({w[:6] for w in words if w not in STOP})
    return " | ".join(f"{s}:*" for s in stems) or None


async def search(pool: asyncpg.Pool, text: str, limit: int = 3) -> list[dict]:
    query = build_tsquery(text)
    if not query:
        return []
    rows = await pool.fetch(
        """
        SELECT id, title, body,
               ts_rank(to_tsvector('simple', title || ' ' || body), to_tsquery('simple', $1)) AS rank
        FROM articles
        WHERE to_tsvector('simple', title || ' ' || body) @@ to_tsquery('simple', $1)
        ORDER BY rank DESC, id
        LIMIT $2
        """,
        query,
        limit,
    )
    return [{"id": r["id"], "title": r["title"], "body": r["body"]} for r in rows]
