import asyncio
import os
import random
import time

import httpx

API_URL = "https://api.anthropic.com/v1/messages"
MODEL = os.getenv("LLM_MODEL", "claude-haiku-5-5")
API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
# the brief says one call takes 5-30s; the stub mode imitates exactly that
STUB_DELAY = (float(os.getenv("STUB_DELAY_MIN", "5")), float(os.getenv("STUB_DELAY_MAX", "30")))


class RetryableLLMError(Exception):
    """429 / 5xx / network trouble: worth another attempt later."""


def _prompt(message: str, sources: list[dict]) -> str:
    kb = "\n\n".join(f"[{i}] {s['title']}\n{s['body']}" for i, s in enumerate(sources, 1))
    return (
        "Ти асистент служби підтримки. Підготуй чернетку відповіді клієнту українською, "
        "ґрунтуючись ЛИШЕ на статтях бази знань нижче. Посилайся на статті як [1], [2]. "
        "Якщо статті не відповідають на питання, скажи, що передаєш звернення колегам.\n\n"
        f"Статті:\n{kb or '(нічого не знайдено)'}\n\nЗвернення клієнта:\n{message}"
    )


def static_draft(message: str, sources: list[dict]) -> str:
    if not sources:
        return (
            "Дякуємо за звернення! У нашій базі знань немає готової відповіді на ваше питання, "
            "тому ми передали його колезі, який відповість найближчим часом."
        )
    first = sources[0]
    refs = ", ".join(f"[{i}] {s['title']}" for i, s in enumerate(sources, 1))
    return (
        f"Дякуємо за звернення! Ось що може допомогти ({first['title']} [1]):\n\n"
        f"{first['body']}\n\nДетальніше: {refs}.\n\nЯкщо проблема лишиться, напишіть нам ще раз."
    )


async def draft(message: str, sources: list[dict]) -> str:
    """Draft a reply with the LLM.

    Without ANTHROPIC_API_KEY the real request is still sent (it gets rejected, and its
    outcome is ignored) and a static draft is returned after the same 5-30s latency a
    real call would have, so the rest of the system behaves identically.
    """
    started = time.monotonic()
    payload = {
        "model": MODEL,
        "max_tokens": 600,
        "messages": [{"role": "user", "content": _prompt(message, sources)}],
    }
    headers = {"x-api-key": API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(API_URL, headers=headers, json=payload)
    except httpx.HTTPError as exc:
        if API_KEY:
            raise RetryableLLMError(f"network error: {exc!r}") from exc
        resp = None  # stub mode: an unreachable provider changes nothing

    if API_KEY:
        if resp.status_code == 429 or resp.status_code >= 500:
            raise RetryableLLMError(f"provider returned {resp.status_code}")
        resp.raise_for_status()
        return resp.json()["content"][0]["text"].strip()

    await asyncio.sleep(max(0.0, random.uniform(*STUB_DELAY) - (time.monotonic() - started)))
    return static_draft(message, sources)
