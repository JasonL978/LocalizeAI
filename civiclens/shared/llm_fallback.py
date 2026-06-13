"""
LLM Fallback Chain: Groq → Gemini → HuggingFace → Static

Each provider is tried in order. On rate-limit or error, the next is used.
Adding a new provider: implement _call_<name>, append to PROVIDERS list.
Removing a provider: delete its entry from PROVIDERS. Nothing else changes.
"""
from __future__ import annotations
import asyncio
import json
import logging
import re
from typing import Any, Callable, Coroutine, Optional
import httpx
import config

logger = logging.getLogger(__name__)

LLMCallable = Callable[[str, str], Coroutine[Any, Any, str]]


# ── Provider 1: Groq ───────────────────────────────────────────────────────────

async def _call_groq(system_prompt: str, user_prompt: str) -> str:
    if not config.GROQ_API_KEY:
        raise ValueError("GROQ_API_KEY not set")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {config.GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": config.GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.3,
                "max_tokens": 1024,
            },
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


# ── Provider 2: Google Gemini ──────────────────────────────────────────────────

async def _call_gemini(system_prompt: str, user_prompt: str) -> str:
    if not config.GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not set")
    combined = f"{system_prompt}\n\n{user_prompt}"
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{config.GEMINI_MODEL}:generateContent?key={config.GEMINI_API_KEY}"
    )
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            json={
                "contents": [{"parts": [{"text": combined}]}],
                "generationConfig": {"temperature": 0.3, "maxOutputTokens": 1024},
            },
        )
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]


# ── Provider 3: HuggingFace Inference API ─────────────────────────────────────

async def _call_huggingface(system_prompt: str, user_prompt: str) -> str:
    if not config.HF_API_TOKEN:
        raise ValueError("HF_API_TOKEN not set")
    prompt = f"<s>[INST] {system_prompt}\n\n{user_prompt} [/INST]"
    url = f"https://api-inference.huggingface.co/models/{config.HF_MODEL}"
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {config.HF_API_TOKEN}"},
            json={"inputs": prompt, "parameters": {"max_new_tokens": 512, "temperature": 0.3}},
        )
        resp.raise_for_status()
        result = resp.json()
        if isinstance(result, list):
            return result[0].get("generated_text", "").replace(prompt, "").strip()
        return str(result)


# ── Fallback chain ─────────────────────────────────────────────────────────────

PROVIDERS: list[tuple[str, LLMCallable]] = [
    ("Groq", _call_groq),
    ("Gemini", _call_gemini),
    ("HuggingFace", _call_huggingface),
]


async def call_llm(system_prompt: str, user_prompt: str) -> str:
    """Try each provider in order. On 429, waits for Retry-After then retries once."""
    last_error: Optional[Exception] = None
    for name, fn in PROVIDERS:
        try:
            logger.info("LLM call via %s", name)
            result = await fn(system_prompt, user_prompt)
            logger.info("LLM success via %s", name)
            return result
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("retry-after", 5))
                retry_after = min(retry_after, 30)  # cap at 30s
                logger.warning("Rate limited by %s — waiting %ds then retrying", name, retry_after)
                await asyncio.sleep(retry_after)
                try:
                    result = await fn(system_prompt, user_prompt)
                    logger.info("LLM success via %s (after retry)", name)
                    return result
                except Exception as retry_err:
                    logger.warning("LLM provider %s failed after retry: %s", name, retry_err)
                    last_error = retry_err
            else:
                logger.warning("LLM provider %s failed: %s", name, e)
                last_error = e
        except Exception as e:
            logger.warning("LLM provider %s failed: %s", name, e)
            last_error = e

    logger.error("All LLM providers failed. Last error: %s", last_error)
    raise RuntimeError("All LLM providers exhausted") from last_error


def extract_json(text: str) -> dict:
    """Pull the first JSON object out of an LLM response string."""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON found in LLM response: {text[:200]}")
    return json.loads(match.group())
