"""
Cache abstraction.

LOCAL_MODE=true  → in-process dict (zero config)
LOCAL_MODE=false → Upstash Redis REST API (production, zero cost)

TTL is enforced in both modes.
"""
from __future__ import annotations
import time
import json
import logging
from typing import Any, Optional
import httpx
import config

logger = logging.getLogger(__name__)

# ── In-process cache (local mode) ─────────────────────────────────────────────
# Stores: { key: (value, expires_at_epoch) }
_local_cache: dict[str, tuple[Any, float]] = {}


def _local_get(key: str) -> Optional[Any]:
    entry = _local_cache.get(key)
    if entry is None:
        return None
    value, expires_at = entry
    if time.time() > expires_at:
        del _local_cache[key]
        return None
    return value


def _local_set(key: str, value: Any, ttl: int) -> None:
    _local_cache[key] = (value, time.time() + ttl)


# ── Upstash Redis REST helpers ─────────────────────────────────────────────────
def _redis_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {config.REDIS_TOKEN}"}


async def _redis_get(key: str) -> Optional[Any]:
    url = f"{config.REDIS_URL}/get/{key}"
    async with httpx.AsyncClient(timeout=5) as client:
        resp = await client.get(url, headers=_redis_headers())
        data = resp.json()
        if data.get("result") is None:
            return None
        return json.loads(data["result"])


async def _redis_set(key: str, value: Any, ttl: int) -> None:
    url = f"{config.REDIS_URL}/set/{key}"
    async with httpx.AsyncClient(timeout=5) as client:
        await client.post(
            url,
            headers=_redis_headers(),
            json={"value": json.dumps(value), "ex": ttl},
        )


# ── Public API ─────────────────────────────────────────────────────────────────

async def get(key: str) -> Optional[Any]:
    try:
        if config.LOCAL_MODE:
            return _local_get(key)
        return await _redis_get(key)
    except Exception as e:
        logger.warning("Cache GET failed: %s", e)
        return None


async def set(key: str, value: Any, ttl: int = config.CACHE_TTL_SECONDS) -> None:
    try:
        if config.LOCAL_MODE:
            _local_set(key, value, ttl)
        else:
            await _redis_set(key, value, ttl)
    except Exception as e:
        logger.warning("Cache SET failed: %s", e)


def build_cache_key(category: str, city: str, language: str, query: str = "") -> str:
    import hashlib
    base = f"civiclens:{category}:{city.lower().replace(' ', '_')}:{language}"
    if query:
        q_hash = hashlib.md5(query.lower().strip().encode()).hexdigest()[:8]
        return f"{base}:{q_hash}"
    return base
