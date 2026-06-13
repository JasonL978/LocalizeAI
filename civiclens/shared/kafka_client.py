"""
Message bus abstraction.

LOCAL_MODE=true  → asyncio.Queue (in-process, zero config)
LOCAL_MODE=false → Upstash Kafka REST API (production, zero cost)

Services never import from each other — they only call publish/consume here.
Swapping the backend requires changing nothing outside this file.
"""
from __future__ import annotations
import asyncio
import json
import base64
import logging
from typing import Any, Dict, Optional
import httpx
import config

logger = logging.getLogger(__name__)

# ── In-process queues (local mode) ────────────────────────────────────────────
_local_queues: Dict[str, asyncio.Queue] = {}


def _get_local_queue(topic: str) -> asyncio.Queue:
    if topic not in _local_queues:
        _local_queues[topic] = asyncio.Queue()
    return _local_queues[topic]


# ── Upstash Kafka REST helpers ─────────────────────────────────────────────────
def _kafka_headers() -> Dict[str, str]:
    creds = base64.b64encode(
        f"{config.KAFKA_USERNAME}:{config.KAFKA_PASSWORD}".encode()
    ).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}


async def _kafka_produce(topic: str, payload: Dict) -> None:
    url = f"{config.KAFKA_URL}/produce/{topic}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            url,
            headers=_kafka_headers(),
            json={"value": json.dumps(payload)},
        )
        resp.raise_for_status()


async def _kafka_consume(topic: str, group: str) -> Optional[Dict]:
    url = f"{config.KAFKA_URL}/consume/{group}/consumer/{topic}"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, headers=_kafka_headers())
        resp.raise_for_status()
        messages = resp.json()
        if messages:
            return json.loads(messages[0]["value"])
    return None


# ── Public API ─────────────────────────────────────────────────────────────────

async def publish(topic: str, payload: Dict[str, Any]) -> None:
    if config.LOCAL_MODE:
        await _get_local_queue(topic).put(payload)
        logger.debug("LOCAL publish → %s", topic)
    else:
        await _kafka_produce(topic, payload)
        logger.debug("KAFKA publish → %s", topic)


async def consume(topic: str, group: str = "civiclens") -> Optional[Dict[str, Any]]:
    if config.LOCAL_MODE:
        try:
            return await asyncio.wait_for(_get_local_queue(topic).get(), timeout=30)
        except asyncio.TimeoutError:
            return None
    else:
        return await _kafka_consume(topic, group)


async def consume_nowait(topic: str) -> Optional[Dict[str, Any]]:
    """Non-blocking consume for local mode polling loops."""
    if config.LOCAL_MODE:
        try:
            return _get_local_queue(topic).get_nowait()
        except asyncio.QueueEmpty:
            return None
    return await consume(topic)
