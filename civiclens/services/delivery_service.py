"""
Delivery Service
Responsibility: Route the final response to the correct delivery channel.
In LOCAL mode: stores in an in-process dict keyed by correlation_id.
In PRODUCTION mode: pushes to Supabase Realtime so the frontend WebSocket receives it.
Knows nothing about how the response was generated.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Dict, Optional
from shared.schemas import ResponseEvent
import config

logger = logging.getLogger(__name__)

# ── Local delivery store (in-process) ─────────────────────────────────────────
# Maps correlation_id → ResponseEvent (set by delivery, polled by frontend)
_pending: Dict[str, ResponseEvent] = {}
_events: Dict[str, asyncio.Event] = {}


def register_correlation(correlation_id: str) -> asyncio.Event:
    """Frontend calls this before sending the message, then awaits the event."""
    ev = asyncio.Event()
    _events[correlation_id] = ev
    return ev


def get_response(correlation_id: str) -> Optional[ResponseEvent]:
    return _pending.pop(correlation_id, None)


# ── Supabase Realtime delivery (production) ────────────────────────────────────

async def _push_supabase_realtime(response: ResponseEvent) -> None:
    from supabase import create_client
    client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
    client.table("responses").insert({
        "correlation_id": response.correlation_id,
        "detected_language": response.detected_language,
        "response_text": response.response_text,
        "resources_count": response.resources_count,
        "timestamp": response.timestamp,
    }).execute()


# ── Public processor ───────────────────────────────────────────────────────────

async def process(event_data: dict) -> None:
    response = ResponseEvent(**event_data)
    logger.info("Delivery [%s] → channel=%s",
                response.correlation_id,
                "local" if config.LOCAL_MODE else "supabase-realtime")

    if config.LOCAL_MODE:
        _pending[response.correlation_id] = response
        ev = _events.pop(response.correlation_id, None)
        if ev:
            ev.set()
    else:
        await _push_supabase_realtime(response)

    logger.info("Delivered [%s]", response.correlation_id)
