"""
Gateway Service
Responsibility: Accept a raw user message, assign a correlation ID,
                publish to raw-messages topic.
Knows nothing about intent, matching, or response.
"""
from __future__ import annotations
import logging
from shared.schemas import RawMessageEvent, Channel
from shared import kafka_client
import config

logger = logging.getLogger(__name__)


async def handle(raw_message: str, channel: str = "web") -> str:
    """
    Entry point called by the frontend.
    Returns the correlation_id so the frontend can await the response.
    """
    event = RawMessageEvent(
        raw_message=raw_message.strip(),
        channel=Channel(channel),
    )
    await kafka_client.publish(config.TOPIC_RAW_MESSAGES, event.model_dump())
    logger.info("Gateway published [%s] → raw-messages", event.correlation_id)
    return event.correlation_id
