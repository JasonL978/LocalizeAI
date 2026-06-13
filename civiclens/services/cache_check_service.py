"""
Cache Check Service
Responsibility: Check if an identical query was recently served.
Cache HIT  → publish directly to 'responses' (skip matching + LLM response gen).
Cache MISS → forward to 'needs-matching'.
Knows nothing about LLMs or how resources are fetched.
"""
from __future__ import annotations
import logging
from shared.schemas import IntentEvent, ResponseEvent
from shared import kafka_client, redis_client
import config

logger = logging.getLogger(__name__)


async def process(event_data: dict) -> None:
    intent = IntentEvent(**event_data)
    logger.info("Cache check [%s]", intent.correlation_id)

    cache_key = redis_client.build_cache_key(
        intent.category.value, intent.city, intent.detected_language
    )
    cached = await redis_client.get(cache_key)

    if cached:
        logger.info("Cache HIT [%s] → bypassing matcher + LLM", intent.correlation_id)
        response = ResponseEvent(
            correlation_id=intent.correlation_id,
            detected_language=intent.detected_language,
            response_text=cached["response_text"],
            resources_count=cached["resources_count"],
        )
        await kafka_client.publish(config.TOPIC_RESPONSES, response.model_dump())
    else:
        logger.info("Cache MISS [%s] → forwarding to matcher", intent.correlation_id)
        await kafka_client.publish(config.TOPIC_NEEDS_MATCHING, intent.model_dump())
