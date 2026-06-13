"""
CivicLens — Main Orchestrator

LOCAL_MODE (default): Runs the full event-driven pipeline in-process using
asyncio queues. All 6 services run as independent coroutines that communicate
only through the message bus abstraction — identical contract to Kafka production.

PRODUCTION MODE: Each service is deployed as an independent Cloudflare Worker
or separate process. This file becomes the gateway entry point only.

Entry points:
  - run_pipeline(message) → response text  (called by Gradio frontend)
  - python main.py                         (standalone CLI for testing)
"""
from __future__ import annotations
import asyncio
import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

import config
from shared import kafka_client
from services import (
    gateway_service,
    intent_service,
    cache_check_service,
    matcher_service,
    response_service,
    delivery_service,
)


async def _run_local_pipeline(raw_message: str) -> str:
    """
    Drives all services sequentially through the message bus.
    Each service only reads from its input topic and writes to its output topic —
    no direct calls between services. Swap to Kafka by setting LOCAL_MODE=false.
    """
    correlation_id = await gateway_service.handle(raw_message)

    raw_event = await kafka_client.consume(config.TOPIC_RAW_MESSAGES)
    await intent_service.process(raw_event)

    intent_event = await kafka_client.consume(config.TOPIC_INTENTS)
    await cache_check_service.process(intent_event)

    # After cache check: message lands in either needs-matching (cache miss)
    # or responses (cache hit). Poll both with a short timeout to find which.
    needs_matching_q = kafka_client._get_local_queue(config.TOPIC_NEEDS_MATCHING)
    responses_q = kafka_client._get_local_queue(config.TOPIC_RESPONSES)

    cache_hit_event = None
    try:
        cache_hit_event = needs_matching_q.get_nowait()
    except asyncio.QueueEmpty:
        pass

    if cache_hit_event is not None:
        # Cache miss path: run matcher → response generator
        await matcher_service.process(cache_hit_event)
        matches_event = await kafka_client.consume(config.TOPIC_MATCHES)
        await response_service.process(matches_event)

    response_event = await asyncio.wait_for(responses_q.get(), timeout=60)
    await delivery_service.process(response_event)

    delivered = delivery_service.get_response(correlation_id)
    return delivered.response_text if delivered else response_event.get("response_text", "")


async def run_pipeline(message: str) -> str:
    """Public API called by the Gradio frontend."""
    try:
        return await _run_local_pipeline(message)
    except Exception as e:
        logger.error("Pipeline error: %s", e, exc_info=True)
        raise


# ── CLI entry point ────────────────────────────────────────────────────────────

async def _cli():
    print("\n" + "═" * 60)
    print("  CivicLens — Multilingual Community Resource Finder")
    print("  Type your need in any language. Type 'exit' to quit.")
    print("═" * 60 + "\n")

    while True:
        try:
            message = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye / Adiós / 再见")
            break

        if not message:
            continue
        if message.lower() in ("exit", "quit", "bye"):
            print("Goodbye / Adiós / 再见")
            break

        print("Finding resources...\n")
        try:
            response = await run_pipeline(message)
            print(f"CivicLens: {response}\n")
        except Exception as e:
            print(f"Error: {e}\nPlease call 211 for immediate assistance.\n")


if __name__ == "__main__":
    asyncio.run(_cli())
