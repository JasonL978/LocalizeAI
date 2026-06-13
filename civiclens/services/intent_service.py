"""
Intent Service
Responsibility: Detect language, translate, extract structured intent from raw message.
Publishes IntentEvent to 'intents' topic.
Knows nothing about resources or response generation.
"""
from __future__ import annotations
import logging
from shared.schemas import RawMessageEvent, IntentEvent, ResourceCategory, Urgency
from shared import kafka_client
from shared.llm_fallback import call_llm, extract_json
import config

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are the query optimization engine for NeighborBridge, a civic platform connecting people to resources.
Analyze the user's request (written in any language) and return ONLY a valid JSON object — no explanation, no markdown.

Your tasks:
1. Detect the user's native language (ISO 639-1 code AND human-readable name).
2. Translate the message to English.
3. Extract the physical location (city, neighborhood, or zip code). If none found, return "unknown".
4. Generate a 'search_query': a clean, descriptive English phrase optimized for semantic vector search.
   Focus on the underlying human need (hunger, safety, legal trouble) — not strict keywords.
5. Classify the primary need into a category.
6. Assess urgency and household context.

JSON schema to return:
{
  "detected_language": "<ISO 639-1 code, e.g. en, es, zh, ar, fr, vi, ko, pl>",
  "detected_language_name": "<Human-readable language name, e.g. Spanish, Mandarin Chinese, Arabic>",
  "english_translation": "<Full English translation of the user message>",
  "search_query": "<Optimized English descriptive phrase for semantic search, 10-20 words>",
  "category": "<exactly one of: food, shelter, legal, health, utility, other>",
  "location": "<city and state if mentioned, e.g. 'Chicago, IL'; 'unknown' if not mentioned>",
  "city": "<city name only; 'unknown' if not mentioned>",
  "zip_code": "<zip code if mentioned, null otherwise>",
  "urgency": "<urgent if immediate danger/crisis, otherwise standard>",
  "household_context": "<brief description: 'family with children', 'single adult', 'elderly', null if unclear>"
}

Rules:
- category MUST be one of: food, shelter, legal, health, utility, other
- urgency MUST be: urgent or standard
- search_query must describe the underlying human need, not repeat the message verbatim
- Return ONLY the JSON object, nothing else
"""


async def process(event_data: dict) -> None:
    raw = RawMessageEvent(**event_data)
    logger.info("Intent processing [%s]", raw.correlation_id)

    user_prompt = f"User message: {raw.raw_message}"

    try:
        llm_response = await call_llm(SYSTEM_PROMPT, user_prompt)
        parsed = extract_json(llm_response)

        category_raw = parsed.get("category", "other").lower()
        if category_raw not in config.VALID_CATEGORIES:
            category_raw = "other"

        urgency_raw = parsed.get("urgency", "standard").lower()
        if urgency_raw not in ("urgent", "standard"):
            urgency_raw = "standard"

        intent = IntentEvent(
            correlation_id=raw.correlation_id,
            raw_message=raw.raw_message,
            detected_language=parsed.get("detected_language", "en"),
            detected_language_name=parsed.get("detected_language_name", "English"),
            english_translation=parsed.get("english_translation", raw.raw_message),
            search_query=parsed.get("search_query", parsed.get("english_translation", raw.raw_message)),
            category=ResourceCategory(category_raw),
            location=parsed.get("location", "unknown"),
            city=parsed.get("city", "unknown"),
            zip_code=parsed.get("zip_code"),
            urgency=Urgency(urgency_raw),
            household_context=parsed.get("household_context"),
        )
    except Exception as e:
        logger.error("Intent extraction failed [%s]: %s", raw.correlation_id, e)
        intent = IntentEvent(
            correlation_id=raw.correlation_id,
            raw_message=raw.raw_message,
            detected_language="en",
            detected_language_name="English",
            english_translation=raw.raw_message,
            search_query=raw.raw_message,
            category=ResourceCategory.OTHER,
            location="unknown",
            city="unknown",
        )

    await kafka_client.publish(config.TOPIC_INTENTS, intent.model_dump())
    logger.info("Intent published [%s] → lang=%s cat=%s city=%s",
                intent.correlation_id, intent.detected_language,
                intent.category, intent.city)
