"""
Response Service
Responsibility: Generate a warm, culturally competent response in the user's language.
Writes the result to cache, then publishes to 'responses' topic.
Knows nothing about delivery channel or UI.
"""
from __future__ import annotations
import logging
from shared.schemas import MatchesEvent, ResponseEvent, Resource
from shared import kafka_client, redis_client
from shared.llm_fallback import call_llm
import config

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are a warm, empathetic, and culturally competent community care coordinator for a nonprofit.
Your role is to help community members find local resources with genuine care and dignity.

CRITICAL RULES:
1. Respond entirely in the user's detected native language — never switch languages.
2. Use a warm, non-judgmental, supportive tone — never clinical or bureaucratic.
3. Rely ONLY on the provided resource data (addresses, hours, phone numbers, details).
   Do NOT invent or assume any information. If a field is missing from the data, omit it entirely.
4. Format output using clean markdown bullet points — optimized for mobile readability.
5. If no resources were found, express genuine empathy and direct them to call 211
   (free, confidential, 24/7 nationwide helpline) — never say you cannot help.
6. End with a brief offer to help further.
7. Keep total response under 220 words.
"""


def _format_resources(resources: list[Resource]) -> str:
    if not resources:
        return "No matching resources found in the database."
    lines = []
    for i, r in enumerate(resources, 1):
        lines.append(f"{i}. {r.name} ({r.organization})")
        lines.append(f"   Address: {r.address}, {r.city}, {r.state}")
        if r.phone:
            lines.append(f"   Phone: {r.phone}")
        if r.hours:
            lines.append(f"   Hours: {r.hours}")
        if r.accepts_walk_in:
            lines.append(f"   Walk-ins welcome")
        if r.languages_supported:
            lines.append(f"   Languages: {r.languages_supported.replace('|', ', ')}")
    return "\n".join(lines)


_STATIC_FALLBACK = {
    "en": "I'm sorry, I'm having trouble right now. Please call 211 — it's a free, confidential helpline available 24/7 that can connect you with local resources. Is there anything else I can help with?",
    "es": "Lo siento, estoy teniendo problemas ahora mismo. Por favor llame al 211 — es una línea de ayuda gratuita y confidencial disponible las 24 horas que puede conectarle con recursos locales. ¿Hay algo más en que pueda ayudar?",
    "zh": "抱歉，我现在遇到了问题。请拨打211——这是一个免费、保密的全天候帮助热线，可以为您联系当地资源。还有什么我可以帮助您的吗？",
    "ar": "أنا آسف، أواجه مشكلة الآن. يرجى الاتصال بـ 211 - إنه خط مساعدة مجاني وسري متاح على مدار الساعة يمكنه ربطك بالموارد المحلية. هل هناك أي شيء آخر يمكنني مساعدتك به؟",
}


async def process(event_data: dict) -> None:
    matches = MatchesEvent(**event_data)
    intent = matches.intent
    logger.info("Response service [%s] lang=%s resources=%d",
                matches.correlation_id, intent.detected_language, len(matches.resources))

    resource_text = _format_resources(matches.resources)
    found_count = len(matches.resources)

    user_prompt = f"""\
Detected Language: {intent.detected_language_name}

User's original request: {intent.raw_message}
Search query (their underlying need): {intent.search_query}
Urgency: {intent.urgency.value}
Household: {intent.household_context or 'not specified'}

Verified Resource Matches from DB:
{resource_text}

Respond entirely in {intent.detected_language_name} using the resource data above.
"""

    try:
        response_text = await call_llm(SYSTEM_PROMPT, user_prompt)
    except RuntimeError:
        response_text = _STATIC_FALLBACK.get(intent.detected_language,
                                              _STATIC_FALLBACK["en"])

    cache_key = redis_client.build_cache_key(
        intent.category.value, intent.city, intent.detected_language
    )
    await redis_client.set(cache_key, {
        "response_text": response_text,
        "resources_count": found_count,
    })

    response = ResponseEvent(
        correlation_id=matches.correlation_id,
        detected_language=intent.detected_language,
        response_text=response_text,
        resources_count=found_count,
    )
    await kafka_client.publish(config.TOPIC_RESPONSES, response.model_dump())
    logger.info("Response published [%s]", matches.correlation_id)
