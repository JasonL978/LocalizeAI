"""
Cache Check Service
Responsibility: Check if an identical query was recently served.
Cache HIT       -> publish directly to 'responses' (skip matching + LLM response gen).
Cache MISS      -> forward to 'needs-matching'.
No city in msg  -> publish a city-prompt response and short-circuit the pipeline.
Knows nothing about LLMs or how resources are fetched.
"""
from __future__ import annotations
import logging
from shared.schemas import IntentEvent, ResponseEvent
from shared import kafka_client, redis_client
import config

logger = logging.getLogger(__name__)

_CITY_PROMPT: dict[str, str] = {
    "en": "I'd love to help! Could you let me know which city or neighborhood you're in? That way I can find the closest resources for you.",
    "es": "Con gusto te ayudo! Podrias decirme en que ciudad o vecindario estas? Asi puedo encontrar los recursos mas cercanos para ti.",
    "zh": "我很乐意帮助您！请问您在哪个城市或社区？这样我可以为您找到最近的资源。",
    "ar": "يسعدني مساعدتك！ هل يمكنك إخباري بالمدينة أو الحي الذي تقيم فيه？ حتى أتمكن من إيجاد أقرب الموارد لك.",
    "fr": "Je serais ravi de vous aider ! Pourriez-vous me dire dans quelle ville ou quartier vous vous trouvez ? Je pourrai ainsi trouver les ressources les plus proches.",
    "vi": "Tôi rất vui được giúp bạn! Bạn có thể cho tôi biết bạn đang ở thành phố hoặc khu phố nào không? Như vậy tôi có thể tìm các nguồn hỗ trợ gần nhất cho bạn.",
    "pl": "Chetnie pomoge! Czy mozesz podac mi miasto lub dzielnice, w ktorej mieszkasz? Dzieki temu znajde dla Ciebie najblizsze zasoby.",
    "ko": "도와드리고 싶습니다! 어느 도시나 동네에 계신지 알려주실 수 있나요? 가장 가까운 지원 시설을 찾아드리겠습니다.",
}

_CITY_PROMPT_DEFAULT = _CITY_PROMPT["en"]


async def process(event_data: dict) -> None:
    intent = IntentEvent(**event_data)
    logger.info("Cache check [%s]", intent.correlation_id)

    if intent.city.lower() == "unknown":
        logger.info("No city detected [%s] -> prompting user", intent.correlation_id)
        prompt_text = _CITY_PROMPT.get(intent.detected_language, _CITY_PROMPT_DEFAULT)
        response = ResponseEvent(
            correlation_id=intent.correlation_id,
            detected_language=intent.detected_language,
            response_text=prompt_text,
            resources_count=0,
        )
        await kafka_client.publish(config.TOPIC_RESPONSES, response.model_dump())
        return

    cache_key = redis_client.build_cache_key(
        intent.category.value, intent.city, intent.detected_language, intent.search_query
    )
    cached = await redis_client.get(cache_key)

    if cached:
        logger.info("Cache HIT [%s] -> bypassing matcher + LLM", intent.correlation_id)
        response = ResponseEvent(
            correlation_id=intent.correlation_id,
            detected_language=intent.detected_language,
            response_text=cached["response_text"],
            resources_count=cached["resources_count"],
        )
        await kafka_client.publish(config.TOPIC_RESPONSES, response.model_dump())
    else:
        logger.info("Cache MISS [%s] -> forwarding to matcher", intent.correlation_id)
        await kafka_client.publish(config.TOPIC_NEEDS_MATCHING, intent.model_dump())
