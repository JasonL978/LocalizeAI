"""
Matcher Service
Responsibility: Query the resource database and return ranked matches.
Supports both CSV (local/HF Spaces) and Supabase PostgreSQL (production).
Knows nothing about LLMs or response formatting.
"""
from __future__ import annotations
import logging
import pandas as pd
from rapidfuzz import fuzz
from typing import List
from shared.schemas import IntentEvent, MatchesEvent, Resource
from shared import kafka_client
import config

logger = logging.getLogger(__name__)

# ── Data loading ───────────────────────────────────────────────────────────────

_df_cache: pd.DataFrame | None = None


def _load_csv() -> pd.DataFrame:
    global _df_cache
    if _df_cache is None:
        _df_cache = pd.read_csv(config.CSV_PATH, dtype=str).fillna("")
        logger.info("Loaded %d resources from CSV", len(_df_cache))
    return _df_cache


async def _load_supabase() -> pd.DataFrame:
    from supabase import create_client
    client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
    result = client.table("resources").select("*").execute()
    return pd.DataFrame(result.data).fillna("")


async def _get_resources() -> pd.DataFrame:
    if config.USE_SUPABASE:
        return await _load_supabase()
    return _load_csv()


# ── Scoring ────────────────────────────────────────────────────────────────────

def _score_row(row: pd.Series, intent: IntentEvent) -> int:
    score = 0
    city_match = fuzz.token_set_ratio(
        row.get("city", "").lower(), intent.city.lower()
    )
    score += city_match  # 0–100

    if row.get("zip", "") == intent.zip_code:
        score += 20

    if row.get("accepts_walk_in", "").lower() == "true":
        score += 10

    lang = intent.detected_language
    supported = row.get("languages_supported", "en").split("|")
    if lang in supported:
        score += 15

    if intent.urgency.value == "urgent" and row.get("accepts_walk_in", "").lower() == "true":
        score += 25

    return score


# ── Public processor ───────────────────────────────────────────────────────────

async def process(event_data: dict) -> None:
    intent = IntentEvent(**event_data)
    logger.info("Matcher processing [%s] cat=%s city=%s",
                intent.correlation_id, intent.category, intent.city)

    df = await _get_resources()

    filtered = df[df["category"].str.lower() == intent.category.value]

    if intent.city.lower() != "unknown":
        city_mask = filtered["city"].str.lower().apply(
            lambda c: fuzz.token_set_ratio(c, intent.city.lower()) >= 60
        )
        city_filtered = filtered[city_mask]
        if not city_filtered.empty:
            filtered = city_filtered

    if filtered.empty:
        filtered = df[df["category"].str.lower() == intent.category.value]

    if filtered.empty:
        filtered = df

    scored = filtered.copy()
    scored["_score"] = scored.apply(lambda r: _score_row(r, intent), axis=1)
    top = scored.nlargest(config.MAX_RESULTS, "_score")

    resources: List[Resource] = []
    for _, row in top.iterrows():
        try:
            resources.append(Resource(
                name=row.get("name", ""),
                organization=row.get("organization", ""),
                category=row.get("category", ""),
                subcategory=row.get("subcategory") or None,
                address=row.get("address", ""),
                city=row.get("city", ""),
                state=row.get("state", ""),
                zip=row.get("zip") or None,
                phone=row.get("phone") or None,
                website=row.get("website") or None,
                hours=row.get("hours") or None,
                languages_supported=row.get("languages_supported") or None,
                eligibility=row.get("eligibility") or None,
                accepts_walk_in=str(row.get("accepts_walk_in", "true")).lower() == "true",
                description=row.get("description") or None,
            ))
        except Exception as e:
            logger.warning("Skipping malformed resource row: %s", e)

    matches = MatchesEvent(
        correlation_id=intent.correlation_id,
        intent=intent,
        resources=resources,
    )
    await kafka_client.publish(config.TOPIC_MATCHES, matches.model_dump())
    logger.info("Matcher published [%s] → %d resources", intent.correlation_id, len(resources))
