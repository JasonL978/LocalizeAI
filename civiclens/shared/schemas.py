from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum
import uuid
from datetime import datetime, timezone


class ResourceCategory(str, Enum):
    FOOD = "food"
    SHELTER = "shelter"
    LEGAL = "legal"
    HEALTH = "health"
    UTILITY = "utility"
    OTHER = "other"


class Urgency(str, Enum):
    URGENT = "urgent"
    STANDARD = "standard"


class Channel(str, Enum):
    WEB = "web"
    SMS = "sms"


def new_correlation_id() -> str:
    return str(uuid.uuid4())


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Event contracts (each Kafka topic has one schema) ──────────────────────────

class RawMessageEvent(BaseModel):
    """Published by: gateway_service → Topic: raw-messages"""
    correlation_id: str = Field(default_factory=new_correlation_id)
    raw_message: str
    channel: Channel = Channel.WEB
    timestamp: str = Field(default_factory=utc_now)


class IntentEvent(BaseModel):
    """Published by: intent_service → Topic: intents"""
    correlation_id: str
    raw_message: str
    detected_language: str           # ISO 639-1 code, e.g. "es"
    detected_language_name: str      # Human-readable, e.g. "Spanish"
    english_translation: str
    search_query: str                # Semantic search phrase optimised for matching
    category: ResourceCategory
    location: str                    # "Chicago, IL"
    city: str
    zip_code: Optional[str] = None
    urgency: Urgency = Urgency.STANDARD
    household_context: Optional[str] = None
    timestamp: str = Field(default_factory=utc_now)


class Resource(BaseModel):
    name: str
    organization: str
    category: str
    subcategory: Optional[str] = None
    address: str
    city: str
    state: str
    zip: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    hours: Optional[str] = None
    languages_supported: Optional[str] = None  # pipe-separated: "en|es|pl"
    eligibility: Optional[str] = None
    accepts_walk_in: bool = True
    description: Optional[str] = None


class MatchesEvent(BaseModel):
    """Published by: matcher_service → Topic: matches"""
    correlation_id: str
    intent: IntentEvent
    resources: List[Resource]
    cache_hit: bool = False
    timestamp: str = Field(default_factory=utc_now)


class ResponseEvent(BaseModel):
    """Published by: response_service → Topic: responses"""
    correlation_id: str
    detected_language: str
    response_text: str
    resources_count: int
    timestamp: str = Field(default_factory=utc_now)
