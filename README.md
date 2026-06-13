# CivicLens

**A multilingual, event-driven AI assistant that connects people in crisis to local community resources — in whatever language they speak.**

When someone needs food, shelter, legal aid, healthcare, or help paying a utility bill, they shouldn't have to navigate English-only bureaucracy under stress. CivicLens takes a free-text message in any language ("Necesito comida para mi familia en Chicago", "我需要法律帮助"), understands the underlying need, finds verified local resources, and responds in the user's own language with concrete next steps.

---

## Why I built this

I wanted a project that mirrors how production AI systems are actually architected — not a single monolithic prompt, but a pipeline of specialized, independently-deployable services communicating through a message bus. CivicLens runs the **exact same code path** in two modes:

- **Local mode** — the full 6-stage pipeline runs in-process on `asyncio` queues, zero external infrastructure required. Clone and run.
- **Production mode** — each stage becomes an independent worker communicating over Upstash Kafka, backed by Supabase (Postgres) and Redis.

The message-bus abstraction means the service logic never changes between the two — only the transport does.

---

## Architecture

CivicLens is an **event-driven pipeline** of six single-responsibility services. Each service reads only from its input topic and writes only to its output topic — there are no direct calls between services.

```
User message
    │
    ▼
┌─────────────────┐
│ Gateway         │  Normalizes input → RawMessageEvent
└────────┬────────┘
         │  raw-messages
         ▼
┌─────────────────┐
│ Intent          │  LLM extracts: language, category, location,
└────────┬────────┘  urgency, household context, search query
         │  intents
         ▼
┌─────────────────┐
│ Cache Check     │  Redis lookup by (category, city, language).
└────────┬────────┘  Hit → skip straight to response. Miss → match.
         │
    ┌────┴─────┐
    │          │
 cache hit  cache miss (needs-matching)
    │          │
    │          ▼
    │  ┌─────────────────┐
    │  │ Matcher         │  Fuzzy city + zip + category matching
    │  └────────┬────────┘  against resource DB (rapidfuzz)
    │           │  matches
    │           ▼
    │  ┌─────────────────┐
    └─▶│ Response        │  LLM composes answer in user's language
       └────────┬────────┘  grounded ONLY in matched resources
                │  responses
                ▼
       ┌─────────────────┐
       │ Delivery        │  Returns final text to the channel
       └─────────────────┘
```

Every event has a strict **Pydantic schema** ([civiclens/shared/schemas.py](civiclens/shared/schemas.py)) and carries a `correlation_id` end-to-end for traceability.

---

## Key engineering decisions

**Same contract, two transports.** [main.py](civiclens/main.py) orchestrates the local pipeline over `asyncio` queues; flipping `LOCAL_MODE=false` swaps the bus for Kafka with no change to service code. The abstraction lives in [shared/kafka_client.py](civiclens/shared/kafka_client.py).

**Resilient multi-provider LLM layer.** [shared/llm_fallback.py](civiclens/shared/llm_fallback.py) cascades across Groq (Llama 3.3 70B) → Gemini 1.5 Flash → HuggingFace (Mistral 7B), so a single provider outage or rate limit doesn't take the system down.

**Grounded responses.** The response service composes answers using *only* the verified resources returned by the matcher — it doesn't let the model invent phone numbers or addresses for people in crisis.

**Cache-aware.** Repeated needs ("food assistance in Chicago, in Spanish") hit a Redis cache keyed on the semantic dimensions of the request, skipping the matcher and a second LLM call entirely.

**Location-agnostic by design.** Location is extracted from natural language (city, neighborhood, or zip) rather than hardcoded. Coverage scales purely with the resource dataset — no code changes needed to support a new city.

---

## Tech stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ (`async`/`await` throughout) |
| LLMs | Groq · Gemini · HuggingFace (fallback cascade) |
| Schemas | Pydantic v2 |
| Matching | rapidfuzz |
| Message bus | asyncio queues (local) · Upstash Kafka (prod) |
| Cache | Upstash Redis |
| Database | CSV (local) · Supabase / Postgres (prod) |
| Frontend | Gradio |

---

## Running locally

```bash
cd civiclens
pip install -r requirements.txt

cp .env.example .env
# Add at least one LLM key (GROQ_API_KEY is free and recommended)

# Interactive CLI
python main.py

# Or launch the Gradio web UI
python frontend/app.py
```

Local mode needs **no Kafka, Redis, or Supabase** — only one LLM API key. Resources are read from [civiclens/data/resources.csv](civiclens/data/resources.csv).

Try:
```
You: I need fresh vegetables for my family in Chicago
You: Necesito ayuda con mi factura de electricidad en Chicago
You: 我正在逃离家庭暴力，需要紧急庇护所
```

---

## Project structure

```
civiclens/
├── main.py                 # Orchestrator + CLI entry point
├── config.py               # Mode flags, models, topic names, taxonomy
├── frontend/app.py         # Gradio web UI
├── services/
│   ├── gateway_service.py        # Ingest → RawMessageEvent
│   ├── intent_service.py         # NLU: language, category, location, urgency
│   ├── cache_check_service.py    # Redis cache lookup
│   ├── matcher_service.py        # Resource matching
│   ├── response_service.py       # Grounded multilingual response generation
│   └── delivery_service.py       # Final delivery to channel
├── shared/
│   ├── schemas.py          # Pydantic event contracts
│   ├── kafka_client.py     # Message-bus abstraction (local ↔ Kafka)
│   ├── redis_client.py     # Cache client + key construction
│   └── llm_fallback.py     # Multi-provider LLM cascade
├── data/resources.csv      # Local resource dataset
└── infra/supabase_schema.sql
```

---

## Roadmap

- **Broaden resource coverage** beyond Chicago via a live resource API (211.org / Google Places) behind the existing matcher interface.
- **Harden prompt-injection defenses** — isolate untrusted user content with structured delimiters and validate LLM-parsed fields before they re-enter downstream prompts.
- **SMS channel** — the schema already models `Channel.SMS`; wiring a Twilio gateway would reach users without smartphones or data.

---

## About

Built by **Jason Lee** as a portfolio project demonstrating production-style AI system design: event-driven architecture, schema-enforced contracts, resilient multi-provider LLM integration, and a local-to-production deployment story.
