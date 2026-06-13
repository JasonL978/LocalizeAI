import os
from dotenv import load_dotenv

load_dotenv()

# --- Runtime mode ---
# LOCAL: services communicate via in-process asyncio queues (no external deps needed)
# PRODUCTION: services communicate via Upstash Kafka
LOCAL_MODE = os.getenv("LOCAL_MODE", "true").lower() == "true"

# --- LLM providers (free tiers) ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
HF_API_TOKEN = os.getenv("HF_API_TOKEN", "")

# --- Upstash Kafka (production mode) ---
KAFKA_URL = os.getenv("UPSTASH_KAFKA_REST_URL", "")
KAFKA_USERNAME = os.getenv("UPSTASH_KAFKA_REST_USERNAME", "")
KAFKA_PASSWORD = os.getenv("UPSTASH_KAFKA_REST_PASSWORD", "")

# --- Upstash Redis (cache) ---
REDIS_URL = os.getenv("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

# --- Supabase (production database) ---
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY", "")

# --- Data source ---
# LOCAL: reads from data/resources.csv
# PRODUCTION: reads from Supabase PostgreSQL
USE_SUPABASE = os.getenv("USE_SUPABASE", "false").lower() == "true"
CSV_PATH = os.getenv("CSV_PATH", "data/resources.csv")

# --- Kafka topic names ---
TOPIC_RAW_MESSAGES = "raw-messages"
TOPIC_INTENTS = "intents"
TOPIC_NEEDS_MATCHING = "needs-matching"
TOPIC_MATCHES = "matches"
TOPIC_RESPONSES = "responses"

# --- LLM models ---
GROQ_MODEL = "llama-3.3-70b-versatile"
GEMINI_MODEL = "gemini-1.5-flash"
HF_MODEL = "mistralai/Mistral-7B-Instruct-v0.3"

# --- Cache ---
CACHE_TTL_SECONDS = 3600  # 1 hour

# --- Resource matching ---
MAX_RESULTS = 3

# --- Category taxonomy (constrains LLM output) ---
VALID_CATEGORIES = {"food", "shelter", "legal", "health", "utility", "other"}

CATEGORY_LABELS = {
    "food": "Food Assistance",
    "shelter": "Housing & Shelter",
    "legal": "Legal Aid",
    "health": "Healthcare",
    "utility": "Utility Assistance",
    "other": "General Services",
}
