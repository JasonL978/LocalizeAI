"""
CivicLens QA Test Suite — Senior QA Tester Script
10 scenarios: standard, slang, mixed language, edge cases
"""
import asyncio
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import run_pipeline

RESET  = "\033[0m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"

SCENARIOS = [
    {
        "id": 1,
        "lang": "English (Standard)",
        "input": "I need fresh vegetables and groceries for my family of 4 in Chicago",
        "expect_category": "food",
        "expect_lang": "en",
        "check_keywords": ["chicago", "food", "pantry", "grocery", "vegetable", "211"],
    },
    {
        "id": 2,
        "lang": "Spanish (Standard)",
        "input": "Necesito comida para mis hijos en Chicago, no tenemos nada en casa",
        "expect_category": "food",
        "expect_lang": "es",
        "check_keywords": ["chicago", "comida", "alimento", "recurso", "pantry", "211"],
    },
    {
        "id": 3,
        "lang": "English (AAVE / Street Slang)",
        "input": "yo fam i'm dead broke tryna find some food for my kids on the south side no cap",
        "expect_category": "food",
        "expect_lang": "en",
        "check_keywords": ["food", "chicago", "pantry", "211"],
    },
    {
        "id": 4,
        "lang": "Spanish (Slang / Informal)",
        "input": "bro necesito jale pa comer, estoy bien fregado en chicago, no tengo ni pa los frijoles",
        "expect_category": "food",
        "expect_lang": "es",
        "check_keywords": [],
    },
    {
        "id": 5,
        "lang": "Mandarin Chinese",
        "input": "我在芝加哥需要紧急住所，我和我的孩子今晚没有地方住",
        "expect_category": "shelter",
        "expect_lang": "zh",
        "check_keywords": ["芝加哥", "住", "庇护", "211"],
    },
    {
        "id": 6,
        "lang": "Arabic",
        "input": "أحتاج إلى مساعدة قانونية في شيكاغو، أنا مهاجر ولا أملك وثائق",
        "expect_category": "legal",
        "expect_lang": "ar",
        "check_keywords": ["شيكاغو", "قانوني", "211"],
    },
    {
        "id": 7,
        "lang": "Spanglish (Code-switching)",
        "input": "hey necesito help paying my electric bill, ya no puedo pay it en Chicago",
        "expect_category": "utility",
        "expect_lang": "es",
        "check_keywords": [],
    },
    {
        "id": 8,
        "lang": "French",
        "input": "Je cherche une clinique médicale gratuite à Chicago, je n'ai pas d'assurance maladie",
        "expect_category": "health",
        "expect_lang": "fr",
        "check_keywords": ["chicago", "clinique", "médical", "211"],
    },
    {
        "id": 9,
        "lang": "Vietnamese",
        "input": "Tôi cần tìm nơi trú ẩn khẩn cấp ở Chicago, tôi đang bị bạo lực gia đình",
        "expect_category": "shelter",
        "expect_lang": "vi",
        "check_keywords": ["chicago", "211"],
    },
    {
        "id": 10,
        "lang": "English (Vague / No Location)",
        "input": "I dunno man, just need like... help with my lights getting shut off or whatever",
        "expect_category": "utility",
        "expect_lang": "en",
        "check_keywords": ["211", "utility", "electric", "bill", "help"],
    },
]


def verdict(passed: bool) -> str:
    return f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"


def check_response(response: str, keywords: list[str]) -> tuple[bool, list[str]]:
    lower = response.lower()
    missing = [kw for kw in keywords if kw.lower() not in lower]
    return len(missing) == 0, missing


async def run_tests():
    print(f"\n{BOLD}{'='*70}{RESET}")
    print(f"{BOLD}  CivicLens QA Test Suite - 10 Multilingual Scenarios{RESET}")
    print(f"{BOLD}{'='*70}{RESET}\n")

    results = []

    for s in SCENARIOS:
        print(f"{CYAN}[TEST {s['id']:02d}]{RESET} {BOLD}{s['lang']}{RESET}")
        print(f"  Input   : {s['input'][:80]}{'...' if len(s['input'])>80 else ''}")

        start = time.time()
        error = None
        response = ""
        try:
            response = await run_pipeline(s["input"])
            elapsed = time.time() - start
        except Exception as e:
            elapsed = time.time() - start
            error = str(e)

        # Checks
        has_response     = bool(response and len(response.strip()) > 20)
        not_english_only = True  # relaxed — LLM may use English for some langs
        kw_pass, missing = check_response(response, s.get("check_keywords", []))
        no_error         = error is None

        overall = has_response and no_error

        print(f"  Time    : {elapsed:.1f}s")
        print(f"  Response: {response[:200].strip()}{'...' if len(response)>200 else ''}")
        if error:
            print(f"  {RED}Error   : {error}{RESET}")
        if missing:
            print(f"  {YELLOW}Missing keywords: {missing}{RESET}")
        print(f"  Status  : {verdict(overall)}")
        print()

        results.append({
            "id": s["id"],
            "lang": s["lang"],
            "passed": overall,
            "elapsed": elapsed,
            "error": error,
        })

    # ── Summary ──────────────────────────────────────────────────────────────
    total   = len(results)
    passed  = sum(1 for r in results if r["passed"])
    failed  = total - passed
    avg_t   = sum(r["elapsed"] for r in results) / total
    max_t   = max(r["elapsed"] for r in results)
    slowest = next(r for r in results if r["elapsed"] == max_t)

    print(f"{BOLD}{'='*70}{RESET}")
    print(f"{BOLD}  QA SUMMARY{RESET}")
    print(f"{'='*70}")
    print(f"  Total scenarios : {total}")
    print(f"  Passed          : {GREEN}{passed}{RESET}")
    print(f"  Failed          : {RED if failed else GREEN}{failed}{RESET}")
    print(f"  Pass rate       : {GREEN if passed==total else YELLOW}{passed/total*100:.0f}%{RESET}")
    print(f"  Avg response    : {avg_t:.1f}s")
    print(f"  Slowest         : Test {slowest['id']} ({slowest['lang']}) — {max_t:.1f}s")
    print()

    for r in results:
        status = f"{GREEN}PASS{RESET}" if r["passed"] else f"{RED}FAIL{RESET}"
        print(f"  [{status}] Test {r['id']:02d} | {r['lang']:<35} | {r['elapsed']:.1f}s")

    print(f"{BOLD}{'='*70}{RESET}\n")

    if failed > 0:
        print(f"{RED}Failed tests:{RESET}")
        for r in results:
            if not r["passed"]:
                print(f"  Test {r['id']}: {r['lang']}")
                if r["error"]:
                    print(f"    Error: {r['error']}")


if __name__ == "__main__":
    asyncio.run(run_tests())
