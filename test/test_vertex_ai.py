"""
Quick smoke test – verify Vertex AI (Gemini) is reachable and responding.

Run from the project root:
    python test_vertex_ai.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env ────────────────────────────────────────────────────────────────
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=False)

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip('"')
LOCATION = os.getenv("VERTEX_AI_LOCATION", "us-central1")
CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
MODEL_NAME = "gemini-2.5-flash"

# ── Pre-flight checks ───────────────────────────────────────────────────────
def preflight():
    errors = []
    if not PROJECT:
        errors.append("GOOGLE_CLOUD_PROJECT is not set in .env")
    if not CREDENTIALS:
        errors.append("GOOGLE_APPLICATION_CREDENTIALS is not set in .env")
    elif not Path(CREDENTIALS).exists():
        errors.append(f"Service-account key not found: {CREDENTIALS}")
    if errors:
        for e in errors:
            print(f"  [FAIL] {e}")
        return False
    print(f"  [OK]  Project         : {PROJECT}")
    print(f"  [OK]  Location        : {LOCATION}")
    print(f"  [OK]  Credentials     : {CREDENTIALS}")
    print(f"  [OK]  Model           : {MODEL_NAME}")
    return True


# ── Test 1: Basic text generation ────────────────────────────────────────────
def test_basic_generation():
    """Send a trivial prompt and check we get a non-empty response."""
    import vertexai
    from vertexai.generative_models import GenerativeModel

    vertexai.init(project=PROJECT, location=LOCATION)
    model = GenerativeModel(MODEL_NAME)

    prompt = "Respond with exactly: HELLO_VERTEX_AI_OK"
    response = model.generate_content(prompt)
    text = response.text.strip()

    assert text, "Response was empty"
    assert "HELLO" in text.upper(), f"Unexpected response: {text!r}"
    return text


# ── Test 2: JSON structured output ──────────────────────────────────────────
def test_json_output():
    """Ask for JSON and verify it parses correctly."""
    import json
    import vertexai
    from vertexai.generative_models import GenerativeModel, GenerationConfig

    vertexai.init(project=PROJECT, location=LOCATION)
    model = GenerativeModel(
        MODEL_NAME,
        generation_config=GenerationConfig(temperature=0),
    )

    prompt = (
        'Return ONLY a JSON object (no markdown fences) with these keys:\n'
        '{"model": "<your model name>", "status": "ok", "language": "vi"}'
    )
    response = model.generate_content(prompt)
    raw = response.text.strip()

    # Strip markdown fences if present
    import re
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    data = json.loads(cleaned)
    assert "status" in data, f"Missing 'status' key in: {data}"
    return data


# ── Test 3: Vietnamese language understanding ────────────────────────────────
def test_vietnamese():
    """Verify the model handles Vietnamese text correctly."""
    import vertexai
    from vertexai.generative_models import GenerativeModel

    vertexai.init(project=PROJECT, location=LOCATION)
    model = GenerativeModel(MODEL_NAME)

    prompt = "Thủ đô của Việt Nam là gì? Trả lời bằng một từ duy nhất."
    response = model.generate_content(prompt)
    text = response.text.strip().lower()
    normalized = text.replace(" ", "")

    assert "hànội" in normalized or "hanoi" in normalized, f"Unexpected answer: {text!r}"
    return text


# ── Runner ───────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(" Vertex AI (Gemini) Smoke Test")
    print("=" * 60)

    print("\n[Preflight]")
    if not preflight():
        sys.exit(1)

    tests = [
        ("Basic generation", test_basic_generation),
        ("JSON structured output", test_json_output),
        ("Vietnamese language", test_vietnamese),
    ]

    passed = 0
    failed = 0

    for name, fn in tests:
        print(f"\n[Test] {name} ... ", end="", flush=True)
        try:
            result = fn()
            print("PASSED")
            print(f"        → {result!r}")
            passed += 1
        except Exception as exc:
            print("FAILED")
            print(f"        → {exc}")
            failed += 1

    print("\n" + "=" * 60)
    print(f" Results: {passed} passed, {failed} failed out of {len(tests)}")
    print("=" * 60)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
