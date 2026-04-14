"""Chunk-based Gemini entity extraction with retry, merge, and deduplication."""

from __future__ import annotations

import json
import re
import time
import logging
from typing import TYPE_CHECKING

from google import genai
from google.genai import types

from config import Config
from chunker import TextChunk

if TYPE_CHECKING:
    from entity_generator.generator import TextbookEntityGenerator

logger = logging.getLogger(__name__)

# ── Vertex AI client ─────────────────────────────────────────────────────────
_client = genai.Client(
    vertexai=True,
    project=Config.GOOGLE_CLOUD_PROJECT,
    location=Config.VERTEX_AI_LOCATION,
)

_MODEL_NAME = "gemini-2.5-flash"

MAX_RETRIES = 4
RETRY_BASE_DELAY_S = 10  # exponential backoff starting point

# Keywords indicating a DAILY quota error (retrying within the same day won't help)
_DAILY_QUOTA_KEYWORDS = [
    "daily limit",
    "per-day",
    "per day",
    "perdayperproject",
    "perday",
]

# Keywords indicating a transient / rate-limit error (worth retrying)
_RETRYABLE_KEYWORDS = [
    "429",
    "rate limit",
    "resource_exhausted",
    "resourceexhausted",
    "quota",
    "503",
    "service unavailable",
    "internal",
    "500",
]


class DailyQuotaExhausted(Exception):
    """Raised when the API daily quota is exhausted."""


def _is_daily_quota_error(exc: Exception) -> bool:
    """Return True if the error is a *daily* quota limit (don't retry)."""
    msg = str(exc).lower()
    return any(kw in msg for kw in _DAILY_QUOTA_KEYWORDS)


def _is_retryable_error(exc: Exception) -> bool:
    """Return True if the error is transient and worth retrying."""
    if _is_daily_quota_error(exc):
        return False
    msg = str(exc).lower()
    return any(kw in msg for kw in _RETRYABLE_KEYWORDS)


def _extract_retry_delay(exc: Exception) -> float | None:
    """Extract the API-suggested retry delay (seconds) from the error, if any."""
    msg = str(exc)
    # Pattern: "retry_delay { seconds: 50 }"
    match = re.search(r"seconds:\s*(\d+)", msg)
    if match:
        return float(match.group(1))
    # Pattern: "Retry-After: 60"
    match = re.search(r"retry[_\-]?after[:\s]+(\d+)", msg, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None


def extract_entities(
    chunks: list[TextChunk],
    subject: str,
    grade: str,
    book_id: str,
    generator: "TextbookEntityGenerator",
    chapter_limit: int | None = None,
) -> dict:
    """Extract entities from every chunk, merge, and return validated data.

    Args:
        chapter_limit: If set, stop processing chunks once this many unique
                       chapters have been collected. ``None`` means no limit.
    """
    total = len(chunks)
    logger.info("Starting entity extraction: %d chunks.", total)

    system_prompt = generator.get_system_prompt()
    partial_results: list[dict] = []
    seen_chapter_ids: set[str] = set()

    for chunk in chunks:
        idx = chunk.index + 1
        logger.info(
            "[%d/%d] Processing chunk (pages %d-%d, %d chars): %s",
            idx, total,
            chunk.page_start, chunk.page_end,
            chunk.char_count,
            chunk.label[:80],
        )

        try:
            result = _extract_single_chunk(
                chunk.text, subject, grade, system_prompt, generator
            )
        except DailyQuotaExhausted:
            logger.error(
                "Daily quota exhausted at chunk %d/%d. "
                "Keeping %d partial result(s) already collected.",
                idx, total, len(partial_results),
            )
            break

        if result is not None:
            partial_results.append(result)
            pt_count = len(result.get("parts", []))
            ch_count = sum(
                len(p.get("chapters", []))
                for p in result.get("parts", [])
            )
            logger.info(
                "[%d/%d] Got %d part(s), %d chapter(s) from chunk.",
                idx, total, pt_count, ch_count,
            )

            # Track unique chapter IDs for early stop
            for part in result.get("parts", []):
                for ch in part.get("chapters", []):
                    ch_id = ch.get("id", "")
                    if ch_id:
                        seen_chapter_ids.add(ch_id)

            if chapter_limit is not None and len(seen_chapter_ids) >= chapter_limit:
                logger.info(
                    "[%d/%d] Reached CHAPTER_LIMIT=%d (%d unique chapter(s) found). "
                    "Stopping early.",
                    idx, total, chapter_limit, len(seen_chapter_ids),
                )
                break
        else:
            logger.warning("[%d/%d] Chunk failed — skipping.", idx, total)

    if not partial_results:
        raise ValueError(
            "All chunks failed entity extraction. "
            "Check Gemini API key and input content."
        )

    # Merge partials
    merged = _merge_results(partial_results, subject, grade)

    # Validate, normalise, prefix IDs
    return generator.parse_response(merged, book_id)



def _extract_single_chunk(
    text: str,
    subject: str,
    grade: str,
    system_prompt: str,
    generator: "TextbookEntityGenerator",
) -> dict | None:
    """Call Gemini via Vertex AI for one chunk with smart retry.

    Raises:
        DailyQuotaExhausted: when a daily quota limit is detected.
    """
    user_prompt = generator.get_extraction_prompt(text, subject, grade)

    for attempt in range(1 + MAX_RETRIES):
        try:
            response = _client.models.generate_content(
                model=_MODEL_NAME,
                contents=user_prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0,
                    top_p=1.0,
                ),
            )

            raw_response = response.text
            logger.debug("Gemini raw (%d chars): %s…", len(raw_response), raw_response[:300])

            parsed = _parse_json(raw_response)
            return parsed

        except Exception as exc:
            # Daily quota → abort all remaining chunks
            if _is_daily_quota_error(exc):
                logger.error(
                    "Daily quota limit hit — retrying won't help: %s", exc,
                )
                raise DailyQuotaExhausted(str(exc)) from exc

            # Non-retryable error → skip this chunk
            if not _is_retryable_error(exc):
                logger.error("Non-retryable error: %s", exc)
                return None

            if attempt < MAX_RETRIES:
                # Prefer API-suggested delay; fall back to exponential backoff
                api_delay = _extract_retry_delay(exc)
                backoff_delay = RETRY_BASE_DELAY_S * (2 ** attempt)
                delay = max(api_delay or 0, backoff_delay)
                logger.warning(
                    "Attempt %d/%d failed (%s). Retrying in %ds "
                    "(api_suggested=%s, backoff=%ds)…",
                    attempt + 1, 1 + MAX_RETRIES, exc, delay,
                    api_delay, backoff_delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "All %d attempts failed for chunk: %s",
                    1 + MAX_RETRIES, exc,
                )
    return None



def _merge_results(
    partials: list[dict],
    subject: str,
    grade: str,
) -> dict:
    """Merge partial results: match parts by id, chapters by id, dedup concepts/locations."""
    merged: dict = {"subject": subject, "grade": grade, "parts": []}
    part_map: dict[str, dict] = {}        # part_id → part dict
    chapter_map: dict[str, dict] = {}     # chapter_id → chapter dict
    lesson_ids_seen: set[str] = set()

    for partial in partials:
        for part in partial.get("parts", []):
            pt_id = part.get("id", "")

            if pt_id in part_map:
                existing_pt = part_map[pt_id]
            else:
                # New part
                existing_pt = {
                    "id": pt_id,
                    "name": part.get("name", ""),
                    "order": part.get("order", 0),
                    "chapters": [],
                    "lessons": [],
                }
                part_map[pt_id] = existing_pt
                merged["parts"].append(existing_pt)

            # Merge chapters
            for chapter in part.get("chapters", []):
                ch_id = chapter.get("id", "")
                if ch_id in chapter_map:
                    existing_ch = chapter_map[ch_id]
                    for lesson in chapter.get("lessons", []):
                        ls_id = lesson.get("id", "")
                        if ls_id and ls_id not in lesson_ids_seen:
                            existing_ch.setdefault("lessons", []).append(lesson)
                            lesson_ids_seen.add(ls_id)
                        elif ls_id in lesson_ids_seen:
                            _merge_lesson_sections(existing_ch, lesson)
                else:
                    chapter_map[ch_id] = chapter
                    existing_pt["chapters"].append(chapter)
                    for lesson in chapter.get("lessons", []):
                        ls_id = lesson.get("id", "")
                        if ls_id:
                            lesson_ids_seen.add(ls_id)

            # Merge part-level lessons (no chapter)
            for lesson in part.get("lessons", []):
                ls_id = lesson.get("id", "")
                if ls_id and ls_id not in lesson_ids_seen:
                    existing_pt["lessons"].append(lesson)
                    lesson_ids_seen.add(ls_id)
                elif ls_id in lesson_ids_seen:
                    _merge_part_lesson_sections(existing_pt, lesson)

    # Sort parts by order
    merged["parts"].sort(key=lambda p: p.get("order", 0))

    # Sort chapters and lessons within each part
    for pt in merged["parts"]:
        pt.get("chapters", []).sort(key=lambda c: c.get("order", 0))
        pt.get("lessons", []).sort(key=lambda l: l.get("order", 0))
        for ch in pt.get("chapters", []):
            ch.get("lessons", []).sort(key=lambda l: l.get("order", 0))

    # Deduplicate concepts / locations within each section
    all_lessons = []
    for pt in merged["parts"]:
        all_lessons.extend(pt.get("lessons", []))
        for ch in pt.get("chapters", []):
            all_lessons.extend(ch.get("lessons", []))
    for ls in all_lessons:
        for sec in ls.get("sections", []):
            sec["concepts"] = _dedup_by_name(sec.get("concepts", []))
            sec["locations"] = _dedup_by_name(sec.get("locations", []))

    total_chapters = sum(
        len(pt.get("chapters", [])) for pt in merged["parts"]
    )
    total_lessons = sum(
        len(pt.get("lessons", []))
        + sum(len(ch.get("lessons", [])) for ch in pt.get("chapters", []))
        for pt in merged["parts"]
    )
    logger.info(
        "Merged %d partials → %d parts, %d chapters, %d lessons.",
        len(partials),
        len(merged["parts"]),
        total_chapters,
        total_lessons,
    )
    return merged


def _merge_part_lesson_sections(part: dict, lesson: dict) -> None:
    """Merge new sections from lesson into the matching lesson in part."""
    ls_id = lesson.get("id", "")
    for existing_ls in part.get("lessons", []):
        if existing_ls.get("id") == ls_id:
            existing_sec_ids = {
                s.get("id") for s in existing_ls.get("sections", [])
            }
            for sec in lesson.get("sections", []):
                if sec.get("id") not in existing_sec_ids:
                    existing_ls.setdefault("sections", []).append(sec)
            return


def _merge_lesson_sections(chapter: dict, lesson: dict) -> None:
    """Merge new sections from lesson into the matching lesson in chapter."""
    ls_id = lesson.get("id", "")
    for existing_ls in chapter.get("lessons", []):
        if existing_ls.get("id") == ls_id:
            existing_sec_ids = {
                s.get("id") for s in existing_ls.get("sections", [])
            }
            for sec in lesson.get("sections", []):
                if sec.get("id") not in existing_sec_ids:
                    existing_ls.setdefault("sections", []).append(sec)
            return

def _dedup_by_name(items: list[dict]) -> list[dict]:
    """Remove dicts with duplicate 'name' (case-insensitive)."""
    seen: set[str] = set()
    result: list[dict] = []
    for item in items:
        key = item.get("name", "").strip().lower()
        if key and key not in seen:
            seen.add(key)
            result.append(item)
    return result



def _parse_json(raw: str) -> dict:
    """Strip markdown fences and parse JSON."""
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse Gemini response as JSON: %s", e)
        logger.error("Cleaned text:\n%s", cleaned[:1000])
        raise ValueError(f"Gemini returned invalid JSON: {e}") from e
