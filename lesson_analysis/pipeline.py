"""Pipeline: Download → Extract → Fetch standard data → Evaluate (async)."""

import asyncio
import os
import logging
from typing import Awaitable, Callable
from gcs_handler import download_from_gcs
from extractor import extract_text
from neo4j_client import (
    find_book,
    list_lessons,
    get_concepts_by_lesson_id,
    get_locations_by_lesson_id,
    get_sections_by_lesson_id,
    get_standard_concepts,
    get_standard_locations,
)
from evaluator import identify_lesson, evaluate_lesson_plan

logger = logging.getLogger(__name__)

# Type alias for async progress callback
ProgressCallback = Callable[[str, int, str], Awaitable[None]]


def _build_full_lesson_id(subject: str, grade: str, lesson_code: str) -> str:
    """
    Build the full Neo4j lesson ID from parts.

    Neo4j ID format: {subject}_lop_{grade}_L{number}

    Examples:
        ("dia_li", "10", "bai_1")              → "dia_li_lop_10_L1"
        ("dia_li", "10", "dia_li_lop_10_L1")   → "dia_li_lop_10_L1"  (already full)
        ("dia_li", "lop_10", "bai_1")          → "dia_li_lop_10_L1"  (strips prefix)
    """
    # If it already looks like a full ID (contains subject prefix), return as-is
    if subject and lesson_code.startswith(subject):
        return lesson_code

    # Extract grade number: "lop_10" → "10", "10" → "10"
    grade_number = grade.replace("lop_", "") if grade.startswith("lop_") else grade

    # Extract lesson number from lesson code: "bai_1" → "1"
    import re
    match = re.search(r"(\d+)$", lesson_code)
    lesson_number = match.group(1) if match else lesson_code

    # Build: dia_li_lop_10_L1
    if subject and grade_number:
        return f"{subject}_lop_{grade_number}_L{lesson_number}"

    return lesson_code


async def run(
    gcs_uri: str,
    subject: str,
    grade: str,
    lesson_id: str | None = None,
    on_progress: ProgressCallback | None = None,
) -> dict:
    """
    Run the lesson-analysis pipeline (async).

    Args:
        gcs_uri:  GCS path to the lesson plan file.
        subject:  Subject code (e.g. "dia_li").
        grade:    Grade code (e.g. "lop_10").
        lesson_id: Neo4j lesson ID (e.g. "dia_li_10_bai_1").
                   If provided, skips LLM lesson identification.
        on_progress: Optional async callback(step, progress, detail) for live updates.

    Flow (when lesson_id is provided):
      1. Download file from GCS
      2. Extract text from PDF/DOCX
      3. Fetch concepts/locations/sections for the lesson
      4. Evaluate the lesson plan against the standard data

    Flow (when lesson_id is NOT provided — fallback):
      1. Download file from GCS
      2. Extract text from PDF/DOCX
      3. Find the book in Neo4j
      4. Use LLM to identify which lesson the plan covers
      5. Fetch concepts/locations/sections for that lesson
      6. Evaluate the lesson plan against the standard data
    """

    async def _progress(step: str, progress: int, detail: str = "") -> None:
        """Log + invoke callback if provided."""
        logger.info("[%s] %d%% — %s", step, progress, detail)
        if on_progress:
            await on_progress(step, progress, detail)

    local_path = None
    try:
        # ── Step 1: Download from GCS ─────────────────────────────────
        await _progress("downloading", 10, f"Downloading from GCS: {gcs_uri}")
        local_path = await asyncio.to_thread(download_from_gcs, gcs_uri)

        # ── Step 2: Extract text ──────────────────────────────────────
        await _progress("extracting_text", 25, "Extracting text from file...")
        raw_text = await asyncio.to_thread(extract_text, local_path)
        if not raw_text:
            raise ValueError("No text could be extracted from %s" % gcs_uri)
        await _progress("extracting_text", 30, f"Extracted {len(raw_text)} characters")

        # ── Step 3: Resolve lesson ────────────────────────────────────
        if lesson_id:
            # Build full Neo4j ID from parts if needed
            # e.g. ("dia_li", "lop_10", "bai_1") → "dia_li_10_bai_1"
            matched_id = _build_full_lesson_id(subject, grade, lesson_id)
            if matched_id != lesson_id:
                logger.info("Resolved lesson ID: %s → %s", lesson_id, matched_id)

            await _progress("fetching_data", 45, f"Fetching standard data for lesson {matched_id}")
            matched_name = lesson_id  # Will be enriched from Neo4j data if available
            confidence = "provided"

            standard_concepts, standard_locations, section_content = await asyncio.gather(
                asyncio.to_thread(get_concepts_by_lesson_id, matched_id),
                asyncio.to_thread(get_locations_by_lesson_id, matched_id),
                asyncio.to_thread(get_sections_by_lesson_id, matched_id),
            )

        else:
            # Fallback: use LLM to identify the lesson
            await _progress("finding_book", 35, f"Finding book for {subject} - {grade}")
            book = await asyncio.to_thread(find_book, subject, grade)
            if not book:
                raise ValueError(
                    "No book found in Neo4j matching subject='%s', grade='%s'"
                    % (subject, grade)
                )

            await _progress("identifying_lesson", 45, "Identifying lesson via LLM...")
            available_lessons = await asyncio.to_thread(list_lessons, book["id"])
            if not available_lessons:
                raise ValueError(
                    "No lessons found in Neo4j for book %s" % book["id"]
                )

            match_result = await identify_lesson(raw_text, available_lessons)
            matched_id = match_result.get("matched_lesson_id")
            matched_name = match_result.get("matched_lesson_name")
            confidence = match_result.get("confidence", "unknown")

            if not matched_id:
                logger.warning(
                    "LLM could not match a lesson. Falling back to whole-book data."
                )

            await _progress(
                "identifying_lesson", 55,
                f"Matched → {matched_name} (confidence: {confidence})"
            )

            # Fetch standard data
            if matched_id:
                standard_concepts, standard_locations, section_content = await asyncio.gather(
                    asyncio.to_thread(get_concepts_by_lesson_id, matched_id),
                    asyncio.to_thread(get_locations_by_lesson_id, matched_id),
                    asyncio.to_thread(get_sections_by_lesson_id, matched_id),
                )
            else:
                standard_concepts, standard_locations = await asyncio.gather(
                    asyncio.to_thread(get_standard_concepts, book["subject"], book["grade"]),
                    asyncio.to_thread(get_standard_locations, book["subject"], book["grade"]),
                )
                section_content = []

        await _progress(
            "fetching_data", 60,
            f"Loaded {len(standard_concepts)} concepts, "
            f"{len(standard_locations)} locations, "
            f"{len(section_content)} sections",
        )

        if not standard_concepts:
            logger.warning(
                "No standard concepts found — evaluation will still proceed."
            )

        # ── Step 4: Evaluate via LLM ───────────────────────────────
        await _progress("evaluating", 70, "Evaluating lesson plan via Gemini...")
        evaluation = await evaluate_lesson_plan(
            raw_text,
            standard_concepts,
            standard_locations,
            matched_lesson_name=matched_name,
            section_content=section_content,
        )

        await _progress("evaluating", 90, "Evaluation complete, assembling result...")

        # Assemble the final response
        result = {
            "lesson_plan_file": gcs_uri,
            "subject": subject,
            "grade": grade,
            "matched_lesson": {
                "id": matched_id,
                "name": matched_name,
                "confidence": confidence,
            },
            "evaluation": evaluation,
            "lesson_plan_text": raw_text,
            "textbook_sections": section_content,
        }

        await _progress("completed", 100, "Pipeline finished successfully")
        return result

    except Exception:
        logger.exception("Pipeline failed for %s", gcs_uri)
        raise

    finally:
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            logger.info("Cleaned up temp file: %s", local_path)
