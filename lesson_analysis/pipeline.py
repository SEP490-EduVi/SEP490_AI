"""Pipeline: Download → Extract → Fetch standard data → Evaluate."""

import os
import logging
from typing import Callable
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

# Type alias for progress callback
ProgressCallback = Callable[[str, int, str], None]  # (step, progress, detail)


def run(
    gcs_uri: str,
    subject: str,
    grade: str,
    lesson_id: str | None = None,
    on_progress: ProgressCallback | None = None,
) -> dict:
    """
    Run the lesson-analysis pipeline.

    Args:
        gcs_uri:  GCS path to the lesson plan file.
        subject:  Subject code (e.g. "dia_li").
        grade:    Grade code (e.g. "lop_10").
        lesson_id: Neo4j lesson ID (e.g. "dia_li_10_bai_1").
                   If provided, skips LLM lesson identification.
        on_progress: Optional callback(step, progress, detail) for live updates.

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

    def _progress(step: str, progress: int, detail: str = "") -> None:
        """Log + invoke callback if provided."""
        logger.info("[%s] %d%% — %s", step, progress, detail)
        if on_progress:
            on_progress(step, progress, detail)

    local_path = None
    try:
        # ── Step 1: Download from GCS ─────────────────────────────────
        _progress("downloading", 10, f"Downloading from GCS: {gcs_uri}")
        local_path = download_from_gcs(gcs_uri)

        # ── Step 2: Extract text ──────────────────────────────────────
        _progress("extracting_text", 25, "Extracting text from file...")
        raw_text = extract_text(local_path)
        if not raw_text:
            raise ValueError("No text could be extracted from %s" % gcs_uri)
        _progress("extracting_text", 30, f"Extracted {len(raw_text)} characters")

        # ── Step 3: Resolve lesson ────────────────────────────────────
        if lesson_id:
            # Lesson ID provided by the main system — skip LLM identification
            _progress("fetching_data", 45, f"Fetching standard data for lesson {lesson_id}")
            matched_id = lesson_id
            matched_name = lesson_id  # Will be enriched from Neo4j data if available
            confidence = "provided"

            standard_concepts = get_concepts_by_lesson_id(matched_id)
            standard_locations = get_locations_by_lesson_id(matched_id)
            section_content = get_sections_by_lesson_id(matched_id)

        else:
            # Fallback: use LLM to identify the lesson
            _progress("finding_book", 35, f"Finding book for {subject} - {grade}")
            book = find_book(subject, grade)
            if not book:
                raise ValueError(
                    "No book found in Neo4j matching subject='%s', grade='%s'"
                    % (subject, grade)
                )

            _progress("identifying_lesson", 45, "Identifying lesson via LLM...")
            available_lessons = list_lessons(book["id"])
            if not available_lessons:
                raise ValueError(
                    "No lessons found in Neo4j for book %s" % book["id"]
                )

            match_result = identify_lesson(raw_text, available_lessons)
            matched_id = match_result.get("matched_lesson_id")
            matched_name = match_result.get("matched_lesson_name")
            confidence = match_result.get("confidence", "unknown")

            if not matched_id:
                logger.warning(
                    "LLM could not match a lesson. Falling back to whole-book data."
                )

            _progress(
                "identifying_lesson", 55,
                f"Matched → {matched_name} (confidence: {confidence})"
            )

            # Fetch standard data
            if matched_id:
                standard_concepts = get_concepts_by_lesson_id(matched_id)
                standard_locations = get_locations_by_lesson_id(matched_id)
                section_content = get_sections_by_lesson_id(matched_id)
            else:
                standard_concepts = get_standard_concepts(
                    book["subject"], book["grade"]
                )
                standard_locations = get_standard_locations(
                    book["subject"], book["grade"]
                )
                section_content = []

        _progress(
            "fetching_data", 60,
            f"Loaded {len(standard_concepts)} concepts, "
            f"{len(standard_locations)} locations, "
            f"{len(section_content)} sections",
        )

        if not standard_concepts:
            logger.warning(
                "No standard concepts found — evaluation will still proceed."
            )

        # ── Step 4: Evaluate via LLM ─────────────────────────────────
        _progress("evaluating", 70, "Evaluating lesson plan via Gemini...")
        evaluation = evaluate_lesson_plan(
            raw_text,
            standard_concepts,
            standard_locations,
            matched_lesson_name=matched_name,
            section_content=section_content,
        )

        _progress("evaluating", 90, "Evaluation complete, assembling result...")

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
        }

        _progress("completed", 100, "Pipeline finished successfully")
        return result

    except Exception:
        logger.exception("Pipeline failed for %s", gcs_uri)
        raise

    finally:
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            logger.info("Cleaned up temp file: %s", local_path)
