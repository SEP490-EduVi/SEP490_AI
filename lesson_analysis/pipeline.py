"""Pipeline: Download → Extract text → Fetch curriculum YeuCau → Evaluate (async)."""

import asyncio
import os
import logging
from typing import Awaitable, Callable
from gcs_handler import download_from_gcs
from extractor import extract_text
from neo4j_client import get_yeu_cau_by_lesson_id
from evaluator import evaluate_lesson_plan

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
    lesson_id: str,
    curriculum_year: int | None = None,
    on_progress: ProgressCallback | None = None,
) -> dict:
    """
    Run the lesson-analysis pipeline (async).

    lesson_id (lessonCode) is required — the backend always provides it.
    curriculum_year is optional metadata, passed through to the result.

    Flow:
      1. Download file from GCS
      2. Extract text from PDF/DOCX
      3. Build full Neo4j lesson ID
      4. Fetch YeuCau/ChuDe from curriculum graph (Lesson → COVERS → ChuDe → HAS → YeuCau)
      5. Evaluate the lesson plan against curriculum standards via Gemini
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

        # ── Step 3: Resolve lesson ID ─────────────────────────────────
        matched_id = _build_full_lesson_id(subject, grade, lesson_id)
        if matched_id != lesson_id:
            logger.info("Resolved lesson ID: %s → %s", lesson_id, matched_id)

        # ── Step 4: Fetch curriculum YeuCau/ChuDe ────────────────────
        await _progress("fetching_data", 40, f"Fetching curriculum data for lesson {matched_id}...")
        curriculum_data = await asyncio.to_thread(get_yeu_cau_by_lesson_id, matched_id)
        yeu_cau_count = len(curriculum_data.get("yeu_cau_list", []))
        chu_de_count = len(curriculum_data.get("chu_de_list", []))

        if not yeu_cau_count:
            logger.warning(
                "No YeuCau found for lesson %s — COVERS links may not be created yet. "
                "Evaluation will proceed without curriculum standards.", matched_id,
            )

        await _progress(
            "fetching_data", 55,
            f"Loaded {chu_de_count} chủ đề, {yeu_cau_count} yêu cầu cần đạt",
        )

        # ── Step 5: Evaluate via Gemini ───────────────────────────────
        await _progress("evaluating", 65, "Evaluating lesson plan via Gemini...")
        evaluation = await evaluate_lesson_plan(
            raw_text,
            curriculum_data,
            matched_lesson_name=matched_id,
        )

        await _progress("evaluating", 95, "Evaluation complete, assembling result...")

        result = {
            "lesson_plan_file": gcs_uri,
            "subject": subject,
            "grade": grade,
            "curriculum_year": curriculum_year,
            "matched_lesson": {
                "id": matched_id,
                "name": evaluation.get("detected_lesson_name", matched_id),
                "confidence": "provided",
            },
            "curriculum": curriculum_data,
            "evaluation": evaluation,
            "lesson_plan_text": raw_text,
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
