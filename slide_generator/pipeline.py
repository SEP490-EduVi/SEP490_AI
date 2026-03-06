"""
Pipeline — orchestrates the full slide generation flow.

Phase 1 (internal): planner.py    — Gemini decides slide structure
Phase 2 (internal): content_generator.py — Gemini generates content per slide
Phase 3 (Python):   assembler.py  — builds IDocument JSON from plan + content
"""

import logging
from typing import Callable

from planner import plan_presentation
from content_generator import generate_all_slide_content
from assembler import assemble_document

logger = logging.getLogger(__name__)

# Type alias for progress callback — matches lesson_analysis pattern
ProgressCallback = Callable[[str, int, str], None]  # (step, progress, detail)


def run(
    evaluation_result: dict,
    lesson_plan_text: str,
    textbook_sections: list[dict],
    preferences: dict,
    on_progress: ProgressCallback | None = None,
) -> dict:
    """
    Run the full slide generation pipeline.

    Args:
        evaluation_result: The full evaluation dict from Service 2
                           (stored in Product.EvaluationResult SQL column)
        lesson_plan_text:  Extracted text from the teacher's lesson plan file
        textbook_sections: list of {heading, content} from Neo4j
        preferences:       dict with "slideRange": "short" | "medium" | "detailed"
        on_progress:       callback(step, progress, detail) for RabbitMQ progress messages

    Returns:
        dict with keys: title, cards, activeCardId
        (wrapped into full IDocument by the ASP.NET backend before storing)
    """

    def _progress(step: str, progress: int, detail: str = "") -> None:
        logger.info("[%s] %d%% — %s", step, progress, detail)
        if on_progress:
            on_progress(step, progress, detail)

    # ── Extract data from evaluation result ───────────────────────────

    evaluation = evaluation_result.get("evaluation", {})
    matched_lesson = evaluation_result.get("matched_lesson", {})

    lesson_name = (
        evaluation.get("detected_lesson_name")
        or matched_lesson.get("name")
        or "Bài học"
    )
    subject = evaluation_result.get("subject", "")
    grade = evaluation_result.get("grade", "")
    # Normalise: "lop_10" → "10", plain "10" stays "10"
    if grade.startswith("lop_"):
        grade = grade[4:]
    slide_range = preferences.get("slideRange", "medium")

    # ── Build context dict for Gemini calls ───────────────────────────

    context = {
        "lesson_name": lesson_name,
        "subject": subject,
        "grade": grade,
        "objectives": evaluation.get("objectives", []),
        "covered_concepts": evaluation.get("covered_concepts", []),
        "missing_concepts": evaluation.get("missing_concepts", []),
        "textbook_key_topics": evaluation.get("textbook_key_topics", []),
        "activities": evaluation.get("activities", []),
        "textbook_sections": textbook_sections,
        "lesson_plan_text": lesson_plan_text,
    }

    # ── Phase 1: Plan the presentation structure ──────────────────────

    _progress("planning", 10, "Lên kế hoạch cấu trúc bài thuyết trình...")

    slide_plan = plan_presentation(context, slide_range)
    total_slides = len(slide_plan)

    if total_slides == 0:
        raise ValueError("Planner returned an empty slide plan — cannot generate presentation")

    _progress(
        "planning", 15,
        f"Kế hoạch hoàn tất: {total_slides} slide — bắt đầu tạo nội dung...",
    )

    # ── Phase 2: Generate content per slide ───────────────────────────

    generated_count = 0

    def _on_slide_done(slide_index: int, slide_title: str) -> None:
        nonlocal generated_count
        generated_count += 1
        # Progress: 15% to 90% spread across all slides
        slide_progress = 15 + int((generated_count / total_slides) * 75)
        _progress(
            "generating_slides",
            slide_progress,
            f"Đang tạo slide {generated_count}/{total_slides}: {slide_title}",
        )

    slide_contents = generate_all_slide_content(
        slide_plan,
        context,
        on_slide_done=_on_slide_done,
    )

    # ── Phase 3: Assemble the IDocument ──────────────────────────────

    _progress("assembling", 92, "Đang tổng hợp bài thuyết trình...")

    document = assemble_document(lesson_name, slide_plan, slide_contents, subject=subject, grade=grade)

    # Return only the data the ASP.NET backend needs to store
    # (it will wrap this with its own id, createdAt, updatedAt)
    return {
        "title": document["title"],
        "cards": document["cards"],
        "activeCardId": document["activeCardId"],
    }
