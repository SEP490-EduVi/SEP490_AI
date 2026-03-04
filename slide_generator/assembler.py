"""
Assembler — takes the slide plan + generated content and builds the final IDocument.

This is pure Python — no AI calls. It routes each slide through the template
registry and assembles the complete IDocument JSON matching the FE contract.
"""

import logging
from schema import make_document, validate_document, ValidationError
from templates import get_builder

logger = logging.getLogger(__name__)


def _build_card(slide_plan: dict, slide_content: dict) -> dict:
    """
    Build a single ICard from plan metadata + generated content.

    Routes to the correct builder function based on templateType,
    passing content kwargs extracted from the content dict.
    """
    template_type = slide_plan["templateType"]
    title = slide_content.get("title") or slide_plan.get("title", "")
    content = slide_content.get("content", {})

    builder = get_builder(template_type)

    try:
        if template_type == "TITLE_CARD":
            return builder(
                lesson_name=title,
                subtitle=content.get("subtitle", ""),
            )

        elif template_type == "BULLET_CARD":
            items = content.get("items", [f"Nội dung {title}"])
            return builder(title=title, items=items)

        elif template_type == "SECTION_DIV":
            return builder(title=title)

        elif template_type == "SUMMARY_CARD":
            takeaways = content.get("takeaways", [])
            return builder(title=title, takeaways=takeaways)

        elif template_type in ("template-003", "template-004"):
            return builder(
                title=title,
                left_html=content.get("left_html", "<p></p>"),
                right_html=content.get("right_html", "<p></p>"),
            )

        elif template_type in ("template-005", "template-006"):
            return builder(
                title=title,
                col1_html=content.get("col1_html", "<p></p>"),
                col2_html=content.get("col2_html", "<p></p>"),
                col3_html=content.get("col3_html", "<p></p>"),
            )

        elif template_type in ("template-001", "template-002"):
            # v1: image templates fall back to two-column text
            # (image sourcing is deferred to v2)
            logger.warning("Image template %r used in v1 — rendering as text-only card", template_type)
            left = content.get("left_html", content.get("text_html", "<p></p>"))
            right = content.get("right_html", "<p></p>")
            from templates.fe_templates import build_two_columns_alt
            return build_two_columns_alt(title=title, left_html=left, right_html=right)

        elif template_type == "QUIZ_CARD":
            questions = content.get("questions", [])
            return builder(title=title, questions=questions)

        elif template_type == "FLASHCARD_CARD":
            pairs = content.get("pairs", [])
            return builder(title=title, pairs=pairs)

        elif template_type == "FILL_BLANK_CARD":
            exercises = content.get("exercises", [])
            return builder(title=title, exercises=exercises)

        else:
            # Unknown type — render as a bullet card with the focus text
            logger.warning("Unknown templateType %r — rendering as BULLET_CARD", template_type)
            from templates.freeform import build_bullet_card
            return build_bullet_card(title=title, items=[slide_plan.get("focus", title)])

    except Exception as exc:
        logger.exception(
            "Failed to build card for slide %d (templateType=%r): %s",
            slide_plan.get("index", "?"), template_type, exc,
        )
        # Fallback: simple bullet card so the deck doesn't break entirely
        from templates.freeform import build_bullet_card
        return build_bullet_card(title=title, items=[f"Lỗi tạo slide: {exc}"])


def assemble_document(
    presentation_title: str,
    slide_plan: list[dict],
    slide_contents: list[dict],
) -> dict:
    """
    Assemble the full IDocument from a slide plan and generated content.

    Args:
        presentation_title: The document title
        slide_plan: list of slide plan dicts from planner.py
        slide_contents: list of content dicts from content_generator.py
                        (same length and order as slide_plan)

    Returns:
        A valid IDocument dict ready to be published via RabbitMQ.
    """
    # Index content by slide index for fast lookup
    content_by_index: dict[int, dict] = {
        item["index"]: item for item in slide_contents
    }

    cards = []
    for plan_slide in slide_plan:
        idx = plan_slide["index"]
        content_slide = content_by_index.get(idx, {
            "index": idx,
            "templateType": plan_slide["templateType"],
            "title": plan_slide["title"],
            "content": {},
        })
        card = _build_card(plan_slide, content_slide)
        cards.append(card)
        logger.debug(
            "Assembled card %d: %r (templateType=%r)",
            idx, plan_slide["title"], plan_slide["templateType"],
        )

    document = make_document(presentation_title, cards)

    # Validate before returning
    try:
        validate_document(document)
        logger.info(
            "Document validation passed: %d cards, title=%r",
            len(cards), presentation_title,
        )
    except ValidationError as exc:
        logger.error("Document validation FAILED: %s", exc)
        raise

    return document
