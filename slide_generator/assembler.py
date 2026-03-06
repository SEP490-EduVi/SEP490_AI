"""
Assembler — takes the slide plan + generated content and builds the final IDocument.

This is pure Python — no AI calls. It routes each slide through the template
registry and assembles the complete IDocument JSON matching the FE contract.
"""

import logging
from schema import make_document, validate_document, ValidationError
from templates import get_builder

logger = logging.getLogger(__name__)


def _build_card(slide_plan: dict, slide_content: dict, subject: str = "", grade: str = "") -> dict | list[dict]:
    """
    Build one or more ICards from plan metadata + generated content.

    Returns a single card dict for most template types.
    For interactive types (QUIZ/FLASHCARD/FILL_BLANK), returns a list of cards
    with 1 item per card to prevent FE slide overflow.
    """
    template_type = slide_plan["templateType"]
    title = slide_content.get("title") or slide_plan.get("title", "")
    content = slide_content.get("content", {})

    try:
        builder = get_builder(template_type)

        if template_type == "TITLE_CARD":
            # Build subtitle from pipeline context — never rely on Gemini's subtitle
            # which may echo placeholder text from the prompt.
            subtitle = f"Lớp {grade}" if grade else ""
            return builder(
                lesson_name=title,
                subtitle=subtitle,
            )

        elif template_type == "BULLET_CARD":
            items = content.get("items") or [f"Nội dung {title}"]
            return builder(title=title, items=items)

        elif template_type == "SECTION_DIV":
            return builder(title=title)

        elif template_type == "SUMMARY_CARD":
            takeaways = content.get("takeaways") or [f"Tóm tắt nội dung: {title}"]
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

        elif template_type == "template-001":
            # Image left + text right. Placeholder image — teacher adds real one later.
            return builder(
                title=title,
                image_src="",
                image_alt=content.get("image_alt", "Vui lòng thêm hình ảnh"),
                text_html=content.get("text_html", "<p></p>"),
            )

        elif template_type == "template-002":
            # Text left + image right. Placeholder image — teacher adds real one later.
            return builder(
                title=title,
                text_html=content.get("text_html", "<p></p>"),
                image_src="",
                image_alt=content.get("image_alt", "Vui lòng thêm hình ảnh"),
            )

        elif template_type == "QUIZ_CARD":
            questions = content.get("questions", [])
            if not questions:
                logger.warning("Slide %d QUIZ_CARD has no questions — skipping", slide_plan.get("index"))
                from templates.freeform import build_bullet_card
                return build_bullet_card(title=title, items=["Không có câu hỏi"])
            # Split: 1 question per card to prevent overflow
            total = len(questions)
            cards = []
            for i, q in enumerate(questions, 1):
                card_title = f"{title} ({i}/{total})" if total > 1 else title
                cards.append(builder(title=card_title, questions=[q]))
            return cards

        elif template_type == "FLASHCARD_CARD":
            pairs = content.get("pairs", [])
            if not pairs:
                logger.warning("Slide %d FLASHCARD_CARD has no pairs — skipping", slide_plan.get("index"))
                from templates.freeform import build_bullet_card
                return build_bullet_card(title=title, items=["Không có dữ liệu flashcard"])
            # Split: 1 pair per card to prevent overflow
            total = len(pairs)
            cards = []
            for i, p in enumerate(pairs, 1):
                card_title = f"{title} ({i}/{total})" if total > 1 else title
                cards.append(builder(title=card_title, pairs=[p]))
            return cards

        elif template_type == "FILL_BLANK_CARD":
            exercises = content.get("exercises", [])
            if not exercises:
                logger.warning("Slide %d FILL_BLANK_CARD has no exercises — skipping", slide_plan.get("index"))
                from templates.freeform import build_bullet_card
                return build_bullet_card(title=title, items=["Không có bài tập điền khuyết"])
            # Split: 1 exercise per card to prevent overflow
            total = len(exercises)
            cards = []
            for i, ex in enumerate(exercises, 1):
                card_title = f"{title} ({i}/{total})" if total > 1 else title
                cards.append(builder(title=card_title, exercises=[ex]))
            return cards

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
    subject: str = "",
    grade: str = "",
) -> dict:
    """
    Assemble the full IDocument from a slide plan and generated content.

    Args:
        presentation_title: The document title
        slide_plan: list of slide plan dicts from planner.py
        slide_contents: list of content dicts from content_generator.py
                        (same length and order as slide_plan)
        subject: Subject code (e.g. "dia_li") used to construct TITLE_CARD subtitle
        grade: Grade string (e.g. "10") used to construct TITLE_CARD subtitle

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
        result = _build_card(plan_slide, content_slide, subject=subject, grade=grade)
        if isinstance(result, list):
            cards.extend(result)
            logger.debug(
                "Assembled %d cards from slide %d: %r (templateType=%r)",
                len(result), idx, plan_slide["title"], plan_slide["templateType"],
            )
        else:
            cards.append(result)
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
