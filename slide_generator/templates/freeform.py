"""
Extended (freeform) card builders — no templateId.

These cover educational use cases that the 6 FE templates don't address:
title slides, bullet lists, section dividers, interactive blocks, and summaries.

The labels (TITLE_CARD, BULLET_CARD, etc.) exist ONLY inside our Python code
and Gemini's planning prompt. They NEVER appear in the output JSON.
The FE renders these purely from the CARD/LAYOUT/BLOCK node structure.
"""

from schema import (
    make_card,
    make_text_block,
    make_heading_block,
    make_quiz_block,
    make_flashcard_block,
    make_fill_blank_block,
)


def build_title_card(lesson_name: str, subtitle: str) -> dict:
    """
    TITLE_CARD — First slide only.
    Large lesson name (h1) + subtitle line (subject/teacher info).

    Structure: BLOCK(HEADING h1) + BLOCK(TEXT)
    """
    heading = make_heading_block(lesson_name, level=1)
    sub = make_text_block(f"<p>{subtitle}</p>")
    return make_card(lesson_name, [heading, sub])


def build_bullet_card(title: str, items: list[str]) -> dict:
    """
    BULLET_CARD — Lists of 4+ items that don't fit neatly into 2 or 3 columns.
    e.g., learning objectives, list of real-world applications.

    Structure: BLOCK(HEADING h2) + BLOCK(TEXT with <ul><li>)
    """
    heading = make_heading_block(title, level=2)
    li_items = "".join(f"<li>{item}</li>" for item in items)
    body = make_text_block(f"<ul>{li_items}</ul>")
    return make_card(title, [heading, body])


def build_section_divider(title: str) -> dict:
    """
    SECTION_DIV — Transition slide between major topic sections.
    Single large heading, distinct background color to signal a break.

    Structure: BLOCK(HEADING h1)
    """
    heading = make_heading_block(title, level=1)
    return make_card(
        title,
        [heading],
        background_color="#1e293b",  # dark slate — visually distinct from content slides
    )


def build_summary_card(title: str, takeaways: list[str]) -> dict:
    """
    SUMMARY_CARD — Last slide. Key takeaways as bullet points.

    Structure: BLOCK(HEADING h2) + BLOCK(TEXT with <ul><li>)
    """
    heading = make_heading_block(title, level=2)
    li_items = "".join(f"<li>{point}</li>" for point in takeaways)
    body = make_text_block(f"<ul>{li_items}</ul>")
    return make_card(title, [heading, body])


def build_quiz_card(title: str, questions: list[dict]) -> dict:
    """
    QUIZ_CARD — After a content section. Multiple-choice questions generated
    from covered concepts. Teacher can use for in-class assessment.

    questions: list of dicts with keys:
      - question (str)
      - options (list of str or list of {text: str})
      - correctIndex (int)
      - explanation (str, optional)

    Structure: BLOCK(HEADING h2) + BLOCK(QUIZ content)
    """
    heading = make_heading_block(title, level=2)
    quiz_block = make_quiz_block(title, questions)
    return make_card(title, [heading, quiz_block])


def build_flashcard_card(title: str, pairs: list[dict]) -> dict:
    """
    FLASHCARD_CARD — Review section. Front = concept name, Back = definition.
    Generated directly from covered_concepts in the evaluation result.

    pairs: list of {front: str, back: str}

    Structure: BLOCK(HEADING h2) + multiple BLOCK(FLASHCARD)
    """
    heading = make_heading_block(title, level=2)
    fc_blocks = [
        make_flashcard_block(p["front"], p["back"])
        for p in pairs
    ]
    return make_card(title, [heading] + fc_blocks)


def build_fill_blank_card(title: str, exercises: list[dict]) -> dict:
    """
    FILL_BLANK_CARD — Active recall. Key sentences from textbook with blanks.

    exercises: list of {sentence: str, blanks: list[str]}
    The sentence uses [bracket] notation to mark blanks,
    e.g. "Bản đồ là [hình ảnh] thu nhỏ bề mặt [Trái Đất]"

    Structure: BLOCK(HEADING h2) + multiple BLOCK(FILL_BLANK)
    """
    heading = make_heading_block(title, level=2)
    fb_blocks = [
        make_fill_blank_block(ex["sentence"], ex["blanks"])
        for ex in exercises
    ]
    return make_card(title, [heading] + fb_blocks)
