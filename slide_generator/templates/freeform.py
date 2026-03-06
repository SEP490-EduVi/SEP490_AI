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
    Single TEXT block with <h1> title + <p> subtitle in Tiptap-compatible HTML.
    """
    html = f"<h1>{lesson_name}</h1><p>{subtitle}</p>"
    block = make_text_block(html)
    return make_card(lesson_name, [block])


def build_bullet_card(title: str, items: list[str]) -> dict:
    """
    BULLET_CARD — Lists of 4+ items that don't fit neatly into 2 or 3 columns.

    Structure: BLOCK(HEADING h2) + BLOCK(TEXT with <ul><li>)
    """
    heading = make_heading_block(title, level=2)
    li_items = "".join(f"<li>{item}</li>" for item in items)
    body = make_text_block(f"<ul>{li_items}</ul>")
    return make_card(title, [heading, body])


def build_section_divider(title: str) -> dict:
    """
    SECTION_DIV — Transition slide between major topic sections.
    Single TEXT block with <h1> in HTML, dark background to signal a break.
    """
    block = make_text_block(f"<h1>{title}</h1>")
    return make_card(
        title,
        [block],
        background_color="#1e293b",
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
    QUIZ_CARD — Single QUIZ block (no heading). Title is inside the QUIZ content.

    questions: list of {question, options, correctIndex, explanation?}
    """
    quiz_block = make_quiz_block(title, questions)
    return make_card(title, [quiz_block])


def build_flashcard_card(title: str, pairs: list[dict]) -> dict:
    """
    FLASHCARD_CARD — Single FLASHCARD block containing all pairs.

    pairs: list of {front: str, back: str}
    """
    fc_block = make_flashcard_block(pairs)
    return make_card(title, [fc_block])


def build_fill_blank_card(title: str, exercises: list[dict]) -> dict:
    """
    FILL_BLANK_CARD — Single FILL_BLANK block containing all exercises.

    exercises: list of {sentence: str, blanks: list[str]}
    """
    fb_block = make_fill_blank_block(exercises)
    return make_card(title, [fb_block])
