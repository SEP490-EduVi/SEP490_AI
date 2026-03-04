"""
Template registry — maps templateType string (used internally by Gemini's plan)
to the appropriate builder function.

These type strings appear ONLY in the slide plan JSON exchanged between
planner.py and assembler.py. They NEVER appear in the final IDocument output.
"""

from templates.fe_templates import (
    build_image_text_left,
    build_text_image_right,
    build_two_columns,
    build_two_columns_alt,
    build_three_columns,
    build_three_columns_alt,
)
from templates.freeform import (
    build_title_card,
    build_bullet_card,
    build_section_divider,
    build_summary_card,
    build_quiz_card,
    build_flashcard_card,
    build_fill_blank_card,
)

# Maps the templateType label (from planner.py) → builder function.
# Builder functions are called by assembler.py with content kwargs.
TEMPLATE_REGISTRY: dict = {
    # ── FE templates (produce templateId in output) ──────────────────
    "template-001": build_image_text_left,
    "template-002": build_text_image_right,
    "template-003": build_two_columns,
    "template-004": build_two_columns_alt,
    "template-005": build_three_columns,
    "template-006": build_three_columns_alt,
    # ── Freeform types (no templateId in output) ─────────────────────
    "TITLE_CARD":     build_title_card,
    "BULLET_CARD":    build_bullet_card,
    "SECTION_DIV":    build_section_divider,
    "SUMMARY_CARD":   build_summary_card,
    "QUIZ_CARD":      build_quiz_card,
    "FLASHCARD_CARD": build_flashcard_card,
    "FILL_BLANK_CARD": build_fill_blank_card,
}

# All valid templateType values — used in planner prompt and validation
ALL_TEMPLATE_TYPES = set(TEMPLATE_REGISTRY.keys())

# templateTypes that require image content from the caller
IMAGE_TEMPLATE_TYPES = {"template-001", "template-002"}

# templateTypes that produce interactive blocks
INTERACTIVE_TEMPLATE_TYPES = {"QUIZ_CARD", "FLASHCARD_CARD", "FILL_BLANK_CARD"}


def get_builder(template_type: str):
    """Return the builder function for a given templateType. Raises KeyError if unknown."""
    if template_type not in TEMPLATE_REGISTRY:
        raise KeyError(
            f"Unknown templateType: {template_type!r}. "
            f"Valid types: {sorted(ALL_TEMPLATE_TYPES)}"
        )
    return TEMPLATE_REGISTRY[template_type]
