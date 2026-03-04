"""
The 6 official FE card templates (template-001 through template-006).
Each builder function returns a valid ICard dict matching the FE contract.

Template categorization for Gemini's use:
  template-001 (SIDEBAR_LEFT):  Visual subject — map, diagram, geographic feature.
                                 Image is the anchor; text explains the visual.
  template-002 (SIDEBAR_RIGHT): Explanation-first — definition matters more than visual.
                                 Text is primary; image is supporting evidence.
  template-003 (TWO_COLUMN):    Compare TWO things with SHORT text per column.
                                 e.g., two projection types, two regions, cause/effect.
  template-004 (TWO_COLUMN):    Compare TWO things with DENSE text per column (3+ lines).
                                 e.g., detailed feature comparison, paired concept explanation.
  template-005 (THREE_COLUMN):  Exactly THREE categories/types with BRIEF description each.
                                 e.g., "3 loại phép chiếu: hình trụ / hình nón / phương vị".
  template-006 (THREE_COLUMN):  THREE-way comparison with DENSE text per column.
                                 Use when each of the 3 items needs a paragraph.
"""

from schema import make_card, make_layout, make_text_block, make_heading_block, make_image_block


def build_image_text_left(
    title: str,
    image_src: str,
    image_alt: str,
    text_html: str,
    image_caption: str = "",
) -> dict:
    """
    template-001: Image (left, 1/3 width) + Text (right, 2/3 width).
    SIDEBAR_LEFT layout.
    """
    image_block = make_image_block(image_src, image_alt, caption=image_caption)
    text_block = make_text_block(text_html)
    layout = make_layout("SIDEBAR_LEFT", [image_block, text_block], gap=6)
    return make_card(title, [layout], template_id="template-001")


def build_text_image_right(
    title: str,
    text_html: str,
    image_src: str,
    image_alt: str,
    image_caption: str = "",
) -> dict:
    """
    template-002: Text (left, 2/3 width) + Image (right, 1/3 width).
    SIDEBAR_RIGHT layout.
    """
    text_block = make_text_block(text_html)
    image_block = make_image_block(image_src, image_alt, caption=image_caption)
    layout = make_layout("SIDEBAR_RIGHT", [text_block, image_block], gap=6)
    return make_card(title, [layout], template_id="template-002")


def build_two_columns(
    title: str,
    left_html: str,
    right_html: str,
) -> dict:
    """
    template-003: Centered heading + 2 equal columns of SHORT text.
    Use for brief comparisons (1-3 lines per column).
    """
    heading = make_heading_block(title, level=2)
    left_block = make_text_block(left_html)
    right_block = make_text_block(right_html)
    layout = make_layout("TWO_COLUMN", [left_block, right_block], gap=6)
    return make_card(title, [heading, layout], template_id="template-003")


def build_two_columns_alt(
    title: str,
    left_html: str,
    right_html: str,
) -> dict:
    """
    template-004: Centered heading + 2 equal columns of DENSE text.
    Use for detailed comparisons (3+ lines per column).
    Same structure as template-003 — templateId differentiates them for FE analytics.
    """
    heading = make_heading_block(title, level=2)
    left_block = make_text_block(left_html)
    right_block = make_text_block(right_html)
    layout = make_layout("TWO_COLUMN", [left_block, right_block], gap=6)
    return make_card(title, [heading, layout], template_id="template-004")


def build_three_columns(
    title: str,
    col1_html: str,
    col2_html: str,
    col3_html: str,
) -> dict:
    """
    template-005: Centered heading + 3 equal columns of BRIEF text.
    Use for 3 categories/types with short description each.
    """
    heading = make_heading_block(title, level=2)
    c1 = make_text_block(col1_html)
    c2 = make_text_block(col2_html)
    c3 = make_text_block(col3_html)
    layout = make_layout("THREE_COLUMN", [c1, c2, c3], gap=6)
    return make_card(title, [heading, layout], template_id="template-005")


def build_three_columns_alt(
    title: str,
    col1_html: str,
    col2_html: str,
    col3_html: str,
) -> dict:
    """
    template-006: Centered heading + 3 equal columns of DENSE text.
    Use for 3-way comparisons where each item needs a paragraph.
    Same structure as template-005 — templateId differentiates them for FE analytics.
    """
    heading = make_heading_block(title, level=2)
    c1 = make_text_block(col1_html)
    c2 = make_text_block(col2_html)
    c3 = make_text_block(col3_html)
    layout = make_layout("THREE_COLUMN", [c1, c2, c3], gap=6)
    return make_card(title, [heading, layout], template_id="template-006")
