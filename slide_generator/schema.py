"""
IDocument schema — Python equivalents of the FE TypeScript interfaces.

All node types, content types, layout variants, and a validation function
that checks generated output against the FE contract before publishing.
"""

from __future__ import annotations
import uuid
from datetime import datetime, timezone


# ── ID generation ─────────────────────────────────────────────────────


def gen_id(prefix: str = "") -> str:
    """Generate a short UUID-based node ID, e.g. 'card-a1b2c3ef'."""
    short = str(uuid.uuid4()).replace("-", "")[:8]
    return f"{prefix}{short}" if prefix else short


def now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ── Layout variants ────────────────────────────────────────────────────

LAYOUT_VARIANTS = {
    "SINGLE",
    "TWO_COLUMN",
    "THREE_COLUMN",
    "SIDEBAR_LEFT",
    "SIDEBAR_RIGHT",
    "GRID",
}

# ── Block content types ────────────────────────────────────────────────

BLOCK_CONTENT_TYPES = {
    "TEXT",
    "HEADING",
    "IMAGE",
    "VIDEO",
    "QUIZ",
    "FLASHCARD",
    "FILL_BLANK",
}


# ── Node builder helpers ───────────────────────────────────────────────


def make_text_block(html: str) -> dict:
    """Create a BLOCK node with TEXT content."""
    return {
        "id": gen_id("block-"),
        "type": "BLOCK",
        "content": {
            "type": "TEXT",
            "html": html,
        },
        "children": [],
    }


def make_heading_block(text: str, level: int = 1) -> dict:
    """Create a BLOCK node with HEADING content (level 1-6)."""
    return {
        "id": gen_id("block-"),
        "type": "BLOCK",
        "content": {
            "type": "HEADING",
            "html": text,
            "level": max(1, min(6, level)),
        },
        "children": [],
    }


def make_image_block(src: str, alt: str, caption: str = "") -> dict:
    """Create a BLOCK node with IMAGE content."""
    block: dict = {
        "id": gen_id("block-"),
        "type": "BLOCK",
        "content": {
            "type": "IMAGE",
            "src": src,
            "alt": alt,
        },
        "children": [],
    }
    if caption:
        block["content"]["caption"] = caption
    return block


def make_quiz_block(title: str, questions: list[dict]) -> dict:
    """
    Create a BLOCK node with QUIZ content.
    questions: list of {question, options: [{text}], correctIndex, explanation?}
    """
    formatted_questions = []
    for q in questions:
        fq = {
            "id": gen_id("q-"),
            "question": q.get("question", ""),
            "options": [
                {"id": gen_id("opt-"), "text": opt.get("text", opt) if isinstance(opt, dict) else opt}
                for opt in q.get("options", [])
            ],
            "correctIndex": q.get("correctIndex", 0),
        }
        if q.get("explanation"):
            fq["explanation"] = q["explanation"]
        formatted_questions.append(fq)

    return {
        "id": gen_id("block-"),
        "type": "BLOCK",
        "content": {
            "type": "QUIZ",
            "title": title,
            "questions": formatted_questions,
        },
        "children": [],
    }


def make_flashcard_block(pairs: list[dict]) -> dict:
    """
    Create a BLOCK node with FLASHCARD content.
    pairs: list of {front: str, back: str}
    """
    cards = [
        {"id": gen_id("fc-"), "front": p["front"], "back": p["back"]}
        for p in pairs
    ]
    return {
        "id": gen_id("block-"),
        "type": "BLOCK",
        "content": {
            "type": "FLASHCARD",
            "cards": cards,
        },
        "children": [],
    }


def make_fill_blank_block(exercises: list[dict]) -> dict:
    """
    Create a BLOCK node with FILL_BLANK content.
    exercises: list of {sentence: str, blanks: list[str]}
    """
    items = [
        {"id": gen_id("fb-"), "sentence": ex["sentence"], "blanks": ex["blanks"]}
        for ex in exercises
    ]
    return {
        "id": gen_id("block-"),
        "type": "BLOCK",
        "content": {
            "type": "FILL_BLANK",
            "exercises": items,
        },
        "children": [],
    }


def make_layout(variant: str, children: list[dict], gap: int = 6) -> dict:
    """Create a LAYOUT node with child BLOCKs."""
    return {
        "id": gen_id("layout-"),
        "type": "LAYOUT",
        "variant": variant,
        "gap": gap,
        "children": children,
    }


def make_card(
    title: str,
    children: list[dict],
    template_id: str | None = None,
    background_color: str | None = None,
    background_image: str | None = None,
) -> dict:
    """Create a CARD node. Optional fields are omitted when None."""
    card: dict = {
        "id": gen_id("card-"),
        "type": "CARD",
        "title": title,
        "children": children,
    }
    if template_id:
        card["templateId"] = template_id
    if background_color is not None:
        card["backgroundColor"] = background_color
    if background_image is not None:
        card["backgroundImage"] = background_image
    return card


def make_document(title: str, cards: list[dict]) -> dict:
    """Wrap cards into a full IDocument."""
    active_card_id = cards[0]["id"] if cards else None
    return {
        "id": gen_id("doc-"),
        "title": title,
        "activeCardId": active_card_id,
        "createdAt": now_iso(),
        "updatedAt": now_iso(),
        "cards": cards,
    }


# ── Validation ─────────────────────────────────────────────────────────


class ValidationError(Exception):
    pass


def _validate_block(block: dict, path: str) -> None:
    if block.get("type") != "BLOCK":
        raise ValidationError(f"{path}: type must be 'BLOCK', got {block.get('type')!r}")
    if not isinstance(block.get("id"), str) or not block["id"]:
        raise ValidationError(f"{path}: missing or empty id")
    content = block.get("content")
    if not isinstance(content, dict):
        raise ValidationError(f"{path}: content must be a dict")
    ctype = content.get("type")
    if ctype not in BLOCK_CONTENT_TYPES:
        raise ValidationError(f"{path}: invalid content type {ctype!r}")
    if ctype in ("TEXT",) and not isinstance(content.get("html"), str):
        raise ValidationError(f"{path}: TEXT content missing html string")
    if ctype == "HEADING":
        if not isinstance(content.get("html"), str):
            raise ValidationError(f"{path}: HEADING content missing html string")
        if content.get("level") not in (1, 2, 3, 4, 5, 6):
            raise ValidationError(f"{path}: HEADING level must be 1-6")
    if ctype == "IMAGE":
        if not isinstance(content.get("src"), str):
            raise ValidationError(f"{path}: IMAGE content missing src")
        if not isinstance(content.get("alt"), str):
            raise ValidationError(f"{path}: IMAGE content missing alt")
    if ctype == "QUIZ":
        if not isinstance(content.get("questions"), list) or not content["questions"]:
            raise ValidationError(f"{path}: QUIZ content missing questions list")
    if ctype == "FLASHCARD":
        if not isinstance(content.get("cards"), list) or not content["cards"]:
            raise ValidationError(f"{path}: FLASHCARD content missing cards list")
    if ctype == "FILL_BLANK":
        if not isinstance(content.get("exercises"), list) or not content["exercises"]:
            raise ValidationError(f"{path}: FILL_BLANK content missing exercises list")
    if not isinstance(block.get("children"), list):
        raise ValidationError(f"{path}: children must be a list")


def _validate_layout(layout: dict, path: str) -> None:
    if layout.get("type") != "LAYOUT":
        raise ValidationError(f"{path}: type must be 'LAYOUT', got {layout.get('type')!r}")
    if not isinstance(layout.get("id"), str) or not layout["id"]:
        raise ValidationError(f"{path}: missing or empty id")
    if layout.get("variant") not in LAYOUT_VARIANTS:
        raise ValidationError(f"{path}: invalid variant {layout.get('variant')!r}")
    if not isinstance(layout.get("gap"), int):
        raise ValidationError(f"{path}: gap must be an int")
    for i, child in enumerate(layout.get("children", [])):
        _validate_block(child, f"{path}.children[{i}]")


def _validate_card(card: dict, path: str) -> None:
    if card.get("type") != "CARD":
        raise ValidationError(f"{path}: type must be 'CARD', got {card.get('type')!r}")
    if not isinstance(card.get("id"), str) or not card["id"]:
        raise ValidationError(f"{path}: missing or empty id")
    if not isinstance(card.get("title"), str):
        raise ValidationError(f"{path}: title must be a string")
    if "templateId" in card and not isinstance(card["templateId"], str):
        raise ValidationError(f"{path}: templateId must be a string when present")
    for i, child in enumerate(card.get("children", [])):
        child_path = f"{path}.children[{i}]"
        ctype = child.get("type")
        if ctype == "LAYOUT":
            _validate_layout(child, child_path)
        elif ctype == "BLOCK":
            _validate_block(child, child_path)
        else:
            raise ValidationError(f"{child_path}: unknown node type {ctype!r}")


def validate_document(doc: dict) -> None:
    """
    Validate the full IDocument structure against the FE contract.
    Raises ValidationError with a descriptive message if anything is wrong.
    """
    if not isinstance(doc.get("id"), str):
        raise ValidationError("document: missing id")
    if not isinstance(doc.get("title"), str) or not doc["title"]:
        raise ValidationError("document: missing or empty title")
    if not isinstance(doc.get("cards"), list):
        raise ValidationError("document: cards must be a list")
    for i, card in enumerate(doc["cards"]):
        _validate_card(card, f"cards[{i}]")
