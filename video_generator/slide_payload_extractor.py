"""Utilities to load and extract render-ready slide payloads.

Supports loading JSON from:
- Local file paths
- Google Cloud Storage URIs (gs://bucket/path.json)

And extracting slide data with HTML/CSS/image asset references for
high-fidelity rendering pipelines.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List

from .utils import extract_lesson_data


@dataclass
class RenderSlide:
    """A normalized render-ready slide payload."""

    slide_id: str
    title: str
    html: str
    css: str
    background_color: str
    assets: List[str]


def load_json_document(source: str) -> Dict[str, Any]:
    """Load a JSON document from local path or gs:// URI."""
    if source.startswith("gs://"):
        return _load_json_from_gcs(source)

    with open(source, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_render_slides(document: Dict[str, Any]) -> List[RenderSlide]:
    """Extract normalized render-ready slides from lesson JSON."""
    lesson = extract_lesson_data(document)

    lesson_css = (
        lesson.get("renderCss")
        or lesson.get("css")
        or lesson.get("globalCss")
        or ""
    )

    cards = _extract_cards(lesson)
    slides: List[RenderSlide] = []
    for index, card in enumerate(cards, start=1):
        html_content = _extract_card_html(card)
        card_css = card.get("renderCss") or card.get("css") or lesson_css
        bg = card.get("backgroundColor") or lesson.get("backgroundColor") or ""

        slides.append(
            RenderSlide(
                slide_id=str(card.get("id") or f"slide-{index}"),
                title=str(card.get("title") or ""),
                html=html_content,
                css=str(card_css or ""),
                background_color=str(bg),
                assets=_collect_assets(card),
            )
        )

    return slides


def _extract_cards(lesson: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract cards/slides from heterogeneous lesson shapes."""
    cards = lesson.get("cards")
    if isinstance(cards, list) and cards:
        return cards

    rendered = lesson.get("renderSlides") or lesson.get("slides")
    if isinstance(rendered, list) and rendered:
        return rendered

    return []


def _extract_card_html(card: Dict[str, Any]) -> str:
    """Extract best available HTML for a card.

    Priority:
    1) card.renderedHtml (full HTML document from frontend)
    2) card.renderHtml / card.html
    3) concat block-level content HTML from children
    """
    if card.get("renderedHtml"):
        return str(card.get("renderedHtml"))

    if card.get("renderHtml"):
        return str(card.get("renderHtml"))
    if card.get("html"):
        return str(card.get("html"))

    parts: List[str] = []
    for child in card.get("children", []):
        child_type = str(child.get("type") or "").upper()
        if child_type == "BLOCK":
            content = child.get("content") or {}
            block_html = content.get("renderHtml") or content.get("html")
            if block_html:
                parts.append(str(block_html))
        elif child_type == "LAYOUT":
            parts.append(_extract_layout_html(child))

    return "\n".join(parts)


def _extract_layout_html(layout: Dict[str, Any]) -> str:
    """Best-effort flatten nested layout HTML when present."""
    parts: List[str] = []
    for child in layout.get("children", []):
        if str(child.get("type") or "").upper() == "BLOCK":
            content = child.get("content") or {}
            block_html = content.get("renderHtml") or content.get("html")
            if block_html:
                parts.append(str(block_html))
        elif str(child.get("type") or "").upper() == "LAYOUT":
            nested = _extract_layout_html(child)
            if nested:
                parts.append(nested)
    return "\n".join(parts)


def _collect_assets(card: Dict[str, Any]) -> List[str]:
    """Collect image/data asset references recursively from card."""
    assets: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            content = node.get("content") if isinstance(node.get("content"), dict) else node
            for key in ("src", "url", "imageUrl", "assetUrl", "fontUrl"):
                value = content.get(key)
                if isinstance(value, str) and value.strip():
                    assets.append(value)

            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(card)

    # Keep stable order while de-duplicating.
    dedup: List[str] = []
    seen = set()
    for asset in assets:
        if asset not in seen:
            seen.add(asset)
            dedup.append(asset)
    return dedup


def _load_json_from_gcs(uri: str) -> Dict[str, Any]:
    """Load JSON from Google Cloud Storage URI."""
    # Lazy import so local usage does not require gcs dependency.
    from google.cloud import storage

    bucket, blob = _parse_gs_uri(uri)
    client = storage.Client()
    payload = client.bucket(bucket).blob(blob).download_as_text(encoding="utf-8")
    return json.loads(payload)


def _parse_gs_uri(uri: str) -> tuple[str, str]:
    """Parse gs://bucket/path URI."""
    if not uri.startswith("gs://"):
        raise ValueError("URI must start with gs://")

    raw = uri[5:]
    if "/" not in raw:
        raise ValueError("Invalid gs:// URI, missing object path")

    bucket, blob = raw.split("/", 1)
    if not bucket or not blob:
        raise ValueError("Invalid gs:// URI")
    return bucket, blob
