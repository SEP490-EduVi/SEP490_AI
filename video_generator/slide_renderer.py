"""Slide renderer using Playwright for HTML screenshot.

Strict JSON mode:
- Render exactly what frontend payload provides via `renderedHtml`, `renderHtml`, or `html`.
- No synthetic template or content transformation.
"""

import asyncio
import html
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

SLIDE_WIDTH = 1280
SLIDE_HEIGHT = 720

_browser = None
_playwright = None
_browser_lock = None
_current_loop = None


def reset_browser_state() -> None:
    """Reset browser globals. Must be called at start of each asyncio.run() scope."""
    global _browser, _playwright, _browser_lock, _current_loop
    _browser = None
    _playwright = None
    _browser_lock = None
    _current_loop = None


async def _get_browser():
    """Get or create a shared browser instance for current event loop."""
    global _browser, _playwright, _browser_lock, _current_loop

    # If we're in a different event loop than when the browser was created,
    # the old browser object is dead — reset before reuse.
    running_loop = asyncio.get_running_loop()
    if _browser is not None and _current_loop is not running_loop:
        _browser = None
        _playwright = None
        _browser_lock = None
        _current_loop = None

    if _browser is not None:
        return _browser

    if _browser_lock is None:
        _browser_lock = asyncio.Lock()

    async with _browser_lock:
        if _browser is None:
            from playwright.async_api import async_playwright

            _playwright = await async_playwright().start()
            _browser = await _playwright.chromium.launch()
            _current_loop = running_loop
        return _browser


def _get_card_html_document(card: Dict[str, Any]) -> str:
    """Return best available HTML document/string from card payload."""
    def _looks_like_editor_state(html: str) -> bool:
        lowered = html.casefold()
        markers = (
            "options (click to mark correct answer)",
            "quick edit",
            "enter the question or term",
            "answer or definition",
        )
        return any(marker in lowered for marker in markers)

    rendered_html = card.get("renderedHtml")
    render_html = card.get("renderHtml")
    raw_html = card.get("html")

    if isinstance(rendered_html, str) and rendered_html.strip():
        # Some payloads store editor snapshots in renderedHtml (selected answer, placeholders).
        # Prefer renderHtml/html when this pattern is detected.
        if _looks_like_editor_state(rendered_html):
            for fallback in (render_html, raw_html):
                if isinstance(fallback, str) and fallback.strip():
                    return fallback
        return rendered_html

    for value in (render_html, raw_html):
        if isinstance(value, str) and value.strip():
            return value

    # Legacy compatibility: some payloads keep HTML under first-level blocks.
    for child in card.get("children", []):
        if (child.get("type") or "").upper() != "BLOCK":
            continue
        content = child.get("content") or {}
        for key in ("renderHtml", "html"):
            value = content.get(key)
            if isinstance(value, str) and value.strip():
                return value

    return ""


def _find_first_block_content(card: Dict[str, Any], content_type: str) -> Optional[Dict[str, Any]]:
        """Find first BLOCK content by content.type, including nested LAYOUT nodes."""

        target = content_type.upper()

        def walk(nodes: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
                for node in nodes:
                        node_type = (node.get("type") or "").upper()
                        if node_type == "LAYOUT":
                                hit = walk(node.get("children", []))
                                if hit:
                                        return hit
                                continue

                        if node_type != "BLOCK":
                                continue

                        content = node.get("content") or {}
                        if (content.get("type") or "").upper() == target:
                                return content
                return None

        return walk(card.get("children", []))


def _build_quiz_display_html(card: Dict[str, Any]) -> Optional[str]:
        """Build a quiz display slide that does not reveal correct answers."""
        quiz = _find_first_block_content(card, "QUIZ")
        if not quiz:
                return None

        questions = quiz.get("questions") or []
        if not questions:
                return None

        q = questions[0]
        question = html.escape(str(q.get("question") or "").strip())
        if not question:
                return None

        option_items = []
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        for i, opt in enumerate(q.get("options") or []):
                text = opt.get("text", "") if isinstance(opt, dict) else str(opt)
                text = html.escape(text.strip())
                if not text:
                        continue
                label = letters[i] if i < len(letters) else str(i + 1)
                option_items.append(f"<li><span class='opt-label'>{label}.</span><span>{text}</span></li>")

        title = html.escape(str(card.get("title") or "").strip())
        options_html = "".join(option_items)
        return f"""
<!doctype html>
<html>
<head>
    <meta charset='utf-8' />
    <style>
        * {{ box-sizing: border-box; }}
        body {{ margin: 0; width: 1280px; height: 720px; display: flex; align-items: center; justify-content: center; background: #f6f8ff; font-family: 'Segoe UI', Arial, sans-serif; color: #1f2937; }}
        .card {{ width: 1080px; background: #ffffff; border: 2px solid #dbe4ff; border-radius: 18px; padding: 34px 38px; }}
        .title {{ font-size: 28px; font-weight: 700; margin: 0 0 12px; color: #4f46e5; }}
        .q {{ font-size: 34px; font-weight: 700; line-height: 1.3; margin: 0 0 20px; }}
        ul {{ margin: 0; padding: 0; list-style: none; display: grid; gap: 12px; }}
        li {{ border: 2px solid #e5e7eb; border-radius: 12px; padding: 12px 16px; font-size: 28px; display: flex; align-items: flex-start; gap: 10px; }}
        .opt-label {{ font-weight: 700; min-width: 34px; }}
    </style>
</head>
<body>
    <div class='card'>
        <h2 class='title'>{title}</h2>
        <p class='q'>{question}</p>
        <ul>{options_html}</ul>
    </div>
</body>
</html>
"""


def _build_flashcard_front_html(card: Dict[str, Any]) -> Optional[str]:
        """Build a flashcard display slide showing only front side text."""
        flash = _find_first_block_content(card, "FLASHCARD")
        if not flash:
                return None

        front = ""
        cards = flash.get("cards") or []
        if isinstance(cards, list) and cards:
                front = str((cards[0] or {}).get("front") or "").strip()
        if not front:
                front = str(flash.get("front") or "").strip()
        if not front:
                return None

        title = html.escape(str(card.get("title") or "").strip())
        front = html.escape(front)
        return f"""
<!doctype html>
<html>
<head>
    <meta charset='utf-8' />
    <style>
        * {{ box-sizing: border-box; }}
        body {{ margin: 0; width: 1280px; height: 720px; display: flex; align-items: center; justify-content: center; background: #fff7ed; font-family: 'Segoe UI', Arial, sans-serif; color: #1f2937; }}
        .card {{ width: 1020px; min-height: 520px; background: #ffffff; border: 2px solid #fed7aa; border-radius: 22px; padding: 30px; display: flex; flex-direction: column; }}
        .title {{ font-size: 28px; font-weight: 700; margin: 0; color: #ea580c; }}
        .front-wrap {{ flex: 1; display: flex; align-items: center; justify-content: center; text-align: center; }}
        .front {{ font-size: 52px; line-height: 1.2; font-weight: 700; }}
    </style>
</head>
<body>
    <div class='card'>
        <h2 class='title'>{title}</h2>
        <div class='front-wrap'>
            <div class='front'>{front}</div>
        </div>
    </div>
</body>
</html>
"""


async def render_slide_async(
    card: Dict[str, Any],
    output_path: str,
    slide_number: Optional[int] = None,
) -> str:
    """Render a slide image from card payload HTML using Playwright."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    html_content = _build_quiz_display_html(card)
    if not html_content:
        html_content = _build_flashcard_front_html(card)
    if not html_content:
        html_content = _get_card_html_document(card)
    if not html_content:
        raise ValueError("Card missing renderedHtml/renderHtml/html in strict JSON mode")

    browser = await _get_browser()
    page = await browser.new_page(viewport={"width": SLIDE_WIDTH, "height": SLIDE_HEIGHT})
    await page.set_content(html_content)
    await page.add_style_tag(
        content=(
            "html, body { width: 100%; height: 100%; margin: 0; overflow: hidden; }"
            "body { display: flex; align-items: center; justify-content: center; }"
            "body > * { max-width: 100%; max-height: 100%; }"
        )
    )
    await page.screenshot(path=str(output), type="png")
    await page.close()

    logger.info("Rendered slide: %s", output.name)
    return str(output)


async def cleanup_browser():
    """Close the shared browser instance."""
    global _browser, _playwright, _browser_lock, _current_loop
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()

    async with _browser_lock:
        if _browser:
            await _browser.close()
            _browser = None
        if _playwright:
            await _playwright.stop()
            _playwright = None
        _browser_lock = None
        _current_loop = None
