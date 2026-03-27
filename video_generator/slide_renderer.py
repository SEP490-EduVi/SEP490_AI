"""Slide renderer using Playwright for HTML screenshot.

Strict JSON mode:
- Render exactly what frontend payload provides via `renderedHtml`, `renderHtml`, or `html`.
- No synthetic template or content transformation.
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict

from . import config

logger = logging.getLogger(__name__)

SLIDE_WIDTH = max(320, int(getattr(config, "VIDEO_WIDTH", 1280)))
SLIDE_HEIGHT = max(180, int(getattr(config, "VIDEO_HEIGHT", 720)))

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
            launch_args = [
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-zygote",
            ]
            if os.getenv("PLAYWRIGHT_NO_SANDBOX", "true").strip().lower() == "true":
                launch_args.append("--no-sandbox")

            _browser = await _playwright.chromium.launch(
                headless=True,
                args=launch_args,
            )
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


async def render_slide_async(
    card: Dict[str, Any],
    output_path: str,
) -> str:
    """Render a slide image from card payload HTML using Playwright."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

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
