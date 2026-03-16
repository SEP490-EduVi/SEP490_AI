"""
Utility functions for text extraction and HTML processing.

Supports both legacy format and new API response format:
- Legacy: { "cards": [...] }
- New API: { "result": { "slideEditedDocument": { "cards": [...] } } }
"""

import re
from bs4 import BeautifulSoup


def strip_html_tags(html: str) -> str:
    """Strip HTML tags and return clean text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_lesson_data(data: dict) -> dict:
    """
    Extract lesson data from various JSON formats.
    
    Supports:
    - Direct format: { "title": "...", "cards": [...] }
    - API response: { "result": { "slideEditedDocument": { "title": "...", "cards": [...] } } }
    """
    # Check for API response format
    if "result" in data and isinstance(data.get("result"), dict):
        result = data["result"]
        if "slideEditedDocument" in result:
            return result["slideEditedDocument"]
        return result
    
    # Legacy/direct format
    return data
