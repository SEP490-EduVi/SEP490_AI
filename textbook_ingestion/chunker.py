"""
Split OCR'd text into structural chunks (by lesson/chapter boundaries)
so each chunk can be sent to the LLM independently.
"""

import re
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Minimum chars for a chunk to be worth a separate LLM call.
# Smaller chunks are merged into the next one.
MIN_CHUNK_CHARS = 500

# Vietnamese textbook heading patterns

# "Bài 1", "BÀI 10.", etc.
_LESSON_RE = re.compile(
    r"^(?:---\s*Page\s+\d+\s*---\s*\n)?\s*"
    r"(?:Bài|BÀI|bài)\s+\d+",
    re.MULTILINE,
)

# "Chương 1", "Phần I", etc.
_CHAPTER_RE = re.compile(
    r"^(?:---\s*Page\s+\d+\s*---\s*\n)?\s*"
    r"(?:Chương|CHƯƠNG|Phần|PHẦN|chương|phần)\s+\w+",
    re.MULTILINE,
)

# "--- Page 42 ---"
_PAGE_RE = re.compile(r"---\s*Page\s+(\d+)\s*---")


@dataclass
class TextChunk:
    """A text chunk with position metadata."""
    index: int
    label: str
    text: str
    page_start: int
    page_end: int
    char_count: int


def _extract_page_range(text: str) -> tuple[int, int]:
    """Return (first_page, last_page) from page markers in text."""
    pages = [int(m) for m in _PAGE_RE.findall(text)]
    if pages:
        return min(pages), max(pages)
    return 0, 0


def _make_label(text: str, fallback: str) -> str:
    """First non-trivial line (truncated to 120 chars) as chunk label."""
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("---"):
            continue
        return line[:120]
    return fallback


def chunk_by_lessons(full_text: str, max_chunk_chars: int = 40_000) -> list[TextChunk]:
    """
    Split text at lesson (Bài) boundaries. Falls back to chapter or page splits.
    Oversized chunks are sub-split at page markers.
    """
    # Find split points
    split_positions: list[int] = [m.start() for m in _LESSON_RE.finditer(full_text)]

    if not split_positions:
        # No lesson headers → try chapter headers
        split_positions = [m.start() for m in _CHAPTER_RE.finditer(full_text)]
        if split_positions:
            logger.info("No lesson headers found; splitting by chapter (%d).", len(split_positions))
        else:
            logger.info("No structural headers found; splitting by page count.")

    if not split_positions:
        # Last resort: split by page markers, ~10 pages per chunk
        page_positions = [m.start() for m in _PAGE_RE.finditer(full_text)]
        step = max(1, len(page_positions) // max(1, len(full_text) // max_chunk_chars + 1))
        split_positions = page_positions[::step] if page_positions else [0]

    # Ensure we start from position 0
    if split_positions[0] != 0:
        split_positions.insert(0, 0)

    # Create raw chunks
    raw_chunks: list[str] = []
    for i, start in enumerate(split_positions):
        end = split_positions[i + 1] if i + 1 < len(split_positions) else len(full_text)
        segment = full_text[start:end].strip()
        if segment:
            raw_chunks.append(segment)

    # Sub-split oversized chunks at page boundaries
    final_chunks: list[str] = []
    for chunk_text in raw_chunks:
        if len(chunk_text) <= max_chunk_chars:
            final_chunks.append(chunk_text)
        else:
            # Split at page markers
            pages = list(_PAGE_RE.finditer(chunk_text))
            if len(pages) < 2:
                # Can't split further
                final_chunks.append(chunk_text)
                continue
            # Group pages into sub-chunks
            sub_start = 0
            for pg_idx, pg_match in enumerate(pages):
                remaining = chunk_text[sub_start:]
                if len(remaining) <= max_chunk_chars:
                    break
                # Find page boundary near max_chunk_chars
                target = sub_start + max_chunk_chars
                best = sub_start
                for pm in pages:
                    if pm.start() <= target and pm.start() > best:
                        best = pm.start()
                if best == sub_start:
                    best = pages[min(pg_idx + 1, len(pages) - 1)].start()
                if best > sub_start:
                    final_chunks.append(chunk_text[sub_start:best].strip())
                    sub_start = best
            # Remaining tail
            tail = chunk_text[sub_start:].strip()
            if tail:
                final_chunks.append(tail)

    # Merge tiny chunks into their next neighbour
    merged_chunks: list[str] = []
    carry = ""
    for chunk_text in final_chunks:
        combined = (carry + "\n" + chunk_text).strip() if carry else chunk_text
        if len(combined) < MIN_CHUNK_CHARS:
            carry = combined          # too small — accumulate
        else:
            merged_chunks.append(combined)
            carry = ""
    # Flush remaining carry
    if carry:
        if merged_chunks:
            merged_chunks[-1] = (merged_chunks[-1] + "\n" + carry).strip()
        else:
            merged_chunks.append(carry)

    logger.info(
        "Chunk merging: %d raw → %d after merging tiny (<=%d char) chunks.",
        len(final_chunks), len(merged_chunks), MIN_CHUNK_CHARS,
    )

    # Wrap into TextChunk objects
    chunks: list[TextChunk] = []
    for idx, text in enumerate(merged_chunks):
        p_start, p_end = _extract_page_range(text)
        label = _make_label(text, f"Chunk {idx + 1}")
        chunks.append(TextChunk(
            index=idx,
            label=label,
            text=text,
            page_start=p_start,
            page_end=p_end,
            char_count=len(text),
        ))

    logger.info(
        "Chunking complete: %d chunks, total %d chars.",
        len(chunks),
        sum(c.char_count for c in chunks),
    )
    for c in chunks:
        logger.info(
            "  Chunk %d: pages %d-%d, %d chars – %s",
            c.index, c.page_start, c.page_end, c.char_count,
            c.label[:80],
        )

    return chunks
