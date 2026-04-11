"""Pipeline: File → Text → Chunks → Entities (Gemini) → Neo4j."""

import asyncio
import os
import logging
from typing import Awaitable, Callable

from config import Config
from gcs_handler import download_from_gcs
from extractor import extract_text
from chunker import chunk_by_lessons
from keyword_extractor import extract_entities
from neo4j_loader import load_textbook_data, create_constraints, close
from entity_generator import get_entity_generator

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str, int, str], Awaitable[None]]


async def run(
    source: str,
    subject: str,
    grade: str,
    book_id: str,
    on_progress: ProgressCallback | None = None,
) -> dict:
    """Run the full pipeline. Returns structured data dict loaded into Neo4j."""
    local_path = None
    is_temp = False

    async def _progress(step: str, progress: int, detail: str = "") -> None:
        if on_progress:
            await on_progress(step, progress, detail)

    try:
        # Step 0: Resolve entity generator
        generator = get_entity_generator(subject)
        logger.info(
            "[0/5] Using entity generator: %s | book_id: %s",
            type(generator).__name__,
            book_id,
        )

        # Step 1: Get file
        await _progress("downloading", 10, source)
        if os.path.isfile(source):
            local_path = source
            logger.info("[1/5] Using local file: %s", local_path)
        else:
            local_path = await asyncio.get_event_loop().run_in_executor(
                None, download_from_gcs, source
            )
            is_temp = True
            logger.info("[1/5] Downloaded from GCS: %s", local_path)

        # Step 2: Extract text (Vision OCR)
        await _progress("extracting_text", 20, "Running Vision OCR...")
        skip_start, skip_end = generator.get_skip_pages()
        raw_text = await asyncio.get_event_loop().run_in_executor(
            None, extract_text, local_path, skip_start, skip_end
        )
        logger.info("[2/5] Extracted text: %d characters", len(raw_text))
        await _progress("extracting_text", 35, f"{len(raw_text)} characters extracted")

        if len(raw_text.strip()) < 50:
            raise ValueError(
                "Extracted text is too short (%d chars) — "
                "file may be empty or corrupted." % len(raw_text.strip())
            )

        # Step 3: Chunk by lesson/chapter
        await _progress("chunking", 40, "Splitting into lesson chunks...")
        chunks = chunk_by_lessons(raw_text)
        logger.info(
            "[3/5] Split text into %d chunks (total %d chars).",
            len(chunks),
            sum(c.char_count for c in chunks),
        )

        # Filter out tiny chunks (TOC entries, headers, etc.) to save API calls
        MIN_CHUNK_FOR_LLM = 300
        original_count = len(chunks)
        chunks = [c for c in chunks if c.char_count >= MIN_CHUNK_FOR_LLM]
        skipped = original_count - len(chunks)
        if skipped:
            logger.info(
                "[3/5] Filtered out %d tiny chunks (<%d chars). %d chunks remaining.",
                skipped, MIN_CHUNK_FOR_LLM, len(chunks),
            )

        # Step 4: Extract entities via Gemini (per-chunk progress 45–90%)
        logger.info("[4/5] Extracting entities via Gemini (%d chunks)…", len(chunks))

        limit_raw = Config.CHAPTER_LIMIT
        chapter_limit: int | None = None  # None = ALL
        if limit_raw != "ALL":
            try:
                chapter_limit = int(limit_raw)
            except ValueError:
                raise ValueError(
                    f"Invalid CHAPTER_LIMIT='{limit_raw}'. "
                    "Use 'ALL' or a positive integer."
                )
            if chapter_limit < 1:
                raise ValueError("CHAPTER_LIMIT must be >= 1.")
            logger.info(
                "[4/5] CHAPTER_LIMIT=%d → will stop after %d chapter(s).",
                chapter_limit, chapter_limit,
            )

        n_chunks = max(len(chunks), 1)

        async def _on_chunk(i: int) -> None:
            progress = 45 + int((i / n_chunks) * 45)
            await _progress(
                "extracting_entities",
                progress,
                f"Chunk {i}/{n_chunks}",
            )

        structured_data = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: _extract_with_progress(
                chunks, subject, grade, book_id, generator,
                chapter_limit=chapter_limit,
            ),
        )

        # Report per-chunk progress after the blocking call completes
        await _progress("extracting_entities", 90, f"All {n_chunks} chunks processed")

        parts = structured_data.get("parts", [])
        if not parts:
            raise ValueError(
                "Gemini returned no parts. Check the input file content."
            )

        total_chapters = sum(
            len(p.get("chapters", [])) for p in structured_data.get("parts", [])
        )
        total_lessons = sum(
            len(p.get("lessons", []))
            + sum(len(ch.get("lessons", [])) for ch in p.get("chapters", []))
            for p in structured_data.get("parts", [])
        )
        logger.info(
            "[4/5] Extracted %d parts, %d chapters, %d lessons total.",
            len(structured_data.get("parts", [])),
            total_chapters,
            total_lessons,
        )

        # Step 5: Load into Neo4j
        await _progress("loading_neo4j", 95, "Writing to Neo4j...")
        logger.info("[5/5] Loading into Neo4j...")
        create_constraints(generator)
        counts = load_textbook_data(structured_data, generator)
        logger.info("[5/5] Done! %s", counts)

        # Attach counts to structured_data for the caller
        structured_data["_counts"] = counts
        return structured_data

    except Exception:
        logger.exception("Pipeline failed for source: %s", source)
        raise

    finally:
        # Clean up temp file (only if downloaded from GCS)
        if is_temp and local_path and os.path.exists(local_path):
            os.remove(local_path)
            logger.info("Cleaned up temp file: %s", local_path)
        close()


def _extract_with_progress(chunks, subject, grade, book_id, generator, chapter_limit):
    """Synchronous wrapper for extract_entities (runs in thread executor)."""
    return extract_entities(
        chunks, subject, grade, book_id, generator,
        chapter_limit=chapter_limit,
    )
