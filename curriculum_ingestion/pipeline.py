"""
Curriculum ingestion pipeline.

Steps:
  1. downloading  (10–20%) — Download .docx from Google Cloud Storage
  2. extracting   (25–40%) — python-docx raw text dump
  3. parsing      (45–65%) — Gemini structured extraction → ParsedCurriculum
  4. ingesting    (70–88%) — MERGE nodes into Neo4j
  5. mapping      (89–95%) — MERGE COVERS relationships from mapping file
  6. completed    (100%)   — Publish stats
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Awaitable, Callable

from gcs_handler import download_from_gcs
from parser import parse
from neo4j_loader import ingest, apply_mapping
from mapper import auto_map
from config import Config

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str, int, str], Awaitable[None]]


async def run(
    gcs_uri: str,
    subject_code: str,
    education_level: str,
    curriculum_year: int | None = None,
    on_progress: ProgressCallback | None = None,
) -> dict:
    """
    Run the curriculum ingestion pipeline (async).

    Args:
        gcs_uri:          Full GCS path to the .docx file.
        subject_code:     Snake-case subject identifier (e.g. "dia_li").
        education_level:  Education level string (e.g. "THPT").
        on_progress:      Optional async callback(step, progress, detail).

    Returns:
        Stats dict: {lop_count, chu_de_count, yeu_cau_count, covers_count}
    """

    async def _progress(step: str, progress: int, detail: str = "") -> None:
        logger.info("[%s] %d%% — %s", step, progress, detail)
        if on_progress:
            await on_progress(step, progress, detail)

    local_path: str | None = None
    try:
        # ── Step 1: Download from GCS ─────────────────────────────────
        await _progress("downloading", 10, f"Downloading: {gcs_uri}")
        local_path = await asyncio.to_thread(download_from_gcs, gcs_uri)
        await _progress("downloading", 20, f"Downloaded to {local_path}")

        # ── Step 2: Raw document dump + Gemini extraction ─────────────
        await _progress("parsing", 25, "Extracting document structure...")
        # The raw dump happens inside parser.parse() — we emit progress before + after
        await _progress("parsing", 40, "Document extracted, sending to Gemini...")

        # ── Step 3: Gemini structured extraction ──────────────────────
        await _progress("parsing", 45, "Gemini parsing curriculum structure...")
        curriculum = await asyncio.to_thread(parse, local_path)

        lop_count = len(curriculum.lop_list)
        cd_count = sum(len(l.chu_de_list) for l in curriculum.lop_list)
        yc_count = sum(
            len(cd.yeu_cau_list)
            for l in curriculum.lop_list
            for cd in l.chu_de_list
        )
        await _progress(
            "parsing", 65,
            f"Parsed: {lop_count} lớp, {cd_count} chủ đề, {yc_count} yêu cầu cần đạt",
        )

        # ── Step 4: Ingest into Neo4j ─────────────────────────────────
        await _progress("ingesting", 70, "Merging nodes into Neo4j...")
        stats = await asyncio.to_thread(ingest, curriculum, subject_code, curriculum_year)
        await _progress(
            "ingesting", 88,
            f"Neo4j: {stats['lop_count']} lớp, "
            f"{stats['chu_de_count']} chủ đề, "
            f"{stats['yeu_cau_count']} yêu cầu inserted/merged",
        )

        # ── Step 5: Auto-generate Lesson→ChuDe mapping via Gemini ──────────
        await _progress(
            "mapping", 89,
            "Generating Lesson→ChuDe mapping with Gemini (requires textbook to be ingested)...",
        )
        mapping_path = await asyncio.to_thread(auto_map, curriculum, subject_code, education_level, curriculum_year)
        covers_count = await asyncio.to_thread(apply_mapping, mapping_path)
        await _progress(
            "mapping", 95,
            f"Created {covers_count} COVERS relationships. Mapping saved to {mapping_path}",
        )

        stats["covers_count"] = covers_count
        if curriculum_year is not None:
            stats["curriculum_year"] = curriculum_year

        return stats

    except Exception:
        logger.exception("Curriculum ingestion pipeline failed for %s", gcs_uri)
        raise

    finally:
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            logger.info("Cleaned up temp file: %s", local_path)


async def run_apply_mapping(subject_code: str, education_level: str, curriculum_year: int | None = None) -> int:
    """
    Re-apply the saved mapping JSON to Neo4j without re-parsing the document.
    Use this to re-create COVERS relationships after manually editing the mapping file.

    Returns the number of COVERS relationships merged.
    """
    from pathlib import Path
    level_slug = education_level.lower().replace(" ", "_")
    year_suffix = f"_{curriculum_year}" if curriculum_year else ""
    mapping_path = str(Path(__file__).parent / "mapping" / f"{subject_code}_{level_slug}{year_suffix}.json")
    logger.info("Applying saved mapping from %s", mapping_path)
    return await asyncio.to_thread(apply_mapping, mapping_path)
