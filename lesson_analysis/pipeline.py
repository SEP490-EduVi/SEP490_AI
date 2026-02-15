"""Full pipeline: Download → Extract → Lookup → Evaluate → Output."""

import os
import logging
from gcs_handler import download_from_gcs
from extractor import extract_text
from neo4j_client import get_standard_concepts
from evaluator import evaluate_lesson_plan

logger = logging.getLogger(__name__)


def run(gcs_uri: str, subject: str, grade: str) -> dict:
    """
    Run the full lesson-analysis pipeline.

    Args:
        gcs_uri: GCS blob path (or gs:// URI) to the uploaded file.
        subject: Subject name, e.g. "Toán", "Vật lý".
        grade:   Grade level, e.g. "Lớp 10".

    Returns:
        The evaluation result as a dict matching the output schema.
    """
    local_path = None
    try:
        # Step 1: Download from GCS
        logger.info("[1/4] Downloading file from GCS: %s", gcs_uri)
        local_path = download_from_gcs(gcs_uri)

        # Step 2: Extract raw text from PDF / DOCX
        logger.info("[2/4] Extracting text from %s ...", local_path)
        raw_text = extract_text(local_path)
        if not raw_text:
            raise ValueError("No text could be extracted from %s" % gcs_uri)
        logger.info("[2/4] Extracted %d characters of text.", len(raw_text))

        # Step 3: Fetch standard concepts from Neo4j
        logger.info("[3/4] Fetching standard concepts for %s – %s ...", subject, grade)
        standard_concepts = get_standard_concepts(subject, grade)
        if not standard_concepts:
            raise ValueError(
                "No standard concepts found in Neo4j for %s – %s" % (subject, grade)
            )

        # Step 4: Evaluate via LLM
        logger.info("[4/4] Evaluating lesson plan via Gemini ...")
        result = evaluate_lesson_plan(raw_text, standard_concepts)

        logger.info("Pipeline finished successfully.")
        return result

    except Exception:
        logger.exception("Pipeline failed for %s", gcs_uri)
        raise

    finally:
        # Clean up the temp file
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            logger.info("Cleaned up temp file: %s", local_path)
