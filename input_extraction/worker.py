"""Core worker – download from GCS and extract content."""

import os
import logging
from gcs_handler import download_from_gcs
from extractor import extract_text

logger = logging.getLogger(__name__)


def process_file(gcs_uri_or_blob: str) -> str:
    """Download a file from GCS, extract text, clean up, and return the result."""
    local_path = None
    try:
        # Step 1: Download from GCS
        logger.info(f"Downloading: {gcs_uri_or_blob}")
        local_path = download_from_gcs(gcs_uri_or_blob)
        logger.info(f"Saved to: {local_path}")

        # Step 2: Extract text
        logger.info("Extracting text...")
        text = extract_text(local_path)

        return text

    except Exception as e:
        logger.error(f"Error processing {gcs_uri_or_blob}: {e}")
        raise

    finally:
        # Step 3: Clean up temp file
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            logger.info(f"Cleaned up temp file: {local_path}")
