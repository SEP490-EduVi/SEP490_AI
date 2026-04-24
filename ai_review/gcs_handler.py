"""Download files from Google Cloud Storage."""

from __future__ import annotations

import logging
import os

from google.cloud import storage

from config import Config

logger = logging.getLogger(__name__)


def download_from_gcs(gcs_uri: str) -> str:
    """Download a file from GCS URI to local temp directory and return local path."""
    if not gcs_uri.startswith("gs://"):
        raise ValueError("fileUrl must be a gs:// URI")

    without_scheme = gcs_uri[len("gs://") :]
    bucket_name, blob_name = without_scheme.split("/", 1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    filename = os.path.basename(blob_name)
    local_path = os.path.join(Config.TEMP_DIR, filename)
    blob.download_to_filename(local_path)
    logger.info("Downloaded %s -> %s", gcs_uri, local_path)
    return local_path
