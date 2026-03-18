"""Download files from Google Cloud Storage."""

import os
import logging
from google.cloud import storage
from config import Config

logger = logging.getLogger(__name__)


def download_from_gcs(gcs_uri: str) -> str:
    """
    Download a file from Google Cloud Storage to a local temp directory.

    Args:
        gcs_uri: Either a full gs:// URI  (gs://bucket/path/to/file.docx)
                 or just the blob path   (path/to/file.docx).

    Returns:
        The local file path of the downloaded file.
    """
    Config.validate()

    if gcs_uri.startswith("gs://"):
        without_scheme = gcs_uri[len("gs://"):]
        bucket_name, blob_name = without_scheme.split("/", 1)
    else:
        bucket_name = Config.GCS_BUCKET_NAME
        blob_name = gcs_uri

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    filename = os.path.basename(blob_name)
    local_path = os.path.join(Config.TEMP_DIR, filename)

    blob.download_to_filename(local_path)
    logger.info("Downloaded %s → %s", gcs_uri, local_path)

    return local_path
