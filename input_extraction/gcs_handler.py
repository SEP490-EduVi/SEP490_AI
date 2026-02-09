"""Download files from Google Cloud Storage."""

import os
from google.cloud import storage
from config import Config


def download_from_gcs(gcs_uri: str) -> str:
    """
    Download a file from Google Cloud Storage to a local temp directory.

    Args:
        gcs_uri: Either a full gs:// URI  (gs://bucket/path/to/file.pdf)
                 or just the blob path   (path/to/file.pdf).

    Returns:
        The local file path of the downloaded file.
    """
    Config.validate()

    # Parse the URI
    if gcs_uri.startswith("gs://"):
        # gs://bucket-name/path/to/file.pdf
        without_scheme = gcs_uri[len("gs://"):]
        bucket_name, blob_name = without_scheme.split("/", 1)
    else:
        # Treat as a blob path inside the configured bucket
        bucket_name = Config.GCS_BUCKET_NAME
        blob_name = gcs_uri

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Build a local destination path
    filename = os.path.basename(blob_name)
    local_path = os.path.join(Config.TEMP_DIR, filename)

    blob.download_to_filename(local_path)
    print(f"[GCS] Downloaded  : {gcs_uri}")
    print(f"[GCS] Saved to    : {local_path}")

    return local_path
