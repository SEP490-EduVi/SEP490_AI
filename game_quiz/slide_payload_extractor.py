"""Utilities to load JSON documents from local disk or GCS."""

from __future__ import annotations

import json
from typing import Any


def load_json_document(source: str) -> dict[str, Any]:
    """Load a JSON document from local path or gs:// URI."""
    if source.startswith("gs://"):
        return _load_json_from_gcs(source)

    with open(source, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_json_from_gcs(uri: str) -> dict[str, Any]:
    """Load JSON content from a Google Cloud Storage URI."""
    from google.cloud import storage

    bucket_name, blob_name = _parse_gs_uri(uri)
    client = storage.Client()
    payload = client.bucket(bucket_name).blob(blob_name).download_as_text(encoding="utf-8")
    return json.loads(payload)


def _parse_gs_uri(uri: str) -> tuple[str, str]:
    """Parse a gs://bucket/path URI into bucket and object path."""
    if not uri.startswith("gs://"):
        raise ValueError("URI must start with gs://")

    raw = uri[5:]
    if "/" not in raw:
        raise ValueError("Invalid gs:// URI, missing object path")

    bucket_name, blob_name = raw.split("/", 1)
    if not bucket_name or not blob_name:
        raise ValueError("Invalid gs:// URI")

    return bucket_name, blob_name
