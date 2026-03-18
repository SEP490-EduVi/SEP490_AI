"""Utilities to load and extract render-ready slide payloads.

Supports loading JSON from:
- Local file paths
- Google Cloud Storage URIs (gs://bucket/path.json)
"""

from __future__ import annotations

import json
from typing import Any, Dict


def load_json_document(source: str) -> Dict[str, Any]:
    """Load a JSON document from local path or gs:// URI."""
    if source.startswith("gs://"):
        return _load_json_from_gcs(source)

    with open(source, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_json_from_gcs(uri: str) -> Dict[str, Any]:
    """Load JSON from Google Cloud Storage URI."""
    # Lazy import so local usage does not require gcs dependency.
    from google.cloud import storage

    bucket, blob = _parse_gs_uri(uri)
    client = storage.Client()
    payload = client.bucket(bucket).blob(blob).download_as_text(encoding="utf-8")
    return json.loads(payload)


def _parse_gs_uri(uri: str) -> tuple[str, str]:
    """Parse gs://bucket/path URI."""
    if not uri.startswith("gs://"):
        raise ValueError("URI must start with gs://")

    raw = uri[5:]
    if "/" not in raw:
        raise ValueError("Invalid gs:// URI, missing object path")

    bucket, blob = raw.split("/", 1)
    if not bucket or not blob:
        raise ValueError("Invalid gs:// URI")
    return bucket, blob
