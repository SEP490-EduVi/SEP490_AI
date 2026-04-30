"""Pipeline orchestration for ai_review requests."""

from __future__ import annotations

from typing import Any

from gcs_handler import download_from_gcs
from reviewer import ReviewDecision, evaluate_request


async def run_review(payload: dict[str, Any]) -> ReviewDecision:
    """Download target file and evaluate according to reviewKind rules."""
    file_url = payload.get("fileUrl") or payload.get("file_url")
    if not file_url:
        return ReviewDecision(
            is_valid=False,
            rejection_reason="Thiếu fileUrl trong request.",
            summary="Không đủ điều kiện duyệt tự động",
        )

    local_path = download_from_gcs(str(file_url))
    return await evaluate_request(payload, local_path)
