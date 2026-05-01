"""Core file review logic for verification and material flows."""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import re
from dataclasses import dataclass
from typing import Any

import cv2
from google import genai
from google.genai import types
from docx import Document
from PIL import Image, UnidentifiedImageError
from pypdf import PdfReader

from config import Config


VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tiff"}

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are EduVi File Review AI for expert verification and teaching material review. "
    "Return JSON only. Be strict, concise, and deterministic. "
    "If rejecting, provide one clear rejectionReason in Vietnamese. "
    "If accepting, provide one short Vietnamese summary."
)

@dataclass
class ReviewDecision:
    is_valid: bool
    rejection_reason: str | None
    summary: str


@dataclass
class FileDiagnostics:
    extension: str
    size_bytes: int
    readable: bool
    extracted_text: str
    media_type: str


def _make_client() -> genai.Client:
    if Config.HELICONE_API_KEY:
        loc = Config.VERTEX_AI_LOCATION
        target_host = "aiplatform.googleapis.com" if loc == "global" else f"{loc}-aiplatform.googleapis.com"
        return genai.Client(
            vertexai=True,
            project=Config.GOOGLE_CLOUD_PROJECT,
            location=loc,
            http_options=types.HttpOptions(
                base_url="https://gateway.helicone.ai",
                headers={
                    "Helicone-Auth": f"Bearer {Config.HELICONE_API_KEY}",
                    "Helicone-Target-Url": f"https://{target_host}",
                },
            ),
        )

    return genai.Client(
        vertexai=True,
        project=Config.GOOGLE_CLOUD_PROJECT,
        location=Config.VERTEX_AI_LOCATION,
    )


_client: genai.Client | None = None


def _get_client() -> genai.Client:
    global _client
    if _client is None:
        _client = _make_client()
    return _client


def _extract_pdf_text(path: str, max_pages: int = 12) -> str:
    reader = PdfReader(path)
    parts: list[str] = []
    for page in reader.pages[:max_pages]:
        parts.append(page.extract_text() or "")
    return "\n".join(parts).strip()


def _extract_docx_text(path: str) -> str:
    doc = Document(path)
    return "\n".join(p.text for p in doc.paragraphs).strip()


def _extract_txt_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read().strip()


def _laplacian_variance(image) -> float:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


def _image_is_clear(path: str) -> bool:
    image = cv2.imread(path)
    if image is None:
        return False
    h, w = image.shape[:2]
    if min(h, w) < 500:
        return False
    return _laplacian_variance(image) >= 35.0


def _video_is_playable(path: str) -> bool:
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        return False
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    fps = float(cap.get(cv2.CAP_PROP_FPS) or 0)
    ok, _frame = cap.read()
    cap.release()
    if not ok:
        return False
    if frame_count <= 0 and fps <= 0:
        return False
    return True


def _extract_video_frames(path: str, max_frames: int = 2) -> list[bytes]:
    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        return []

    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    indices: list[int] = []
    if frame_count > 0:
        indices = sorted({int(frame_count * 0.1), int(frame_count * 0.6)})
    if not indices:
        indices = [0]

    frames: list[bytes] = []
    for idx in indices[:max_frames]:
        cap.set(cv2.CAP_PROP_POS_FRAMES, max(0, idx))
        ok, frame = cap.read()
        if not ok or frame is None:
            continue
        ok, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 75])
        if ok:
            frames.append(encoded.tobytes())
        if len(frames) >= max_frames:
            break

    cap.release()
    return frames


def inspect_file(local_path: str) -> FileDiagnostics:
    ext = os.path.splitext(local_path)[1].lower()
    size_bytes = os.path.getsize(local_path)
    text = ""
    readable = size_bytes > 0

    media_type = "unknown"

    if ext == ".pdf":
        media_type = "document"
        text = _extract_pdf_text(local_path)
        readable = readable and bool(text.strip())
    elif ext == ".docx":
        media_type = "document"
        text = _extract_docx_text(local_path)
        readable = readable and bool(text.strip())
    elif ext == ".txt":
        media_type = "document"
        text = _extract_txt_text(local_path)
        readable = readable and bool(text.strip())
    elif ext in IMAGE_EXTENSIONS:
        media_type = "image"
        try:
            with Image.open(local_path) as img:
                img.verify()
            readable = readable and _image_is_clear(local_path)
        except (UnidentifiedImageError, OSError, ValueError):
            readable = False
    elif ext in VIDEO_EXTENSIONS:
        media_type = "video"
        readable = readable and _video_is_playable(local_path)
    else:
        readable = False

    return FileDiagnostics(
        extension=ext,
        size_bytes=size_bytes,
        readable=readable,
        extracted_text=text,
        media_type=media_type,
    )


def _parse_json_response(raw: str) -> dict[str, Any]:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", (raw or "").strip())
    data = json.loads(cleaned)
    if not isinstance(data, dict):
        raise ValueError("AI response is not a JSON object")
    return data


def _build_verification_prompt(payload: dict[str, Any], diag: FileDiagnostics) -> str:
    extracted_text = (diag.extracted_text or "")[: Config.MAX_EVIDENCE_CHARS]
    doc_type_hint = (
        payload.get("documentType")
        or payload.get("document_type")
        or payload.get("fileType")
        or payload.get("file_type")
        or payload.get("docType")
        or payload.get("doc_type")
        or payload.get("credentialType")
        or payload.get("credential_type")
        or payload.get("verificationType")
        or payload.get("verification_type")
    )
    return (
        "Task: Evaluate verification for expert credentials.\n"
        "Document types:\n"
        "- Certificate (chung chi)\n"
        "- Degree/Diploma (bang cap)\n"
        "First decide which type applies (use docTypeHint/fileType if provided). Then apply the matching criteria.\n\n"
        "Pass criteria (Certificate):\n"
        "- Core info present: certificate name, issuing organization, issue date (expiry date if present).\n"
        "- Certificate/registry number is optional; it MUST NOT be the sole reason to reject.\n\n"
        "Pass criteria (Degree/Diploma):\n"
        "- Core info present: institution/university name, degree title, graduation/issue date.\n"
        "- Major/field is helpful if present but NOT required.\n\n"
        "General rules:\n"
        "- OCR/text is clear enough to verify; not overly blurred/cropped.\n"
        "- If document type is unclear but core info for either type is readable, accept.\n"
        "- Reject only if core info is missing or the document is too blurred to verify.\n"
        "- If multiple pages exist, evaluate any page that contains the credential.\n\n"
        "If NOT passed: isValid=false and rejectionReason is required.\n"
        "If passed: isValid=true, rejectionReason=null, summary should be short.\n"
        "Return JSON object with schema:\n"
        "{\n"
        '  "isValid": true|false,\n'
        '  "rejectionReason": "..." | null,\n'
        '  "summary": "..."\n'
        "}\n\n"
        "INPUT:\n"
        f"description: {payload.get('description')}\n"
        f"fileName: {payload.get('fileName') or payload.get('file_name')}\n"
        f"contentType: {payload.get('contentType') or payload.get('content_type')}\n"
        f"docTypeHint: {doc_type_hint}\n"
        f"extension: {diag.extension}\n"
        f"mediaType: {diag.media_type}\n"
        f"sizeBytes: {diag.size_bytes}\n"
        f"readable: {diag.readable}\n"
        "extractedText:\n"
        f"{extracted_text}\n"
    )


def _build_material_prompt(payload: dict[str, Any], diag: FileDiagnostics) -> str:
    extracted_text = (diag.extracted_text or "")[: Config.MAX_EVIDENCE_CHARS]
    return (
        "Task: Evaluate expert uploaded material.\n"
        "Pass criteria:\n"
        "- File is readable, not corrupted, correct format.\n"
        "- Content matches subjectCode/gradeCode (if provided).\n"
        "- Content matches title/description.\n"
        "- No severely misleading or inappropriate educational content.\n\n"
        "For video files, use the provided keyframes as the primary visual evidence.\n\n"
        "If NOT passed: isValid=false and rejectionReason is required.\n"
        "If passed: isValid=true, rejectionReason=null, summary should be short.\n"
        "Return JSON object with schema:\n"
        "{\n"
        '  "isValid": true|false,\n'
        '  "rejectionReason": "..." | null,\n'
        '  "summary": "..."\n'
        "}\n\n"
        "INPUT:\n"
        f"title: {payload.get('title')}\n"
        f"description: {payload.get('description')}\n"
        f"subjectCode: {payload.get('subjectCode') or payload.get('subject_code')}\n"
        f"gradeCode: {payload.get('gradeCode') or payload.get('grade_code')}\n"
        f"fileName: {payload.get('fileName') or payload.get('file_name')}\n"
        f"contentType: {payload.get('contentType') or payload.get('content_type')}\n"
        f"extension: {diag.extension}\n"
        f"mediaType: {diag.media_type}\n"
        f"sizeBytes: {diag.size_bytes}\n"
        f"readable: {diag.readable}\n"
        "extractedText:\n"
        f"{extracted_text}\n"
    )


def _guess_mime_type(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    overrides = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".bmp": "image/bmp",
        ".tiff": "image/tiff",
    }
    if ext in overrides:
        return overrides[ext]

    guessed, _enc = mimetypes.guess_type(path)
    return guessed or "application/octet-stream"


async def _generate_with_retry(contents: Any) -> str:
    client = _get_client()
    attempts = max(1, Config.AI_RETRY_COUNT + 1)
    last_exc: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = await asyncio.wait_for(
                client.aio.models.generate_content(
                    model=Config.GEMINI_MODEL,
                    contents=contents,
                    config=types.GenerateContentConfig(
                        system_instruction=SYSTEM_PROMPT,
                        temperature=0,
                        response_mime_type="application/json",
                    ),
                ),
                timeout=Config.AI_TIMEOUT_SEC,
            )
            return response.text
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            logger.warning(
                "AI call failed attempt %d/%d: %s",
                attempt,
                attempts,
                type(exc).__name__,
            )
            if attempt < attempts:
                await asyncio.sleep(Config.AI_RETRY_BACKOFF_SEC * attempt)

    assert last_exc is not None
    raise last_exc


async def _evaluate_with_ai_multimodal(prompt: str, local_path: str, diag: FileDiagnostics) -> dict[str, Any]:
    contents: Any = prompt

    # Provide image bytes so Gemini can read text or see material content.
    if diag.media_type == "image":
        with open(local_path, "rb") as f:
            image_bytes = f.read()
        contents = [
            prompt,
            types.Part.from_bytes(data=image_bytes, mime_type=_guess_mime_type(local_path)),
        ]

    if diag.media_type == "video":
        frames = _extract_video_frames(local_path, max_frames=2)
        if frames:
            contents = [prompt]
            for frame_bytes in frames:
                contents.append(types.Part.from_bytes(data=frame_bytes, mime_type="image/jpeg"))

    raw = await _generate_with_retry(contents)
    return _parse_json_response(raw)


def _coerce_ai_decision(ai_data: dict[str, Any]) -> ReviewDecision:
    is_valid = bool(ai_data.get("isValid", False))
    rejection_reason = ai_data.get("rejectionReason")
    summary = str(ai_data.get("summary") or "").strip()

    if is_valid:
        return ReviewDecision(
            is_valid=True,
            rejection_reason=None,
            summary=summary or "Tài liệu hợp lệ, đạt điều kiện duyệt sơ bộ.",
        )

    reason_text = str(rejection_reason or "").strip()
    return ReviewDecision(
        is_valid=False,
        rejection_reason=reason_text or "Không đạt tiêu chí duyệt tự động.",
        summary=summary or "Không đủ điều kiện duyệt tự động",
    )


def _is_optional_serial_only_rejection(reason: str) -> bool:
    normalized = reason.lower()
    has_serial_context = any(
        token in normalized for token in ["so hieu", "so vao so", "ma chung chi", "serial", "certificate"]
    )
    has_unreadable_context = any(
        token in normalized for token in ["mo", "khong doc", "khong the doc", "khong ro"]
    )
    has_hard_missing_core = any(
        token in normalized
        for token in ["to chuc cap", "ngay cap", "ten chung chi", "thieu thong tin cot loi"]
    )
    return has_serial_context and has_unreadable_context and not has_hard_missing_core


def _stabilize_verification_decision(decision: ReviewDecision) -> ReviewDecision:
    # Guardrail: do not reject solely because optional serial/registry number is unclear.
    if decision.is_valid:
        return decision

    if decision.rejection_reason and _is_optional_serial_only_rejection(decision.rejection_reason):
        return ReviewDecision(
            is_valid=True,
            rejection_reason=None,
            summary=(
                "Chứng chỉ đạt điều kiện xác minh cơ bản; thông tin serial/số vào sổ "
                "chưa rõ nên đề nghị staff kiểm tra bổ sung."
            ),
        )

    return decision


def _tech_fail_decision(diag: FileDiagnostics) -> ReviewDecision | None:
    if diag.readable:
        return None
    return ReviewDecision(
        is_valid=False,
        rejection_reason="File hỏng, mờ hoặc không đọc được nội dung để đánh giá.",
        summary="Không đủ điều kiện duyệt tự động",
    )


async def evaluate_request(payload: dict[str, Any], local_path: str) -> ReviewDecision:
    review_kind = str(payload.get("reviewKind") or payload.get("review_kind") or "").strip().lower()
    diag = inspect_file(local_path)

    tech_fail = _tech_fail_decision(diag)
    if tech_fail is not None:
        return tech_fail

    try:
        if review_kind == "verification":
            prompt = _build_verification_prompt(payload, diag)
            ai_data = await _evaluate_with_ai_multimodal(prompt, local_path, diag)
            return _stabilize_verification_decision(_coerce_ai_decision(ai_data))

        if review_kind == "material":
            prompt = _build_material_prompt(payload, diag)
            ai_data = await _evaluate_with_ai_multimodal(prompt, local_path, diag)
            return _coerce_ai_decision(ai_data)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("AI evaluation failed")
        if isinstance(exc, asyncio.TimeoutError):
            return ReviewDecision(
                is_valid=False,
                rejection_reason="Hệ thống AI quá thời gian phản hồi, vui lòng thử lại.",
                summary="Không đủ điều kiện duyệt tự động",
            )
        return ReviewDecision(
            is_valid=False,
            rejection_reason="Hệ thống AI tạm thời không đánh giá được tài liệu.",
            summary=f"Đánh giá tự động thất bại: {type(exc).__name__}",
        )

    return ReviewDecision(
        is_valid=False,
        rejection_reason="reviewKind không hợp lệ. Chỉ hỗ trợ verification hoặc material.",
        summary="Không đủ điều kiện duyệt tự động",
    )
