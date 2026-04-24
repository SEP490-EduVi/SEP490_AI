"""Minimal video generation pipeline for EduVi."""

import asyncio
import logging
import re
import shutil
import tempfile
import time
import uuid
from html import unescape
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from . import config
from .slide_renderer import cleanup_browser, render_slide_async
from .tts import generate_audio_async
from .utils import extract_lesson_data, strip_html_tags

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(config.OUTPUT_DIR)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ProgressCallback = Callable[[str, int, str | None], Awaitable[None] | None]


def _find_binary(name: str) -> str:
    exe = shutil.which(name)
    if exe:
        return exe
    raise RuntimeError(f"{name} not found in PATH")


def _ffmpeg_threads() -> str:
    return str(max(1, int(getattr(config, "FFMPEG_THREADS", 1))))


def _ffmpeg_preset() -> str:
    preset = str(getattr(config, "FFMPEG_PRESET", "ultrafast") or "ultrafast").strip()
    return preset or "ultrafast"


def _video_fps() -> str:
    fps = max(12, int(getattr(config, "VIDEO_FPS", 24)))
    return str(fps)


def _video_scale_filter() -> str:
    width = max(320, int(getattr(config, "VIDEO_WIDTH", 960)))
    height = max(180, int(getattr(config, "VIDEO_HEIGHT", 540)))
    return (
        f"fps={_video_fps()},"
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2"
    )


def _video_track_timescale() -> str:
    timescale = max(1000, int(getattr(config, "VIDEO_TRACK_TIMESCALE", 90000)))
    return str(timescale)


def _audio_channels() -> str:
    channels = max(1, int(getattr(config, "AUDIO_CHANNELS", 1)))
    return str(channels)


def _audio_sample_rate() -> str:
    sample_rate = max(8000, int(getattr(config, "AUDIO_SAMPLE_RATE", 24000)))
    return str(sample_rate)


def _audio_bitrate() -> str:
    bitrate = str(getattr(config, "AUDIO_BITRATE", "96k") or "96k").strip()
    return bitrate or "96k"


async def _emit_progress(
    progress_callback: ProgressCallback | None,
    step: str,
    progress: int,
    detail: str | None = None,
) -> None:
    if not progress_callback:
        return
    maybe_awaitable = progress_callback(step, int(progress), detail)
    if asyncio.iscoroutine(maybe_awaitable):
        await maybe_awaitable


def _iter_block_contents(nodes: list[dict]) -> Any:
    for node in nodes:
        node_type = str(node.get("type") or "").upper()
        if node_type == "LAYOUT":
            yield from _iter_block_contents(node.get("children") or [])
            continue
        if node_type != "BLOCK":
            continue
        content = node.get("content") or {}
        if isinstance(content, dict):
            yield content


def _extract_interaction_payload(card: dict) -> dict | None:
    title = str(card.get("title") or "").strip()

    for content in _iter_block_contents(card.get("children") or []):
        ctype = str(content.get("type") or "").upper()

        if ctype == "QUIZ":
            questions = content.get("questions") or []
            if not questions:
                return None
            q = questions[0] or {}
            options = []
            for opt in q.get("options") or []:
                if isinstance(opt, dict):
                    text = str(opt.get("text") or "").strip()
                else:
                    text = str(opt).strip()
                if text:
                    options.append(text)

            correct_index = q.get("correctIndex")
            try:
                correct_index = int(correct_index)
            except (TypeError, ValueError):
                correct_index = None

            return {
                "type": "quiz",
                "title": title,
                "question": str(q.get("question") or "").strip(),
                "options": options,
                "correctIndex": correct_index,
            }

        if ctype == "FLASHCARD":
            cards = content.get("cards") or []
            front = ""
            back = ""
            if isinstance(cards, list) and cards:
                front = str((cards[0] or {}).get("front") or "").strip()
                back = str((cards[0] or {}).get("back") or "").strip()
            if not front:
                front = str(content.get("front") or "").strip()
            if not back:
                back = str(content.get("back") or "").strip()
            if front:
                return {
                    "type": "flashcard",
                    "title": title,
                    "front": front,
                    "back": back,
                }
            return None

        if ctype == "FILL_BLANK":
            sentence = str(content.get("sentence") or "").strip()
            blanks = content.get("blanks") or []
            if (not sentence or not blanks) and isinstance(content.get("exercises"), list):
                exercise = (content.get("exercises") or [{}])[0] or {}
                sentence = sentence or str(exercise.get("sentence") or "").strip()
                blanks = blanks or (exercise.get("blanks") or [])
            if sentence:
                return {
                    "type": "fill_blank",
                    "title": title,
                    "sentence": sentence,
                    "blanks": [str(v).strip() for v in blanks if str(v).strip()],
                }
            return None

    return None


def _extract_video_source(card: dict) -> dict | None:
    def normalize(value: Any) -> str:
        if not isinstance(value, str):
            return ""
        v = unescape(value).replace("\\u0026", "&").replace("\\/", "/").strip()
        if not v:
            return ""
        if v.startswith("//"):
            return f"https:{v}"
        if v.startswith(("http://", "https://", "gs://", "file://", "/")):
            return v
        return ""

    def from_html(raw_html: Any) -> str:
        if not isinstance(raw_html, str) or not raw_html.strip():
            return ""
        hit = re.search(
            r"<(?:iframe|video|source)\\b[^>]*\\bsrc\\s*=\\s*[\"']([^\"']+)[\"']",
            unescape(raw_html),
            flags=re.IGNORECASE,
        )
        if hit:
            return normalize(hit.group(1))
        return ""

    for content in _iter_block_contents(card.get("children") or []):
        ctype = str(content.get("type") or "").upper()
        if ctype != "VIDEO":
            continue

        material = content.get("material") or {}
        src = (
            normalize(content.get("src"))
            or normalize(content.get("url"))
            or normalize(content.get("videoUrl"))
            or normalize(material.get("url"))
            or normalize(material.get("src"))
            or from_html(content.get("renderHtml"))
            or from_html(content.get("html"))
        )
        if not src:
            continue

        def as_float(v: Any) -> float | None:
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        return {
            "src": src,
            "start": as_float(content.get("startTime") or content.get("start")),
            "end": as_float(content.get("endTime") or content.get("end")),
        }

    return None


def _extract_narration(card: dict) -> str:
    parts: list[str] = []

    def _normalize_for_dedupe(value: str) -> str:
        compact = re.sub(r"\s+", " ", str(value or "")).strip().lower()
        compact = re.sub(r"[^\w\s]", " ", compact, flags=re.UNICODE)
        compact = compact.replace("_", " ")
        return re.sub(r"\s+", " ", compact).strip()

    def _strip_repeated_title_prefix(text: str, title_text: str) -> str:
        txt = str(text or "").strip()
        title_clean = str(title_text or "").strip()
        if not txt or not title_clean:
            return txt

        normalized_txt = _normalize_for_dedupe(txt)
        normalized_title_local = _normalize_for_dedupe(title_clean)
        if len(normalized_title_local) < 6 or not normalized_txt.startswith(normalized_title_local):
            return txt

        # Prefer exact prefix strip first (keeps the rest of sentence unchanged).
        direct_prefix = re.sub(
            rf"^\s*{re.escape(title_clean)}\s*[:\-\u2013\u2014,.;!?]*\s*",
            "",
            txt,
            count=1,
            flags=re.IGNORECASE,
        ).strip()
        if direct_prefix:
            return direct_prefix

        # Fallback for punctuation/spacing variants between title and body text.
        tokens = re.findall(r"\w+", title_clean, flags=re.UNICODE)
        if len(tokens) >= 2:
            sep = r"[\s\-\u2013\u2014:;,.!?()\"'_/]+"
            token_prefix_pattern = (
                r"^\s*"
                + sep.join(re.escape(token) for token in tokens)
                + r"(?:\s*[:\-\u2013\u2014,.;!?)]\s*)?"
            )
            token_prefix = re.sub(
                token_prefix_pattern,
                "",
                txt,
                count=1,
                flags=re.IGNORECASE,
            ).strip()
            if token_prefix:
                return token_prefix

        return txt

    def _extract_text_chunks_from_html(raw_html: str) -> list[str]:
        if not isinstance(raw_html, str) or not raw_html.strip():
            return []

        # Add sentence boundaries for common block tags so TTS has clearer pauses.
        normalized_html = unescape(raw_html)
        normalized_html = re.sub(r"(?i)<br\s*/?>", ". ", normalized_html)
        normalized_html = re.sub(
            r"(?i)</(p|div|li|h1|h2|h3|h4|h5|h6|tr|section|article)>",
            ". ",
            normalized_html,
        )

        # Many templates put small section headings in leading <strong> tags.
        # Insert a boundary right after the heading text.
        normalized_html = re.sub(
            r"(?is)(<p[^>]*>\s*<strong[^>]*>\s*[^<]{2,80}\s*</strong>)(\s*)(?=[^\s<])",
            r"\1. ",
            normalized_html,
        )

        text = strip_html_tags(normalized_html)
        if not text:
            return []

        text = re.sub(r"\s+", " ", text).strip()
        chunks = [c.strip() for c in re.split(r"(?<=[.!?;:])\s+", text) if c.strip()]
        return chunks or [text]

    def _extract_content_chunks(content: dict) -> list[str]:
        html_candidates = [
            content.get("html"),
            content.get("renderHtml"),
            content.get("rawHtml"),
            content.get("textHtml"),
            content.get("valueHtml"),
        ]
        for raw_html in html_candidates:
            chunks = _extract_text_chunks_from_html(raw_html)
            if chunks:
                return chunks

        text_candidates = [
            content.get("text"),
            content.get("title"),
            content.get("label"),
            content.get("description"),
            content.get("value"),
            content.get("caption"),
            content.get("content"),
        ]
        for raw_text in text_candidates:
            if isinstance(raw_text, str) and raw_text.strip():
                text = strip_html_tags(raw_text)
                if text:
                    return [text]

        return []

    normalized_parts: list[str] = []

    def _append_part(value: str) -> None:
        text = str(value or "").strip()
        if not text:
            return
        normalized = _normalize_for_dedupe(text)
        if not normalized:
            return
        if normalized_parts and normalized_parts[-1] == normalized:
            return
        parts.append(text)
        normalized_parts.append(normalized)

    title = str(card.get("title") or "").strip()
    if title:
        _append_part(title)

    normalized_title = _normalize_for_dedupe(title)

    for content in _iter_block_contents(card.get("children") or []):
        ctype = str(content.get("type") or "").upper()
        if ctype in {"TEXT", "HEADING"}:
            chunks = _extract_content_chunks(content)
            for chunk_idx, chunk_text in enumerate(chunks):
                txt = chunk_text
                if chunk_idx == 0:
                    txt = _strip_repeated_title_prefix(txt, title)
                if txt and _normalize_for_dedupe(txt) != normalized_title:
                    _append_part(txt)
            continue

        if ctype == "QUIZ":
            for q in content.get("questions") or []:
                q_text = str((q or {}).get("question") or "").strip()
                if q_text:
                    _append_part(q_text)
                for opt in (q or {}).get("options") or []:
                    if isinstance(opt, dict):
                        o = str(opt.get("text") or "").strip()
                    else:
                        o = str(opt).strip()
                    if o:
                        _append_part(o)
            continue

        if ctype == "FLASHCARD":
            cards = content.get("cards") or []
            if isinstance(cards, list):
                for item in cards:
                    front = str((item or {}).get("front") or "").strip()
                    if front:
                        _append_part(front)
            continue

        if ctype == "FILL_BLANK":
            sentence = str(content.get("sentence") or "").strip()
            if sentence:
                _append_part(re.sub(r"\[.*?\]", "chỗ trống", sentence))
            continue

        # Fallback: some payloads use custom block types but still carry readable text.
        fallback_chunks = _extract_content_chunks(content)
        for chunk_idx, chunk_text in enumerate(fallback_chunks):
            fallback_text = chunk_text
            if chunk_idx == 0:
                fallback_text = _strip_repeated_title_prefix(fallback_text, title)
            if fallback_text and _normalize_for_dedupe(fallback_text) != normalized_title:
                _append_part(fallback_text)

    narration = ". ".join([p for p in parts if p])
    if narration and narration[-1] not in ".!?":
        narration += "."
    return narration


def _extract_card_index_label(card: dict, card_num: int) -> str:
    """Extract a short index label for one card."""
    title = str(card.get("title") or "").strip()
    if title:
        return title

    for content in _iter_block_contents(card.get("children") or []):
        ctype = str(content.get("type") or "").upper()
        if ctype not in {"HEADING", "TEXT"}:
            continue

        candidates = [
            content.get("title"),
            content.get("text"),
            content.get("label"),
            content.get("description"),
            content.get("value"),
            content.get("html"),
            content.get("renderHtml"),
        ]
        for candidate in candidates:
            if not isinstance(candidate, str):
                continue
            plain = strip_html_tags(unescape(candidate))
            if plain:
                return plain[:100].strip()

    return f"Slide {card_num}"


def _build_table_of_contents(cards: list[dict]) -> list[dict]:
    """Build ordered table-of-contents entries from lesson cards."""
    entries: list[dict] = []
    for card_num, card in enumerate(cards, start=1):
        entries.append(
            {
                "card_index": card_num,
                "label": _extract_card_index_label(card, card_num),
            }
        )
    return entries


def _is_youtube_url(value: str) -> bool:
    try:
        host = (urlparse(value).netloc or "").lower()
    except Exception:
        return False
    return "youtube.com" in host or "youtu.be" in host or "youtube-nocookie.com" in host


def _resolve_youtube_url_sync(source: str) -> str:
    import yt_dlp

    with yt_dlp.YoutubeDL(
        {
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "format": "best[acodec!=none][vcodec!=none]/best",
        }
    ) as ydl:
        info = ydl.extract_info(source, download=False)
        if isinstance(info, dict) and isinstance(info.get("url"), str):
            return info["url"].strip()
        formats = (info or {}).get("formats") or []
        for fmt in formats:
            url = fmt.get("url")
            if isinstance(url, str) and url.strip():
                return url.strip()

    raise RuntimeError("Cannot resolve Youtube media URL")


def _download_gcs_to_file_sync(gcs_uri: str, local_path: str) -> None:
    from google.cloud import storage as gcs

    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Not a gs:// URI: {gcs_uri}")
    raw = gcs_uri[5:]
    bucket_name, blob_name = raw.split("/", 1)
    client = gcs.Client()
    client.bucket(bucket_name).blob(blob_name).download_to_filename(local_path)


async def _resolve_material_source(source: str, tmp_dir: Path | None = None) -> str:
    src = str(source or "").strip()
    if not src:
        raise RuntimeError("Empty material source")
    if _is_youtube_url(src):
        return await asyncio.to_thread(_resolve_youtube_url_sync, src)
    if src.startswith("gs://"):
        suffix = Path(src).suffix or ".mp4"
        dest = tmp_dir / f"material_{uuid.uuid4().hex[:8]}{suffix}" if tmp_dir else Path(tempfile.mktemp(suffix=suffix))
        await asyncio.to_thread(_download_gcs_to_file_sync, src, str(dest))
        return str(dest)
    return src


async def _create_silent_black_clip(output_path: str, duration: float = 0.1) -> None:
    """Create a short silent black video clip used as a pause-point placeholder."""
    ffmpeg = _find_binary("ffmpeg")
    width = max(320, int(getattr(config, "VIDEO_WIDTH", 960)))
    height = max(180, int(getattr(config, "VIDEO_HEIGHT", 540)))
    cmd = [
        ffmpeg,
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"color=black:size={width}x{height}:rate={_video_fps()}",
        "-f",
        "lavfi",
        "-i",
        f"anullsrc=channel_layout=mono:sample_rate={_audio_sample_rate()}",
        "-t",
        str(duration),
        "-threads",
        _ffmpeg_threads(),
        "-c:v",
        "libx264",
        "-preset",
        _ffmpeg_preset(),
        "-r",
        _video_fps(),
        "-c:a",
        "aac",
        "-ac",
        _audio_channels(),
        "-ar",
        _audio_sample_rate(),
        "-b:a",
        _audio_bitrate(),
        "-pix_fmt",
        "yuv420p",
        "-video_track_timescale",
        _video_track_timescale(),
        "-movflags",
        "+faststart",
        "-loglevel",
        "error",
        output_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    ffmpeg_timeout = max(30, int(getattr(config, "FFMPEG_TIMEOUT_SEC", 120)))
    try:
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=ffmpeg_timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise RuntimeError(f"ffmpeg create silent black clip timed out after {ffmpeg_timeout}s")
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg create silent black clip failed: {stderr.decode()}")


async def _create_slide_clip(image_path: str, audio_path: str, output_path: str) -> None:
    ffmpeg = _find_binary("ffmpeg")
    cmd = [
        ffmpeg,
        "-y",
        "-loop",
        "1",
        "-framerate",
        _video_fps(),
        "-i",
        image_path,
        "-i",
        audio_path,
        "-threads",
        _ffmpeg_threads(),
        "-c:v",
        "libx264",
        "-preset",
        _ffmpeg_preset(),
        "-tune",
        "stillimage",
        "-r",
        _video_fps(),
        "-c:a",
        "aac",
        "-ac",
        _audio_channels(),
        "-ar",
        _audio_sample_rate(),
        "-b:a",
        _audio_bitrate(),
        "-pix_fmt",
        "yuv420p",
        "-video_track_timescale",
        _video_track_timescale(),
        "-shortest",
        "-movflags",
        "+faststart",
        "-loglevel",
        "error",
        output_path,
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    ffmpeg_timeout = max(30, int(getattr(config, "FFMPEG_TIMEOUT_SEC", 120)))
    try:
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=ffmpeg_timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise RuntimeError(f"ffmpeg create slide clip timed out after {ffmpeg_timeout}s")
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg create slide clip failed: {stderr.decode()}")


async def _create_material_clip(
    source_video: str,
    output_path: str,
    start_time: float | None,
    end_time: float | None,
    tmp_dir: Path | None = None,
) -> None:
    ffmpeg = _find_binary("ffmpeg")
    source = await _resolve_material_source(source_video, tmp_dir=tmp_dir)

    cmd = [ffmpeg, "-y"]
    if start_time is not None and start_time >= 0:
        cmd.extend(["-ss", str(start_time)])
    cmd.extend(["-i", source])
    if (
        start_time is not None
        and end_time is not None
        and end_time > start_time
    ):
        cmd.extend(["-t", str(end_time - start_time)])

    cmd.extend(
        [
            "-vf",
            _video_scale_filter(),
            "-threads",
            _ffmpeg_threads(),
            "-c:v",
            "libx264",
            "-preset",
            _ffmpeg_preset(),
            "-r",
            _video_fps(),
            "-pix_fmt",
            "yuv420p",
            "-video_track_timescale",
            _video_track_timescale(),
            "-c:a",
            "aac",
            "-ac",
            _audio_channels(),
            "-ar",
            _audio_sample_rate(),
            "-b:a",
            _audio_bitrate(),
            "-af",
            "aresample=async=1:first_pts=0",
            "-movflags",
            "+faststart",
            "-loglevel",
            "error",
            output_path,
        ]
    )

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    ffmpeg_timeout = max(30, int(getattr(config, "FFMPEG_TIMEOUT_SEC", 120)))
    try:
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=ffmpeg_timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise RuntimeError(f"ffmpeg create material clip timed out after {ffmpeg_timeout}s")
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg create material clip failed: {stderr.decode()}")


async def _concat_videos(video_paths: list[str], output_path: str) -> None:
    if not video_paths:
        raise ValueError("No clips to concat")

    if len(video_paths) == 1:
        shutil.copy(video_paths[0], output_path)
        logger.info("concat_mode=single_copy clips=1 output=%s", output_path)
        return

    ffmpeg = _find_binary("ffmpeg")
    concat_file = Path(output_path).with_suffix(".concat.txt")
    concat_started = time.perf_counter()

    with open(concat_file, "w", encoding="utf-8") as fp:
        for clip in video_paths:
            fp.write(f"file '{Path(clip).resolve().as_posix()}'\n")

    try:
        copy_cmd = [
            ffmpeg,
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_file),
            "-c",
            "copy",
            "-loglevel",
            "error",
            output_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *copy_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode == 0:
            logger.info(
                "concat_mode=stream_copy clips=%d elapsed=%.2fs output=%s",
                len(video_paths),
                time.perf_counter() - concat_started,
                output_path,
            )
            return

        copy_error = stderr.decode().strip()
        if not bool(getattr(config, "CONCAT_ALLOW_REENCODE_FALLBACK", False)):
            logger.error(
                "concat_mode=stream_copy_failed fallback=disabled clips=%d error=%s",
                len(video_paths),
                copy_error,
            )
            raise RuntimeError(
                "Stream-copy concat failed and CONCAT_ALLOW_REENCODE_FALLBACK=false"
            )

        logger.warning("Concat copy failed, fallback re-encode: %s", copy_error)
        safe_cmd = [
            ffmpeg,
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_file),
            "-threads",
            _ffmpeg_threads(),
            "-c:v",
            "libx264",
            "-preset",
            _ffmpeg_preset(),
            "-c:a",
            "aac",
            "-movflags",
            "+faststart",
            "-loglevel",
            "error",
            output_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *safe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg concat failed: {stderr.decode()}")

        logger.info(
            "concat_mode=reencode clips=%d elapsed=%.2fs output=%s",
            len(video_paths),
            time.perf_counter() - concat_started,
            output_path,
        )
    finally:
        concat_file.unlink(missing_ok=True)


async def _get_video_duration(video_path: str) -> float:
    try:
        ffprobe = _find_binary("ffprobe")
    except RuntimeError:
        return 0.0

    cmd = [
        ffprobe,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        video_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    try:
        return float(stdout.decode().strip())
    except ValueError:
        return 0.0


async def _get_video_duration_limited(path: str, semaphore: asyncio.Semaphore) -> float:
    async with semaphore:
        return await _get_video_duration(path)


def _build_gcs_uri(bucket: str, blob_name: str) -> str:
    return f"gs://{bucket}/{blob_name}"


def _upload_video_to_gcs(local_path: str, request_id: str) -> str | None:
    bucket_name = (config.GCS_BUCKET_NAME or "").strip()
    if not bucket_name:
        return None

    prefix = str(config.VIDEO_GCS_PREFIX or "generated_videos").strip("/")
    blob_name = f"{prefix}/{request_id}.mp4" if prefix else f"{request_id}.mp4"

    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path, content_type="video/mp4")

    return _build_gcs_uri(bucket_name, blob_name)


async def _process_card(
    card: dict,
    card_num: int,
    tmp_dir: Path,
    render_sem: asyncio.Semaphore,
    tts_sem: asyncio.Semaphore,
    ffmpeg_sem: asyncio.Semaphore,
) -> dict:
    """Process one card by exactly one of 3 branches."""
    interaction = _extract_interaction_payload(card)
    if interaction and interaction.get("type") in {"quiz", "flashcard", "fill_blank"}:
        # Branch 1: interactive card -> short silent clip as pause-point buffer + metadata.
        # Without a real clip, pause_time would equal the previous clip's end_time exactly,
        # giving the frontend zero margin to intercept playback before the next clip starts.
        out = str(tmp_dir / f"slide_{card_num}.mp4")
        interaction_clip_duration = max(
            0.12,
            float(getattr(config, "INTERACTION_CLIP_DURATION_SEC", 1.0)),
        )
        if interaction.get("type") == "quiz":
            # Keep quiz clip long enough so pause_time can stay ~1s after clip start.
            quiz_offset = max(0.0, float(getattr(config, "QUIZ_PAUSE_OFFSET_SEC", 1.0)))
            edge_guard = max(
                0.001,
                float(getattr(config, "QUIZ_PAUSE_EDGE_GUARD_SEC", 1.0)),
            )
            interaction_clip_duration = max(interaction_clip_duration, quiz_offset + edge_guard + 0.05)
        async with ffmpeg_sem:
            await _create_silent_black_clip(out, duration=interaction_clip_duration)
        return {
            "card_num": card_num,
            "video_path": out,
            "interaction": interaction,
        }

    material = _extract_video_source(card)
    if material:
        # Branch 2: card with video block -> normalize clip.
        out = str(tmp_dir / f"slide_{card_num}.mp4")
        async with ffmpeg_sem:
            await _create_material_clip(
                source_video=material["src"],
                output_path=out,
                start_time=material.get("start"),
                end_time=material.get("end"),
                tmp_dir=tmp_dir,
            )
        return {
            "card_num": card_num,
            "video_path": out,
            "interaction": interaction,
        }

    # Branch 3: normal card -> render + tts in parallel -> slide clip.
    narration = _extract_narration(card)
    if not narration.strip():
        return {
            "card_num": card_num,
            "video_path": None,
            "interaction": interaction,
        }

    image_path = str(tmp_dir / f"slide_{card_num}.png")
    audio_path = str(tmp_dir / f"slide_{card_num}.mp3")
    video_path = str(tmp_dir / f"slide_{card_num}.mp4")

    async def do_render():
        async with render_sem:
            await render_slide_async(card, image_path)

    async def do_tts():
        async with tts_sem:
            await generate_audio_async(narration, audio_path)

    await asyncio.gather(do_render(), do_tts())

    async with ffmpeg_sem:
        await _create_slide_clip(image_path, audio_path, video_path)

    return {
        "card_num": card_num,
        "video_path": video_path,
        "interaction": interaction,
    }


async def generate_video_async(
    lesson_data: dict,
    request_id: str | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict:
    """Generate final lesson video and metadata."""
    request_id = request_id or uuid.uuid4().hex[:12]
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"vidgen_{request_id}_"))

    try:
        await _emit_progress(progress_callback, "extracting_cards", 10, "Resolving lesson payload")

        lesson = extract_lesson_data(lesson_data)
        cards = list(lesson.get("cards") or [])
        if not cards:
            raise ValueError("No cards found in lesson payload")
        table_of_contents = _build_table_of_contents(cards)

        await _emit_progress(
            progress_callback,
            "processing_cards",
            15,
            f"Processing {len(cards)} card(s)",
        )

        max_workers = max(1, int(config.MAX_SLIDE_CONCURRENCY))
        render_sem = asyncio.Semaphore(max(1, int(config.RENDER_CONCURRENCY)))
        tts_sem = asyncio.Semaphore(max(1, int(config.TTS_CONCURRENCY)))
        ffmpeg_sem = asyncio.Semaphore(max(1, int(config.FFMPEG_CONCURRENCY)))
        probe_sem = asyncio.Semaphore(max(1, int(config.PROBE_CONCURRENCY)))

        queue: asyncio.Queue[int | None] = asyncio.Queue()
        for idx in range(len(cards)):
            queue.put_nowait(idx)
        for _ in range(max_workers):
            queue.put_nowait(None)

        results: list[dict] = []
        results_q: asyncio.Queue[dict | None] = asyncio.Queue()

        async def worker() -> None:
            while True:
                idx = await queue.get()
                if idx is None:
                    return
                card_num = idx + 1
                card = cards[idx]
                try:
                    result = await _process_card(
                        card,
                        card_num,
                        tmp_dir,
                        render_sem,
                        tts_sem,
                        ffmpeg_sem,
                    )
                except Exception as exc:
                    logger.error("Card %d failed, skipping: %s", card_num, exc)
                    result = {"card_num": card_num, "video_path": None, "interaction": None}
                await results_q.put(result)
                cards[idx] = None

        workers = [asyncio.create_task(worker()) for _ in range(max_workers)]
        last_progress_sent = -1
        progress_granularity = max(1, int(getattr(config, "PROGRESS_STEP_GRANULARITY", 10)))

        for done_count in range(1, len(cards) + 1):
            item = await results_q.get()
            if item:
                results.append(item)
            progress = min(75, 15 + int((done_count / len(cards)) * 60))
            is_last = done_count == len(cards)
            should_emit = (
                is_last
                or last_progress_sent < 0
                or progress >= (last_progress_sent + progress_granularity)
            )
            if not should_emit:
                continue

            last_progress_sent = progress
            await _emit_progress(
                progress_callback,
                "processing_cards",
                progress,
                f"Completed {done_count}/{len(cards)} card(s)",
            )

        await asyncio.gather(*workers)

        results.sort(key=lambda x: int(x.get("card_num") or 0))
        video_paths = [r["video_path"] for r in results if r.get("video_path")]
        if not video_paths:
            raise ValueError("No video clips generated")

        await _emit_progress(progress_callback, "concatenating_video", 80, "Concatenating clips")

        output_filename = f"{request_id}.mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await _concat_videos(video_paths, output_path)

        await _emit_progress(progress_callback, "building_timeline", 88, "Building interactions timeline")

        duration_jobs: list[tuple[dict, asyncio.Task[float]]] = []
        for item in results:
            clip = item.get("video_path")
            if clip:
                duration_jobs.append(
                    (item, asyncio.create_task(_get_video_duration_limited(clip, probe_sem)))
                )
            else:
                item["clip_duration"] = 0.0

        if duration_jobs:
            durations = await asyncio.gather(*[job for _, job in duration_jobs])
            for (item, _), duration in zip(duration_jobs, durations):
                item["clip_duration"] = float(duration or 0.0)

        cursor = 0.0
        interactions: list[dict] = []
        pause_points: list[float] = []
        card_timings: dict[int, dict[str, float]] = {}
        for slide_index, item in enumerate(results, start=1):
            clip_duration = float(item.get("clip_duration") or 0.0)
            start_time = cursor
            end_time = start_time + clip_duration
            if clip_duration > 0:
                cursor = end_time

            card_idx = int(item.get("card_num") or slide_index)
            card_timings[card_idx] = {
                "start_time": round(start_time, 3),
                "end_time": round(end_time, 3),
                "duration": round(max(0.0, clip_duration), 3),
            }

            interaction = item.get("interaction")
            if interaction:
                # Default pause_time points to the start of the interaction clip.
                # For quiz clips, shift by +0.1s but keep pause_time strictly before end_time.
                pause_time_base = start_time
                if interaction.get("type") == "quiz":
                    quiz_offset = max(0.0, float(getattr(config, "QUIZ_PAUSE_OFFSET_SEC", 1.0)))
                    edge_guard = max(
                        0.001,
                        float(getattr(config, "QUIZ_PAUSE_EDGE_GUARD_SEC", 1.0)),
                    )
                    pause_upper_bound = max(start_time, end_time - edge_guard)
                    pause_time_base = min(pause_upper_bound, start_time + quiz_offset)
                pause_time = round(pause_time_base, 3)
                interactions.append(
                    {
                        "type": interaction["type"],
                        "slide_index": slide_index,
                        "card_index": item["card_num"],
                        "start_time": round(start_time, 3),
                        "end_time": round(end_time, 3),
                        "pause_time": pause_time,
                        "payload": {k: v for k, v in interaction.items() if k != "type"},
                    }
                )
                pause_points.append(pause_time)

        toc_with_timeline: list[dict] = []
        for entry in table_of_contents:
            card_idx = int(entry.get("card_index") or 0)
            timing = card_timings.get(card_idx, {})
            start_time = timing.get("start_time")
            toc_with_timeline.append(
                {
                    "card_index": card_idx,
                    "label": str(entry.get("label") or "").strip() or f"Slide {card_idx}",
                    "start_time": start_time,
                    "end_time": timing.get("end_time"),
                    "duration": timing.get("duration"),
                    "seek_time": start_time,
                }
            )

        # Expose table-of-contents as interaction items so frontend can render index
        # from a single `interactions` collection.
        interactions_with_index = list(interactions)
        for toc_item in toc_with_timeline:
            card_idx = int(toc_item.get("card_index") or 0)
            start_time = toc_item.get("start_time")
            if start_time is None:
                continue

            end_time = toc_item.get("end_time")
            interactions_with_index.append(
                {
                    "type": "index",
                    "slide_index": card_idx,
                    "card_index": card_idx,
                    "start_time": start_time,
                    "end_time": end_time,
                    "pause_time": start_time,
                    "payload": {
                        "label": str(toc_item.get("label") or "").strip() or f"Slide {card_idx}",
                        "seek_time": toc_item.get("seek_time", start_time),
                    },
                }
            )

        interactions_with_index.sort(
            key=lambda item: (
                float(item.get("start_time") or 0.0),
                0 if item.get("type") == "index" else 1,
                int(item.get("card_index") or 0),
            )
        )

        duration = await _get_video_duration(output_path)

        await _emit_progress(progress_callback, "uploading_video", 95, "Uploading final video")

        local_video_url = f"{config.VIDEO_BASE_URL}/{output_filename}"
        video_gcs_uri = await asyncio.to_thread(_upload_video_to_gcs, output_path, request_id)
        if video_gcs_uri and config.VIDEO_RETURN_GCS_URI:
            final_url = video_gcs_uri
        else:
            final_url = local_video_url

        return {
            "video_url": final_url,
            "video_gcs_uri": video_gcs_uri,
            "video_local_url": local_video_url,
            "duration": duration,
            "request_id": request_id,
            "interactions": interactions_with_index,
            "pause_points": pause_points,
            "table_of_contents": toc_with_timeline,
        }

    finally:
        if config.CLEANUP_BROWSER_EACH_REQUEST:
            try:
                await cleanup_browser()
            except Exception:
                logger.warning("Browser cleanup failed", exc_info=True)

        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            logger.warning("Temp cleanup failed: %s", tmp_dir, exc_info=True)


def generate_video(lesson_data: dict, request_id: str | None = None) -> dict:
    return asyncio.run(generate_video_async(lesson_data, request_id=request_id))
