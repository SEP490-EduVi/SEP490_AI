"""
Video generation pipeline — optimized version.

Flow:
    JSON → Extract cards (keeps original HTML)
        → Parallel: [Playwright screenshot] + [Edge TTS]
        → ffmpeg: image + audio → slide video
        → ffmpeg concat → final video
"""

import asyncio
import logging
import re
import shutil
import tempfile
import uuid
import os
from pathlib import Path
from typing import List, Dict, Any, Callable, Awaitable

from .slide_renderer import render_slide_async, cleanup_browser
from .tts import generate_audio_async
from .utils import extract_lesson_data, strip_html_tags
from . import config

logger = logging.getLogger(__name__)

_BINARY_CACHE: Dict[str, str] = {}

# Directory where finished videos are served from
OUTPUT_DIR = Path(config.OUTPUT_DIR)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


ProgressCallback = Callable[[str, int, str | None], Awaitable[None] | None]


async def _emit_progress(
    progress_callback: ProgressCallback | None,
    step: str,
    progress: int,
    detail: str | None = None,
) -> None:
    """Emit progress via callback if provided."""
    if not progress_callback:
        return

    maybe_awaitable = progress_callback(step, progress, detail)
    if asyncio.iscoroutine(maybe_awaitable):
        await maybe_awaitable


def _build_gcs_uri(bucket: str, blob_name: str) -> str:
    return f"gs://{bucket}/{blob_name}"


def _upload_video_to_gcs(local_path: str, request_id: str) -> str | None:
    """Upload local video to GCS and return gs:// URI."""
    bucket_name = (getattr(config, "VIDEO_GCS_BUCKET", "") or "").strip()
    if not bucket_name:
        return None

    prefix = str(getattr(config, "VIDEO_GCS_PREFIX", "generated_videos") or "generated_videos").strip("/")
    blob_name = f"{prefix}/{request_id}.mp4" if prefix else f"{request_id}.mp4"

    # Lazy import to keep local-only usage lightweight.
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path, content_type="video/mp4")

    uri = _build_gcs_uri(bucket_name, blob_name)
    logger.info("Uploaded video to GCS: %s", uri)
    return uri


def _find_binary(binary_name: str) -> str:
    """Find executable path with Windows-friendly fallbacks."""
    cached = _BINARY_CACHE.get(binary_name)
    if cached:
        return cached

    # 1) Respect current PATH first.
    exe = shutil.which(binary_name)
    if exe:
        _BINARY_CACHE[binary_name] = exe
        return exe

    # 2) Optional explicit override from environment.
    bin_dir = os.getenv("FFMPEG_BIN_DIR", "").strip()
    if bin_dir:
        candidate = Path(bin_dir) / f"{binary_name}.exe"
        if candidate.exists():
            resolved = str(candidate)
            _BINARY_CACHE[binary_name] = resolved
            return resolved

    # 3) Windows Winget common install location fallback.
    if os.name == "nt":
        local_app_data = os.getenv("LOCALAPPDATA", "")
        if local_app_data:
            winget_base = Path(local_app_data) / "Microsoft" / "WinGet" / "Packages"
            if winget_base.exists():
                patterns = [
                    "Gyan.FFmpeg_*/*/bin",
                    "*FFmpeg*/*/bin",
                ]
                for pattern in patterns:
                    for bin_path in winget_base.glob(pattern):
                        candidate = bin_path / f"{binary_name}.exe"
                        if candidate.exists():
                            resolved = str(candidate)
                            _BINARY_CACHE[binary_name] = resolved
                            return resolved

    raise RuntimeError(f"{binary_name} not found in PATH")


def _normalize_text_for_compare(value: str) -> str:
    """Normalize text for fuzzy duplicate checks."""
    return re.sub(r"\s+", " ", (value or "").strip()).casefold()


def _card_has_title_in_children_for_tts(card: Dict[str, Any], card_title: str) -> bool:
    """Return True if card children already include the title text."""
    target = _normalize_text_for_compare(card_title)
    if not target:
        return False

    def walk(nodes: List[Dict[str, Any]]) -> bool:
        for node in nodes:
            node_type = (node.get("type") or "").upper()

            if node_type == "LAYOUT":
                if walk(node.get("children", [])):
                    return True
                continue

            if node_type != "BLOCK":
                continue

            content = node.get("content", {})
            content_type = (content.get("type") or "").upper()
            if content_type not in ("TEXT", "HEADING"):
                continue

            text = strip_html_tags(content.get("html", ""))
            normalized = _normalize_text_for_compare(text)
            if normalized == target or normalized.startswith(target):
                return True

        return False

    return walk(card.get("children", []))


def _get_ffmpeg() -> str:
    """Get ffmpeg executable path."""
    return _find_binary("ffmpeg")


def _iter_block_contents(nodes: List[Dict[str, Any]]):
    """Yield BLOCK content objects from nested card tree."""
    for node in nodes:
        node_type = (node.get("type") or "").upper()
        if node_type == "LAYOUT":
            yield from _iter_block_contents(node.get("children", []))
            continue
        if node_type != "BLOCK":
            continue
        content = node.get("content") or {}
        if isinstance(content, dict):
            yield content


def _extract_interaction_payload(card: Dict[str, Any]) -> Dict[str, Any] | None:
    """Extract FE interaction payload from card when card is interactive."""
    card_title = str(card.get("title") or "").strip()

    for content in _iter_block_contents(card.get("children", [])):
        content_type = (content.get("type") or "").upper()

        if content_type == "QUIZ":
            questions = content.get("questions") or []
            if not questions:
                return None
            q = questions[0] or {}

            options: List[str] = []
            for opt in q.get("options", []):
                options.append((opt.get("text", "") if isinstance(opt, dict) else str(opt)).strip())

            return {
                "type": "quiz",
                "title": card_title,
                "question": str(q.get("question") or "").strip(),
                "options": [o for o in options if o],
            }

        if content_type == "FLASHCARD":
            front = ""
            cards = content.get("cards") or []
            if isinstance(cards, list) and cards:
                front = str((cards[0] or {}).get("front") or "").strip()
            if not front:
                front = str(content.get("front") or "").strip()
            if not front:
                return None

            return {
                "type": "flashcard",
                "title": card_title,
                "front": front,
            }

    return None


def _extract_narration_from_card(card: Dict[str, Any]) -> str:
    """Extract text for TTS narration from a card."""
    parts = []
    
    # Add title
    title = (card.get("title") or "").strip()
    if title and not _card_has_title_in_children_for_tts(card, title):
        parts.append(title)
    
    # Extract text from children
    for child in card.get("children", []):
        _extract_text_recursive(child, parts)
    
    narration = ". ".join(parts)
    if narration and narration[-1] not in ".!?…":
        narration += "."
    return narration


def _extract_text_recursive(node: Dict[str, Any], parts: List[str]):
    """Recursively extract text from BLOCK/LAYOUT nodes."""
    node_type = node.get("type", "").upper()
    
    if node_type == "LAYOUT":
        for child in node.get("children", []):
            _extract_text_recursive(child, parts)
    
    elif node_type == "BLOCK":
        content = node.get("content", {})
        content_type = content.get("type", "").upper()
        
        if content_type in ("TEXT", "HEADING"):
            html = content.get("html", "")
            text = strip_html_tags(html)
            if text:
                parts.append(text)
        elif content_type == "QUIZ":
            questions = content.get("questions", [])
            for q in questions:
                question_text = (q.get("question") or "").strip()
                if question_text:
                    parts.append(question_text)

                options = q.get("options", [])
                for opt in options:
                    option_text = opt.get("text", "") if isinstance(opt, dict) else str(opt)
                    option_text = option_text.strip()
                    if option_text:
                        parts.append(option_text)

        elif content_type == "FLASHCARD":
            cards = content.get("cards") or []
            if cards and isinstance(cards, list):
                for fc in cards:
                    front = (fc.get("front") or "").strip()
                    if front:
                        parts.append(front)
                    else:
                        # Fallback only when front is missing.
                        back = strip_html_tags(fc.get("back", ""))
                        if back:
                            parts.append(back)
            else:
                front = (content.get("front") or "").strip()
                if front:
                    parts.append(front)
                else:
                    back = strip_html_tags(content.get("back", ""))
                    if back:
                        parts.append(back)

        elif content_type == "FILL_BLANK":
            sentence = (content.get("sentence") or "").strip()
            if sentence:
                # Replace [blank] markers with spoken placeholder.
                spoken = re.sub(r"\[.*?\]", "chỗ trống", sentence)
                parts.append(spoken)

            blanks = content.get("blanks") or []
            for b in blanks:
                blank_text = str(b).strip()
                if blank_text:
                    parts.append(blank_text)


async def _create_slide_video(
    image_path: str,
    audio_path: str,
    output_path: str,
) -> str:
    """Create a video clip from image + audio using ffmpeg."""
    ffmpeg = _get_ffmpeg()
    
    cmd = [
        ffmpeg, "-y",
        "-loop", "1",
        "-i", image_path,
        "-i", audio_path,
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-c:a", "aac",
        "-b:a", "192k",
        "-pix_fmt", "yuv420p",
        "-shortest",
        "-movflags", "+faststart",
        "-loglevel", "error",
        output_path
    ]
    
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {stderr.decode()}")
    
    logger.info("Created slide video: %s", Path(output_path).name)
    return output_path


async def _concat_videos(video_paths: List[str], output_path: str) -> str:
    """Concatenate multiple videos using ffmpeg."""
    if not video_paths:
        raise ValueError("No videos to concatenate")
    
    if len(video_paths) == 1:
        shutil.copy(video_paths[0], output_path)
        return output_path
    
    ffmpeg = _get_ffmpeg()
    
    output_dir = Path(output_path).parent
    concat_file = output_dir / f"concat_{Path(output_path).stem}_{uuid.uuid4().hex}.txt"
    with open(concat_file, "w", encoding="utf-8") as f:
        for vp in video_paths:
            f.write(f"file '{Path(vp).resolve().as_posix()}'\n")
    
    cmd = [
        ffmpeg, "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_file),
        "-c", "copy",
        "-movflags", "+faststart",
        "-loglevel", "error",
        output_path
    ]
    
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg concat failed: {stderr.decode()}")
    
    concat_file.unlink(missing_ok=True)
    return output_path


async def _process_card(
    card: Dict[str, Any],
    card_num: int,
    tmp_dir: Path,
    render_semaphore: asyncio.Semaphore,
    tts_semaphore: asyncio.Semaphore,
    ffmpeg_semaphore: asyncio.Semaphore,
) -> Dict[str, Any] | None:
    """
    Process a single CARD: screenshot + TTS + create video.
    Returns processed clip metadata.
    """
    # Get narration text
    narration = _extract_narration_from_card(card)
    if not narration.strip():
        logger.warning("Card %d has no text, skipping", card_num)
        return None

    interaction = _extract_interaction_payload(card)
    
    # Paths
    image_path = str(tmp_dir / f"slide_{card_num}.png")
    audio_path = str(tmp_dir / f"slide_{card_num}.mp3")
    video_path = str(tmp_dir / f"slide_{card_num}.mp4")
    
    async def _render_one() -> None:
        async with render_semaphore:
            await render_slide_async(card, image_path, slide_number=card_num)

    async def _tts_one() -> None:
        async with tts_semaphore:
            await generate_audio_async(narration, audio_path)

    # Parallel per card, but each stage is globally throttled.
    await asyncio.gather(_render_one(), _tts_one())
    
    # Create video from image + audio
    async with ffmpeg_semaphore:
        await _create_slide_video(image_path, audio_path, video_path)
    
    return {
        "card_num": card_num,
        "video_path": video_path,
        "interaction": interaction,
    }


async def _process_card_limited(
    card: Dict[str, Any],
    card_num: int,
    tmp_dir: Path,
    semaphore: asyncio.Semaphore,
    render_semaphore: asyncio.Semaphore,
    tts_semaphore: asyncio.Semaphore,
    ffmpeg_semaphore: asyncio.Semaphore,
) -> Dict[str, Any] | None:
    """Process one card while respecting global per-job concurrency limit."""
    async with semaphore:
        return await _process_card(
            card,
            card_num,
            tmp_dir,
            render_semaphore,
            tts_semaphore,
            ffmpeg_semaphore,
        )


async def _get_video_duration_limited(video_path: str, semaphore: asyncio.Semaphore) -> float:
    """Get duration while throttling ffprobe process fan-out."""
    async with semaphore:
        return await _get_video_duration(video_path)


async def _get_video_duration(video_path: str) -> float:
    """Get video duration using ffprobe."""
    try:
        ffprobe = _find_binary("ffprobe")
    except RuntimeError:
        return 0.0
    
    cmd = [
        ffprobe,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        video_path
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


async def generate_video_async(
    lesson_data: dict,
    request_id: str = None,
    progress_callback: ProgressCallback | None = None,
) -> dict:
    """
    Async video generation pipeline.
    
    Args:
        lesson_data: Lesson JSON from text.md (supports API response format)
        request_id: Optional correlation ID for tracking

    Returns:
        dict with video_url, duration, and interactions metadata.
    """
    request_id = request_id or uuid.uuid4().hex[:12]
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"vidgen_{request_id}_"))
    logger.info("Pipeline started [%s] tmp=%s", request_id, tmp_dir)

    try:
        await _emit_progress(
            progress_callback,
            step="extracting_cards",
            progress=10,
            detail="Resolving lesson payload",
        )

        # Extract lesson data from API response format
        lesson = extract_lesson_data(lesson_data)
        cards = lesson.get("cards", [])
        
        if not cards:
            raise ValueError("No cards found in the input JSON.")

        logger.info("Processing %d card(s)...", len(cards))
        await _emit_progress(
            progress_callback,
            step="rendering_slides",
            progress=20,
            detail=f"Rendering {len(cards)} slide(s)",
        )

        # Process cards in bounded parallelism to protect worker resources.
        max_concurrency = max(1, int(getattr(config, "MAX_SLIDE_CONCURRENCY", 4)))
        semaphore = asyncio.Semaphore(max_concurrency)
        render_concurrency = max(1, int(getattr(config, "RENDER_CONCURRENCY", 2)))
        tts_concurrency = max(1, int(getattr(config, "TTS_CONCURRENCY", 4)))
        ffmpeg_concurrency = max(1, int(getattr(config, "FFMPEG_CONCURRENCY", 1)))
        probe_concurrency = max(1, int(getattr(config, "PROBE_CONCURRENCY", 2)))

        render_semaphore = asyncio.Semaphore(render_concurrency)
        tts_semaphore = asyncio.Semaphore(tts_concurrency)
        ffmpeg_semaphore = asyncio.Semaphore(ffmpeg_concurrency)
        probe_semaphore = asyncio.Semaphore(probe_concurrency)

        tasks = [
            _process_card_limited(
                card,
                i + 1,
                tmp_dir,
                semaphore,
                render_semaphore,
                tts_semaphore,
                ffmpeg_semaphore,
            )
            for i, card in enumerate(cards)
        ]

        processed_cards: List[Dict[str, Any]] = []
        total_cards = len(cards)
        completed_cards = 0
        for done in asyncio.as_completed(tasks):
            item = await done
            completed_cards += 1
            if item and item.get("video_path"):
                processed_cards.append(item)

            render_progress = 20 + int((completed_cards / total_cards) * 50)
            await _emit_progress(
                progress_callback,
                step="rendering_slides",
                progress=min(render_progress, 70),
                detail=f"Completed {completed_cards}/{total_cards} slide(s)",
            )
        
        # Filter out skipped cards and restore original slide order.
        processed_cards = [item for item in processed_cards if item and item.get("video_path")]
        processed_cards.sort(key=lambda item: int(item.get("card_num") or 0))
        video_paths = [item["video_path"] for item in processed_cards]
        
        if not video_paths:
            raise ValueError("No valid slides to render.")

        # Concatenate all slide videos
        output_filename = f"{request_id}.mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        
        logger.info("Concatenating %d videos...", len(video_paths))
        await _emit_progress(
            progress_callback,
            step="concatenating_video",
            progress=80,
            detail="Merging slide clips",
        )
        await _concat_videos(video_paths, output_path)

        # Build timing map for FE pause/overlay interactions.
        await _emit_progress(
            progress_callback,
            step="building_timeline",
            progress=88,
            detail="Calculating interaction timeline",
        )
        clip_durations = await asyncio.gather(
            *[
                _get_video_duration_limited(item["video_path"], probe_semaphore)
                for item in processed_cards
            ]
        )
        for item, clip_duration in zip(processed_cards, clip_durations):
            item["clip_duration"] = clip_duration

        timeline_cursor = 0.0
        interactions: List[Dict[str, Any]] = []
        pause_points: List[float] = []
        for slide_index, item in enumerate(processed_cards, start=1):
            clip_duration = float(item.get("clip_duration") or 0.0)
            start_time = timeline_cursor
            end_time = start_time + clip_duration
            timeline_cursor = end_time

            interaction = item.get("interaction")
            if interaction:
                pause_time = round(end_time, 3)
                interactions.append({
                    "type": interaction["type"],
                    "slide_index": slide_index,
                    "card_index": item["card_num"],
                    "start_time": round(start_time, 3),
                    "end_time": round(end_time, 3),
                    "pause_time": pause_time,
                    "payload": {k: v for k, v in interaction.items() if k != "type"},
                })
                pause_points.append(pause_time)

        # Get final duration
        duration = await _get_video_duration(output_path)
        await _emit_progress(
            progress_callback,
            step="uploading_video",
            progress=95,
            detail="Uploading output video",
        )

        local_video_url = f"{config.VIDEO_BASE_URL}/{output_filename}"
        video_gcs_uri = _upload_video_to_gcs(output_path, request_id=request_id)
        if video_gcs_uri and getattr(config, "VIDEO_RETURN_GCS_URI", True):
            video_url = video_gcs_uri
        else:
            video_url = local_video_url

        logger.info(
            "Pipeline finished [%s] → %s (%.1fs)",
            request_id, video_url, duration,
        )

        return {
            "video_url": video_url,
            "video_gcs_uri": video_gcs_uri,
            "video_local_url": local_video_url,
            "duration": duration,
            "request_id": request_id,
            "interactions": interactions,
            "pause_points": pause_points,
        }

    except Exception:
        logger.exception("Pipeline failed [%s]", request_id)
        raise

    finally:
        if getattr(config, "CLEANUP_BROWSER_EACH_REQUEST", False):
            try:
                await cleanup_browser()
            except Exception:
                logger.warning("Could not cleanup browser", exc_info=True)

        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            logger.warning("Could not clean temp dir %s", tmp_dir, exc_info=True)


def generate_video(lesson_data: dict, request_id: str = None) -> dict:
    """Sync wrapper for generate_video_async."""
    return asyncio.run(generate_video_async(lesson_data, request_id))
