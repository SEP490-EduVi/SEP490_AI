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
    title = str(card.get("title") or "").strip()
    if title:
        parts.append(title)

    for content in _iter_block_contents(card.get("children") or []):
        ctype = str(content.get("type") or "").upper()
        if ctype in {"TEXT", "HEADING"}:
            txt = strip_html_tags(str(content.get("html") or ""))
            if txt and txt != title:
                parts.append(txt)
            continue

        if ctype == "QUIZ":
            for q in content.get("questions") or []:
                q_text = str((q or {}).get("question") or "").strip()
                if q_text:
                    parts.append(q_text)
                for opt in (q or {}).get("options") or []:
                    if isinstance(opt, dict):
                        o = str(opt.get("text") or "").strip()
                    else:
                        o = str(opt).strip()
                    if o:
                        parts.append(o)
            continue

        if ctype == "FLASHCARD":
            cards = content.get("cards") or []
            if isinstance(cards, list):
                for item in cards:
                    front = str((item or {}).get("front") or "").strip()
                    if front:
                        parts.append(front)
            continue

        if ctype == "FILL_BLANK":
            sentence = str(content.get("sentence") or "").strip()
            if sentence:
                parts.append(re.sub(r"\[.*?\]", "chỗ trống", sentence))

    narration = ". ".join([p for p in parts if p])
    if narration and narration[-1] not in ".!?":
        narration += "."
    return narration


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
        # Branch 1: interactive card -> 0.1s silent black clip as pause-point buffer + metadata.
        # Without a real clip, pause_time would equal the previous clip's end_time exactly,
        # giving the frontend zero margin to intercept playback before the next clip starts.
        out = str(tmp_dir / f"slide_{card_num}.mp4")
        async with ffmpeg_sem:
            await _create_silent_black_clip(out, duration=0.1)
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
                    result = await _process_card(card, card_num, tmp_dir, render_sem, tts_sem, ffmpeg_sem)
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
        for slide_index, item in enumerate(results, start=1):
            clip_duration = float(item.get("clip_duration") or 0.0)
            start_time = cursor
            end_time = start_time + clip_duration
            if clip_duration > 0:
                cursor = end_time

            interaction = item.get("interaction")
            if interaction:
                # pause_time points to the START of the interaction clip so the frontend
                # has the full 0.1 s clip duration as a buffer before the next content
                # clip begins.  Using end_time here would give zero margin.
                pause_time = round(start_time, 3)
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
            "interactions": interactions,
            "pause_points": pause_points,
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
