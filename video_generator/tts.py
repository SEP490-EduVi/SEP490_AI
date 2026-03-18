"""
Text-to-Speech module using Edge TTS (Microsoft Edge).

Much faster than gTTS (async, batch processing).
High quality Vietnamese voice.
"""

import asyncio
import logging
from pathlib import Path

import edge_tts
from edge_tts.exceptions import NoAudioReceived

from . import config

logger = logging.getLogger(__name__)

# Vietnamese voices (Edge TTS)
DEFAULT_VOICE = "vi-VN-HoaiMyNeural"  # Female, natural

DEFAULT_RATE = "+10%"  # Slightly faster


async def generate_audio_async(
    text: str,
    output_path: str,
    voice: str = DEFAULT_VOICE,
    rate: str = DEFAULT_RATE,
) -> str:
    """Generate MP3 audio from text using Edge TTS (async)."""
    if not text or not text.strip():
        raise ValueError("Cannot generate audio from empty text.")

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    normalized_text = text.strip()
    max_retries = max(1, int(getattr(config, "TTS_MAX_RETRIES", 3)))
    retry_delay = max(0.1, float(getattr(config, "TTS_RETRY_DELAY_SEC", 0.8)))

    for attempt in range(1, max_retries + 1):
        try:
            communicate = edge_tts.Communicate(normalized_text, voice, rate=rate)
            await communicate.save(str(output))
            break
        except NoAudioReceived as exc:
            if attempt >= max_retries:
                logger.error(
                    "Edge TTS exhausted retries for %s (%d chars): %s",
                    output.name,
                    len(normalized_text),
                    exc,
                )
                raise

            delay = retry_delay * attempt
            logger.warning(
                "Edge TTS returned no audio for %s (attempt %d/%d), retrying in %.1fs",
                output.name,
                attempt,
                max_retries,
                delay,
            )
            await asyncio.sleep(delay)
        except Exception:
            # Network hiccups/timeouts from the underlying websocket can be transient.
            if attempt >= max_retries:
                raise
            delay = retry_delay * attempt
            logger.warning(
                "Edge TTS transient error for %s (attempt %d/%d), retrying in %.1fs",
                output.name,
                attempt,
                max_retries,
                delay,
                exc_info=True,
            )
            await asyncio.sleep(delay)
    
    logger.info("Generated audio: %s (%d chars)", output.name, len(text))
    return str(output)
