"""
Text-to-Speech module using Edge TTS (Microsoft Edge).

Much faster than gTTS (async, batch processing).
High quality Vietnamese voice.
"""

import logging
from pathlib import Path

import edge_tts

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

    communicate = edge_tts.Communicate(text.strip(), voice, rate=rate)
    await communicate.save(str(output))
    
    logger.info("Generated audio: %s (%d chars)", output.name, len(text))
    return str(output)
