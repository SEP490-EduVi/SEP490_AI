"""
Text-to-Speech module using Edge TTS (Microsoft Edge).

Much faster than gTTS (async, batch processing).
High quality Vietnamese voice.
"""

import asyncio
import logging
from pathlib import Path

import edge_tts

logger = logging.getLogger(__name__)

# Vietnamese voices (Edge TTS)
DEFAULT_VOICE = "vi-VN-HoaiMyNeural"  # Female, natural
MALE_VOICE = "vi-VN-NamMinhNeural"    # Male alternative

DEFAULT_RATE = "+10%"  # Slightly faster
DEFAULT_VOLUME = "+0%"


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


def generate_audio(
    text: str,
    output_path: str,
    voice: str = DEFAULT_VOICE,
    rate: str = DEFAULT_RATE,
) -> str:
    """Sync wrapper for generate_audio_async."""
    return asyncio.run(generate_audio_async(text, output_path, voice, rate))


async def generate_audio_batch(items: list[tuple[str, str]]) -> list[str]:
    """
    Generate multiple audio files in parallel.
    
    Args:
        items: List of (text, output_path) tuples
        
    Returns:
        List of output paths
    """
    tasks = [
        generate_audio_async(text, path)
        for text, path in items
        if text and text.strip()
    ]
    return await asyncio.gather(*tasks)
