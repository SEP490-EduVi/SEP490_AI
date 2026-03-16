"""
Local test script — runs video generation without RabbitMQ.

Usage with a specific JSON file:
    python -m video_generator.test_local path/to/lesson.json

Or from Google Cloud Storage:
    python -m video_generator.test_local gs://bucket/path/to/lesson.json
"""

import asyncio
import json
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("test_local")

# Override config for local testing
from . import config
config.OUTPUT_DIR = str(Path(__file__).parent.parent / "videos")
config.VIDEO_BASE_URL = "file://" + config.OUTPUT_DIR

from .pipeline import generate_video_async
from .slide_payload_extractor import load_json_document


async def main():
    if len(sys.argv) <= 1:
        logger.error("Missing input source. Use local path or gs:// URI")
        logger.info("Example: python -m video_generator.test_local gs://bucket/path/to/file.json")
        sys.exit(1)

    input_file = sys.argv[1]
    
    logger.info("Loading lesson data from: %s", input_file)
    
    try:
        lesson_data = load_json_document(input_file)
    except FileNotFoundError:
        logger.error("File not found: %s", input_file)
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Could not load lesson source: %s", e)
        sys.exit(1)
    
    logger.info("Starting video generation (optimized pipeline)...")
    
    try:
        result = await generate_video_async(lesson_data, request_id="test_local")
        
        logger.info("=" * 60)
        logger.info("Video generation complete!")
        logger.info("  Video URL: %s", result["video_url"])
        logger.info("  Duration:  %.1f seconds", result["duration"])
        logger.info("  Interactions: %d", len(result.get("interactions", [])))
        logger.info("=" * 60)
        
        # Print result as JSON
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
    except Exception as e:
        logger.exception("Video generation failed")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
