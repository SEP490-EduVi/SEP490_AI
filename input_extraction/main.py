"""
Input Extraction Worker Service
================================
Entry point for the extraction service.

Usage
-----
Manual mode (current – push GCS URIs directly):
    python main.py <gcs_uri_or_blob_path>

Examples:
    python main.py gs://my-bucket/uploads/sample.pdf
    python main.py uploads/lecture_notes.docx

RabbitMQ mode (future – listens for messages):
    python main.py --rabbitmq
"""

import sys
from config import Config
from worker import process_file


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    Config.validate()

    # Future: start RabbitMQ consumer
    if sys.argv[1] == "--rabbitmq":
        from rabbitmq_consumer import start_consumer
        start_consumer()
        return

    # Manual mode – accept one or more GCS URIs from the command line
    gcs_uris = sys.argv[1:]
    for uri in gcs_uris:
        try:
            process_file(uri)
        except Exception as exc:
            print(f"[ERROR] Failed to process '{uri}': {exc}")


if __name__ == "__main__":
    main()
