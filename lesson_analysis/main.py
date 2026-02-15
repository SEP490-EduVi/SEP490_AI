"""
Lesson Analysis Service
========================
Entry point – analyse a teacher's lesson plan against the standard knowledge graph.

Usage
-----
    python main.py <gcs_uri> <subject> <grade>

Examples:
    python main.py "uploads/lesson.pdf" "Toán" "Lớp 10"
    python main.py "gs://my-bucket/uploads/bai_giang.docx" "Vật lý" "Lớp 11"
"""

import json
import logging
import sys
from pipeline import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)


def main() -> None:
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)

    gcs_uri = sys.argv[1]
    subject = sys.argv[2]
    grade = sys.argv[3]

    result = run(gcs_uri, subject, grade)

    # ── For now: console output.  Later: publish to RabbitMQ. ──
    print("\n" + "=" * 60)
    print("EVALUATION RESULT")
    print("=" * 60)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
