"""
Textbook Ingestion — CLI entry point.

Usage:
    python main.py <source> <subject> <grade> [book_id]
    python main.py "D:/books/dia_li_10.pdf" "Dia Li" "Lop 10"
"""

import sys
import json
import logging
import re
from config import Config
from pipeline import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _make_book_id(subject: str, grade: str) -> str:
    """Generate a safe book_id, e.g. 'dia_li_10'."""
    raw = f"{subject}_{grade}".lower()
    safe = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return safe


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        print("Error: Expected 3-4 arguments: <source> <subject> <grade> [book_id]")
        sys.exit(1)

    source = sys.argv[1]
    subject = sys.argv[2]
    grade = sys.argv[3]
    book_id = sys.argv[4] if len(sys.argv) >= 5 else _make_book_id(subject, grade)

    logger.info("=" * 60)
    logger.info("Textbook Ingestion Pipeline")
    logger.info("  Source:  %s", source)
    logger.info("  Subject: %s", subject)
    logger.info("  Grade:   %s", grade)
    logger.info("  Book ID: %s", book_id)
    logger.info("=" * 60)

    Config.validate()

    result = run(source, subject, grade, book_id)

    # Print summary
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    for part in result.get("parts", []):
        pt_id = part.get("id", "?")
        pt_name = part.get("name", "Untitled")
        print(f"\n  {pt_id}: {pt_name}")

        # Lessons directly under part
        for ls in part.get("lessons", []):
            ls_id = ls.get("id", "?")
            ls_name = ls.get("name", "Untitled")
            sec_count = len(ls.get("sections", []))
            print(f"    {ls_id}: {ls_name} ({sec_count} sections)")
            for sec in ls.get("sections", []):
                sec_id = sec.get("id", "?")
                heading = sec.get("heading", "Untitled")
                n_concepts = len(sec.get("concepts", []))
                n_locations = len(sec.get("locations", []))
                n_figures = len(sec.get("figures", []))
                print(
                    f"      {sec_id}: {heading}  "
                    f"[C:{n_concepts} L:{n_locations} F:{n_figures}]"
                )

        for ch in part.get("chapters", []):
            ch_id = ch.get("id", "?")
            ch_name = ch.get("name", "Untitled")
            print(f"\n    {ch_id}: {ch_name}")
            for ls in ch.get("lessons", []):
                ls_id = ls.get("id", "?")
                ls_name = ls.get("name", "Untitled")
                sec_count = len(ls.get("sections", []))
                print(f"      {ls_id}: {ls_name} ({sec_count} sections)")
                for sec in ls.get("sections", []):
                    sec_id = sec.get("id", "?")
                    heading = sec.get("heading", "Untitled")
                    n_concepts = len(sec.get("concepts", []))
                    n_locations = len(sec.get("locations", []))
                    n_figures = len(sec.get("figures", []))
                    print(
                        f"        {sec_id}: {heading}  "
                        f"[C:{n_concepts} L:{n_locations} F:{n_figures}]"
                    )

    # Save JSON output
    output_path = f"output_{book_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\nFull data saved to: {output_path}")


if __name__ == "__main__":
    main()
