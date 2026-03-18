"""
Auto-mapper — semantically matches Lesson nodes (textbook schema) to
ChuDe nodes (curriculum schema) using Gemini, then saves the result
to mapping/{subject_code}_{level_slug}.json for auditability.

The saved JSON can be hand-edited and re-applied without re-parsing
using: python main.py --apply-mapping <subject_code> <education_level>
"""

import json
import logging
import re
from pathlib import Path

from google import genai
from google.genai import types
from neo4j import GraphDatabase

from config import Config
from parser import ParsedCurriculum

logger = logging.getLogger(__name__)

_DEFAULT_MAPPING_DIR = Path(__file__).parent / "mapping"

_MAPPING_PROMPT = """\
Bạn là chuyên gia giáo dục Việt Nam. Nhiệm vụ: ghép mỗi Bài học (Lesson) từ sách giáo khoa \
vào các Chủ đề (ChuDe) tương ứng trong Chương trình Giáo dục Phổ thông.

=== DANH SÁCH BÀI HỌC (từ Sách giáo khoa, Lớp {grade}) ===
{lessons_json}

=== DANH SÁCH CHỦ ĐỀ (từ Chương trình GDPT, Lớp {grade}) ===
{chu_de_json}

=== QUY TẮC ===
1. Mỗi Lesson ghép với 1 hoặc nhiều ChuDe mà bài học đó chủ yếu đề cập đến.
2. Dựa vào tên bài (Lesson.name) và tên chủ đề (ChuDe.ten_chu_de + ChuDe.phan_mon) để so sánh ngữ nghĩa.
3. Nếu không tìm được ChuDe phù hợp cho một Lesson, để mảng rỗng [].
4. KHÔNG bịa đặt ID — chỉ dùng các ID có trong danh sách trên.
5. Trả về TẤT CẢ các Lesson ID, kể cả những Lesson có mảng rỗng.

=== OUTPUT ===
Trả về CHỈ một JSON object, không có markdown fence, không có giải thích:
{{
  "dia_li_lop_10_L1": ["dia_li_thpt_lop_10_CD01"],
  "dia_li_lop_10_L2": ["dia_li_thpt_lop_10_CD03", "dia_li_thpt_lop_10_CD04"],
  "dia_li_lop_10_L3": []
}}
"""


# ── Gemini client ────────────────────────────────────────────────────────────

def _make_client() -> genai.Client:
    if Config.HELICONE_API_KEY:
        return genai.Client(
            vertexai=True,
            project=Config.GOOGLE_CLOUD_PROJECT,
            location=Config.VERTEX_AI_LOCATION,
            http_options=types.HttpOptions(
                base_url="https://gateway.helicone.ai",
                headers={
                    "Helicone-Auth": f"Bearer {Config.HELICONE_API_KEY}",
                    "Helicone-Target-Url": f"https://{'aiplatform.googleapis.com' if Config.VERTEX_AI_LOCATION == 'global' else Config.VERTEX_AI_LOCATION + '-aiplatform.googleapis.com'}",
                },
            ),
        )
    return genai.Client(
        vertexai=True,
        project=Config.GOOGLE_CLOUD_PROJECT,
        location=Config.VERTEX_AI_LOCATION,
    )


def _strip_json_fence(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


# ── Neo4j queries ────────────────────────────────────────────────────────────

def _fetch_lessons(lesson_id_prefix: str) -> list[dict]:
    """
    Fetch Lesson nodes whose ID starts with the given prefix.
    Returns list of {id, name}.
    """
    driver = GraphDatabase.driver(
        Config.NEO4J_URI, auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD)
    )
    try:
        with driver.session() as session:
            result = session.run(
                """
                MATCH (l:Lesson)
                WHERE l.id STARTS WITH $prefix
                RETURN l.id AS id, l.name AS name
                ORDER BY l.id
                """,
                prefix=lesson_id_prefix,
            )
            return [{"id": r["id"], "name": r["name"]} for r in result]
    finally:
        driver.close()


def _fetch_chu_de(lop_id: str) -> list[dict]:
    """
    Fetch ChuDe nodes linked to the given Lop ID.
    Returns list of {id, ten_chu_de, phan_mon}.
    """
    driver = GraphDatabase.driver(
        Config.NEO4J_URI, auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD)
    )
    try:
        with driver.session() as session:
            result = session.run(
                """
                MATCH (l:Lop {id: $lop_id})-[:HAS]->(cd:ChuDe)
                RETURN cd.id AS id, cd.ten_chu_de AS ten_chu_de, cd.phan_mon AS phan_mon
                ORDER BY cd.id
                """,
                lop_id=lop_id,
            )
            return [
                {"id": r["id"], "ten_chu_de": r["ten_chu_de"], "phan_mon": r["phan_mon"]}
                for r in result
            ]
    finally:
        driver.close()


# ── Gemini matching ──────────────────────────────────────────────────────────

def _gemini_match(
    lessons: list[dict],
    chu_de_list: list[dict],
    grade: str,
) -> dict[str, list[str]]:
    """
    Use Gemini to semantically match each Lesson to its ChuDe(s).
    Returns {lesson_id: [chu_de_id, ...]}
    """
    client = _make_client()
    prompt = _MAPPING_PROMPT.format(
        grade=grade,
        lessons_json=json.dumps(lessons, ensure_ascii=False, indent=2),
        chu_de_json=json.dumps(chu_de_list, ensure_ascii=False, indent=2),
    )

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0,
            response_mime_type="application/json",
        ),
    )

    raw_json = _strip_json_fence(response.text)
    mapping = json.loads(raw_json)

    # Ensure every lesson in the input is present in output (even if empty)
    for lesson in lessons:
        if lesson["id"] not in mapping:
            mapping[lesson["id"]] = []

    return mapping


# ── Public API ───────────────────────────────────────────────────────────────

def auto_map(
    curriculum: ParsedCurriculum,
    subject_code: str,
    education_level: str,
    curriculum_year: int | None = None,
) -> str:
    """
    Auto-generate the Lesson→ChuDe mapping for all grades in the curriculum.

    For each Lop in the parsed curriculum:
      1. Fetch Lesson nodes from Neo4j (textbook schema) for that grade
      2. Fetch ChuDe nodes from Neo4j (curriculum schema) for that grade
      3. Call Gemini to semantically match them
      4. Merge results across all grades

    Saves the combined mapping to:
      mapping/{subject_code}_{level_slug}_{year}.json

    Returns the path to the saved mapping file.
    """
    level_slug = education_level.lower().replace(" ", "_")
    year_suffix = f"_{curriculum_year}" if curriculum_year else ""
    mon_hoc_id = f"{subject_code}_{level_slug}{year_suffix}"
    all_mapping: dict[str, list[str]] = {}

    for lop in curriculum.lop_list:
        grade_match = re.search(r"\d+", lop.ten_lop)
        if not grade_match:
            logger.warning("Could not extract grade number from '%s', skipping", lop.ten_lop)
            continue
        grade_number = grade_match.group(0)

        lesson_prefix = f"{subject_code}_lop_{grade_number}_"
        lop_id = f"{mon_hoc_id}_lop_{grade_number}"

        logger.info("Auto-mapping grade %s: fetching Lessons (prefix=%s)...", grade_number, lesson_prefix)
        lessons = _fetch_lessons(lesson_prefix)

        if not lessons:
            logger.warning(
                "No Lesson nodes found for prefix '%s'. "
                "Ensure textbook has been ingested before running auto-mapping.",
                lesson_prefix,
            )
            continue

        logger.info("  Found %d lessons. Fetching ChuDe for lop_id=%s...", len(lessons), lop_id)
        chu_de_list = _fetch_chu_de(lop_id)

        if not chu_de_list:
            logger.warning("No ChuDe nodes found for lop_id='%s', skipping grade", lop_id)
            continue

        logger.info(
            "  Found %d ChuDe. Calling Gemini to match %d lessons...",
            len(chu_de_list), len(lessons),
        )
        grade_mapping = _gemini_match(lessons, chu_de_list, grade=grade_number)
        all_mapping.update(grade_mapping)

        covered = sum(1 for v in grade_mapping.values() if v)
        logger.info(
            "  Grade %s: %d/%d lessons matched to at least one ChuDe",
            grade_number, covered, len(lessons),
        )

    # Save to disk for auditability — can be hand-edited and re-applied
    _DEFAULT_MAPPING_DIR.mkdir(exist_ok=True)
    mapping_path = _DEFAULT_MAPPING_DIR / f"{subject_code}_{level_slug}{year_suffix}.json"
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(all_mapping, f, ensure_ascii=False, indent=2)

    total_covered = sum(1 for v in all_mapping.values() if v)
    logger.info(
        "Auto-mapping complete: %d/%d lessons mapped. Saved to %s",
        total_covered, len(all_mapping), mapping_path,
    )
    return str(mapping_path)
