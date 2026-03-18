"""Neo4j queries for lesson analysis.

Textbook schema (created by textbook_ingestion):
    Book → Part → Chapter → Lesson → Section → Concept / Location / Figure
    Lessons may sit directly under Part (no Chapter).

Curriculum schema (created by curriculum_ingestion):
    MonHoc → Lop → ChuDe → YeuCau
    Cross-schema: (Lesson)-[:COVERS]->(ChuDe)
"""

import logging
import unicodedata
from neo4j import GraphDatabase
from config import Config

logger = logging.getLogger(__name__)

_driver = GraphDatabase.driver(
    Config.NEO4J_URI,
    auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD),
)


def _strip_diacritics(text: str) -> str:
    """Remove Vietnamese diacritics: 'Đia Li' / 'Địa Lí' → 'Dia Li'."""
    text = text.replace("Đ", "D").replace("đ", "d")
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# ── Book discovery ─────────────────────────────────────────────────────


def find_book(subject: str, grade: str) -> dict | None:
    """Find a Book node. Strips diacritics so 'Đia Li' still matches 'Dia Li'."""
    clean_subject = _strip_diacritics(subject)
    clean_grade = _strip_diacritics(grade)
    logger.info("Searching book: subject='%s' → '%s', grade='%s' → '%s'",
                subject, clean_subject, grade, clean_grade)

    query = """
    MATCH (b:Book)
    WHERE toLower(b.subject) CONTAINS toLower($subject)
      AND toLower(b.grade)   CONTAINS toLower($grade)
    RETURN b.id AS id, b.subject AS subject, b.grade AS grade
    LIMIT 1
    """
    with _driver.session() as session:
        record = session.run(query, subject=clean_subject, grade=clean_grade).single()
    if record:
        logger.info("Found book: %s (%s - %s)", record["id"], record["subject"], record["grade"])
        return dict(record)
    logger.warning("No book found for subject=%s, grade=%s", clean_subject, clean_grade)
    return None


# ── Lesson discovery ───────────────────────────────────────────────────


def list_lessons(book_id: str) -> list[dict]:
    """List all lessons under a book. Returns [{"id", "name", "order"}]."""
    query = """
    MATCH (b:Book {id: $book_id})-[:HAS*1..3]->(l:Lesson)
    RETURN l.id AS id, l.name AS name, l.order AS order
    ORDER BY l.order
    """
    with _driver.session() as session:
        results = session.run(query, book_id=book_id)
        lessons = [dict(r) for r in results]
    logger.info("Found %d lessons in book %s", len(lessons), book_id)
    return lessons


# ── Concepts ───────────────────────────────────────────────────────────


def get_concepts_by_lesson_id(lesson_id: str) -> list[dict]:
    """Fetch concepts for a specific lesson node by its id."""
    query = """
    MATCH (l:Lesson {id: $lesson_id})
          -[:HAS]->(s:Section)
          -[:MENTIONS]->(co:Concept)
    RETURN DISTINCT co.name AS name, co.definition AS definition
    ORDER BY name
    """
    with _driver.session() as session:
        results = session.run(query, lesson_id=lesson_id)
        concepts = [{"name": r["name"], "definition": r["definition"]} for r in results]
    logger.info("Found %d concepts for lesson %s", len(concepts), lesson_id)
    return concepts


def get_standard_concepts(subject: str, grade: str) -> list[dict]:
    """Fetch ALL concepts for a subject/grade (whole book). Fallback only."""
    query = """
    MATCH (b:Book {subject: $subject, grade: $grade})
          -[:HAS*2..4]->(s:Section)
          -[:MENTIONS]->(co:Concept)
    RETURN DISTINCT co.name AS name, co.definition AS definition
    ORDER BY name
    """
    with _driver.session() as session:
        results = session.run(query, subject=subject, grade=grade)
        concepts = [{"name": r["name"], "definition": r["definition"]} for r in results]
    logger.info("Found %d concepts for %s - %s (whole book)", len(concepts), subject, grade)
    return concepts


# ── Locations ──────────────────────────────────────────────────────────


def get_locations_by_lesson_id(lesson_id: str) -> list[dict]:
    """Fetch locations for a specific lesson node by its id."""
    query = """
    MATCH (l:Lesson {id: $lesson_id})
          -[:HAS]->(s:Section)
          -[:MENTIONS]->(lo:Location)
    RETURN DISTINCT lo.name AS name, lo.type AS type
    ORDER BY name
    """
    with _driver.session() as session:
        results = session.run(query, lesson_id=lesson_id)
        locations = [{"name": r["name"], "type": r["type"]} for r in results]
    logger.info("Found %d locations for lesson %s", len(locations), lesson_id)
    return locations


def get_standard_locations(subject: str, grade: str) -> list[dict]:
    """Fetch ALL locations for a subject/grade (whole book). Fallback only."""
    query = """
    MATCH (b:Book {subject: $subject, grade: $grade})
          -[:HAS*2..4]->(s:Section)
          -[:MENTIONS]->(lo:Location)
    RETURN DISTINCT lo.name AS name, lo.type AS type
    ORDER BY name
    """
    with _driver.session() as session:
        results = session.run(query, subject=subject, grade=grade)
        locations = [{"name": r["name"], "type": r["type"]} for r in results]
    logger.info("Found %d locations for %s - %s (whole book)", len(locations), subject, grade)
    return locations


# ── Section content ────────────────────────────────────────────────────


def get_sections_by_lesson_id(lesson_id: str) -> list[dict]:
    """Fetch section content for a specific lesson (for RAG context)."""
    query = """
    MATCH (l:Lesson {id: $lesson_id})-[:HAS]->(s:Section)
    RETURN s.heading AS heading, s.content AS content
    ORDER BY s.id
    """
    with _driver.session() as session:
        results = session.run(query, lesson_id=lesson_id)
        sections = [{"heading": r["heading"], "content": r["content"]} for r in results]
    logger.info("Found %d sections for lesson %s", len(sections), lesson_id)
    return sections


# ── Curriculum YeuCau ─────────────────────────────────────────────────────────


def get_yeu_cau_by_lesson_id(lesson_id: str) -> dict:
    """
    Fetch curriculum YeuCau/ChuDe linked to a lesson via COVERS relationships.
    Traverses: (Lesson)-[:COVERS]->(ChuDe)-[:MENTIONS]->(YeuCau)

    Returns:
        {
            "chu_de_list": [{"id", "ten_chu_de", "phan_mon"}, ...],
            "yeu_cau_list": [{"id", "noi_dung", "tieu_chuan", "chu_de_id"}, ...],
        }
    """
    query = """
    MATCH (l:Lesson {id: $lesson_id})-[:COVERS]->(cd:ChuDe)-[:MENTIONS]->(yc:YeuCau)
    RETURN cd.id AS chu_de_id, cd.ten_chu_de AS ten_chu_de, cd.phan_mon AS phan_mon,
           yc.id AS yeu_cau_id, yc.noi_dung AS noi_dung, yc.tieu_chuan AS tieu_chuan
    ORDER BY cd.id, yc.id
    """
    with _driver.session() as session:
        records = [dict(r) for r in session.run(query, lesson_id=lesson_id)]

    chu_de_map: dict = {}
    yeu_cau_list: list = []
    for r in records:
        cd_id = r["chu_de_id"]
        if cd_id not in chu_de_map:
            chu_de_map[cd_id] = {
                "id": cd_id,
                "ten_chu_de": r["ten_chu_de"],
                "phan_mon": r["phan_mon"],
            }
        yeu_cau_list.append({
            "id": r["yeu_cau_id"],
            "noi_dung": r["noi_dung"],
            "tieu_chuan": r["tieu_chuan"],
            "chu_de_id": cd_id,
        })

    logger.info(
        "Found %d YeuCau across %d ChuDe for lesson %s",
        len(yeu_cau_list), len(chu_de_map), lesson_id,
    )
    return {
        "chu_de_list": list(chu_de_map.values()),
        "yeu_cau_list": yeu_cau_list,
    }


def close():
    """Close the Neo4j driver connection."""
    _driver.close()
