"""Neo4j queries for slide generator — reads textbook content only.

Textbook schema (created by textbook_ingestion):
    Book → Part → Chapter → Lesson → Section → Concept / Location / Figure
    Lessons may sit directly under Part (no Chapter).
"""

import logging
from neo4j import GraphDatabase
from config import Config

logger = logging.getLogger(__name__)

_driver = GraphDatabase.driver(
    Config.NEO4J_URI,
    auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD),
)


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


def close() -> None:
    """Close the Neo4j driver connection."""
    _driver.close()
