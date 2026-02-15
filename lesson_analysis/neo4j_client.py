"""Fetch standard concepts from Neo4j knowledge graph."""

import logging
from neo4j import GraphDatabase
from config import Config

logger = logging.getLogger(__name__)

# Created once when the module loads
_driver = GraphDatabase.driver(
    Config.NEO4J_URI,
    auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD),
)

def get_standard_concepts(subject: str, grade: str) -> list[str]:
    """
    Query Neo4j for the required concepts given a subject and grade level.

    Args:
        subject: e.g. "Toán", "Vật lý"
        grade:   e.g. "Lớp 10", "Lớp 11"

    Returns:
        A list of concept name strings.
    """
    # Reuses the existing connection
    with _driver.session() as session:
        # TODO: Adjust this Cypher query to match your actual Neo4j schema.
        #       The labels (Lesson, Concept) and relationship (COVERS)
        #       are placeholders – update them once the graph is designed.
        query = """
        MATCH (l:Lesson)-[]->(e:Entity)
        WHERE l.subject = $subject AND l.grade = $grade
        RETURN e.name AS concept
        """
        results = session.run(query, subject=subject, grade=grade)
        concepts = [record["concept"] for record in results]
    logger.info(
        "Found %d standard concepts for %s – %s",
        len(concepts), subject, grade,
    )
    return concepts
