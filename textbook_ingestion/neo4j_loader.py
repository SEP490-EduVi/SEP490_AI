"""Load textbook data into Neo4j. Delegates schema logic to entity generators."""

import logging
from neo4j import GraphDatabase
from config import Config
from entity_generator.base import BaseEntityGenerator

logger = logging.getLogger(__name__)

# Created once when the module loads
_driver = GraphDatabase.driver(
    Config.NEO4J_URI,
    auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD),
)


def create_constraints(generator: BaseEntityGenerator) -> None:
    """Create uniqueness constraints (idempotent)."""
    constraints = generator.get_constraints()
    with _driver.session() as session:
        for cypher in constraints:
            try:
                session.run(cypher)
            except Exception as e:
                # Neo4j Aura free tier may not support all constraint types
                logger.warning("Constraint creation skipped: %s", e)
    logger.info(
        "Constraints created/verified (%d statements).", len(constraints)
    )


def load_textbook_data(
    data: dict, generator: BaseEntityGenerator
) -> dict[str, int]:
    """Persist entities into Neo4j via the generator's Cypher queries."""
    with _driver.session() as session:
        counts = generator.load_to_neo4j(session, data)

    logger.info(
        "Loaded into Neo4j: %s",
        ", ".join(f"{k}={v}" for k, v in counts.items()),
    )
    return counts


def delete_book(book_id: str) -> dict[str, int]:
    """
    Delete all Neo4j data for a book, including orphaned Concept/Location nodes.

    Strategy:
      1. Snapshot Concept/Location nodes that are ONLY referenced by this book's
         sections (exclusive nodes — safe to delete after structural nodes are gone).
      2. DETACH DELETE all structural nodes (Book/Part/Chapter/Lesson/Section/Figure)
         by id prefix. This also removes MENTIONS and COVERS relationships.
      3. DELETE the snapshotted exclusive Concept/Location nodes (now orphaned).

    Returns: {"deleted_structural": N, "deleted_concepts": M}
    """
    with _driver.session() as session:
        # Step 1: Find Concept/Location nodes exclusively connected to this book
        exclusive_result = session.run(
            """
            MATCH (s:Section)-[:MENTIONS]->(n)
            WHERE s.id STARTS WITH $book_id
              AND (n:Concept OR n:Location)
              AND NOT EXISTS {
                MATCH (s2:Section)-[:MENTIONS]->(n)
                WHERE NOT s2.id STARTS WITH $book_id
              }
            RETURN collect(elementId(n)) AS exclusive_ids
            """,
            book_id=book_id,
        )
        exclusive_ids = exclusive_result.single()["exclusive_ids"]
        logger.info(
            "Found %d exclusive Concept/Location nodes for book %s",
            len(exclusive_ids), book_id,
        )

        # Step 2: DETACH DELETE all structural nodes (removes all relationships too)
        structural_result = session.run(
            """
            MATCH (n)
            WHERE n.id STARTS WITH $book_id
            WITH count(n) AS total
            CALL {
                MATCH (n)
                WHERE n.id STARTS WITH $book_id
                DETACH DELETE n
            }
            RETURN total
            """,
            book_id=book_id,
        )
        deleted_structural = structural_result.single()["total"]
        logger.info(
            "DETACH DELETEd %d structural nodes for book %s",
            deleted_structural, book_id,
        )

        # Step 3: DELETE now-orphaned exclusive Concept/Location nodes
        if exclusive_ids:
            concept_result = session.run(
                """
                MATCH (n)
                WHERE elementId(n) IN $exclusive_ids
                WITH count(n) AS total
                CALL {
                    MATCH (n)
                    WHERE elementId(n) IN $exclusive_ids
                    DELETE n
                }
                RETURN total
                """,
                exclusive_ids=exclusive_ids,
            )
            deleted_concepts = concept_result.single()["total"]
        else:
            deleted_concepts = 0

        logger.info(
            "Deleted %d orphaned Concept/Location nodes for book %s",
            deleted_concepts, book_id,
        )

    return {
        "deleted_structural": deleted_structural,
        "deleted_concepts": deleted_concepts,
    }


def close():
    """Close the Neo4j driver connection."""
    _driver.close()

