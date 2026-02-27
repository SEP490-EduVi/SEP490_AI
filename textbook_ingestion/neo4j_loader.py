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


def close():
    """Close the Neo4j driver connection."""
    _driver.close()
