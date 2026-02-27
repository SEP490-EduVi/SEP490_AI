"""Abstract base class for subject-specific entity generators."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from neo4j import Session


@dataclass
class NodeProperty:
    """A single property on a node type."""
    name: str
    type: str          # "String", "Integer", etc.
    description: str
    required: bool = True


@dataclass
class NodeType:
    """A node label and its properties."""
    label: str
    description: str
    properties: list[NodeProperty] = field(default_factory=list)


@dataclass
class RelationshipType:
    """A relationship between two node labels."""
    name: str
    from_label: str
    to_label: str
    description: str


# ── Abstract base class ──────────────────────────────────────────────


class BaseEntityGenerator(ABC):
    """
    Abstract base for subject-specific entity generators.

    Subclasses must implement every abstract method.  The pipeline calls
    them in this order:

        1. get_system_prompt()              → system-level LLM instruction
        2. get_extraction_prompt(...)       → user-level LLM prompt
        3. parse_response(raw_json, book_id) → validate & normalise LLM output
        4. get_constraints()                → Neo4j uniqueness constraints
        5. load_to_neo4j(session, data)     → persist nodes + relationships
    """

    # ── Schema introspection (for documentation / UI) ────────────────

    @abstractmethod
    def get_node_types(self) -> list[NodeType]:
        """Return the full list of node types this generator creates."""

    @abstractmethod
    def get_relationship_types(self) -> list[RelationshipType]:
        """Return the full list of relationship types this generator creates."""

    # ── LLM prompt generation ────────────────────────────────────────

    @abstractmethod
    def get_system_prompt(self) -> str:
        """
        Return the system-level prompt that instructs the LLM about
        its role and output format.
        """

    @abstractmethod
    def get_extraction_prompt(
        self, raw_text: str, subject: str, grade: str
    ) -> str:
        """
        Build the user-level prompt that asks the LLM to analyse the
        provided *raw_text* and return structured JSON entities.

        Args:
            raw_text: Extracted text from PDF/DOCX.
            subject:  Subject name (e.g. "Dia Li").
            grade:    Grade level  (e.g. "Lop 10").
        """

    @abstractmethod
    def parse_response(self, raw_json: dict, book_id: str) -> dict:
        """
        Validate and normalise the JSON dict returned by the LLM.

        Must prefix all structural node IDs with *book_id* so that
        multiple books can coexist in a single Neo4j database.

        Should raise ``ValueError`` if required keys are missing.
        Returns a cleaned dict that ``load_to_neo4j`` can consume.
        """

    # ── Neo4j persistence ────────────────────────────────────────────

    @abstractmethod
    def get_constraints(self) -> list[str]:
        """
        Return a list of Cypher statements that create uniqueness /
        existence constraints.  Each statement should use
        ``IF NOT EXISTS`` for idempotency.
        """

    @abstractmethod
    def load_to_neo4j(self, session: Session, data: dict) -> dict[str, int]:
        """
        Persist the extracted entities into Neo4j.

        Args:
            session: An open ``neo4j.Session``.
            data:    The dict returned by ``parse_response()``.

        Returns:
            A summary dict with counts, e.g.
            ``{"books": 1, "chapters": 3, "lessons": 12, ...}``
        """

    # ── Page-range config (override per subject) ──────────────────

    def get_skip_pages(self) -> tuple[int, int]:
        """
        Return ``(skip_start, skip_end)`` page counts.

        - *skip_start*: number of leading pages to skip (cover, TOC, …).
        - *skip_end*:   number of trailing pages to skip (index, credits, …).

        Default ``(0, 0)`` — override in subject-specific subclasses.
        """
        return (0, 0)
