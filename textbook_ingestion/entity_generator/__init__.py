"""Entity generator package — exposes a single generic generator for all subjects."""

from entity_generator.generator import TextbookEntityGenerator


def get_entity_generator(subject: str) -> TextbookEntityGenerator:
    """Return a TextbookEntityGenerator instance (works for any subject)."""
    return TextbookEntityGenerator()
