"""Subject-specific entity generators for node types, prompts, and Neo4j loading."""

from entity_generator.base import BaseEntityGenerator
from entity_generator.geography import GeographyEntityGenerator

_REGISTRY: dict[str, type[BaseEntityGenerator]] = {
    "Dia Li": GeographyEntityGenerator,
    "Địa Lí": GeographyEntityGenerator,
    "Địa lí": GeographyEntityGenerator,
    "dia li": GeographyEntityGenerator,
}


def get_entity_generator(subject: str) -> BaseEntityGenerator:
    """Return the entity generator for a subject. Raises ValueError if unknown."""
    generator_cls = _REGISTRY.get(subject)
    if generator_cls is None:
        supported = ", ".join(sorted(set(
            cls.__name__ for cls in _REGISTRY.values()
        )))
        raise ValueError(
            f"No entity generator registered for subject '{subject}'. "
            f"Available generators: {supported}"
        )
    return generator_cls()
