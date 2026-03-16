"""Video generator package — RabbitMQ-based worker for scalable video generation."""


def generate_video(*args, **kwargs):
	"""Lazy import to avoid pipeline side effects during package initialization."""
	from .pipeline import generate_video as _generate_video

	return _generate_video(*args, **kwargs)


__all__ = ["generate_video"]
