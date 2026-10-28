"""ASGI entry point: ``uvicorn durable.asgi:app``."""

from .api import create_app

app = create_app()
