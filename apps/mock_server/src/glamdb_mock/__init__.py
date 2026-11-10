"""Disposable development adapter; never a production storage implementation."""

from .server import create_app

__all__ = ["create_app"]
