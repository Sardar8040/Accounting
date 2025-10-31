"""Database package for Teleshop Auto Reporting."""

from .models import init_db, get_connection

__all__ = ["init_db", "get_connection"]
