import psycopg
from psycopg.rows import dict_row

from src.utils.config import settings


def get_connection() -> psycopg.Connection:
    """Get a database connection."""
    return psycopg.connect(settings.database_url, row_factory=dict_row)


def get_async_connection() -> psycopg.AsyncConnection:
    """Get an async database connection."""
    return psycopg.AsyncConnection.connect(settings.database_url, row_factory=dict_row)
