from contextlib import contextmanager
from typing import Generator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from src.utils.config import settings

# Connection pool - initialized lazily
_pool: ConnectionPool | None = None


def _get_pool() -> ConnectionPool:
    """Get or create the connection pool."""
    global _pool
    if _pool is None:
        if not settings.database_url:
            raise ValueError("DATABASE_URL is not configured")
        _pool = ConnectionPool(
            settings.database_url,
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row},
        )
    return _pool


@contextmanager
def get_connection() -> Generator[psycopg.Connection, None, None]:
    """Get a database connection from the pool."""
    pool = _get_pool()
    with pool.connection() as conn:
        yield conn


async def get_async_connection() -> psycopg.AsyncConnection:
    """Get an async database connection."""
    if not settings.database_url:
        raise ValueError("DATABASE_URL is not configured")
    return await psycopg.AsyncConnection.connect(settings.database_url, row_factory=dict_row)


def close_pool() -> None:
    """Close the connection pool."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
