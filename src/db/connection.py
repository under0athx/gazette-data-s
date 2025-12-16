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


def check_connectivity() -> bool:
    """Check database connectivity.

    Returns True if database is reachable, False otherwise.
    Useful for startup health checks and readiness probes.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone() is not None
    except Exception:
        return False


def wait_for_database(max_retries: int = 5, retry_interval: float = 2.0) -> bool:
    """Wait for database to become available.

    Args:
        max_retries: Maximum number of connection attempts.
        retry_interval: Seconds to wait between retries.

    Returns:
        True if database became available, False if all retries exhausted.
    """
    import logging
    import time

    logger = logging.getLogger(__name__)

    for attempt in range(1, max_retries + 1):
        if check_connectivity():
            logger.info("Database connection established on attempt %d", attempt)
            return True

        if attempt < max_retries:
            logger.warning(
                "Database not available (attempt %d/%d), retrying in %.1fs...",
                attempt,
                max_retries,
                retry_interval,
            )
            time.sleep(retry_interval)

    logger.error("Failed to connect to database after %d attempts", max_retries)
    return False
