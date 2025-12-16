"""CCOD sync service - refreshes Land Registry CCOD data monthly."""

import csv
import io
import logging
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, TextIO

import httpx
from psycopg import sql

from src.db.connection import get_connection
from src.utils.config import settings

logger = logging.getLogger(__name__)

CCOD_URL = "https://use-land-property-data.service.gov.uk/datasets/ccod/download"

# Column mapping from CSV headers to database columns
CCOD_COLUMNS = [
    ("Title Number", "title_number"),
    ("Property Address", "property_address"),
    ("Proprietor Name (1)", "company_name"),
    ("Company Registration No. (1)", "company_number"),
    ("Tenure", "tenure"),
    ("Date Proprietor Added", "date_proprietor_added"),
]


class CCODSyncService:
    """Syncs CCOD data from Land Registry.

    The CCOD dataset is several GB in size, so we stream it to disk
    and process it using PostgreSQL COPY for 10-100x faster bulk inserts.
    """

    def download_ccod(self, dest_path: Path) -> None:
        """Stream download CCOD dataset to a file."""
        logger.info("Downloading CCOD dataset to %s", dest_path)

        with httpx.Client(timeout=600.0) as client:
            with client.stream(
                "GET",
                CCOD_URL,
                headers={"Authorization": f"Bearer {settings.ccod_gov_uk_credentials}"},
            ) as response:
                response.raise_for_status()
                total = int(response.headers.get("content-length", 0))

                with open(dest_path, "wb") as f:
                    downloaded = 0
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        # Log every 50MB
                        if total and downloaded % (50 * 1024 * 1024) == 0:
                            mb_down = downloaded // (1024 * 1024)
                            mb_total = total // (1024 * 1024)
                            logger.info("Downloaded %d MB / %d MB", mb_down, mb_total)

        logger.info("Download complete: %s", dest_path)

    @contextmanager
    def stream_csv_from_zip(self, zip_path: Path) -> Generator[TextIO, None, None]:
        """Stream CSV content from zip file without loading into memory."""
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_name = None
            for name in zf.namelist():
                if name.endswith(".csv"):
                    csv_name = name
                    break

            if not csv_name:
                raise ValueError("No CSV found in CCOD zip")

            logger.info("Streaming CSV: %s", csv_name)
            with zf.open(csv_name) as csv_file:
                # Wrap in TextIOWrapper for csv.DictReader
                yield io.TextIOWrapper(csv_file, encoding="utf-8")

    def _row_generator(
        self, csv_file: TextIO
    ) -> Generator[tuple[int, tuple], None, None]:
        """Generate rows from CSV file for COPY command."""
        reader = csv.DictReader(csv_file)
        count = 0

        for row in reader:
            count += 1
            yield count, tuple(row.get(csv_col) or None for csv_col, _ in CCOD_COLUMNS)

    def load_from_zip_with_copy(self, zip_path: Path) -> int:
        """Load CCOD data using PostgreSQL COPY for maximum performance.

        COPY is 10-100x faster than INSERT for bulk loading as it:
        - Bypasses SQL parsing overhead
        - Uses binary protocol
        - Batches WAL writes
        """
        rows_processed = 0
        db_columns = [db_col for _, db_col in CCOD_COLUMNS]

        with self.stream_csv_from_zip(zip_path) as csv_file:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    # Use a staging table for atomic swap
                    logger.info("Creating staging table...")
                    cur.execute("""
                        CREATE TEMP TABLE ccod_staging (LIKE ccod_properties)
                        ON COMMIT DROP
                    """)

                    # Use COPY for bulk insert into staging
                    logger.info("Loading data with COPY...")
                    copy_sql = sql.SQL("COPY ccod_staging ({}) FROM STDIN").format(
                        sql.SQL(", ").join(sql.Identifier(col) for col in db_columns)
                    )

                    with cur.copy(copy_sql) as copy:
                        for count, row in self._row_generator(csv_file):
                            copy.write_row(row)
                            if count % 100000 == 0:
                                logger.info("Loaded %d rows...", count)
                        rows_processed = count

                    # Atomic swap: truncate and insert from staging
                    logger.info("Swapping data into main table...")
                    cur.execute("TRUNCATE TABLE ccod_properties")
                    cur.execute(f"""
                        INSERT INTO ccod_properties ({', '.join(db_columns)})
                        SELECT {', '.join(db_columns)} FROM ccod_staging
                    """)

                    conn.commit()

        return rows_processed

    def load_from_zip(self, zip_path: Path) -> int:
        """Load CCOD data from zip file directly into PostgreSQL.

        Uses COPY command for 10-100x faster bulk inserts compared to INSERT.
        Falls back to batch INSERT if COPY fails.
        """
        try:
            return self.load_from_zip_with_copy(zip_path)
        except Exception as e:
            logger.warning("COPY failed (%s), falling back to batch INSERT", e)
            return self._load_from_zip_batch(zip_path)

    def _load_from_zip_batch(self, zip_path: Path) -> int:
        """Fallback: Load CCOD data using batch INSERT."""
        rows_processed = 0
        db_columns = [db_col for _, db_col in CCOD_COLUMNS]

        with self.stream_csv_from_zip(zip_path) as csv_file:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE ccod_properties")

                    batch = []
                    batch_size = 10000

                    for count, row in self._row_generator(csv_file):
                        batch.append(row)

                        if len(batch) >= batch_size:
                            self._insert_batch(cur, batch, db_columns)
                            rows_processed += len(batch)
                            if rows_processed % 100000 == 0:
                                logger.info("Processed %d rows", rows_processed)
                            batch = []

                    if batch:
                        self._insert_batch(cur, batch, db_columns)
                        rows_processed += len(batch)

                    conn.commit()

        return rows_processed

    def _insert_batch(self, cursor, batch: list[tuple], columns: list[str]):
        """Insert a batch of records using executemany.

        Uses psycopg.sql for safe SQL composition to prevent SQL injection.
        """
        # Build column list safely using sql.Identifier
        cols = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)

        # Build the full query with safe identifiers
        query = sql.SQL("""
            INSERT INTO ccod_properties ({cols})
            VALUES ({placeholders})
            ON CONFLICT (title_number) DO UPDATE SET
                property_address = EXCLUDED.property_address,
                company_name = EXCLUDED.company_name,
                company_number = EXCLUDED.company_number,
                tenure = EXCLUDED.tenure,
                date_proprietor_added = EXCLUDED.date_proprietor_added,
                updated_at = NOW()
        """).format(cols=cols, placeholders=placeholders)

        cursor.executemany(query, batch)

    def sync(self):
        """Full sync: download, stream, and load."""
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "ccod.zip"

            logger.info("Downloading CCOD dataset...")
            self.download_ccod(zip_path)

            logger.info("Loading to database (streaming from zip)...")
            rows = self.load_from_zip(zip_path)

            logger.info("CCOD sync complete: %d rows loaded", rows)


def main():
    """Entry point for CCOD sync service."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    service = CCODSyncService()
    service.sync()


if __name__ == "__main__":
    main()
