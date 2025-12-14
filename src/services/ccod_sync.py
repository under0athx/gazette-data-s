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

from src.db.connection import get_connection
from src.utils.config import settings

logger = logging.getLogger(__name__)

CCOD_URL = "https://use-land-property-data.service.gov.uk/datasets/ccod/download"


class CCODSyncService:
    """Syncs CCOD data from Land Registry.

    The CCOD dataset is several GB in size, so we stream it to disk
    and process it in chunks to avoid memory exhaustion.
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

    def load_from_zip(self, zip_path: Path) -> int:
        """Load CCOD data from zip file directly into PostgreSQL."""
        rows_processed = 0

        with self.stream_csv_from_zip(zip_path) as csv_file:
            reader = csv.DictReader(csv_file)

            with get_connection() as conn:
                with conn.cursor() as cur:
                    # Truncate and reload
                    cur.execute("TRUNCATE TABLE ccod_properties")

                    batch = []
                    batch_size = 5000  # Larger batch for bulk load

                    for row in reader:
                        batch.append(
                            (
                                row.get("Title Number"),
                                row.get("Property Address"),
                                row.get("Proprietor Name (1)"),
                                row.get("Company Registration No. (1)"),
                                row.get("Tenure"),
                                row.get("Date Proprietor Added"),
                            )
                        )

                        if len(batch) >= batch_size:
                            self._insert_batch(cur, batch)
                            rows_processed += len(batch)
                            if rows_processed % 100000 == 0:
                                logger.info("Processed %d rows", rows_processed)
                            batch = []

                    if batch:
                        self._insert_batch(cur, batch)
                        rows_processed += len(batch)

                    conn.commit()

        return rows_processed

    def _insert_batch(self, cursor, batch: list[tuple]):
        """Insert a batch of records."""
        cursor.executemany(
            """
            INSERT INTO ccod_properties (
                title_number, property_address, company_name,
                company_number, tenure, date_proprietor_added
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (title_number) DO UPDATE SET
                property_address = EXCLUDED.property_address,
                company_name = EXCLUDED.company_name,
                company_number = EXCLUDED.company_number,
                tenure = EXCLUDED.tenure,
                date_proprietor_added = EXCLUDED.date_proprietor_added,
                updated_at = NOW()
            """,
            batch,
        )

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
