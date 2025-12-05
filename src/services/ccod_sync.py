"""CCOD sync service - refreshes Land Registry CCOD data monthly."""

import csv
import io
import zipfile
from pathlib import Path

import httpx

from src.db.connection import get_connection
from src.utils.config import settings

CCOD_URL = "https://use-land-property-data.service.gov.uk/datasets/ccod/download"


class CCODSyncService:
    """Syncs CCOD data from Land Registry."""

    def download_ccod(self) -> bytes:
        """Download CCOD dataset (requires gov.uk credentials)."""
        # Note: Actual implementation would need authentication
        # This is a placeholder for the download logic
        with httpx.Client(timeout=600.0) as client:
            response = client.get(
                CCOD_URL,
                headers={"Authorization": f"Bearer {settings.ccod_gov_uk_credentials}"},
            )
            response.raise_for_status()
            return response.content

    def extract_csv(self, zip_data: bytes) -> bytes:
        """Extract CSV from downloaded zip."""
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            for name in zf.namelist():
                if name.endswith(".csv"):
                    return zf.read(name)
        raise ValueError("No CSV found in CCOD zip")

    def load_to_database(self, csv_data: bytes):
        """Load CCOD data into PostgreSQL."""
        content = csv_data.decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Truncate and reload
                cur.execute("TRUNCATE TABLE ccod_properties")

                batch = []
                batch_size = 1000

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
                        batch = []

                if batch:
                    self._insert_batch(cur, batch)

                conn.commit()

    def _insert_batch(self, cursor, batch: list[tuple]):
        """Insert a batch of records."""
        cursor.executemany(
            """
            INSERT INTO ccod_properties
            (title_number, property_address, company_name, company_number, tenure, date_proprietor_added)
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
        """Full sync: download, extract, and load."""
        print("Downloading CCOD dataset...")
        zip_data = self.download_ccod()

        print("Extracting CSV...")
        csv_data = self.extract_csv(zip_data)

        print("Loading to database...")
        self.load_to_database(csv_data)

        print("CCOD sync complete")


def main():
    """Entry point for CCOD sync service."""
    service = CCODSyncService()
    service.sync()


if __name__ == "__main__":
    main()
