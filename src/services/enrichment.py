"""Company enrichment service - enriches Gazette records with CH and CCOD data."""

import csv
import io
import json
import logging
from datetime import date, datetime
from typing import Optional

from src.db.models import EnrichedCompany, GazetteRecord
from src.graph.state import EnrichmentState
from src.graph.workflow import enrichment_graph

logger = logging.getLogger(__name__)


def _parse_date(value: Optional[str]) -> Optional[date]:
    """Parse date from string, trying multiple formats."""
    if not value or not value.strip():
        return None

    formats = [
        "%Y-%m-%d",  # ISO format
        "%d/%m/%Y",  # UK format
        "%d-%m-%Y",  # UK with dashes
        "%Y%m%d",    # Compact
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue

    logger.warning("Could not parse date: %s", value)
    return None


class EnrichmentService:
    """Enriches Gazette records using LangGraph workflow."""

    def parse_gazette_csv(self, csv_bytes: bytes) -> list[GazetteRecord]:
        """Parse Gazette CSV into records."""
        content = csv_bytes.decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        records = []
        for row in reader:
            company_name = row.get("company_name", "").strip()
            if not company_name:
                logger.warning("Skipping row with empty company name: %s", row)
                continue

            records.append(
                GazetteRecord(
                    company_name=company_name,
                    insolvency_type=row.get("insolvency_type"),
                    notice_date=_parse_date(row.get("notice_date")),
                    ip_name=row.get("ip_name"),
                    ip_firm=row.get("ip_firm"),
                )
            )
        return records

    def enrich_all(self, records: list[GazetteRecord]) -> list[EnrichedCompany]:
        """Enrich all records using the LangGraph workflow."""
        initial_state = EnrichmentState(gazette_records=records)
        final_state = enrichment_graph.invoke(initial_state)
        return final_state["enriched_companies"]

    def to_csv(self, enriched: list[EnrichedCompany]) -> bytes:
        """Convert enriched records to CSV."""
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "company_name",
                "company_number",
                "company_status",
                "insolvency_type",
                "ip_name",
                "ip_appointed_date",
                "property_count",
                "properties",
            ],
        )
        writer.writeheader()
        for record in enriched:
            row = record.model_dump()
            # Serialize properties list to JSON string for CSV compatibility
            if row.get("properties"):
                row["properties"] = json.dumps(row["properties"])
            # Format date as ISO string
            if row.get("ip_appointed_date"):
                row["ip_appointed_date"] = row["ip_appointed_date"].isoformat()
            writer.writerow(row)
        return output.getvalue().encode("utf-8")


def main():
    """Entry point for enrichment service."""
    pass


if __name__ == "__main__":
    main()
