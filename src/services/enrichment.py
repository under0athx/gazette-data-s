"""Company enrichment service - enriches Gazette records with CH and CCOD data."""

import csv
import io
import json
import logging
from datetime import date
from typing import Optional

from dateutil import parser as dateutil_parser

from src.db.models import EnrichedCompany, GazetteRecord
from src.graph.state import EnrichmentState
from src.graph.workflow import enrichment_graph

logger = logging.getLogger(__name__)


def _parse_date(value: Optional[str]) -> Optional[date]:
    """Parse date from string using dateutil for robust parsing.

    Handles various formats including:
    - ISO: 2024-01-15
    - UK: 15/01/2024, 15-01-2024
    - US: 01/15/2024
    - Written: 15 January 2024, Jan 15, 2024
    - Compact: 20240115

    For ambiguous dates (like 01/02/2024), assumes UK format (day first).

    Edge cases handled:
    - Empty/whitespace strings
    - Future dates (flagged as warning)
    - Very old dates (before 1900, flagged as warning)
    - Non-date strings that dateutil might misparse
    """
    if not value or not value.strip():
        return None

    cleaned = value.strip()

    # Reject obvious non-dates early
    if cleaned.lower() in ("n/a", "na", "none", "-", "tbc", "tbd", "unknown"):
        return None

    try:
        # Use dateutil with dayfirst=True for UK date format preference
        parsed = dateutil_parser.parse(cleaned, dayfirst=True, fuzzy=False)
        result = parsed.date()

        # Sanity check: flag suspicious dates but still return them
        today = date.today()
        if result.year < 1900:
            logger.warning("Suspiciously old date '%s' parsed as %s", value, result)
        elif result > today:
            logger.warning("Future date '%s' parsed as %s", value, result)

        return result
    except (ValueError, TypeError, OverflowError) as e:
        logger.warning("Could not parse date '%s': %s", value, e)
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
