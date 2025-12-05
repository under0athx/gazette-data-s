"""Company enrichment service - enriches Gazette records with CH and CCOD data."""

import csv
import io

from src.db.models import EnrichedCompany, GazetteRecord
from src.graph.state import EnrichmentState
from src.graph.workflow import enrichment_graph


class EnrichmentService:
    """Enriches Gazette records using LangGraph workflow."""

    def parse_gazette_csv(self, csv_bytes: bytes) -> list[GazetteRecord]:
        """Parse Gazette CSV into records."""
        content = csv_bytes.decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        records = []
        for row in reader:
            records.append(
                GazetteRecord(
                    company_name=row.get("company_name", ""),
                    insolvency_type=row.get("insolvency_type"),
                    notice_date=row.get("notice_date"),
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
            writer.writerow(record.model_dump())
        return output.getvalue().encode("utf-8")


def main():
    """Entry point for enrichment service."""
    pass


if __name__ == "__main__":
    main()
