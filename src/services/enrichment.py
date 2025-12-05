"""Company enrichment service - enriches Gazette records with CH and CCOD data."""

import csv
import io
from typing import Optional

from src.api.claude import ClaudeClient
from src.api.companies_house import CompaniesHouseClient
from src.db.connection import get_connection
from src.db.models import EnrichedCompany, GazetteRecord
from src.utils.name_matching import names_match, normalize_company_name


class EnrichmentService:
    """Enriches Gazette records with Companies House and CCOD data."""

    def __init__(self):
        self.ch_client = CompaniesHouseClient()
        self.claude_client = ClaudeClient()

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

    def find_company_number(self, gazette_name: str) -> tuple[Optional[str], float]:
        """Find company number for a Gazette company name.

        Returns (company_number, confidence).
        """
        # Search Companies House
        candidates = self.ch_client.search_companies(gazette_name)

        if not candidates:
            return None, 0.0

        # Check for exact match
        normalized_gazette = normalize_company_name(gazette_name)
        for candidate in candidates:
            if names_match(gazette_name, candidate.get("title", "")):
                return candidate.get("company_number"), 100.0

        # Use Claude for fuzzy matching
        result = self.claude_client.select_best_match(gazette_name, candidates)
        selected_index = result.get("selected_index", -1)
        confidence = result.get("confidence", 0)

        if selected_index >= 0 and selected_index < len(candidates):
            return candidates[selected_index].get("company_number"), confidence

        return None, 0.0

    def lookup_properties(self, company_number: Optional[str], company_name: str) -> list[dict]:
        """Look up properties in CCOD database."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Try by company number first
                if company_number:
                    cur.execute(
                        """
                        SELECT title_number, property_address
                        FROM ccod_properties
                        WHERE company_number = %s
                        """,
                        (company_number,),
                    )
                    results = cur.fetchall()
                    if results:
                        return [
                            {"title": r["title_number"], "address": r["property_address"]}
                            for r in results
                        ]

                # Fallback to fuzzy name match
                cur.execute(
                    """
                    SELECT title_number, property_address
                    FROM ccod_properties
                    WHERE similarity(company_name, %s) > 0.8
                    ORDER BY similarity(company_name, %s) DESC
                    LIMIT 100
                    """,
                    (company_name, company_name),
                )
                results = cur.fetchall()
                return [
                    {"title": r["title_number"], "address": r["property_address"]}
                    for r in results
                ]

    def enrich_record(self, record: GazetteRecord) -> EnrichedCompany:
        """Enrich a single Gazette record."""
        company_number, confidence = self.find_company_number(record.company_name)

        # Get company details and insolvency info
        company_status = None
        ip_name = record.ip_name
        ip_appointed_date = None

        if company_number:
            company = self.ch_client.get_company(company_number)
            if company:
                company_status = company.get("company_status")

            insolvency = self.ch_client.get_insolvency(company_number)
            if insolvency and insolvency.get("cases"):
                latest_case = insolvency["cases"][0]
                practitioners = latest_case.get("practitioners", [])
                if practitioners:
                    ip_name = practitioners[0].get("name")
                    ip_appointed_date = practitioners[0].get("appointed_on")

        # Look up properties
        properties = self.lookup_properties(company_number, record.company_name)

        return EnrichedCompany(
            company_name=record.company_name,
            company_number=company_number,
            company_status=company_status,
            insolvency_type=record.insolvency_type,
            ip_name=ip_name,
            ip_appointed_date=ip_appointed_date,
            property_count=len(properties),
            properties=properties,
            match_confidence=confidence,
        )

    def enrich_all(self, records: list[GazetteRecord]) -> list[EnrichedCompany]:
        """Enrich all records, filtering for those with properties."""
        enriched = []
        for record in records:
            result = self.enrich_record(record)
            if result.property_count > 0:
                enriched.append(result)
        return enriched

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
    # This would be called with CSV data from email watcher
    pass


if __name__ == "__main__":
    main()
