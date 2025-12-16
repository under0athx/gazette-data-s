"""Tests for the EnrichmentService class."""

from datetime import date

from src.services.enrichment import EnrichmentService


class TestParseGazetteCSV:
    """Test CSV parsing functionality."""

    def setup_method(self):
        self.service = EnrichmentService()

    def test_parse_valid_csv(self):
        """Test parsing a valid CSV with all fields."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
Acme Ltd,Liquidation,2024-01-15,John Smith,Smith & Co
"""
        records = self.service.parse_gazette_csv(csv_content)

        assert len(records) == 1
        assert records[0].company_name == "Acme Ltd"
        assert records[0].insolvency_type == "Liquidation"
        assert records[0].notice_date == date(2024, 1, 15)
        assert records[0].ip_name == "John Smith"
        assert records[0].ip_firm == "Smith & Co"

    def test_parse_multiple_rows(self):
        """Test parsing multiple records."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
Acme Ltd,Liquidation,2024-01-15,John Smith,Smith & Co
Beta Corp,Administration,2024-01-16,Jane Doe,Doe Partners
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert len(records) == 2

    def test_skip_empty_company_name(self):
        """Test that rows with empty company names are skipped."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
,Liquidation,2024-01-15,John Smith,Smith & Co
Beta Corp,Administration,2024-01-16,Jane Doe,Doe Partners
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert len(records) == 1
        assert records[0].company_name == "Beta Corp"

    def test_strip_whitespace_from_company_name(self):
        """Test that company names are trimmed."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
  Acme Ltd  ,Liquidation,2024-01-15,John Smith,Smith & Co
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert records[0].company_name == "Acme Ltd"

    def test_optional_fields_can_be_empty(self):
        """Test that optional fields can be missing."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
Acme Ltd,,,,
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert len(records) == 1
        assert records[0].company_name == "Acme Ltd"
        assert records[0].insolvency_type is None
        assert records[0].notice_date is None

    def test_invalid_date_becomes_none(self):
        """Test that invalid dates are set to None."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
Acme Ltd,Liquidation,not-a-date,John Smith,Smith & Co
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert records[0].notice_date is None


class TestToCSV:
    """Test CSV output functionality."""

    def setup_method(self):
        self.service = EnrichmentService()

    def test_to_csv_basic(self):
        """Test basic CSV generation."""
        from src.db.models import EnrichedCompany

        enriched = [
            EnrichedCompany(
                company_name="Acme Ltd",
                company_number="12345678",
                company_status="active",
                insolvency_type="Liquidation",
                ip_name="John Smith",
                property_count=2,
                properties=[
                    {"title": "DN123", "address": "123 Main St"},
                    {"title": "DN456", "address": "456 High St"},
                ],
                match_confidence=95.0,
            )
        ]

        csv_bytes = self.service.to_csv(enriched)
        csv_text = csv_bytes.decode("utf-8")

        assert "company_name" in csv_text
        assert "Acme Ltd" in csv_text
        assert "12345678" in csv_text
        # Properties should be JSON-serialized
        assert "DN123" in csv_text
        assert "DN456" in csv_text

    def test_to_csv_properties_as_json(self):
        """Test that properties are serialized as JSON string."""
        from src.db.models import EnrichedCompany

        enriched = [
            EnrichedCompany(
                company_name="Test Ltd",
                property_count=1,
                properties=[{"title": "ABC123", "address": "Test Address"}],
            )
        ]

        csv_bytes = self.service.to_csv(enriched)
        csv_text = csv_bytes.decode("utf-8")

        # Verify JSON structure is in the output
        lines = csv_text.strip().split("\n")
        assert len(lines) == 2  # header + 1 row

        # The properties column should contain valid JSON
        data_line = lines[1]
        # Properties field should be JSON-parseable
        assert '"title": "ABC123"' in data_line or '"title":"ABC123"' in data_line

    def test_to_csv_date_formatting(self):
        """Test that dates are formatted as ISO strings."""
        from src.db.models import EnrichedCompany

        enriched = [
            EnrichedCompany(
                company_name="Test Ltd",
                ip_appointed_date=date(2024, 1, 15),
                property_count=0,
                properties=[],
            )
        ]

        csv_bytes = self.service.to_csv(enriched)
        csv_text = csv_bytes.decode("utf-8")

        assert "2024-01-15" in csv_text

    def test_to_csv_empty_list(self):
        """Test CSV generation with empty list."""
        csv_bytes = self.service.to_csv([])
        csv_text = csv_bytes.decode("utf-8")

        # Should only have header
        lines = csv_text.strip().split("\n")
        assert len(lines) == 1
        assert "company_name" in lines[0]
