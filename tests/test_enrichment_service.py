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
        import csv as csv_module
        import io
        import json

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

        # Parse the CSV properly to extract the properties field
        reader = csv_module.DictReader(io.StringIO(csv_text))
        row = next(reader)

        # Properties field should be valid JSON
        properties = json.loads(row["properties"])
        assert len(properties) == 1
        assert properties[0]["title"] == "ABC123"

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

    def test_to_csv_injection_prevention(self):
        """Test that CSV injection characters are sanitized."""
        from src.db.models import EnrichedCompany

        enriched = [
            EnrichedCompany(
                company_name="=CMD|'/C calc'!A0",
                company_status="+1234567890",
                insolvency_type="-malicious",
                ip_name="@SUM(1+1)*cmd|' /C calc'!A0",
                property_count=0,
                properties=[],
            )
        ]

        csv_bytes = self.service.to_csv(enriched)
        csv_text = csv_bytes.decode("utf-8")

        # Values should be prefixed with single quote to prevent formula execution
        assert "'=CMD" in csv_text
        assert "'+1234567890" in csv_text
        assert "'-malicious" in csv_text
        assert "'@SUM" in csv_text

    def test_to_csv_normal_values_not_modified(self):
        """Test that normal values are not affected by sanitization."""
        from src.db.models import EnrichedCompany

        enriched = [
            EnrichedCompany(
                company_name="Acme Ltd",
                company_status="active",
                insolvency_type="Liquidation",
                ip_name="John Smith",
                property_count=0,
                properties=[],
            )
        ]

        csv_bytes = self.service.to_csv(enriched)
        csv_text = csv_bytes.decode("utf-8")

        # Normal values should not be prefixed
        assert "Acme Ltd" in csv_text
        assert "'Acme" not in csv_text

    def test_to_csv_none_values_handled(self):
        """Test that None values don't cause errors."""
        from src.db.models import EnrichedCompany

        enriched = [
            EnrichedCompany(
                company_name="Test Ltd",
                company_number=None,
                company_status=None,
                insolvency_type=None,
                ip_name=None,
                ip_appointed_date=None,
                property_count=0,
                properties=[],
                match_confidence=None,
            )
        ]

        csv_bytes = self.service.to_csv(enriched)
        csv_text = csv_bytes.decode("utf-8")

        assert "Test Ltd" in csv_text


class TestParseGazetteCSVEdgeCases:
    """Edge case tests for CSV parsing."""

    def setup_method(self):
        self.service = EnrichmentService()

    def test_unicode_company_names(self):
        """Test that Unicode characters in company names are handled."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
Caf\xc3\xa9 Holdings Ltd,Liquidation,2024-01-15,John Smith,Smith & Co
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert len(records) == 1
        assert records[0].company_name == "Café Holdings Ltd"

    def test_special_characters_in_fields(self):
        """Test that special characters like commas and quotes are handled."""
        csv_content = b'''company_name,insolvency_type,notice_date,ip_name,ip_firm
"Smith, Jones & Partners Ltd",Liquidation,2024-01-15,"O'Brien, John","O'Brien & Sons"
'''
        records = self.service.parse_gazette_csv(csv_content)
        assert len(records) == 1
        assert records[0].company_name == "Smith, Jones & Partners Ltd"
        assert records[0].ip_name == "O'Brien, John"

    def test_whitespace_only_optional_fields(self):
        """Test that whitespace-only optional fields become None."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
Acme Ltd,   ,2024-01-15,   ,
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert len(records) == 1
        assert records[0].insolvency_type is None
        assert records[0].ip_name is None
        assert records[0].ip_firm is None

    def test_empty_csv(self):
        """Test parsing an empty CSV (headers only)."""
        csv_content = b"""company_name,insolvency_type,notice_date,ip_name,ip_firm
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert len(records) == 0

    def test_missing_columns(self):
        """Test parsing CSV with missing optional columns."""
        csv_content = b"""company_name,insolvency_type
Acme Ltd,Liquidation
"""
        records = self.service.parse_gazette_csv(csv_content)
        assert len(records) == 1
        assert records[0].company_name == "Acme Ltd"
        assert records[0].insolvency_type == "Liquidation"
        assert records[0].notice_date is None
        assert records[0].ip_name is None
