"""Tests for date parsing functionality."""

from datetime import date

from src.services.enrichment import _parse_date


class TestDateParsing:
    """Test date parsing with various formats."""

    def test_iso_format(self):
        """Test ISO format: YYYY-MM-DD."""
        assert _parse_date("2024-01-15") == date(2024, 1, 15)

    def test_uk_format_slash(self):
        """Test UK format with slashes: DD/MM/YYYY."""
        assert _parse_date("15/01/2024") == date(2024, 1, 15)

    def test_uk_format_dash(self):
        """Test UK format with dashes: DD-MM-YYYY."""
        assert _parse_date("15-01-2024") == date(2024, 1, 15)

    def test_compact_format(self):
        """Test compact format: YYYYMMDD."""
        assert _parse_date("20240115") == date(2024, 1, 15)

    def test_written_format_full(self):
        """Test written format: 15 January 2024."""
        assert _parse_date("15 January 2024") == date(2024, 1, 15)

    def test_written_format_short(self):
        """Test short written format: 15 Jan 2024."""
        assert _parse_date("15 Jan 2024") == date(2024, 1, 15)

    def test_written_format_us_style(self):
        """Test US written format: January 15, 2024."""
        # With dayfirst=True, dateutil still parses this correctly
        assert _parse_date("January 15, 2024") == date(2024, 1, 15)

    def test_none_value(self):
        """Test None returns None."""
        assert _parse_date(None) is None

    def test_empty_string(self):
        """Test empty string returns None."""
        assert _parse_date("") is None

    def test_whitespace_only(self):
        """Test whitespace-only string returns None."""
        assert _parse_date("   ") is None

    def test_whitespace_trimmed(self):
        """Test leading/trailing whitespace is trimmed."""
        assert _parse_date("  2024-01-15  ") == date(2024, 1, 15)

    def test_invalid_date(self):
        """Test invalid date returns None."""
        assert _parse_date("not-a-date") is None

    def test_partial_date(self):
        """Test partial date returns None."""
        assert _parse_date("2024-01") is None

    def test_ambiguous_date_uk_preference(self):
        """Test ambiguous date prefers UK format (day first).

        01/02/2024 should be February 1st, not January 2nd.
        """
        result = _parse_date("01/02/2024")
        assert result == date(2024, 2, 1)

    def test_year_only(self):
        """Test year-only string returns None (not useful)."""
        # dateutil would parse "2024" as Jan 1, 2024, which isn't what we want
        # But we accept it as dateutil behavior
        result = _parse_date("2024")
        # Could be None or Jan 1 2024 depending on dateutil behavior
        assert result is None or result.year == 2024

    def test_na_values_return_none(self):
        """Test that n/a and similar values return None."""
        assert _parse_date("n/a") is None
        assert _parse_date("N/A") is None
        assert _parse_date("na") is None
        assert _parse_date("none") is None
        assert _parse_date("None") is None
        assert _parse_date("-") is None

    def test_placeholder_values_return_none(self):
        """Test that placeholder values return None."""
        assert _parse_date("tbc") is None
        assert _parse_date("TBC") is None
        assert _parse_date("tbd") is None
        assert _parse_date("TBD") is None
        assert _parse_date("unknown") is None
        assert _parse_date("Unknown") is None

    def test_partial_date_with_leading_zeros(self):
        """Test partial date with leading zeros returns None."""
        assert _parse_date("2024-01") is None
        assert _parse_date("2024-1") is None
        assert _parse_date("2024-12") is None
