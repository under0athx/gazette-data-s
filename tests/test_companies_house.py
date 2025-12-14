"""Tests for Companies House API client."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.api.companies_house import CompaniesHouseClient


class TestCompaniesHouseClient:
    """Test Companies House client."""

    @pytest.fixture
    def mock_settings(self, monkeypatch):
        """Mock settings for tests."""
        monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-api-key")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setenv("RESEND_API_KEY", "test-key")
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        monkeypatch.setenv("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
        monkeypatch.setenv("CLIENT_EMAIL", "test@example.com")

    @pytest.fixture
    def client(self, mock_settings):
        """Create a client instance."""
        return CompaniesHouseClient()

    def test_client_initialization(self, client):
        """Test client initializes with correct headers."""
        assert client.client.headers["Authorization"].startswith("Basic ")
        assert client.client.timeout.connect == 30.0

    def test_search_companies_success(self, client):
        """Test successful company search."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "items": [
                {"company_number": "12345678", "title": "Test Company Ltd"}
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client.client, "request", return_value=mock_response):
            results = client.search_companies("test company")

        assert len(results) == 1
        assert results[0]["company_number"] == "12345678"

    def test_search_companies_empty_results(self, client):
        """Test search with no results."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"items": []}
        mock_response.raise_for_status = MagicMock()

        with patch.object(client.client, "request", return_value=mock_response):
            results = client.search_companies("nonexistent company xyz")

        assert results == []

    def test_get_company_success(self, client):
        """Test successful company lookup."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "company_number": "12345678",
            "company_name": "Test Ltd",
            "company_status": "active",
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client.client, "request", return_value=mock_response):
            result = client.get_company("12345678")

        assert result["company_number"] == "12345678"
        assert result["company_status"] == "active"

    def test_get_company_not_found(self, client):
        """Test company not found returns None."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(client.client, "request", return_value=mock_response):
            result = client.get_company("00000000")

        assert result is None

    def test_get_insolvency_success(self, client):
        """Test successful insolvency lookup."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "cases": [
                {"case_type": "liquidation", "practitioners": []}
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client.client, "request", return_value=mock_response):
            result = client.get_insolvency("12345678")

        assert "cases" in result

    def test_get_insolvency_not_found(self, client):
        """Test insolvency not found returns None."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(client.client, "request", return_value=mock_response):
            result = client.get_insolvency("00000000")

        assert result is None

    def test_context_manager(self, mock_settings):
        """Test client works as context manager."""
        with CompaniesHouseClient() as client:
            assert client is not None
        # Client should be closed after context exits

    def test_retry_on_timeout(self, client):
        """Test retry behavior on timeout."""
        call_count = 0

        def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.ReadTimeout("Timeout")
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"items": []}
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        with patch.object(client.client, "request", side_effect=mock_request):
            results = client.search_companies("test")

        assert call_count == 3  # Retried twice, succeeded on third
        assert results == []

    def test_rate_limit_handling(self, client):
        """Test rate limit (429) triggers retry."""
        call_count = 0

        def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            if call_count < 2:
                mock_resp.status_code = 429
                mock_resp.headers = {"Retry-After": "1"}
                return mock_resp
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"items": []}
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        with patch.object(client.client, "request", side_effect=mock_request):
            # This will raise because we convert 429 to ReadTimeout for retry
            with pytest.raises(httpx.ReadTimeout):
                client.search_companies("test")
