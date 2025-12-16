"""Tests for configuration validation."""

import pytest
from pydantic import ValidationError

from src.utils.config import Settings


class TestConfigValidation:
    """Test configuration validation."""

    def test_missing_required_fields(self, monkeypatch):
        """Test that missing required fields raise errors."""
        # Clear all environment variables that would provide values
        monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("RESEND_API_KEY", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("GMAIL_CREDENTIALS_JSON", raising=False)
        monkeypatch.delenv("CLIENT_EMAIL", raising=False)

        with pytest.raises(ValidationError) as exc_info:
            Settings(
                _env_file=None,  # Prevent reading .env file
            )

        error_str = str(exc_info.value).lower()
        assert "companies_house_api_key" in error_str
        assert "anthropic_api_key" in error_str

    def test_empty_api_key_rejected(self):
        """Test that empty API keys are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(
                companies_house_api_key="",
                anthropic_api_key="valid-key",
                resend_api_key="valid-key",
                database_url="postgresql://localhost/test",
                gmail_credentials_json='{"installed":{}}',
                client_email="test@example.com",
                _env_file=None,
            )

        assert "companies_house_api_key" in str(exc_info.value).lower()

    def test_invalid_database_url_rejected(self):
        """Test that non-PostgreSQL URLs are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(
                companies_house_api_key="valid-key",
                anthropic_api_key="valid-key",
                resend_api_key="valid-key",
                database_url="mysql://localhost/test",
                gmail_credentials_json='{"installed":{}}',
                client_email="test@example.com",
                _env_file=None,
            )

        assert "postgresql" in str(exc_info.value).lower()

    def test_invalid_email_rejected(self):
        """Test that invalid email addresses are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(
                companies_house_api_key="valid-key",
                anthropic_api_key="valid-key",
                resend_api_key="valid-key",
                database_url="postgresql://localhost/test",
                gmail_credentials_json='{"installed":{}}',
                client_email="not-an-email",
                _env_file=None,
            )

        assert "email" in str(exc_info.value).lower()

    def test_valid_config_loads(self):
        """Test that valid config loads successfully."""
        config = Settings(
            companies_house_api_key="valid-key",
            anthropic_api_key="valid-key",
            resend_api_key="valid-key",
            database_url="postgresql://localhost/test",
            gmail_credentials_json='{"installed":{}}',
            client_email="test@example.com",
            _env_file=None,
        )

        assert config.companies_house_api_key == "valid-key"
        assert config.client_email == "test@example.com"

    def test_whitespace_trimmed(self):
        """Test that whitespace is trimmed from values."""
        config = Settings(
            companies_house_api_key="  valid-key  ",
            anthropic_api_key="valid-key",
            resend_api_key="valid-key",
            database_url="  postgresql://localhost/test  ",
            gmail_credentials_json='{"installed":{}}',
            client_email="  test@example.com  ",
            _env_file=None,
        )

        assert config.companies_house_api_key == "valid-key"
        assert config.database_url == "postgresql://localhost/test"
        assert config.client_email == "test@example.com"

    def test_optional_fields_default_to_none(self):
        """Test that optional fields default to None."""
        config = Settings(
            companies_house_api_key="valid-key",
            anthropic_api_key="valid-key",
            resend_api_key="valid-key",
            database_url="postgresql://localhost/test",
            gmail_credentials_json='{"installed":{}}',
            client_email="test@example.com",
            _env_file=None,
        )

        assert config.gmail_token_json is None
        assert config.ccod_gov_uk_credentials is None
        assert config.resend_from_email is None

    def test_llm_model_has_default(self):
        """Test that LLM model has a sensible default."""
        config = Settings(
            companies_house_api_key="valid-key",
            anthropic_api_key="valid-key",
            resend_api_key="valid-key",
            database_url="postgresql://localhost/test",
            gmail_credentials_json='{"installed":{}}',
            client_email="test@example.com",
            _env_file=None,
        )

        assert config.llm_model == "claude-sonnet-4-20250514"

    def test_llm_model_can_be_overridden(self):
        """Test that LLM model can be set via parameter."""
        config = Settings(
            companies_house_api_key="valid-key",
            anthropic_api_key="valid-key",
            resend_api_key="valid-key",
            database_url="postgresql://localhost/test",
            gmail_credentials_json='{"installed":{}}',
            client_email="test@example.com",
            llm_model="claude-3-opus-20240229",
            _env_file=None,
        )

        assert config.llm_model == "claude-3-opus-20240229"
