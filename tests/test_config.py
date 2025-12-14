"""Tests for configuration validation."""


import pytest
from pydantic import ValidationError


class TestConfigValidation:
    """Test configuration validation."""

    def test_missing_required_fields(self, monkeypatch):
        """Test that missing required fields raise errors."""
        # Clear all relevant env vars
        env_vars = [
            "COMPANIES_HOUSE_API_KEY",
            "ANTHROPIC_API_KEY",
            "RESEND_API_KEY",
            "DATABASE_URL",
            "GMAIL_CREDENTIALS_JSON",
            "CLIENT_EMAIL",
        ]
        for var in env_vars:
            monkeypatch.delenv(var, raising=False)

        # Import fresh to trigger validation
        from importlib import reload

        import src.utils.config

        with pytest.raises(ValidationError):
            reload(src.utils.config)

    def test_empty_api_key_rejected(self, monkeypatch):
        """Test that empty API keys are rejected."""
        monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "valid-key")
        monkeypatch.setenv("RESEND_API_KEY", "valid-key")
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        monkeypatch.setenv("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
        monkeypatch.setenv("CLIENT_EMAIL", "test@example.com")

        from importlib import reload

        import src.utils.config

        with pytest.raises(ValidationError) as exc_info:
            reload(src.utils.config)

        assert "companies_house_api_key" in str(exc_info.value).lower()

    def test_invalid_database_url_rejected(self, monkeypatch):
        """Test that non-PostgreSQL URLs are rejected."""
        monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "valid-key")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "valid-key")
        monkeypatch.setenv("RESEND_API_KEY", "valid-key")
        monkeypatch.setenv("DATABASE_URL", "mysql://localhost/test")
        monkeypatch.setenv("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
        monkeypatch.setenv("CLIENT_EMAIL", "test@example.com")

        from importlib import reload

        import src.utils.config

        with pytest.raises(ValidationError) as exc_info:
            reload(src.utils.config)

        assert "postgresql" in str(exc_info.value).lower()

    def test_invalid_email_rejected(self, monkeypatch):
        """Test that invalid email addresses are rejected."""
        monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "valid-key")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "valid-key")
        monkeypatch.setenv("RESEND_API_KEY", "valid-key")
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        monkeypatch.setenv("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
        monkeypatch.setenv("CLIENT_EMAIL", "not-an-email")

        from importlib import reload

        import src.utils.config

        with pytest.raises(ValidationError) as exc_info:
            reload(src.utils.config)

        assert "email" in str(exc_info.value).lower()

    def test_valid_config_loads(self, monkeypatch):
        """Test that valid config loads successfully."""
        monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "valid-key")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "valid-key")
        monkeypatch.setenv("RESEND_API_KEY", "valid-key")
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        monkeypatch.setenv("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
        monkeypatch.setenv("CLIENT_EMAIL", "test@example.com")

        from importlib import reload

        import src.utils.config

        config = reload(src.utils.config)
        assert config.settings.companies_house_api_key == "valid-key"
        assert config.settings.client_email == "test@example.com"

    def test_whitespace_trimmed(self, monkeypatch):
        """Test that whitespace is trimmed from values."""
        monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "  valid-key  ")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "valid-key")
        monkeypatch.setenv("RESEND_API_KEY", "valid-key")
        monkeypatch.setenv("DATABASE_URL", "  postgresql://localhost/test  ")
        monkeypatch.setenv("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
        monkeypatch.setenv("CLIENT_EMAIL", "  test@example.com  ")

        from importlib import reload

        import src.utils.config

        config = reload(src.utils.config)
        assert config.settings.companies_house_api_key == "valid-key"
        assert config.settings.database_url == "postgresql://localhost/test"
        assert config.settings.client_email == "test@example.com"

    def test_optional_fields_default_to_none(self, monkeypatch):
        """Test that optional fields default to None."""
        monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "valid-key")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "valid-key")
        monkeypatch.setenv("RESEND_API_KEY", "valid-key")
        monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
        monkeypatch.setenv("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
        monkeypatch.setenv("CLIENT_EMAIL", "test@example.com")
        monkeypatch.delenv("GMAIL_TOKEN_JSON", raising=False)
        monkeypatch.delenv("CCOD_GOV_UK_CREDENTIALS", raising=False)
        monkeypatch.delenv("RESEND_FROM_EMAIL", raising=False)

        from importlib import reload

        import src.utils.config

        config = reload(src.utils.config)
        assert config.settings.gmail_token_json is None
        assert config.settings.ccod_gov_uk_credentials is None
        assert config.settings.resend_from_email is None
