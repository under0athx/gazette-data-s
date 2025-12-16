from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Required environment variables:
    - COMPANIES_HOUSE_API_KEY: API key from Companies House
    - ANTHROPIC_API_KEY: Anthropic API key for Claude
    - RESEND_API_KEY: Resend API key for sending emails
    - DATABASE_URL: PostgreSQL connection URL
    - GMAIL_CREDENTIALS_JSON: OAuth client credentials JSON from Google Cloud Console
    - CLIENT_EMAIL: Email address to send enriched reports to

    Optional:
    - GMAIL_TOKEN_JSON: Cached OAuth tokens (auto-generated after first auth)
    - CCOD_GOV_UK_CREDENTIALS: Credentials for CCOD data download

    Gmail OAuth Setup:
    1. Create a project in Google Cloud Console
    2. Enable Gmail API
    3. Create OAuth 2.0 credentials (Desktop app)
    4. Download credentials JSON and set as GMAIL_CREDENTIALS_JSON
    5. On first run, a browser window will open for OAuth consent
    6. Tokens are cached in GMAIL_TOKEN_JSON for subsequent runs

    Note: OAuth flow requires local browser access. For production/server
    deployment, use a service account with domain-wide delegation instead.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # API Keys - required for operation
    companies_house_api_key: str
    anthropic_api_key: str
    resend_api_key: str

    # Database - required
    database_url: str

    # Gmail - credentials required, token is optional (cached after first auth)
    gmail_credentials_json: str
    gmail_token_json: Optional[str] = None

    # Config
    client_email: str
    ccod_gov_uk_credentials: Optional[str] = None
    resend_from_email: Optional[str] = None  # Defaults to onboarding@resend.dev for testing

    # LLM Configuration
    llm_model: str = "claude-sonnet-4-5"  # Default model for company matching

    @field_validator("companies_house_api_key", "anthropic_api_key", "resend_api_key")
    @classmethod
    def validate_api_keys(cls, v: str, info) -> str:
        """Validate that API keys are not empty."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} must be set and non-empty")
        return v.strip()

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        """Validate database URL format."""
        if not v or not v.strip():
            raise ValueError("DATABASE_URL must be set")
        v = v.strip()
        if not v.startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must be a PostgreSQL connection URL")
        return v

    @field_validator("client_email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate email format."""
        if not v or not v.strip():
            raise ValueError("CLIENT_EMAIL must be set")
        if "@" not in v:
            raise ValueError("CLIENT_EMAIL must be a valid email address")
        return v.strip()


settings = Settings()
