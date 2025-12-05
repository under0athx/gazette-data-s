from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # API Keys
    companies_house_api_key: str = ""
    anthropic_api_key: str = ""
    resend_api_key: str = ""

    # Database
    database_url: str = ""

    # Gmail
    gmail_credentials_json: str = ""
    gmail_token_json: str = ""

    # Config
    client_email: str = ""
    ccod_gov_uk_credentials: str = ""


settings = Settings()
