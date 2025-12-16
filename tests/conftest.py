"""Pytest configuration and fixtures.

Sets up environment variables before any test modules are imported
to prevent settings validation errors during collection.
"""

import os

# Set required environment variables BEFORE any imports that use settings
# This must happen at module level during pytest collection
os.environ.setdefault("COMPANIES_HOUSE_API_KEY", "test-api-key")
os.environ.setdefault("ANTHROPIC_API_KEY", "test-anthropic-key")
os.environ.setdefault("RESEND_API_KEY", "test-resend-key")
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/test")
os.environ.setdefault("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
os.environ.setdefault("CLIENT_EMAIL", "test@example.com")

import pytest


@pytest.fixture
def mock_env(monkeypatch):
    """Set up environment variables for tests.

    This fixture can be used to override default test environment variables
    for specific test cases.
    """
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-api-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-anthropic-key")
    monkeypatch.setenv("RESEND_API_KEY", "test-resend-key")
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test")
    monkeypatch.setenv("GMAIL_CREDENTIALS_JSON", '{"installed":{}}')
    monkeypatch.setenv("CLIENT_EMAIL", "test@example.com")
