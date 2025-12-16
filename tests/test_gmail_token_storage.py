"""Tests for Gmail token storage."""

import json
import os

import pytest
from cryptography.fernet import Fernet

from src.api.gmail import TokenStorage


class TestTokenStorage:
    """Test encrypted token storage."""

    @pytest.fixture
    def temp_token_path(self, tmp_path):
        """Create a temporary path for token storage."""
        return tmp_path / "test_token"

    @pytest.fixture
    def encryption_key(self):
        """Generate a valid encryption key."""
        return Fernet.generate_key().decode("utf-8")

    def test_generate_key(self):
        """Test key generation produces valid Fernet key."""
        key = TokenStorage.generate_key()
        # Should be valid for Fernet
        fernet = Fernet(key.encode())
        assert fernet is not None

    def test_save_and_load_encrypted(self, temp_token_path, encryption_key):
        """Test saving and loading encrypted tokens."""
        storage = TokenStorage(temp_token_path, encryption_key)

        token_data = {
            "token": "test-access-token",
            "refresh_token": "test-refresh-token",
            "client_id": "test-client-id",
        }

        storage.save(token_data)
        loaded = storage.load()

        assert loaded == token_data

    def test_encrypted_file_not_readable_as_json(self, temp_token_path, encryption_key):
        """Test that encrypted file is not plain JSON."""
        storage = TokenStorage(temp_token_path, encryption_key)

        token_data = {"token": "secret"}
        storage.save(token_data)

        # File should not be valid JSON
        raw_content = temp_token_path.read_bytes()
        with pytest.raises(json.JSONDecodeError):
            json.loads(raw_content)

    def test_save_and_load_unencrypted(self, temp_token_path):
        """Test saving and loading without encryption."""
        storage = TokenStorage(temp_token_path, encryption_key=None)

        token_data = {"token": "test-token"}
        storage.save(token_data)
        loaded = storage.load()

        assert loaded == token_data

    def test_unencrypted_file_is_json(self, temp_token_path):
        """Test that unencrypted file is plain JSON."""
        storage = TokenStorage(temp_token_path, encryption_key=None)

        token_data = {"token": "test-token"}
        storage.save(token_data)

        raw_content = temp_token_path.read_text()
        parsed = json.loads(raw_content)
        assert parsed == token_data

    def test_load_nonexistent_file(self, temp_token_path, encryption_key):
        """Test loading from nonexistent file returns None."""
        storage = TokenStorage(temp_token_path, encryption_key)
        assert storage.load() is None

    def test_load_corrupted_file(self, temp_token_path, encryption_key):
        """Test loading corrupted file returns None."""
        storage = TokenStorage(temp_token_path, encryption_key)

        # Write garbage data
        temp_token_path.write_bytes(b"corrupted data")

        assert storage.load() is None

    def test_file_permissions(self, temp_token_path, encryption_key):
        """Test saved file has restrictive permissions (Unix only)."""
        if os.name != "posix":
            pytest.skip("Permission test only runs on Unix")

        storage = TokenStorage(temp_token_path, encryption_key)
        storage.save({"token": "test"})

        mode = temp_token_path.stat().st_mode & 0o777
        assert mode == 0o600

    def test_creates_parent_directories(self, tmp_path, encryption_key):
        """Test that parent directories are created."""
        nested_path = tmp_path / "nested" / "dirs" / "token"
        storage = TokenStorage(nested_path, encryption_key)

        storage.save({"token": "test"})

        assert nested_path.exists()

    def test_env_key_fallback(self, temp_token_path, monkeypatch, encryption_key):
        """Test fallback to env var for encryption key."""
        monkeypatch.setenv("GMAIL_TOKEN_ENCRYPTION_KEY", encryption_key)

        storage = TokenStorage(temp_token_path)  # No key provided
        storage.save({"token": "test"})

        loaded = storage.load()
        assert loaded == {"token": "test"}

    def test_warning_without_encryption(self, temp_token_path, monkeypatch, caplog):
        """Test warning is logged when no encryption key available."""
        monkeypatch.delenv("GMAIL_TOKEN_ENCRYPTION_KEY", raising=False)

        with caplog.at_level("WARNING"):
            TokenStorage(temp_token_path)

        assert "encryption key" in caplog.text.lower()
