import base64
import json
import logging
import os
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from src.utils.config import settings

logger = logging.getLogger(__name__)

# Need modify scope to mark emails as read
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


class TokenStorage:
    """Encrypted file-based token storage.

    Stores OAuth tokens in an encrypted file rather than environment variables
    to prevent leakage in logs, error messages, or process listings.

    The encryption key should be stored securely (e.g., in env var or secret manager).
    """

    def __init__(self, token_path: Path, encryption_key: Optional[str] = None):
        self.token_path = token_path
        # Use provided key or generate from env var
        key = encryption_key or os.environ.get("GMAIL_TOKEN_ENCRYPTION_KEY")
        if key:
            # Ensure key is valid Fernet format (32 url-safe base64-encoded bytes)
            self.fernet = Fernet(key.encode() if isinstance(key, str) else key)
        else:
            self.fernet = None
            logger.warning(
                "No encryption key provided. Tokens will be stored unencrypted. "
                "Set GMAIL_TOKEN_ENCRYPTION_KEY for encrypted storage."
            )

    def load(self) -> Optional[dict]:
        """Load and decrypt token from file."""
        if not self.token_path.exists():
            return None

        try:
            data = self.token_path.read_bytes()
            if self.fernet:
                data = self.fernet.decrypt(data)
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            logger.warning("Failed to load token from %s: %s", self.token_path, e)
            return None

    def save(self, token_data: dict) -> None:
        """Encrypt and save token to file."""
        try:
            data = json.dumps(token_data).encode("utf-8")
            if self.fernet:
                data = self.fernet.encrypt(data)
            # Ensure directory exists
            self.token_path.parent.mkdir(parents=True, exist_ok=True)
            # Write with restrictive permissions
            self.token_path.write_bytes(data)
            os.chmod(self.token_path, 0o600)
            logger.info("Saved token to %s", self.token_path)
        except Exception as e:
            logger.error("Failed to save token to %s: %s", self.token_path, e)

    @staticmethod
    def generate_key() -> str:
        """Generate a new encryption key."""
        return Fernet.generate_key().decode("utf-8")


class GmailClient:
    """Client for Gmail API to watch for Gazette emails.

    Supports multiple token storage backends:
    1. Encrypted file storage (recommended for production)
    2. Environment variable (legacy, for backwards compatibility)

    For encrypted file storage, set GMAIL_TOKEN_ENCRYPTION_KEY env var
    and optionally GMAIL_TOKEN_PATH (defaults to ~/.distress-signal/gmail_token).
    """

    def __init__(self, token_storage: Optional[TokenStorage] = None):
        self._token_storage = token_storage or self._default_token_storage()
        self.creds = self._get_credentials()
        self.service = build("gmail", "v1", credentials=self.creds)

    def _default_token_storage(self) -> Optional[TokenStorage]:
        """Create default token storage if encryption key is available."""
        if os.environ.get("GMAIL_TOKEN_ENCRYPTION_KEY"):
            token_path = Path(
                os.environ.get("GMAIL_TOKEN_PATH", "~/.distress-signal/gmail_token")
            ).expanduser()
            return TokenStorage(token_path)
        return None

    def _get_credentials(self) -> Credentials:
        """Get or refresh Gmail credentials."""
        creds = None

        # Try encrypted file storage first
        if self._token_storage:
            token_data = self._token_storage.load()
            if token_data:
                creds = Credentials.from_authorized_user_info(token_data, SCOPES)

        # Fall back to env var for backwards compatibility
        if not creds and settings.gmail_token_json:
            creds = Credentials.from_authorized_user_info(
                json.loads(settings.gmail_token_json), SCOPES
            )

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                # Persist refreshed token
                self._save_credentials(creds)
            else:
                credentials_data = json.loads(settings.gmail_credentials_json)
                flow = InstalledAppFlow.from_client_config(credentials_data, SCOPES)
                creds = flow.run_local_server(port=0)
                # Persist new token
                self._save_credentials(creds)

        return creds

    def _save_credentials(self, creds: Credentials) -> None:
        """Save credentials to storage."""
        if self._token_storage:
            token_data = {
                "token": creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": creds.scopes,
            }
            self._token_storage.save(token_data)
        else:
            logger.info(
                "Token refreshed. To persist, set GMAIL_TOKEN_ENCRYPTION_KEY "
                "or update GMAIL_TOKEN_JSON env var."
            )

    def search_messages(self, query: str, max_results: int = 10) -> list[dict]:
        """Search for messages matching query."""
        results = (
            self.service.users()
            .messages()
            .list(userId="me", q=query, maxResults=max_results)
            .execute()
        )
        return results.get("messages", [])

    def get_message(self, message_id: str) -> dict:
        """Get full message by ID."""
        return self.service.users().messages().get(userId="me", id=message_id).execute()

    def get_attachment(self, message_id: str, attachment_id: str) -> bytes:
        """Get attachment data."""
        attachment = (
            self.service.users()
            .messages()
            .attachments()
            .get(userId="me", messageId=message_id, id=attachment_id)
            .execute()
        )
        return base64.urlsafe_b64decode(attachment["data"])

    def find_gazette_emails(self) -> list[dict]:
        """Find unread Gazette emails with CSV attachments."""
        query = "from:thegazette.co.uk is:unread has:attachment"
        return self.search_messages(query)

    def extract_csv_attachment(self, message_id: str) -> Optional[bytes]:
        """Extract CSV attachment from a message."""
        message = self.get_message(message_id)
        payload = message.get("payload", {})

        for part in payload.get("parts", []):
            filename = part.get("filename", "")
            if filename.endswith(".csv"):
                attachment_id = part.get("body", {}).get("attachmentId")
                if attachment_id:
                    return self.get_attachment(message_id, attachment_id)

        return None

    def mark_as_read(self, message_id: str) -> None:
        """Mark a message as read by removing UNREAD label."""
        try:
            self.service.users().messages().modify(
                userId="me",
                id=message_id,
                body={"removeLabelIds": ["UNREAD"]},
            ).execute()
            logger.info("Marked message %s as read", message_id)
        except Exception as e:
            logger.error("Failed to mark message %s as read: %s", message_id, e)
