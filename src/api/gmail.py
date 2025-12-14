import base64
import json
import logging
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from src.utils.config import settings

logger = logging.getLogger(__name__)

# Need modify scope to mark emails as read
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


class GmailClient:
    """Client for Gmail API to watch for Gazette emails."""

    def __init__(self):
        self.creds = self._get_credentials()
        self.service = build("gmail", "v1", credentials=self.creds)

    def _get_credentials(self) -> Credentials:
        """Get or refresh Gmail credentials."""
        creds = None
        token_data = settings.gmail_token_json

        if token_data:
            creds = Credentials.from_authorized_user_info(json.loads(token_data), SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                credentials_data = json.loads(settings.gmail_credentials_json)
                flow = InstalledAppFlow.from_client_config(credentials_data, SCOPES)
                creds = flow.run_local_server(port=0)

        return creds

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
