import logging
from typing import Optional

import resend

from src.utils.config import settings

logger = logging.getLogger(__name__)


class ResendClient:
    """Client for sending emails via Resend.

    Configure the sender domain in your Resend dashboard and set
    RESEND_FROM_EMAIL in your environment (defaults to onboarding@resend.dev
    for testing).
    """

    def __init__(self, from_email: Optional[str] = None):
        resend.api_key = settings.resend_api_key
        # Default to Resend's test address; override in production via env var
        default_email = "onboarding@resend.dev"
        self.from_email = from_email or getattr(settings, "resend_from_email", default_email)

    def send_enriched_csv(self, csv_content: bytes, filename: str, subject: str) -> dict:
        """Send enriched CSV to client."""
        params = {
            "from": f"Distress Signal <{self.from_email}>",
            "to": [settings.client_email],
            "subject": subject,
            "html": """
                <p>Please find attached the latest enriched property data from The Gazette.</p>
                <p>This CSV contains companies with confirmed property ownership.</p>
            """,
            "attachments": [
                {
                    "filename": filename,
                    "content": list(csv_content),
                }
            ],
        }
        result = resend.Emails.send(params)
        logger.info("Sent email to %s: %s", settings.client_email, result.get("id"))
        return result
