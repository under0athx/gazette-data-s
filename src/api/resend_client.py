import resend

from src.utils.config import settings


class ResendClient:
    """Client for sending emails via Resend."""

    def __init__(self):
        resend.api_key = settings.resend_api_key

    def send_enriched_csv(self, csv_content: bytes, filename: str, subject: str) -> dict:
        """Send enriched CSV to client."""
        params = {
            "from": "Distress Signal <alerts@yourdomain.com>",
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
        return resend.Emails.send(params)
