"""Email watcher service - polls Gmail for Gazette emails."""

import time
from datetime import datetime

from src.api.gmail import GmailClient
from src.api.resend_client import ResendClient
from src.services.enrichment import EnrichmentService


class EmailWatcher:
    """Watches for Gazette emails and triggers enrichment."""

    def __init__(self):
        self.gmail = GmailClient()
        self.enrichment = EnrichmentService()
        self.resend = ResendClient()

    def process_gazette_email(self, message_id: str) -> bool:
        """Process a single Gazette email."""
        csv_data = self.gmail.extract_csv_attachment(message_id)
        if not csv_data:
            print(f"No CSV attachment found in message {message_id}")
            return False

        # Parse and enrich
        records = self.enrichment.parse_gazette_csv(csv_data)
        print(f"Parsed {len(records)} records from Gazette CSV")

        enriched = self.enrichment.enrich_all(records)
        print(f"Found {len(enriched)} companies with properties")

        if enriched:
            # Generate output CSV
            output_csv = self.enrichment.to_csv(enriched)
            filename = f"enriched-gazette-{datetime.now().strftime('%Y%m%d')}.csv"

            # Send to client
            self.resend.send_enriched_csv(
                output_csv,
                filename,
                f"Distress Signal: {len(enriched)} Companies with Properties",
            )
            print(f"Sent enriched CSV to client: {filename}")

        return True

    def poll(self):
        """Poll for new Gazette emails."""
        messages = self.gmail.find_gazette_emails()

        for message in messages:
            message_id = message["id"]
            print(f"Processing Gazette email: {message_id}")
            self.process_gazette_email(message_id)


def main():
    """Entry point for email watcher service."""
    watcher = EmailWatcher()
    poll_interval = 300  # 5 minutes

    print("Starting email watcher...")
    while True:
        try:
            watcher.poll()
        except Exception as e:
            print(f"Error polling: {e}")

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
