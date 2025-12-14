"""Email watcher service - polls Gmail for Gazette emails."""

import logging
import time
from datetime import datetime

from src.api.gmail import GmailClient
from src.api.resend_client import ResendClient
from src.services.enrichment import EnrichmentService

logger = logging.getLogger(__name__)


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
            logger.warning("No CSV attachment found in message %s", message_id)
            # Mark as read anyway to avoid reprocessing
            self.gmail.mark_as_read(message_id)
            return False

        # Parse and enrich
        records = self.enrichment.parse_gazette_csv(csv_data)
        logger.info("Parsed %d records from Gazette CSV", len(records))

        enriched = self.enrichment.enrich_all(records)
        logger.info("Found %d companies with properties", len(enriched))

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
            logger.info("Sent enriched CSV to client: %s", filename)

        # Mark as read after successful processing
        self.gmail.mark_as_read(message_id)
        return True

    def poll(self):
        """Poll for new Gazette emails."""
        messages = self.gmail.find_gazette_emails()

        for message in messages:
            message_id = message["id"]
            logger.info("Processing Gazette email: %s", message_id)
            try:
                self.process_gazette_email(message_id)
            except Exception as e:
                logger.exception("Error processing message %s: %s", message_id, e)
                # Don't mark as read on error - will retry next poll


def main():
    """Entry point for email watcher service."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    watcher = EmailWatcher()
    poll_interval = 300  # 5 minutes

    logger.info("Starting email watcher...")
    while True:
        try:
            watcher.poll()
        except Exception as e:
            logger.exception("Error polling: %s", e)

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
