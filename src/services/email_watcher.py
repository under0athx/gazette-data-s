"""Email watcher service - polls Gmail for Gazette emails."""

import logging
import signal
import sys
import threading
import time
from datetime import datetime

from src.api.gmail import GmailClient
from src.api.resend_client import ResendClient
from src.db.connection import close_pool, wait_for_database
from src.services.enrichment import EnrichmentService

logger = logging.getLogger(__name__)

# Graceful shutdown flag
_shutdown_event = threading.Event()


def _signal_handler(signum: int, frame) -> None:
    """Handle shutdown signals gracefully."""
    sig_name = signal.Signals(signum).name
    logger.info("Received %s signal, initiating graceful shutdown...", sig_name)
    _shutdown_event.set()


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

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # Check database connectivity before starting
    logger.info("Checking database connectivity...")
    if not wait_for_database():
        logger.error("Cannot start email watcher: database not available")
        sys.exit(1)

    watcher = EmailWatcher()
    poll_interval = 300  # 5 minutes

    logger.info("Starting email watcher...")
    try:
        while not _shutdown_event.is_set():
            try:
                watcher.poll()
            except Exception as e:
                logger.exception("Error polling: %s", e)

            # Use wait() instead of sleep() to respond quickly to shutdown
            _shutdown_event.wait(timeout=poll_interval)
    finally:
        logger.info("Shutting down email watcher...")
        close_pool()
        logger.info("Email watcher stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
