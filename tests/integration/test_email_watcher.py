"""Integration tests for the email watcher service.

Tests graceful shutdown and email processing flow.
"""

import signal
import threading
import time
from unittest.mock import MagicMock, patch

import pytest


class TestEmailWatcherIntegration:
    """Integration tests for email watcher service."""

    def test_shutdown_event_is_set_on_sigterm(self):
        """Test that SIGTERM sets the shutdown event."""
        from src.services.email_watcher import _shutdown_event, _signal_handler

        # Clear any previous state
        _shutdown_event.clear()

        # Simulate signal handler
        _signal_handler(signal.SIGTERM, None)

        assert _shutdown_event.is_set()

        # Clean up
        _shutdown_event.clear()

    def test_shutdown_event_is_set_on_sigint(self):
        """Test that SIGINT sets the shutdown event."""
        from src.services.email_watcher import _shutdown_event, _signal_handler

        # Clear any previous state
        _shutdown_event.clear()

        # Simulate signal handler
        _signal_handler(signal.SIGINT, None)

        assert _shutdown_event.is_set()

        # Clean up
        _shutdown_event.clear()

    def test_email_watcher_processes_emails(self):
        """Test that email watcher processes Gazette emails."""
        from src.services.email_watcher import EmailWatcher

        with (
            patch("src.services.email_watcher.GmailClient") as mock_gmail_cls,
            patch("src.services.email_watcher.EnrichmentService") as mock_enrich_cls,
            patch("src.services.email_watcher.ResendClient") as mock_resend_cls,
        ):
            # Set up mocks
            mock_gmail = MagicMock()
            mock_gmail_cls.return_value = mock_gmail
            mock_gmail.find_gazette_emails.return_value = [{"id": "msg123"}]
            mock_gmail.extract_csv_attachment.return_value = b"company_name\nTest Ltd"

            mock_enrich = MagicMock()
            mock_enrich_cls.return_value = mock_enrich
            mock_enrich.parse_gazette_csv.return_value = [MagicMock(company_name="Test Ltd")]
            mock_enrich.enrich_all.return_value = [MagicMock(property_count=1)]
            mock_enrich.to_csv.return_value = b"enriched,data"

            mock_resend = MagicMock()
            mock_resend_cls.return_value = mock_resend

            watcher = EmailWatcher()
            watcher.poll()

            # Verify email was processed
            mock_gmail.find_gazette_emails.assert_called_once()
            mock_gmail.extract_csv_attachment.assert_called_once_with("msg123")
            mock_enrich.parse_gazette_csv.assert_called_once()
            mock_enrich.enrich_all.assert_called_once()
            mock_resend.send_enriched_csv.assert_called_once()
            mock_gmail.mark_as_read.assert_called_once_with("msg123")

    def test_email_watcher_handles_no_csv_attachment(self):
        """Test that email watcher handles emails without CSV attachments."""
        from src.services.email_watcher import EmailWatcher

        with (
            patch("src.services.email_watcher.GmailClient") as mock_gmail_cls,
            patch("src.services.email_watcher.EnrichmentService"),
            patch("src.services.email_watcher.ResendClient"),
        ):
            mock_gmail = MagicMock()
            mock_gmail_cls.return_value = mock_gmail
            mock_gmail.find_gazette_emails.return_value = [{"id": "msg456"}]
            mock_gmail.extract_csv_attachment.return_value = None  # No CSV

            watcher = EmailWatcher()
            result = watcher.process_gazette_email("msg456")

            assert result is False
            # Should still mark as read to avoid reprocessing
            mock_gmail.mark_as_read.assert_called_once_with("msg456")

    def test_email_watcher_handles_enrichment_with_no_results(self):
        """Test email watcher when enrichment returns no companies with properties."""
        from src.services.email_watcher import EmailWatcher

        with (
            patch("src.services.email_watcher.GmailClient") as mock_gmail_cls,
            patch("src.services.email_watcher.EnrichmentService") as mock_enrich_cls,
            patch("src.services.email_watcher.ResendClient") as mock_resend_cls,
        ):
            mock_gmail = MagicMock()
            mock_gmail_cls.return_value = mock_gmail
            mock_gmail.extract_csv_attachment.return_value = b"company_name\nNo Props Ltd"

            mock_enrich = MagicMock()
            mock_enrich_cls.return_value = mock_enrich
            mock_enrich.parse_gazette_csv.return_value = [MagicMock()]
            mock_enrich.enrich_all.return_value = []  # No enriched companies

            mock_resend = MagicMock()
            mock_resend_cls.return_value = mock_resend

            watcher = EmailWatcher()
            result = watcher.process_gazette_email("msg789")

            assert result is True
            # Should NOT send email when no enriched results
            mock_resend.send_enriched_csv.assert_not_called()
            # Should still mark as read
            mock_gmail.mark_as_read.assert_called_once_with("msg789")

    def test_email_watcher_handles_processing_error(self):
        """Test email watcher handles errors during processing."""
        from src.services.email_watcher import EmailWatcher

        with (
            patch("src.services.email_watcher.GmailClient") as mock_gmail_cls,
            patch("src.services.email_watcher.EnrichmentService") as mock_enrich_cls,
            patch("src.services.email_watcher.ResendClient"),
        ):
            mock_gmail = MagicMock()
            mock_gmail_cls.return_value = mock_gmail
            mock_gmail.find_gazette_emails.return_value = [{"id": "msg_err"}]
            mock_gmail.extract_csv_attachment.return_value = b"company_name\nTest"

            mock_enrich = MagicMock()
            mock_enrich_cls.return_value = mock_enrich
            mock_enrich.parse_gazette_csv.side_effect = ValueError("Parse error")

            watcher = EmailWatcher()
            watcher.poll()

            # Should NOT mark as read on error (will retry next poll)
            mock_gmail.mark_as_read.assert_not_called()


class TestGracefulShutdownIntegration:
    """Integration tests for graceful shutdown behavior."""

    def test_shutdown_event_wait_is_interruptible(self):
        """Test that shutdown event wait can be interrupted."""
        from src.services.email_watcher import _shutdown_event

        _shutdown_event.clear()

        def set_shutdown():
            time.sleep(0.1)
            _shutdown_event.set()

        # Start thread that will set shutdown after delay
        thread = threading.Thread(target=set_shutdown)
        thread.start()

        # Wait should be interrupted when shutdown is set
        start = time.time()
        _shutdown_event.wait(timeout=5.0)  # Would wait 5s without interrupt
        elapsed = time.time() - start

        assert elapsed < 1.0  # Should have been interrupted quickly
        thread.join()

        # Clean up
        _shutdown_event.clear()
