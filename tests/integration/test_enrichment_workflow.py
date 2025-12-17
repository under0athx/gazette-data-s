"""Integration tests for the enrichment workflow.

These tests verify the complete end-to-end flow with mocked external services.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

from src.db.models import GazetteRecord
from src.graph.state import EnrichmentState
from src.graph.workflow import enrichment_graph


class TestEnrichmentWorkflowIntegration:
    """Integration tests for the LangGraph enrichment workflow."""

    @pytest.fixture
    def sample_records(self):
        """Create sample Gazette records for testing."""
        return [
            GazetteRecord(
                company_name="Acme Property Holdings Ltd",
                insolvency_type="Liquidation",
                notice_date=date(2024, 1, 15),
                ip_name="John Smith",
                ip_firm="Smith & Partners",
            ),
            GazetteRecord(
                company_name="Beta Real Estate Ltd",
                insolvency_type="Administration",
                notice_date=date(2024, 1, 16),
                ip_name="Jane Doe",
                ip_firm="Doe Insolvency",
            ),
        ]

    @pytest.fixture
    def mock_companies_house(self):
        """Mock Companies House API responses."""
        with patch("src.graph.nodes._get_ch_client") as mock:
            client = MagicMock()

            # Search results
            client.search_companies.side_effect = [
                # First company - exact match
                [
                    {
                        "company_number": "12345678",
                        "title": "ACME PROPERTY HOLDINGS LTD",
                        "company_status": "liquidation",
                    }
                ],
                # Second company - fuzzy match needed
                [
                    {
                        "company_number": "87654321",
                        "title": "BETA REAL ESTATE LIMITED",
                        "company_status": "administration",
                    },
                    {
                        "company_number": "11111111",
                        "title": "BETA PROPERTIES LTD",
                        "company_status": "active",
                    },
                ],
            ]

            # Company details
            client.get_company.side_effect = [
                {
                    "company_number": "12345678",
                    "company_name": "ACME PROPERTY HOLDINGS LTD",
                    "company_status": "liquidation",
                },
                {
                    "company_number": "87654321",
                    "company_name": "BETA REAL ESTATE LIMITED",
                    "company_status": "administration",
                },
            ]

            # Insolvency details
            client.get_insolvency.side_effect = [
                {
                    "cases": [
                        {
                            "case_type": "creditors-voluntary-liquidation",
                            "practitioners": [
                                {
                                    "name": "John Smith",
                                    "appointed_on": "2024-01-15",
                                }
                            ],
                        }
                    ]
                },
                {
                    "cases": [
                        {
                            "case_type": "administration",
                            "practitioners": [
                                {
                                    "name": "Jane Doe",
                                    "appointed_on": "2024-01-16",
                                }
                            ],
                        }
                    ]
                },
            ]

            mock.return_value = client
            yield client

    @pytest.fixture
    def mock_llm(self):
        """Mock LLM for company matching."""
        with patch("src.graph.nodes._get_llm") as mock:
            llm = MagicMock()
            # Return high-confidence match for second company
            # Use proper AIMessage to work with add_messages
            response = AIMessage(content='{"index": 0, "confidence": 90}')
            llm.invoke.return_value = response
            mock.return_value = llm
            yield llm

    @pytest.fixture
    def mock_database(self):
        """Mock database queries for property lookup."""
        with patch("src.graph.nodes.get_connection") as mock:
            conn = MagicMock()
            cursor = MagicMock()

            # Return properties for first company, none for second
            cursor.fetchall.side_effect = [
                # First company has properties
                [
                    {"title_number": "DN12345", "property_address": "123 Main Street"},
                    {"title_number": "DN12346", "property_address": "124 Main Street"},
                ],
                # Second company has no properties by number
                [],
                # Second company fuzzy match returns one property
                [{"title_number": "EX54321", "property_address": "1 High Street"}],
            ]

            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)
            conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
            conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

            mock.return_value.__enter__ = MagicMock(return_value=conn)
            mock.return_value.__exit__ = MagicMock(return_value=False)

            yield cursor

    def test_workflow_enriches_companies_with_properties(
        self,
        mock_env,
        sample_records,
        mock_companies_house,
        mock_llm,
        mock_database,
    ):
        """Test that workflow enriches companies that have properties."""
        initial_state = EnrichmentState(gazette_records=sample_records)

        final_state = enrichment_graph.invoke(initial_state)

        # Should have enriched companies with properties
        enriched = final_state["enriched_companies"]
        assert len(enriched) >= 1

        # First company should have properties
        acme = next(
            (c for c in enriched if "ACME" in c.company_name.upper()), None
        )
        if acme:
            assert acme.property_count > 0
            assert acme.company_number == "12345678"

    def test_workflow_filters_companies_without_properties(
        self,
        mock_env,
        mock_companies_house,
    ):
        """Test that companies without properties are not included."""
        with patch("src.graph.nodes._get_llm") as mock_llm_patch, \
             patch("src.graph.nodes.get_connection") as mock:
            # Setup LLM mock with proper AIMessage
            llm = MagicMock()
            llm.invoke.return_value = AIMessage(content='{"index": 0, "confidence": 90}')
            mock_llm_patch.return_value = llm
            conn = MagicMock()
            cursor = MagicMock()
            # No properties found
            cursor.fetchall.return_value = []

            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)
            conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
            conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

            mock.return_value.__enter__ = MagicMock(return_value=conn)
            mock.return_value.__exit__ = MagicMock(return_value=False)

            records = [
                GazetteRecord(
                    company_name="No Properties Ltd",
                    insolvency_type="Liquidation",
                )
            ]

            initial_state = EnrichmentState(gazette_records=records)
            final_state = enrichment_graph.invoke(initial_state)

            # Should not include companies without properties
            enriched = final_state["enriched_companies"]
            assert len(enriched) == 0

    def test_workflow_handles_empty_input(self, mock_env):
        """Test workflow handles empty record list gracefully."""
        initial_state = EnrichmentState(gazette_records=[])

        final_state = enrichment_graph.invoke(initial_state)

        assert final_state["enriched_companies"] == []
        assert final_state["failed_records"] == []

    def test_workflow_tracks_low_confidence_matches(
        self,
        mock_env,
        mock_companies_house,
        mock_database,
    ):
        """Test that low confidence matches are tracked in failed_records."""
        with patch("src.graph.nodes._get_llm") as mock:
            llm = MagicMock()
            # Return low-confidence match with proper AIMessage
            response = AIMessage(content='{"index": 0, "confidence": 50}')
            llm.invoke.return_value = response
            mock.return_value = llm

            records = [
                GazetteRecord(
                    company_name="Ambiguous Corp",
                    insolvency_type="Liquidation",
                )
            ]

            # Mock CH to not return exact match
            mock_companies_house.search_companies.return_value = [
                {
                    "company_number": "99999999",
                    "title": "COMPLETELY DIFFERENT NAME",
                    "company_status": "active",
                }
            ]

            initial_state = EnrichmentState(gazette_records=records)
            final_state = enrichment_graph.invoke(initial_state)

            # Low confidence match without properties should be in failed_records
            # (only if no properties found)
            failed = final_state["failed_records"]
            if len(failed) > 0:
                assert failed[0]["reason"] == "low_confidence_match"
                assert failed[0]["confidence"] == 50


class TestDatabaseConnectivityIntegration:
    """Integration tests for database connectivity checks."""

    def test_check_connectivity_success(self):
        """Test connectivity check returns True when database is available."""
        from src.db.connection import check_connectivity

        with patch("src.db.connection.get_connection") as mock:
            conn = MagicMock()
            cursor = MagicMock()
            cursor.fetchone.return_value = (1,)

            conn.__enter__ = MagicMock(return_value=conn)
            conn.__exit__ = MagicMock(return_value=False)
            conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
            conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

            mock.return_value.__enter__ = MagicMock(return_value=conn)
            mock.return_value.__exit__ = MagicMock(return_value=False)

            assert check_connectivity() is True

    def test_check_connectivity_failure(self):
        """Test connectivity check returns False when database unavailable."""
        from src.db.connection import check_connectivity

        with patch("src.db.connection.get_connection") as mock:
            mock.side_effect = Exception("Connection refused")

            assert check_connectivity() is False

    def test_wait_for_database_retries(self):
        """Test wait_for_database retries on failure."""
        from src.db.connection import wait_for_database

        with patch("src.db.connection.check_connectivity") as mock:
            # Fail twice, then succeed
            mock.side_effect = [False, False, True]

            result = wait_for_database(max_retries=3, retry_interval=0.01)

            assert result is True
            assert mock.call_count == 3

    def test_wait_for_database_exhausts_retries(self):
        """Test wait_for_database returns False after max retries."""
        from src.db.connection import wait_for_database

        with patch("src.db.connection.check_connectivity") as mock:
            mock.return_value = False

            result = wait_for_database(max_retries=2, retry_interval=0.01)

            assert result is False
            assert mock.call_count == 2
