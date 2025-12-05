"""State definitions for the enrichment graph."""

from typing import Annotated, Optional

from langgraph.graph.message import add_messages
from pydantic import BaseModel

from src.db.models import EnrichedCompany, GazetteRecord


class EnrichmentState(BaseModel):
    """State for the company enrichment workflow."""

    # Input
    gazette_records: list[GazetteRecord] = []

    # Processing state
    current_index: int = 0
    current_record: Optional[GazetteRecord] = None

    # Enrichment data
    company_number: Optional[str] = None
    company_details: Optional[dict] = None
    insolvency_details: Optional[dict] = None
    properties: list[dict] = []
    match_confidence: float = 0.0

    # Output
    enriched_companies: list[EnrichedCompany] = []
    failed_records: list[dict] = []

    # Agent messages for reasoning
    messages: Annotated[list, add_messages] = []

    class Config:
        arbitrary_types_allowed = True
