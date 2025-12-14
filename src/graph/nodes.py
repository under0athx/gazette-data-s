"""Node functions for the enrichment graph."""

import json
import logging

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

from src.api.companies_house import CompaniesHouseClient
from src.db.connection import get_connection
from src.db.models import EnrichedCompany
from src.graph.state import EnrichmentState
from src.utils.config import settings
from src.utils.name_matching import names_match

logger = logging.getLogger(__name__)


def _get_ch_client() -> CompaniesHouseClient:
    """Get Companies House client (lazy initialization)."""
    return CompaniesHouseClient()


def _get_llm() -> ChatAnthropic:
    """Get LLM client (lazy initialization)."""
    return ChatAnthropic(model="claude-sonnet-4-20250514", api_key=settings.anthropic_api_key)


def get_next_record(state: EnrichmentState) -> EnrichmentState:
    """Get the next record to process."""
    if state.current_index >= len(state.gazette_records):
        return state

    state.current_record = state.gazette_records[state.current_index]
    state.company_number = None
    state.company_details = None
    state.insolvency_details = None
    state.properties = []
    state.match_confidence = 0.0
    state.search_candidates = []

    return state


def search_companies_house(state: EnrichmentState) -> EnrichmentState:
    """Search Companies House for the company."""
    if not state.current_record:
        return state

    company_name = state.current_record.company_name

    with _get_ch_client() as ch_client:
        candidates = ch_client.search_companies(company_name)

    if not candidates:
        state.match_confidence = 0.0
        return state

    # Store candidates in state to avoid re-fetching in agent_match
    state.search_candidates = candidates

    # Check for exact match first
    for candidate in candidates:
        if names_match(company_name, candidate.get("title", "")):
            state.company_number = candidate.get("company_number")
            state.match_confidence = 100.0
            return state

    # Store candidates for agent matching
    system_msg = "You are helping match company names between The Gazette and Companies House."
    user_prompt = f"""Match this Gazette company to the best Companies House result.

Gazette name: {company_name}

Candidates:
{_format_candidates(candidates)}

Respond with JSON: {{"index": <0-based index or -1 if no match>, "confidence": <0-100>}}"""

    state.messages = [
        SystemMessage(content=system_msg),
        HumanMessage(content=user_prompt),
    ]

    return state


def agent_match(state: EnrichmentState) -> EnrichmentState:
    """Use LLM to match company when exact match fails."""
    if state.match_confidence == 100.0 or not state.messages:
        return state

    llm = _get_llm()
    response = llm.invoke(state.messages)
    state.messages = state.messages + [response]

    # Parse response - use candidates from state to avoid re-fetching
    content = response.content
    try:
        # Extract JSON from response
        if "{" in content:
            json_str = content[content.index("{"):content.rindex("}") + 1]
            result = json.loads(json_str)

            if result.get("index", -1) >= 0:
                candidates = state.search_candidates or []
                idx = result["index"]
                if 0 <= idx < len(candidates):
                    state.company_number = candidates[idx].get("company_number")
                    state.match_confidence = result.get("confidence", 0)
        else:
            logger.warning(
                "LLM response missing JSON for company '%s'. Response: %s",
                state.current_record.company_name if state.current_record else "unknown",
                content[:500],  # Truncate for logging
            )
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(
            "Failed to parse LLM response for company '%s': %s. Response content: %s",
            state.current_record.company_name if state.current_record else "unknown",
            e,
            content[:500],  # Include truncated response for debugging
        )

    return state


def get_company_details(state: EnrichmentState) -> EnrichmentState:
    """Fetch company details from Companies House."""
    if not state.company_number:
        return state

    with _get_ch_client() as ch_client:
        state.company_details = ch_client.get_company(state.company_number)
        state.insolvency_details = ch_client.get_insolvency(state.company_number)

    return state


def lookup_properties(state: EnrichmentState) -> EnrichmentState:
    """Look up properties in CCOD database."""
    if not state.current_record:
        return state

    company_name = state.current_record.company_name

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Try by company number first
            if state.company_number:
                query = """
                    SELECT title_number, property_address
                    FROM ccod_properties WHERE company_number = %s
                """
                cur.execute(query, (state.company_number,))
                results = cur.fetchall()
                if results:
                    state.properties = [
                        {"title": r["title_number"], "address": r["property_address"]}
                        for r in results
                    ]
                    return state

            # Fallback to fuzzy name match
            cur.execute(
                """
                SELECT title_number, property_address
                FROM ccod_properties
                WHERE similarity(company_name, %s) > 0.8
                ORDER BY similarity(company_name, %s) DESC
                LIMIT 100
                """,
                (company_name, company_name),
            )
            results = cur.fetchall()
            state.properties = [
                {"title": r["title_number"], "address": r["property_address"]}
                for r in results
            ]

    return state


def build_enriched_record(state: EnrichmentState) -> EnrichmentState:
    """Build the enriched company record."""
    if not state.current_record:
        return state

    record = state.current_record

    # Extract IP info
    ip_name = record.ip_name
    ip_appointed_date = None

    if state.insolvency_details and state.insolvency_details.get("cases"):
        latest_case = state.insolvency_details["cases"][0]
        practitioners = latest_case.get("practitioners", [])
        if practitioners:
            ip_name = practitioners[0].get("name")
            ip_appointed_date = practitioners[0].get("appointed_on")

    company_status = None
    if state.company_details:
        company_status = state.company_details.get("company_status")

    enriched = EnrichedCompany(
        company_name=record.company_name,
        company_number=state.company_number,
        company_status=company_status,
        insolvency_type=record.insolvency_type,
        ip_name=ip_name,
        ip_appointed_date=ip_appointed_date,
        property_count=len(state.properties),
        properties=state.properties,
        match_confidence=state.match_confidence,
    )

    # Only keep if has properties
    if enriched.property_count > 0:
        state.enriched_companies.append(enriched)
    elif state.match_confidence < 80:
        state.failed_records.append({
            "company_name": record.company_name,
            "reason": "low_confidence_match",
            "confidence": state.match_confidence,
        })

    # Move to next record
    state.current_index += 1

    return state


def _format_candidates(candidates: list[dict]) -> str:
    """Format candidates for LLM prompt."""
    lines = []
    for i, c in enumerate(candidates):
        lines.append(
            f"{i}. {c.get('title', 'N/A')} "
            f"(Number: {c.get('company_number', 'N/A')}, "
            f"Status: {c.get('company_status', 'N/A')})"
        )
    return "\n".join(lines)


def should_continue(state: EnrichmentState) -> str:
    """Determine if we should continue processing or end."""
    if state.current_index >= len(state.gazette_records):
        return "end"
    return "continue"
