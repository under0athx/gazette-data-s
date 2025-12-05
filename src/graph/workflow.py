"""LangGraph workflow for company enrichment."""

from langgraph.graph import END, StateGraph

from src.graph.nodes import (
    agent_match,
    build_enriched_record,
    get_company_details,
    get_next_record,
    lookup_properties,
    search_companies_house,
    should_continue,
)
from src.graph.state import EnrichmentState


def create_enrichment_graph() -> StateGraph:
    """Create the enrichment workflow graph."""

    # Build the graph
    workflow = StateGraph(EnrichmentState)

    # Add nodes
    workflow.add_node("get_next_record", get_next_record)
    workflow.add_node("search_companies_house", search_companies_house)
    workflow.add_node("agent_match", agent_match)
    workflow.add_node("get_company_details", get_company_details)
    workflow.add_node("lookup_properties", lookup_properties)
    workflow.add_node("build_enriched_record", build_enriched_record)

    # Set entry point
    workflow.set_entry_point("get_next_record")

    # Add edges
    workflow.add_edge("get_next_record", "search_companies_house")
    workflow.add_edge("search_companies_house", "agent_match")
    workflow.add_edge("agent_match", "get_company_details")
    workflow.add_edge("get_company_details", "lookup_properties")
    workflow.add_edge("lookup_properties", "build_enriched_record")

    # Conditional edge to loop or end
    workflow.add_conditional_edges(
        "build_enriched_record",
        should_continue,
        {
            "continue": "get_next_record",
            "end": END,
        },
    )

    return workflow.compile()


# Compiled graph instance
enrichment_graph = create_enrichment_graph()
