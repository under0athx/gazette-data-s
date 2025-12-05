from anthropic import Anthropic

from src.utils.config import settings


class ClaudeClient:
    """Client for Claude API - used for fuzzy company name matching."""

    def __init__(self):
        self.client = Anthropic(api_key=settings.anthropic_api_key)
        self.model = "claude-sonnet-4-20250514"

    def select_best_match(
        self, gazette_name: str, candidates: list[dict], context: dict | None = None
    ) -> dict:
        """Use Claude to select the best company match from candidates."""
        prompt = f"""Given a company name from The Gazette and a list of potential matches from Companies House, select the best match.

Gazette company name: {gazette_name}

Candidates:
{self._format_candidates(candidates)}

Additional context: {context or 'None'}

Respond with a JSON object containing:
- "selected_index": the 0-based index of the best match, or -1 if none are good matches
- "confidence": a score from 0 to 100
- "reasoning": brief explanation

Only respond with the JSON object, no other text."""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )

        import json
        return json.loads(response.content[0].text)

    def _format_candidates(self, candidates: list[dict]) -> str:
        """Format candidate companies for the prompt."""
        lines = []
        for i, c in enumerate(candidates):
            lines.append(
                f"{i}. {c.get('title', 'N/A')} "
                f"(Number: {c.get('company_number', 'N/A')}, "
                f"Status: {c.get('company_status', 'N/A')})"
            )
        return "\n".join(lines)
