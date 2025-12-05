import base64
from typing import Optional

import httpx

from src.utils.config import settings

BASE_URL = "https://api.company-information.service.gov.uk"


class CompaniesHouseClient:
    """Client for Companies House API."""

    def __init__(self):
        api_key = settings.companies_house_api_key
        auth = base64.b64encode(f"{api_key}:".encode()).decode()
        self.client = httpx.Client(
            base_url=BASE_URL,
            headers={"Authorization": f"Basic {auth}"},
            timeout=30.0,
        )

    def search_companies(self, query: str, limit: int = 5) -> list[dict]:
        """Search for companies by name."""
        response = self.client.get("/search/companies", params={"q": query, "items_per_page": limit})
        response.raise_for_status()
        return response.json().get("items", [])

    def get_company(self, company_number: str) -> Optional[dict]:
        """Get company details by number."""
        response = self.client.get(f"/company/{company_number}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def get_insolvency(self, company_number: str) -> Optional[dict]:
        """Get insolvency details for a company."""
        response = self.client.get(f"/company/{company_number}/insolvency")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
