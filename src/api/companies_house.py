import base64
import logging
from typing import Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.utils.config import settings

logger = logging.getLogger(__name__)

BASE_URL = "https://api.company-information.service.gov.uk"

# Retry on network errors and rate limits (429)
RETRYABLE_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.ConnectTimeout,
)


def _is_rate_limited(response: httpx.Response) -> bool:
    """Check if response indicates rate limiting."""
    return response.status_code == 429


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

    @retry(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=lambda retry_state: logger.warning(
            "Retrying Companies House API call (attempt %d)", retry_state.attempt_number
        ),
    )
    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Make a request with retry logic."""
        response = self.client.request(method, path, **kwargs)

        # Handle rate limiting with retry
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            logger.warning("Rate limited, will retry after %d seconds", retry_after)
            raise httpx.ReadTimeout(f"Rate limited, retry after {retry_after}s")

        return response

    def search_companies(self, query: str, limit: int = 5) -> list[dict]:
        """Search for companies by name."""
        params = {"q": query, "items_per_page": limit}
        response = self._request("GET", "/search/companies", params=params)
        response.raise_for_status()
        return response.json().get("items", [])

    def get_company(self, company_number: str) -> Optional[dict]:
        """Get company details by number."""
        response = self._request("GET", f"/company/{company_number}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def get_insolvency(self, company_number: str) -> Optional[dict]:
        """Get insolvency details for a company."""
        response = self._request("GET", f"/company/{company_number}/insolvency")
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
