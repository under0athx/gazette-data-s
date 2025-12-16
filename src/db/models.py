from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class CCODProperty(BaseModel):
    """Land Registry CCOD property record."""

    title_number: str
    property_address: Optional[str] = None
    company_name: str
    company_number: Optional[str] = None
    tenure: Optional[str] = None
    date_proprietor_added: Optional[date] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class GazetteRecord(BaseModel):
    """Input record from Gazette CSV."""

    company_name: str
    insolvency_type: Optional[str] = None
    notice_date: Optional[date] = None
    ip_name: Optional[str] = None
    ip_firm: Optional[str] = None


class EnrichedCompany(BaseModel):
    """Output record with enriched data."""

    company_name: str
    company_number: Optional[str] = None
    company_status: Optional[str] = None
    insolvency_type: Optional[str] = None
    ip_name: Optional[str] = None
    ip_appointed_date: Optional[date] = None
    property_count: int = 0
    properties: list[dict] = []
    match_confidence: Optional[float] = None
