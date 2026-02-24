from __future__ import annotations

from datetime import date
from enum import Enum
from pydantic import BaseModel, Field


class EnrichmentSource(str, Enum):
    IRS_MASTER = "IRS Master"
    IRS_PARTNER = "IRS Partner"
    IRS_CONTACT = "IRS Contact"
    PTIN = "PTIN"
    GOOGLE_PLACES = "Google Places"
    WEBSITE = "Website"
    APOLLO = "Apollo"
    EMAIL_GUESS = "Email Guess"


class ReturnVolume(BaseModel):
    year: int
    individual_returns: int = 0  # 1040
    business_returns: int = 0    # 1120, 1065, etc.
    total_returns: int = 0

    @property
    def individual_pct(self) -> float:
        if self.total_returns == 0:
            return 0.0
        return round(self.individual_returns / self.total_returns * 100, 1)


class Contact(BaseModel):
    name: str = ""
    title: str = ""
    email: str = ""
    email_verified: bool = False
    phone: str = ""
    source: EnrichmentSource = EnrichmentSource.IRS_MASTER
    linkedin_url: str = ""


class Firm(BaseModel):
    # Core identity (from IRS Master Extract)
    efin: str
    firm_name: str
    dba: str = ""
    street_address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    phone: str = ""
    website: str = ""
    email: str = ""

    # Return volumes (3-year rolling window from Master Extract)
    return_volumes: list[ReturnVolume] = Field(default_factory=list)

    # Computed
    yoy_growth_pct: float | None = None

    # Contacts (from Partner Extract, PTIN, Apollo, Website)
    contacts: list[Contact] = Field(default_factory=list)

    # PTIN cross-reference
    preparer_count: int = 0

    # Google Places
    google_rating: float | None = None
    google_review_count: int | None = None

    # Flags
    flagged_chain: bool = False
    flagged_chain_match: str = ""  # which chain name it matched
    no_website: bool = False

    # Enrichment tracking
    enrichment_sources: list[EnrichmentSource] = Field(default_factory=list)
    is_enriched: bool = False

    # Return type breakdown (latest year)
    individual_return_pct: float | None = None
    business_return_pct: float | None = None

    # Metadata
    last_updated: date | None = None

    def compute_yoy_growth(self) -> None:
        """Compute year-over-year growth from the two most recent years."""
        if len(self.return_volumes) < 2:
            self.yoy_growth_pct = None
            return
        sorted_vols = sorted(self.return_volumes, key=lambda v: v.year, reverse=True)
        current = sorted_vols[0].total_returns
        previous = sorted_vols[1].total_returns
        if previous == 0:
            self.yoy_growth_pct = None
        else:
            self.yoy_growth_pct = round((current - previous) / previous * 100, 1)

    def compute_return_breakdown(self) -> None:
        """Compute individual/business return percentages from latest year."""
        if not self.return_volumes:
            return
        latest = max(self.return_volumes, key=lambda v: v.year)
        if latest.total_returns > 0:
            self.individual_return_pct = latest.individual_pct
            self.business_return_pct = round(100 - latest.individual_pct, 1)

    @property
    def latest_returns(self) -> int:
        if not self.return_volumes:
            return 0
        return max(self.return_volumes, key=lambda v: v.year).total_returns

    @property
    def key_contact(self) -> Contact | None:
        """Return the highest-priority contact (owner/partner first)."""
        priority_titles = ["owner", "managing partner", "partner", "president",
                           "ceo", "principal", "founder", "director"]
        for title_keyword in priority_titles:
            for c in self.contacts:
                if title_keyword in c.title.lower():
                    return c
        return self.contacts[0] if self.contacts else None
