from __future__ import annotations

import time

import click
import httpx
from tqdm import tqdm

from src.config import APOLLO_API_KEY
from src.models.firm import Firm, Contact, EnrichmentSource

APOLLO_SEARCH_URL = "https://api.apollo.io/v1/mixed_people/search"
APOLLO_ENRICH_URL = "https://api.apollo.io/api/v1/people/match"


def _search_people_at_company(
    firm_name: str,
    titles: list[str],
    api_key: str,
) -> list[dict]:
    """Search Apollo for people at a company with specific titles."""
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
    }

    payload = {
        "api_key": api_key,
        "q_organization_name": firm_name,
        "person_titles": titles,
        "page": 1,
        "per_page": 5,
    }

    try:
        resp = httpx.post(APOLLO_SEARCH_URL, json=payload, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("people", [])
        elif resp.status_code == 429:
            click.echo("  Apollo rate limit hit, pausing...")
            time.sleep(60)
            return []
        else:
            return []
    except (httpx.HTTPError, httpx.TimeoutException):
        return []


def enrich_with_apollo(firms: list[Firm], settings: dict) -> list[Firm]:
    """Enrich firms with high-level staff contacts from Apollo.io."""
    if not APOLLO_API_KEY:
        click.echo("  No APOLLO_API_KEY set, skipping Apollo enrichment.")
        click.echo("  Set APOLLO_API_KEY in .env to enable.")
        return firms

    enrich_settings = settings.get("enrich", {})
    target_titles = enrich_settings.get("apollo_titles", [
        "owner", "managing partner", "partner", "director",
        "president", "ceo", "principal", "founder",
    ])

    # Only enrich firms that don't already have high-value contacts
    candidates = []
    high_value_titles = {"owner", "president", "ceo", "principal", "founder",
                         "managing partner", "partner", "director"}

    for firm in firms:
        has_high_value = any(
            any(t in c.title.lower() for t in high_value_titles)
            for c in firm.contacts
        )
        if not has_high_value:
            candidates.append(firm)

    click.echo(f"  {len(candidates)} firms need Apollo enrichment (no high-level contacts yet)")

    enriched = 0
    for firm in tqdm(candidates, desc="  Apollo lookups", unit=" firms"):
        people = _search_people_at_company(firm.firm_name, target_titles, APOLLO_API_KEY)

        for person in people:
            name = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
            if not name:
                continue

            existing_names = {c.name.lower() for c in firm.contacts}
            if name.lower() in existing_names:
                continue

            contact = Contact(
                name=name,
                title=person.get("title", ""),
                email=person.get("email", ""),
                email_verified=bool(person.get("email")),
                phone=person.get("phone_number", ""),
                source=EnrichmentSource.APOLLO,
                linkedin_url=person.get("linkedin_url", ""),
            )
            firm.contacts.append(contact)
            enriched += 1

        if people and EnrichmentSource.APOLLO not in firm.enrichment_sources:
            firm.enrichment_sources.append(EnrichmentSource.APOLLO)

        # Respect rate limits (free tier: ~50 requests/min)
        time.sleep(1.2)

    click.echo(f"  Added {enriched} contacts from Apollo.io")
    return firms
