from __future__ import annotations

import time
from urllib.parse import urlparse

import click
import httpx
from tqdm import tqdm

from src.config import SERPER_API_KEY
from src.models.firm import Firm, EnrichmentSource

SERPER_URL = "https://google.serper.dev/search"

# Directories and aggregators -- not the firm's own website
_SKIP_DOMAINS = frozenset({
    "yelp.com", "yellowpages.com", "bbb.org", "facebook.com",
    "linkedin.com", "mapquest.com", "manta.com", "chamberofcommerce.com",
    "dnb.com", "buzzfile.com", "google.com", "apple.com", "bing.com",
    "superpages.com", "whitepages.com", "angi.com", "thumbtack.com",
    "birdeye.com", "expertise.com", "bark.com", "nextdoor.com",
    "instagram.com", "twitter.com", "x.com", "tiktok.com",
    "indeed.com", "glassdoor.com", "salary.com", "ziprecruiter.com",
    "sec.gov", "irs.gov", "state.gov",
})


def _build_query(firm: Firm) -> str:
    """Build a search query from the firm's DBA, city, and state."""
    name = firm.dba or firm.firm_name
    parts = [f'"{name}"']
    if firm.city:
        parts.append(f'"{firm.city}"')
    if firm.state:
        parts.append(firm.state)
    return " ".join(parts)


def _extract_website(results: dict) -> str | None:
    """Return the first organic result URL that isn't a directory/aggregator."""
    for result in results.get("organic", [])[:5]:
        link = result.get("link", "")
        if not link:
            continue
        domain = urlparse(link).netloc.lower().removeprefix("www.")
        if not any(skip in domain for skip in _SKIP_DOMAINS):
            return link
    return None


def enrich_with_serper(firms: list[Firm], settings: dict) -> list[Firm]:
    """Search Google (via Serper.dev) to find websites for firms that lack one."""
    if not SERPER_API_KEY:
        click.echo("  No SERPER_API_KEY set, skipping Google search enrichment.")
        click.echo("  Sign up at https://serper.dev/ for 2,500 free searches.")
        return firms

    candidates = [f for f in firms if not f.website]
    if not candidates:
        click.echo("  All firms already have websites, skipping Google search.")
        return firms

    click.echo(f"  {len(candidates)} firms need website lookup")

    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}
    found = 0

    for firm in tqdm(candidates, desc="  Google search", unit=" firms"):
        query = _build_query(firm)

        try:
            resp = httpx.post(
                SERPER_URL,
                headers=headers,
                json={"q": query, "num": 5},
                timeout=10,
            )

            if resp.status_code == 429:
                click.echo("\n  Rate limited, pausing 60s...")
                time.sleep(60)
                continue

            if resp.status_code != 200:
                continue

            website = _extract_website(resp.json())
            if website:
                firm.website = website
                found += 1
                if EnrichmentSource.GOOGLE_PLACES not in firm.enrichment_sources:
                    firm.enrichment_sources.append(EnrichmentSource.GOOGLE_PLACES)

        except (httpx.HTTPError, httpx.TimeoutException):
            continue

        # Polite delay (~3 requests/sec)
        time.sleep(0.3)

    click.echo(f"  Found websites for {found}/{len(candidates)} firms via Google search")
    return firms
