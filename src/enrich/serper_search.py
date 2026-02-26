from __future__ import annotations

import re
import time
from urllib.parse import urlparse

import click
import httpx
from tqdm import tqdm

from src.config import SERPER_API_KEY
from src.models.firm import Firm, EnrichmentSource

SERPER_URL = "https://google.serper.dev/search"

# Words too generic to use for domain matching (appear in many unrelated domains)
_GENERIC_WORDS = frozenset({
    "tax", "taxes", "cpa", "accounting", "financial", "services", "service",
    "group", "inc", "llc", "pc", "pllc", "plc", "pa", "ea",
    "associates", "associate", "company", "corp", "corporation",
    "business", "enterprise", "enterprises", "professional", "pros",
    "the", "and", "of", "by", "a", "an", "for", "in", "at",
    "first", "premier", "global", "star", "best", "pro", "express",
    "national", "american", "united", "liberty", "budget",
})


def _firm_name_tokens(firm: Firm) -> set[str]:
    """Extract meaningful name tokens from a firm (for matching against domains)."""
    name = (firm.dba or firm.firm_name).lower()
    tokens = set(re.split(r"\W+", name)) - _GENERIC_WORDS - {""}
    # Only keep tokens with 3+ chars (skip initials, abbreviations)
    return {t for t in tokens if len(t) >= 3}


def _domain_matches_firm(domain: str, firm: Firm) -> bool:
    """Check if the domain plausibly belongs to this firm.

    A firm's own website almost always contains a recognizable part of
    their name in the domain. E.g. "Edwards CPA Group" -> edwardscpagroup.com.
    """
    tokens = _firm_name_tokens(firm)
    domain_lower = domain.lower()
    return any(token in domain_lower for token in tokens)


def _build_query(firm: Firm) -> str:
    """Build a search query from the firm's DBA, city, and state."""
    name = firm.dba or firm.firm_name
    parts = [f'"{name}"']
    if firm.city:
        parts.append(f'"{firm.city}"')
    if firm.state:
        parts.append(firm.state)
    return " ".join(parts)


def _extract_website(results: dict, firm: Firm) -> str | None:
    """Extract the firm's website from Serper results.

    Strategy:
    1. Check Google Knowledge Graph (most reliable -- Google's own entity data)
    2. Check organic results, but ONLY accept if the domain contains a
       meaningful part of the firm's name. This filters out directories,
       people-search, and random mentions.
    """
    # 1. Knowledge Graph -- Google's curated entity data
    kg = results.get("knowledgeGraph", {})
    kg_website = kg.get("website")
    if kg_website:
        return kg_website

    # 2. Organic results -- require domain to contain firm name tokens
    for result in results.get("organic", [])[:5]:
        link = result.get("link", "")
        if not link:
            continue
        domain = urlparse(link).netloc.lower().removeprefix("www.")
        if _domain_matches_firm(domain, firm):
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

            website = _extract_website(resp.json(), firm)
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
