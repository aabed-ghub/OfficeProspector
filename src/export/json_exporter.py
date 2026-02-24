from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import click

from src.config import SITE_DIR
from src.models.firm import Firm


def _firm_to_json(firm: Firm) -> dict:
    """Convert a Firm to a JSON-ready dict for the dashboard."""
    key_contact = firm.key_contact

    # Sort return volumes by year descending
    sorted_vols = sorted(firm.return_volumes, key=lambda v: v.year, reverse=True)

    # Build all contacts list
    contacts = []
    for c in firm.contacts:
        contacts.append({
            "name": c.name,
            "title": c.title,
            "email": c.email,
            "emailVerified": c.email_verified,
            "phone": c.phone,
            "source": c.source.value,
            "linkedin": c.linkedin_url,
        })

    return {
        "efin": firm.efin,
        "firmName": firm.firm_name,
        "dba": firm.dba,
        "state": firm.state,
        "city": firm.city,
        "address": firm.street_address,
        "zip": firm.zip_code,
        "phone": firm.phone,
        "website": firm.website,
        "email": firm.email,

        # Return volumes
        "latestReturns": sorted_vols[0].total_returns if sorted_vols else 0,
        "returnsY1": sorted_vols[1].total_returns if len(sorted_vols) > 1 else None,
        "returnsY2": sorted_vols[2].total_returns if len(sorted_vols) > 2 else None,
        "yoyGrowth": firm.yoy_growth_pct,

        # Return type breakdown
        "individualPct": firm.individual_return_pct,
        "businessPct": firm.business_return_pct,

        # Google Places
        "googleRating": firm.google_rating,
        "googleReviews": firm.google_review_count,

        # Key contact
        "keyContact": key_contact.name if key_contact else "",
        "contactTitle": key_contact.title if key_contact else "",
        "contactEmail": key_contact.email if key_contact else "",
        "contactPhone": key_contact.phone if key_contact else "",

        # All contacts
        "contacts": contacts,

        # Metadata
        "preparerCount": firm.preparer_count,
        "flaggedChain": firm.flagged_chain,
        "chainMatch": firm.flagged_chain_match,
        "noWebsite": firm.no_website,
        "isEnriched": firm.is_enriched,
        "enrichmentSources": [s.value for s in firm.enrichment_sources],
        "lastUpdated": firm.last_updated.isoformat() if firm.last_updated else None,
    }


def export_json(firms: list[Firm], settings: dict) -> Path:
    """Export firms as JSON for the GitHub Pages dashboard."""
    SITE_DIR.mkdir(parents=True, exist_ok=True)

    json_filename = settings.get("export", {}).get("json_filename", "firms.json")
    path = SITE_DIR / json_filename

    # Build the full data payload
    data = {
        "generatedAt": date.today().isoformat(),
        "totalFirms": len(firms),
        "totalFlagged": sum(1 for f in firms if f.flagged_chain),
        "totalEnriched": sum(1 for f in firms if f.is_enriched),
        "totalNoWebsite": sum(1 for f in firms if f.no_website),
        "firms": [_firm_to_json(f) for f in firms],
    }

    # Compute state summary
    state_counts: dict[str, int] = {}
    for firm in firms:
        st = firm.state.upper()
        state_counts[st] = state_counts.get(st, 0) + 1
    data["stateSummary"] = dict(sorted(state_counts.items(), key=lambda x: -x[1]))

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))

    size_mb = path.stat().st_size / (1024 * 1024)
    click.echo(f"  JSON exported: {path} ({len(firms)} firms, {size_mb:.1f} MB)")
    return path
