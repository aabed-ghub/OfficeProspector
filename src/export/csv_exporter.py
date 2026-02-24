from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import click

from src.config import PROCESSED_DIR
from src.models.firm import Firm


def _firm_to_row(firm: Firm) -> dict:
    """Convert a Firm to a flat dict for CSV export."""
    key_contact = firm.key_contact
    return {
        "EFIN": firm.efin,
        "Firm Name": firm.firm_name,
        "DBA": firm.dba,
        "State": firm.state,
        "City": firm.city,
        "Address": firm.street_address,
        "ZIP": firm.zip_code,
        "Phone": firm.phone,
        "Website": firm.website,
        "Email": firm.email,
        "Latest Returns": firm.latest_returns,
        "Returns Y-1": firm.return_volumes[-2].total_returns if len(firm.return_volumes) >= 2 else "",
        "Returns Y-2": firm.return_volumes[-3].total_returns if len(firm.return_volumes) >= 3 else "",
        "YoY Growth %": firm.yoy_growth_pct if firm.yoy_growth_pct is not None else "",
        "Individual %": firm.individual_return_pct if firm.individual_return_pct is not None else "",
        "Business %": firm.business_return_pct if firm.business_return_pct is not None else "",
        "Google Rating": firm.google_rating or "",
        "Google Reviews": firm.google_review_count or "",
        "Key Contact": key_contact.name if key_contact else "",
        "Contact Title": key_contact.title if key_contact else "",
        "Contact Email": key_contact.email if key_contact else "",
        "Contact Phone": key_contact.phone if key_contact else "",
        "Preparer Count": firm.preparer_count or "",
        "Flagged Chain": "Yes" if firm.flagged_chain else "",
        "Chain Match": firm.flagged_chain_match,
        "No Website": "Yes" if firm.no_website else "",
        "Enriched": "Yes" if firm.is_enriched else "",
        "Enrichment Sources": ", ".join(s.value for s in firm.enrichment_sources),
        "Last Updated": firm.last_updated.isoformat() if firm.last_updated else "",
    }


def export_csv(firms: list[Firm], settings: dict) -> Path:
    """Export firms to a CSV file."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    csv_dir = Path(settings.get("export", {}).get("csv_dir", str(PROCESSED_DIR)))
    csv_dir.mkdir(parents=True, exist_ok=True)

    filename = f"office_prospector_{date.today().isoformat()}.csv"
    path = csv_dir / filename

    rows = [_firm_to_row(f) for f in firms]
    if not rows:
        click.echo("  No firms to export.")
        return path

    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    click.echo(f"  CSV exported: {path} ({len(rows)} firms)")
    return path
