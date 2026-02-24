from __future__ import annotations

import csv
import sys

csv.field_size_limit(sys.maxsize)

import click
from tqdm import tqdm

from src.config import RAW_DIR
from src.models.firm import Firm, Contact, EnrichmentSource

# Partner Extract columns
COL_CUST_ID = "CUST-ID"
COL_FIRST = "FIRST-NAME"
COL_MIDDLE = "MIDDLE-NAME"
COL_LAST = "LAST-NAME"
COL_SUFFIX = "NAME-SUFFIX"
COL_TITLE = "TITLE"
COL_PROF_TYPE = "PROF-TYPE"

# High-value titles for acquisition prospecting
HIGH_VALUE_TITLES = {
    "owner", "president", "ceo", "principal", "founder",
    "managing partner", "managing member", "partner",
    "director", "vice president", "vp",
}


def _clean(val: str) -> str:
    return val.strip() if val else ""


def _build_name(first: str, middle: str, last: str, suffix: str) -> str:
    parts = [first, middle, last]
    name = " ".join(p for p in parts if p)
    if suffix:
        name += f" {suffix}"
    return name


def _normalize_title(title: str, prof_type: str) -> str:
    """Combine TITLE and PROF-TYPE into a readable title."""
    title = _clean(title)
    prof_type = _clean(prof_type)

    prof_labels = {
        "C": "Contact",
        "P": "Principal",
        "E": "Enrolled Agent",
        "T": "Tax Preparer",
        "A": "Attorney",
        "O": "Officer",
        "R": "Responsible Official",
    }

    if title and prof_type in prof_labels:
        return f"{title} ({prof_labels[prof_type]})"
    elif title:
        return title
    elif prof_type in prof_labels:
        return prof_labels[prof_type]
    return ""


def parse_partner_extract(firms: list[Firm]) -> list[Firm]:
    """Parse the Partner Extract and merge contacts into existing Firm objects."""
    partner_files = list(RAW_DIR.glob("FOIA-PARTNR-*.TXT"))
    if not partner_files:
        click.echo("  Warning: Partner Extract not found, skipping.")
        return firms

    partner_path = partner_files[0]

    # Build EFIN lookup for fast merging
    firm_map: dict[str, Firm] = {f.efin: f for f in firms}

    click.echo(f"  Reading {partner_path.name}...")
    added = 0
    with open(partner_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)

        for row in tqdm(reader, desc="  Parsing Partners", unit=" rows"):
            efin = _clean(row.get(COL_CUST_ID, ""))
            if efin not in firm_map:
                continue

            first = _clean(row.get(COL_FIRST, ""))
            last = _clean(row.get(COL_LAST, ""))
            if not (first or last):
                continue

            name = _build_name(
                first,
                _clean(row.get(COL_MIDDLE, "")),
                last,
                _clean(row.get(COL_SUFFIX, "")),
            )
            title = _normalize_title(
                row.get(COL_TITLE, ""),
                row.get(COL_PROF_TYPE, ""),
            )

            contact = Contact(
                name=name,
                title=title,
                source=EnrichmentSource.IRS_PARTNER,
            )

            firm = firm_map[efin]
            # Avoid duplicate contacts by name
            existing_names = {c.name.lower() for c in firm.contacts}
            if name.lower() not in existing_names:
                firm.contacts.append(contact)
                added += 1

            if EnrichmentSource.IRS_PARTNER not in firm.enrichment_sources:
                firm.enrichment_sources.append(EnrichmentSource.IRS_PARTNER)

    click.echo(f"  Added {added} partner contacts across firms")
    return firms
