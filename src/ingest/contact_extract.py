from __future__ import annotations

import csv
import sys

csv.field_size_limit(sys.maxsize)

import click
from tqdm import tqdm

from src.config import RAW_DIR
from src.models.firm import Firm, Contact, EnrichmentSource

COL_CUST_ID = "CUST-ID"
COL_PRIMARY = "PRIMARY-FLAG"
COL_LAST = "LAST-NAME"
COL_FIRST = "FIRST-NAME"
COL_MIDDLE = "MIDDLE-NAME"
COL_PHONE = "PHONE"


def _clean(val: str) -> str:
    return val.strip() if val else ""


def _format_phone(raw: str | None) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if not raw:
        return ""
    return raw.replace("/", "-")


def parse_contact_extract(firms: list[Firm]) -> list[Firm]:
    """Parse the Contact Extract and merge primary contact info into Firms."""
    contact_files = list(RAW_DIR.glob("FOIA-CONTCT-*.TXT"))
    if not contact_files:
        click.echo("  Warning: Contact Extract not found, skipping.")
        return firms

    contact_path = contact_files[0]
    firm_map: dict[str, Firm] = {f.efin: f for f in firms}

    click.echo(f"  Reading {contact_path.name}...")
    updated = 0
    with open(contact_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)

        for row in tqdm(reader, desc="  Parsing Contacts", unit=" rows"):
            efin = _clean(row.get(COL_CUST_ID, ""))
            if efin not in firm_map:
                continue

            firm = firm_map[efin]

            first = _clean(row.get(COL_FIRST, ""))
            last = _clean(row.get(COL_LAST, ""))
            middle = _clean(row.get(COL_MIDDLE, ""))
            phone = _format_phone(row.get(COL_PHONE, ""))

            name_parts = [first, middle, last]
            name = " ".join(p for p in name_parts if p)

            is_primary = _clean(row.get(COL_PRIMARY, "")).upper() == "Y"

            if not name:
                continue

            # Update firm phone from primary contact if firm has none
            if is_primary and phone and not firm.phone:
                firm.phone = phone

            # Add as contact if not already present
            existing_names = {c.name.lower() for c in firm.contacts}
            if name.lower() not in existing_names:
                title = "Primary Contact" if is_primary else "Contact"
                contact = Contact(
                    name=name,
                    title=title,
                    phone=phone,
                    source=EnrichmentSource.IRS_CONTACT,
                )
                firm.contacts.append(contact)
                updated += 1

            if EnrichmentSource.IRS_CONTACT not in firm.enrichment_sources:
                firm.enrichment_sources.append(EnrichmentSource.IRS_CONTACT)

    click.echo(f"  Added {updated} contact records across firms")
    return firms
