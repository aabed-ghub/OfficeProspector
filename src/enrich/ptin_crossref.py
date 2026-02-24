from __future__ import annotations

import click
from thefuzz import fuzz
from tqdm import tqdm

from src.config import RAW_DIR
from src.ingest.downloader import download_ptin_state
from src.ingest.ptin_extract import load_all_ptin_data, PtinPreparer
from src.models.firm import Firm, Contact, EnrichmentSource


def _build_ptin_index(preparers: list[PtinPreparer]) -> dict[str, list[PtinPreparer]]:
    """Index PTIN preparers by state + city for faster matching."""
    index: dict[str, list[PtinPreparer]] = {}
    for p in preparers:
        key = f"{p.state.upper()}|{p.city.upper()}"
        index.setdefault(key, []).append(p)
    return index


def _match_firm_to_preparers(
    firm: Firm,
    candidates: list[PtinPreparer],
    threshold: int = 75,
) -> list[PtinPreparer]:
    """Match a firm to PTIN preparers by name/address similarity."""
    matches = []
    firm_name_upper = firm.firm_name.upper()
    firm_dba_upper = firm.dba.upper() if firm.dba else ""

    for preparer in candidates:
        dba_upper = preparer.dba.upper() if preparer.dba else ""

        # Match on DBA/firm name
        score = 0
        if dba_upper:
            score = max(
                fuzz.token_set_ratio(firm_name_upper, dba_upper),
                fuzz.token_set_ratio(firm_dba_upper, dba_upper) if firm_dba_upper else 0,
            )

        # Also match on address line 1 if names don't match well
        if score < threshold and preparer.address_line1:
            addr_score = fuzz.token_set_ratio(
                firm.street_address.upper(),
                preparer.address_line1.upper(),
            )
            if addr_score > 85:
                score = max(score, addr_score)

        if score >= threshold:
            matches.append(preparer)

    return matches


def enrich_with_ptin(firms: list[Firm], settings: dict) -> list[Firm]:
    """Cross-reference firms with PTIN preparer data to add contacts and metadata."""
    # Download PTIN state files
    state_names = settings.get("states", [])
    base_url = settings["irs"]["ptin_base_url"]

    click.echo("  Downloading PTIN state files...")
    downloaded = 0
    for state_name in tqdm(state_names, desc="  PTIN downloads", unit=" states"):
        result = download_ptin_state(state_name, base_url)
        if result:
            downloaded += 1
    click.echo(f"  Downloaded {downloaded}/{len(state_names)} PTIN files")

    # Load all PTIN data
    preparers = load_all_ptin_data()
    if not preparers:
        click.echo("  No PTIN data available, skipping cross-reference.")
        return firms

    # Build index
    click.echo("  Building PTIN index...")
    ptin_index = _build_ptin_index(preparers)

    # Match firms to preparers
    click.echo("  Matching firms to PTIN preparers...")
    enriched_count = 0
    for firm in tqdm(firms, desc="  PTIN matching", unit=" firms"):
        key = f"{firm.state.upper()}|{firm.city.upper()}"
        candidates = ptin_index.get(key, [])
        if not candidates:
            continue

        matches = _match_firm_to_preparers(firm, candidates)
        if not matches:
            continue

        firm.preparer_count = len(matches)
        enriched_count += 1

        for preparer in matches:
            # Add website if firm doesn't have one
            if preparer.website and not firm.website:
                firm.website = preparer.website

            # Add phone if firm doesn't have one
            if preparer.phone and not firm.phone:
                firm.phone = preparer.phone

            # Add as contact
            existing_names = {c.name.lower() for c in firm.contacts}
            if preparer.full_name.lower() not in existing_names:
                title = preparer.profession or "Tax Preparer"
                contact = Contact(
                    name=preparer.full_name,
                    title=title,
                    phone=preparer.phone,
                    source=EnrichmentSource.PTIN,
                )
                firm.contacts.append(contact)

        if EnrichmentSource.PTIN not in firm.enrichment_sources:
            firm.enrichment_sources.append(EnrichmentSource.PTIN)

    click.echo(f"  Enriched {enriched_count} firms with PTIN data")
    return firms
