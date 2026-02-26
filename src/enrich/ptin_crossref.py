from __future__ import annotations

import click
from dataclasses import dataclass
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


@dataclass
class _PrepMatch:
    preparer: PtinPreparer
    name_score: int    # Best fuzzy score on firm-name vs preparer DBA (token_set_ratio, generous)
    name_strict: int   # Stricter score (token_sort_ratio, penalizes extra/missing tokens)
    addr_score: int    # Fuzzy score on address


def _match_firm_to_preparers(
    firm: Firm,
    candidates: list[PtinPreparer],
    threshold: int = 75,
) -> list[_PrepMatch]:
    """Match a firm to PTIN preparers by name/address similarity.

    Returns matches with scores so callers can apply different thresholds
    for contacts (lower risk) vs website assignment (higher risk).
    """
    matches = []
    firm_name_upper = firm.firm_name.upper()
    firm_dba_upper = firm.dba.upper() if firm.dba else ""

    for preparer in candidates:
        dba_upper = preparer.dba.upper() if preparer.dba else ""

        # Match on DBA/firm name
        name_score = 0
        name_strict = 0
        if dba_upper:
            name_score = max(
                fuzz.token_set_ratio(firm_name_upper, dba_upper),
                fuzz.token_set_ratio(firm_dba_upper, dba_upper) if firm_dba_upper else 0,
            )
            # token_sort_ratio is stricter: sorts tokens alphabetically and
            # does a straight comparison, so shared generic tokens like "CPA"
            # or "TAX SERVICE" don't inflate the score.
            name_strict = max(
                fuzz.token_sort_ratio(firm_name_upper, dba_upper),
                fuzz.token_sort_ratio(firm_dba_upper, dba_upper) if firm_dba_upper else 0,
            )

        # Also match on address line 1 if names don't match well
        addr_score = 0
        if preparer.address_line1:
            addr_score = fuzz.token_set_ratio(
                firm.street_address.upper(),
                preparer.address_line1.upper(),
            )

        # Accept if name score meets threshold, or address is very close
        overall = name_score
        if overall < threshold and addr_score > 85:
            overall = max(overall, addr_score)

        if overall >= threshold:
            matches.append(_PrepMatch(preparer, name_score, name_strict, addr_score))

    return matches


_STATE_ABBR_TO_NAME: dict[str, str] = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas",
    "CA": "california", "CO": "colorado", "CT": "connecticut", "DE": "delaware",
    "FL": "florida", "GA": "georgia", "HI": "hawaii", "ID": "idaho",
    "IL": "illinois", "IN": "indiana", "IA": "iowa", "KS": "kansas",
    "KY": "kentucky", "LA": "louisiana", "ME": "maine", "MD": "maryland",
    "MA": "massachusetts", "MI": "michigan", "MN": "minnesota", "MS": "mississippi",
    "MO": "missouri", "MT": "montana", "NE": "nebraska", "NV": "nevada",
    "NH": "new-hampshire", "NJ": "new-jersey", "NM": "new-mexico", "NY": "new-york",
    "NC": "north-carolina", "ND": "north-dakota", "OH": "ohio", "OK": "oklahoma",
    "OR": "oregon", "PA": "pennsylvania", "RI": "rhode-island", "SC": "south-carolina",
    "SD": "south-dakota", "TN": "tennessee", "TX": "texas", "UT": "utah",
    "VT": "vermont", "VA": "virginia", "WA": "washington", "WV": "west-virginia",
    "WI": "wisconsin", "WY": "wyoming", "DC": "district-of-columbia",
}


def enrich_with_ptin(firms: list[Firm], settings: dict, only_states: set[str] | None = None) -> list[Firm]:
    """Cross-reference firms with PTIN preparer data to add contacts and metadata."""
    # Download PTIN state files
    state_names = settings.get("states", [])
    base_url = settings["irs"]["ptin_base_url"]

    # When only_states is provided (e.g. from --limit), download only the states
    # that appear in the firm list instead of all 50+
    if only_states:
        needed_names = {_STATE_ABBR_TO_NAME[abbr] for abbr in only_states if abbr in _STATE_ABBR_TO_NAME}
        state_names = [s for s in state_names if s in needed_names]
        click.echo(f"  Limiting PTIN downloads to {len(state_names)} states: {', '.join(sorted(only_states))}")

    click.echo("  Downloading PTIN state files...")
    downloaded = 0
    for state_name in tqdm(state_names, desc="  PTIN downloads", unit=" states"):
        result = download_ptin_state(state_name, base_url)
        if result:
            downloaded += 1
    click.echo(f"  Downloaded {downloaded}/{len(state_names)} PTIN files")

    # Load PTIN data (only for needed states if limited)
    preparers = load_all_ptin_data(state_names if only_states else None)
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

        for m in matches:
            # Only assign website from high-confidence NAME matches.
            # Require BOTH token_set_ratio >= 88 AND token_sort_ratio >= 80.
            # token_set_ratio alone inflates on shared generic tokens like
            # "CPA", "TAX SERVICE" -- token_sort_ratio catches these.
            if (m.preparer.website and not firm.website
                    and m.name_score >= 88 and m.name_strict >= 80):
                firm.website = m.preparer.website

            # Add phone if firm doesn't have one (requires decent name match)
            if m.preparer.phone and not firm.phone and m.name_score >= 75:
                firm.phone = m.preparer.phone

            # Add as contact (any match quality is fine for listing)
            existing_names = {c.name.lower() for c in firm.contacts}
            if m.preparer.full_name.lower() not in existing_names:
                title = m.preparer.profession or "Tax Preparer"
                contact = Contact(
                    name=m.preparer.full_name,
                    title=title,
                    phone=m.preparer.phone,
                    source=EnrichmentSource.PTIN,
                )
                firm.contacts.append(contact)

        if EnrichmentSource.PTIN not in firm.enrichment_sources:
            firm.enrichment_sources.append(EnrichmentSource.PTIN)

    click.echo(f"  Enriched {enriched_count} firms with PTIN data")
    return firms
