from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import click
from tqdm import tqdm

from src.config import RAW_DIR, PROCESSED_DIR, CACHE_DIR, load_exclusion_chains
from src.models.firm import Firm


def _ensure_dirs() -> None:
    for d in [RAW_DIR, PROCESSED_DIR, CACHE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def _save_firms(firms: list[Firm], filename: str) -> Path:
    path = PROCESSED_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        for firm in firms:
            f.write(firm.model_dump_json() + "\n")
    click.echo(f"  Saved {len(firms)} firms to {path}")
    return path


def _load_firms(filename: str) -> list[Firm]:
    path = PROCESSED_DIR / filename
    if not path.exists():
        raise click.ClickException(f"File not found: {path}. Run the previous pipeline stage first.")
    firms = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                firms.append(Firm.model_validate_json(line))
    return firms


def run_ingest(settings: dict) -> list[Firm]:
    """Stage 1: Download and parse IRS FOIA extracts."""
    _ensure_dirs()
    click.echo("=" * 60)
    click.echo("STAGE 1: INGESTION")
    click.echo("=" * 60)

    from src.ingest.downloader import download_all
    from src.ingest.master_extract import parse_master_extract
    from src.ingest.partner_extract import parse_partner_extract
    from src.ingest.contact_extract import parse_contact_extract

    # Download IRS files
    download_all(settings)

    # Parse Master Extract → base Firm objects with return volumes
    click.echo("\n  Parsing Master Extract...")
    firms = parse_master_extract()

    # Merge Partner Extract → add contacts
    click.echo("  Parsing Partner Extract...")
    firms = parse_partner_extract(firms)

    # Merge Contact Extract → add primary contacts
    click.echo("  Parsing Contact Extract...")
    firms = parse_contact_extract(firms)

    click.echo(f"\n  Total firms ingested: {len(firms)}")
    _save_firms(firms, "01_ingested.jsonl")
    return firms


def run_filter(settings: dict, limit: int | None = None) -> list[Firm]:
    """Stage 2: Apply volume, exclusion, and deduplication filters."""
    click.echo("\n" + "=" * 60)
    click.echo("STAGE 2: FILTERING")
    click.echo("=" * 60)

    firms = _load_firms("01_ingested.jsonl")
    click.echo(f"  Loaded {len(firms)} ingested firms")

    if limit:
        firms = firms[:limit]
        click.echo(f"  --limit {limit}: sliced to {len(firms)} firms")

    from src.filter.volume_filter import apply_volume_filter
    from src.filter.exclusion_filter import apply_exclusion_filter
    from src.filter.deduplication import deduplicate

    # Volume filter (100-15K returns)
    min_ret = settings["filter"]["min_returns"]
    max_ret = settings["filter"]["max_returns"]
    firms = apply_volume_filter(firms, min_ret, max_ret)
    click.echo(f"  After volume filter ({min_ret}-{max_ret} returns): {len(firms)} firms")

    # Exclusion filter (flag chains, don't remove)
    chains = load_exclusion_chains()
    threshold = settings["filter"]["fuzzy_match_threshold"]
    flagged_count = apply_exclusion_filter(firms, chains, threshold)
    click.echo(f"  Flagged {flagged_count} firms as potential chains")

    # Deduplication
    before = len(firms)
    firms = deduplicate(firms)
    click.echo(f"  Deduplicated: {before} -> {len(firms)} firms")

    # Compute YoY growth and return breakdown
    for firm in firms:
        firm.compute_yoy_growth()
        firm.compute_return_breakdown()

    _save_firms(firms, "02_filtered.jsonl")
    return firms


def run_enrich(settings: dict, skip_scraping: bool = False, skip_apollo: bool = False, skip_email: bool = False, limit: int | None = None) -> list[Firm]:
    """Stage 3: Enrich with PTIN, website scraping, Apollo."""
    click.echo("\n" + "=" * 60)
    click.echo("STAGE 3: ENRICHMENT")
    click.echo("=" * 60)

    firms = _load_firms("02_filtered.jsonl")
    click.echo(f"  Loaded {len(firms)} filtered firms")

    if limit:
        firms = firms[:limit]
        click.echo(f"  --limit {limit}: sliced to {len(firms)} firms")

    # Stage 3a: PTIN cross-reference (free)
    click.echo("\n  --- PTIN Cross-Reference ---")
    from src.enrich.ptin_crossref import enrich_with_ptin
    firm_states = {f.state.upper() for f in firms if f.state} if limit else None
    firms = enrich_with_ptin(firms, settings, only_states=firm_states)

    # Stage 3b: Google search for websites (Serper.dev)
    click.echo("\n  --- Google Search (Serper.dev) ---")
    from src.enrich.serper_search import enrich_with_serper
    firms = enrich_with_serper(firms, settings)

    # Stage 3c: Website scraping (free)
    if not skip_scraping:
        click.echo("\n  --- Website Scraping ---")
        from src.enrich.website_scraper import enrich_with_websites
        firms = enrich_with_websites(firms, settings)
    else:
        click.echo("\n  --- Skipping website scraping ---")

    # Stage 3d: Apollo.io (free tier)
    if not skip_apollo:
        click.echo("\n  --- Apollo.io Enrichment ---")
        from src.enrich.apollo_enrichment import enrich_with_apollo
        firms = enrich_with_apollo(firms, settings)
    else:
        click.echo("\n  --- Skipping Apollo.io ---")

    # Stage 3e: Email guess + verify
    if not skip_email:
        click.echo("\n  --- Email Verification ---")
        from src.enrich.email_guesser import guess_and_verify_emails
        firms = guess_and_verify_emails(firms, settings)
    else:
        click.echo("\n  --- Skipping email verification ---")

    # Mark enrichment status
    for firm in firms:
        firm.is_enriched = len(firm.enrichment_sources) > 1  # More than just IRS Master
        firm.no_website = not bool(firm.website)

    _save_firms(firms, "03_enriched.jsonl")

    # Also save a dated snapshot
    snapshot_name = f"enriched_firms_{date.today().isoformat()}.jsonl"
    _save_firms(firms, snapshot_name)

    return firms


def run_export(settings: dict, target: str = "all", limit: int | None = None) -> None:
    """Stage 4: Export to CSV and/or JSON."""
    click.echo("\n" + "=" * 60)
    click.echo("STAGE 4: EXPORT")
    click.echo("=" * 60)

    firms = _load_firms("03_enriched.jsonl")
    click.echo(f"  Loaded {len(firms)} enriched firms")

    if limit:
        firms = firms[:limit]
        click.echo(f"  --limit {limit}: sliced to {len(firms)} firms")

    if target in ("csv", "all"):
        from src.export.csv_exporter import export_csv
        export_csv(firms, settings)

    if target in ("json", "all"):
        from src.export.json_exporter import export_json
        export_json(firms, settings)

    click.echo("\n  Export complete!")


def run_pipeline(settings: dict, state_filter: list[str] | None = None, limit: int | None = None,
                  skip_scraping: bool = False, skip_apollo: bool = False, skip_email: bool = False) -> None:
    """Run the full pipeline end-to-end."""
    click.echo("Office Prospector - Full Pipeline Run")
    click.echo(f"Date: {date.today().isoformat()}")
    if state_filter:
        click.echo(f"State filter: {', '.join(state_filter)}")
    if limit:
        click.echo(f"Limit: {limit} firms")
    click.echo()

    firms = run_ingest(settings)

    if state_filter:
        firms = [f for f in firms if f.state.upper() in state_filter]
        click.echo(f"\n  Filtered to {len(firms)} firms in {', '.join(state_filter)}")

    if limit:
        firms = firms[:limit]
        click.echo(f"\n  --limit {limit}: sliced to {len(firms)} firms")

    if state_filter or limit:
        _save_firms(firms, "01_ingested.jsonl")

    run_filter(settings)
    run_enrich(settings, skip_scraping=skip_scraping, skip_apollo=skip_apollo, skip_email=skip_email, limit=limit)
    run_export(settings)

    click.echo("\n" + "=" * 60)
    click.echo("PIPELINE COMPLETE")
    click.echo("=" * 60)
