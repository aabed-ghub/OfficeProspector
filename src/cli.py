import click

from src.config import load_settings


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Office Prospector - Identify boutique tax prep offices for acquisition."""
    ctx.ensure_object(dict)
    ctx.obj["settings"] = load_settings()


@cli.command()
@click.option("--states", default=None, help="Comma-separated state codes (e.g., tx,fl,ca). Default: all states.")
@click.option("--limit", default=None, type=int, help="Process only the first N firms (for fast test runs).")
@click.option("--skip-scraping", is_flag=True, help="Skip website scraping step.")
@click.option("--skip-apollo", is_flag=True, help="Skip Apollo.io enrichment.")
@click.option("--skip-email", is_flag=True, help="Skip email guessing/verification step.")
@click.pass_context
def run(ctx: click.Context, states: str | None, limit: int | None, skip_scraping: bool, skip_apollo: bool, skip_email: bool) -> None:
    """Run the full pipeline: ingest, filter, enrich, export."""
    from src.pipeline import run_pipeline

    state_list = [s.strip().upper() for s in states.split(",")] if states else None
    run_pipeline(ctx.obj["settings"], state_filter=state_list, limit=limit,
                 skip_scraping=skip_scraping, skip_apollo=skip_apollo, skip_email=skip_email)


@cli.command()
@click.pass_context
def ingest(ctx: click.Context) -> None:
    """Download and parse IRS FOIA extracts."""
    from src.pipeline import run_ingest

    run_ingest(ctx.obj["settings"])


@cli.command()
@click.option("--limit", default=None, type=int, help="Process only the first N firms (for fast test runs).")
@click.pass_context
def filter(ctx: click.Context, limit: int | None) -> None:
    """Apply volume, exclusion, and deduplication filters."""
    from src.pipeline import run_filter

    run_filter(ctx.obj["settings"], limit=limit)


@cli.command()
@click.option("--skip-scraping", is_flag=True, help="Skip website scraping step.")
@click.option("--skip-apollo", is_flag=True, help="Skip Apollo.io enrichment.")
@click.option("--skip-email", is_flag=True, help="Skip email guessing/verification step.")
@click.option("--limit", default=None, type=int, help="Process only the first N firms (for fast test runs).")
@click.pass_context
def enrich(ctx: click.Context, skip_scraping: bool, skip_apollo: bool, skip_email: bool, limit: int | None) -> None:
    """Enrich firms with contact info, websites, and emails."""
    from src.pipeline import run_enrich

    run_enrich(ctx.obj["settings"], skip_scraping=skip_scraping, skip_apollo=skip_apollo, skip_email=skip_email, limit=limit)


@cli.command()
@click.option("--target", type=click.Choice(["csv", "json", "all"]), default="all", help="Export format.")
@click.option("--limit", default=None, type=int, help="Process only the first N firms (for fast test runs).")
@click.pass_context
def export(ctx: click.Context, target: str, limit: int | None) -> None:
    """Export enriched data to CSV and/or JSON for the dashboard."""
    from src.pipeline import run_export

    run_export(ctx.obj["settings"], target=target, limit=limit)


if __name__ == "__main__":
    cli()
