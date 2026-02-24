from __future__ import annotations

import zipfile
from pathlib import Path

import click
import httpx
from tqdm import tqdm

from src.config import RAW_DIR, CACHE_DIR


def _download_file(url: str, dest: Path, label: str) -> Path:
    """Download a file with progress bar, skip if already cached."""
    if dest.exists():
        click.echo(f"  [cached] {label}: {dest.name}")
        return dest

    click.echo(f"  Downloading {label}...")
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(dest, "wb") as f:
            with tqdm(total=total, unit="B", unit_scale=True, desc=f"  {label}") as pbar:
                for chunk in resp.iter_bytes(8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
    return dest


def _extract_zip(zip_path: Path, dest_dir: Path) -> Path:
    """Extract ZIP and return path to the extracted .TXT file."""
    with zipfile.ZipFile(zip_path) as zf:
        txt_files = [n for n in zf.namelist() if n.upper().endswith(".TXT")]
        if not txt_files:
            raise click.ClickException(f"No .TXT file found in {zip_path.name}")
        extracted = dest_dir / txt_files[0]
        if not extracted.exists():
            zf.extract(txt_files[0], dest_dir)
            click.echo(f"  Extracted: {txt_files[0]}")
        else:
            click.echo(f"  [cached] Extracted: {txt_files[0]}")
    return extracted


def download_all(settings: dict) -> dict[str, Path]:
    """Download and extract all IRS FOIA files. Returns paths to extracted .TXT files."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    irs = settings["irs"]
    files = {}

    # Master Extract
    master_zip = RAW_DIR / Path(irs["master_url"]).name
    _download_file(irs["master_url"], master_zip, "Master Extract")
    files["master"] = _extract_zip(master_zip, RAW_DIR)

    # Partner Extract
    partner_zip = RAW_DIR / Path(irs["partner_url"]).name
    _download_file(irs["partner_url"], partner_zip, "Partner Extract")
    files["partner"] = _extract_zip(partner_zip, RAW_DIR)

    # Contact Extract
    contact_zip = RAW_DIR / Path(irs["contact_url"]).name
    _download_file(irs["contact_url"], contact_zip, "Contact Extract")
    files["contact"] = _extract_zip(contact_zip, RAW_DIR)

    click.echo(f"\n  All IRS FOIA files ready in {RAW_DIR}")
    return files


def download_ptin_state(state_name: str, base_url: str) -> Path | None:
    """Download a single PTIN state CSV. Returns path or None on failure."""
    filename = f"foia-{state_name}-extract.csv"
    url = f"{base_url}{filename}"
    dest = RAW_DIR / filename

    if dest.exists():
        return dest

    try:
        resp = httpx.get(url, follow_redirects=True, timeout=60)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return dest
    except (httpx.HTTPError, httpx.TimeoutException):
        return None
