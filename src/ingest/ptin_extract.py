from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

import click
from tqdm import tqdm

from src.config import RAW_DIR


@dataclass
class PtinPreparer:
    first_name: str
    last_name: str
    middle_name: str
    suffix: str
    dba: str
    address_line1: str
    address_line2: str
    city: str
    state: str
    zip_code: str
    website: str
    phone: str
    profession: str  # CPA, EA, ATTY, etc.

    @property
    def full_name(self) -> str:
        parts = [self.first_name, self.middle_name, self.last_name]
        name = " ".join(p for p in parts if p)
        if self.suffix:
            name += f" {self.suffix}"
        return name


def _clean(val: str) -> str:
    return val.strip().strip('"') if val else ""


def parse_ptin_file(path: Path) -> list[PtinPreparer]:
    """Parse a single PTIN state CSV into PtinPreparer objects."""
    preparers = []

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                preparer = PtinPreparer(
                    first_name=_clean(row.get("First_NAME", row.get("FIRST_NAME", ""))),
                    last_name=_clean(row.get("LAST_NAME", "")),
                    middle_name=_clean(row.get("MIDDLE_NAME", "")),
                    suffix=_clean(row.get("SUFFIX", "")),
                    dba=_clean(row.get("DBA", "")),
                    address_line1=_clean(row.get("BUS_ADDR_LINE1", "")),
                    address_line2=_clean(row.get("BUS_ADDR_LINE2", "")),
                    city=_clean(row.get("BUS_ADDR_CITY", "")),
                    state=_clean(row.get("BUS_ST_CODE", "")),
                    zip_code=_clean(row.get("BUS_ADDR_ZIP", "")),
                    website=_clean(row.get("WEBSITE", "")),
                    phone=_clean(row.get("BUS_PHNE_NBR", "")),
                    profession=_clean(row.get("PROFESSION", "")),
                )
                preparers.append(preparer)
            except (KeyError, ValueError):
                continue

    return preparers


def load_all_ptin_data(state_names: list[str] | None = None) -> list[PtinPreparer]:
    """Load PTIN data from all downloaded state CSVs."""
    ptin_files = list(RAW_DIR.glob("foia-*-extract.csv"))
    if not ptin_files:
        click.echo("  No PTIN files found in data/raw/")
        return []

    if state_names:
        ptin_files = [f for f in ptin_files
                      if any(s in f.name for s in state_names)]

    all_preparers = []
    for path in tqdm(ptin_files, desc="  Loading PTIN files", unit=" files"):
        preparers = parse_ptin_file(path)
        all_preparers.extend(preparers)

    click.echo(f"  Loaded {len(all_preparers)} PTIN preparers from {len(ptin_files)} state files")
    return all_preparers
