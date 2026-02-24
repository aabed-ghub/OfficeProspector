from __future__ import annotations

import csv
import sys
from datetime import date

csv.field_size_limit(sys.maxsize)

import click
from tqdm import tqdm

from src.config import RAW_DIR
from src.models.firm import Firm, ReturnVolume, EnrichmentSource


# Column indices (0-based) from the Master Extract
COL_CUST_ID = "CUST-ID"
COL_LEGAL_NAME = "LEGAL-NAME"
COL_DBA = "DBA-NAME"

# ERO option flag (Electronic Return Originator - the ones filing returns for clients)
COL_ERO_OPT = "ERO-OPT"
COL_ERO_1040 = "ERO-1040"
COL_ERO_1065 = "ERO-1065"
COL_ERO_1120 = "ERO-1120"

# Address columns
COL_BUSN_ADDR1 = "BUSN-ADDR1"
COL_BUSN_ADDR2 = "BUSN-ADDR2"
COL_BUSN_CITY = "BUSN-CITY"
COL_BUSN_STATE = "BUSN-STATE"
COL_BUSN_POSTAL = "BUSN-POSTAL"
COL_PHONE = "PHONE"

# Return volume columns
COL_YTD_ACT = "YTD-ACT-RET"
COL_PRV1_ACT = "PRV1-ACT-RET"
COL_PRV2_ACT = "PRV2-ACT-RET"

# Entity type
COL_CUST_TYPE = "CUST-TYPE"


def _safe_int(val: str) -> int:
    """Parse string to int, defaulting to 0."""
    try:
        return int(val.strip()) if val.strip() else 0
    except ValueError:
        return 0


def _clean(val: str) -> str:
    return val.strip() if val else ""


def _format_phone(raw: str) -> str:
    """Convert IRS phone format (XXX/XXX-XXXX) to standard format."""
    raw = raw.strip()
    if not raw:
        return ""
    # IRS uses XXX/XXX-XXXX format
    raw = raw.replace("/", "-")
    return raw


def _guess_return_breakdown(row: dict) -> tuple[bool, bool]:
    """Check ERO authorization flags for 1040 (individual) and 1065/1120 (business)."""
    does_individual = _clean(row.get(COL_ERO_1040, "")).upper() == "Y"
    does_business = (
        _clean(row.get(COL_ERO_1065, "")).upper() == "Y"
        or _clean(row.get(COL_ERO_1120, "")).upper() == "Y"
    )
    return does_individual, does_business


def parse_master_extract() -> list[Firm]:
    """Parse the FOIA Master Extract into Firm objects."""
    # Find the master TXT file
    master_files = list(RAW_DIR.glob("FOIA-MASTER-*.TXT"))
    if not master_files:
        raise click.ClickException("Master Extract .TXT not found in data/raw/. Run 'ingest' first.")
    master_path = master_files[0]

    firms = []
    current_year = date.today().year

    click.echo(f"  Reading {master_path.name}...")
    with open(master_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)

        for row in tqdm(reader, desc="  Parsing Master", unit=" rows"):
            efin = _clean(row.get(COL_CUST_ID, ""))
            if not efin:
                continue

            # Build return volumes for 3 years
            ytd_returns = _safe_int(row.get(COL_YTD_ACT, ""))
            prv1_returns = _safe_int(row.get(COL_PRV1_ACT, ""))
            prv2_returns = _safe_int(row.get(COL_PRV2_ACT, ""))

            volumes = []
            if ytd_returns > 0:
                volumes.append(ReturnVolume(
                    year=current_year, total_returns=ytd_returns,
                    individual_returns=ytd_returns, business_returns=0,
                ))
            if prv1_returns > 0:
                volumes.append(ReturnVolume(
                    year=current_year - 1, total_returns=prv1_returns,
                    individual_returns=prv1_returns, business_returns=0,
                ))
            if prv2_returns > 0:
                volumes.append(ReturnVolume(
                    year=current_year - 2, total_returns=prv2_returns,
                    individual_returns=prv2_returns, business_returns=0,
                ))

            # Approximate return type breakdown from ERO authorization flags
            does_individual, does_business = _guess_return_breakdown(row)

            # Build address
            addr_parts = [_clean(row.get(COL_BUSN_ADDR1, "")),
                          _clean(row.get(COL_BUSN_ADDR2, ""))]
            street = ", ".join(p for p in addr_parts if p)

            firm = Firm(
                efin=efin,
                firm_name=_clean(row.get(COL_LEGAL_NAME, "")),
                dba=_clean(row.get(COL_DBA, "")),
                street_address=street,
                city=_clean(row.get(COL_BUSN_CITY, "")),
                state=_clean(row.get(COL_BUSN_STATE, "")),
                zip_code=_clean(row.get(COL_BUSN_POSTAL, "")),
                phone=_format_phone(row.get(COL_PHONE, "")),
                return_volumes=volumes,
                enrichment_sources=[EnrichmentSource.IRS_MASTER],
                last_updated=date.today(),
            )

            # Set return type approximation
            if does_individual and does_business:
                firm.individual_return_pct = 70.0  # approximation
                firm.business_return_pct = 30.0
            elif does_individual:
                firm.individual_return_pct = 100.0
                firm.business_return_pct = 0.0
            elif does_business:
                firm.individual_return_pct = 0.0
                firm.business_return_pct = 100.0

            firms.append(firm)

    click.echo(f"  Parsed {len(firms)} firms from Master Extract")
    return firms
