from __future__ import annotations

import click
from thefuzz import fuzz

from src.models.firm import Firm


def apply_exclusion_filter(
    firms: list[Firm],
    chain_names: list[str],
    threshold: int = 80,
) -> int:
    """Flag firms matching known chains using fuzzy name matching.

    Does NOT remove firms — sets flagged_chain=True and flagged_chain_match
    so users can toggle visibility in the dashboard.

    Returns the count of flagged firms.
    """
    flagged = 0
    # Pre-normalize chain names for faster matching
    normalized_chains = [(name, name.upper().strip()) for name in chain_names]

    for firm in firms:
        firm_upper = firm.firm_name.upper().strip()
        dba_upper = firm.dba.upper().strip() if firm.dba else ""

        best_match = ""
        best_score = 0

        for original_name, chain_upper in normalized_chains:
            # Check firm name
            score = fuzz.token_set_ratio(firm_upper, chain_upper)
            if score > best_score:
                best_score = score
                best_match = original_name

            # Check DBA name too
            if dba_upper:
                dba_score = fuzz.token_set_ratio(dba_upper, chain_upper)
                if dba_score > best_score:
                    best_score = dba_score
                    best_match = original_name

        if best_score >= threshold:
            firm.flagged_chain = True
            firm.flagged_chain_match = best_match
            flagged += 1

    return flagged
