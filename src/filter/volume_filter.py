from __future__ import annotations

import click

from src.models.firm import Firm


def apply_volume_filter(firms: list[Firm], min_returns: int, max_returns: int) -> list[Firm]:
    """Keep firms with return volume between min and max (inclusive).

    Uses the best available year: prior year 1 first (most complete),
    then prior year 2, then YTD as fallback.
    """
    filtered = []
    for firm in firms:
        if not firm.return_volumes:
            continue

        # Use the most recent complete year (not YTD which may be partial)
        sorted_vols = sorted(firm.return_volumes, key=lambda v: v.year, reverse=True)

        # If we have 2+ years, use the second most recent (most recent full year)
        # If only 1 year, use that
        best_vol = sorted_vols[1] if len(sorted_vols) > 1 else sorted_vols[0]
        count = best_vol.total_returns

        if min_returns <= count <= max_returns:
            filtered.append(firm)

    return filtered
