from __future__ import annotations

import click

from src.models.firm import Firm


def deduplicate(firms: list[Firm]) -> list[Firm]:
    """Remove duplicate firms by EFIN. Keep the one with the most data."""
    seen: dict[str, Firm] = {}

    for firm in firms:
        if firm.efin in seen:
            existing = seen[firm.efin]
            # Keep whichever has more contacts or return data
            if len(firm.contacts) > len(existing.contacts):
                seen[firm.efin] = firm
            elif len(firm.return_volumes) > len(existing.return_volumes):
                seen[firm.efin] = firm
        else:
            seen[firm.efin] = firm

    return list(seen.values())
