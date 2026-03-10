# SPDX-FileCopyrightText: Contributors to NGV-FBMC project
#
# SPDX-License-Identifier: MIT
"""
Prepare the FBMC scenario run.

Nothing particular needs to happen here. The FBMC constraints are only added for solving them.

Outputs a prepared model for solving as optimal dispatch.
"""

import logging
from pathlib import Path

import pypsa

from scripts._helpers import (
    configure_logging,
)

logger = logging.getLogger(__name__)


def fix_incorrect_operational_link_limits(n):
    p_max_pu = n.c.links.dynamic.p_max_pu
    p_min_pu = n.c.links.dynamic.p_min_pu

    diff = p_max_pu - p_min_pu
    diff = diff.where(diff < 0).dropna(axis=1, how="all")
    if not diff.empty:
        bad = diff.columns
        too_bad = (abs(diff.loc[:, bad]) > 1e-6).any()
        if too_bad.any():
            raise ValueError(
                f"p_min_pu considerably larger than p_max_pu for: {', '.join(too_bad.index[too_bad])}"
            )

        p_min_pu.loc[:, bad] = p_min_pu.loc[:, bad].clip(upper=p_max_pu.loc[:, bad])


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake(
            Path(__file__).stem,
            planning_horizons=2030,
        )
    # configure_logging(snakemake)

    n = pypsa.Network(snakemake.input["model"])
    n.name = (
        f"Flow-based market coupling (FBMC) - {snakemake.wildcards.planning_horizons}"
    )

    fix_incorrect_operational_link_limits(n)

    # Doesn't hurt
    n.consistency_check(strict=None)

    # Save modified network
    n.export_to_netcdf(snakemake.output["model"])
