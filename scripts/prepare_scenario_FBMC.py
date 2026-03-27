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
from scripts.fbmc import FBMCConstraint

import pypsa

from scripts._helpers import (
    configure_logging,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake(
            Path(__file__).stem,
            planning_horizons=2030,
        )
    configure_logging(snakemake)

    n = pypsa.Network(snakemake.input["model"])
    n.name = (
        f"Flow-based market coupling (FBMC) - {snakemake.wildcards.planning_horizons}"
    )

    # Align the snapshots between the FBMC data and the network to save them for later inspection/use
    fbmc_constraint = FBMCConstraint.from_netcdf(
        snakemake.input["ptdf"], snakemake.input["ram"]
    ).align_snapshots(n.snapshots)
    fbmc_constraint.to_netcdf(snakemake.output["ptdf"], snakemake.output["ram"])

    # Doesn't hurt
    n.consistency_check(strict=None)

    # Save modified network
    n.export_to_netcdf(snakemake.output["model"])
