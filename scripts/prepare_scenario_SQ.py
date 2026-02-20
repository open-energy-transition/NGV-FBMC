# SPDX-FileCopyrightText: Contributors to NGV-FBMC project
#
# SPDX-License-Identifier: MIT
"""
Prepare the Status Quo (SQ) scenario run.

Preparing the scenario run involves:
* Extracting bidding on interconnections capacities from the TF (trader forecast) scenario which adds forecasting errors to the IEM scenario.
* Applying the extracted bidding as exogenous limits (=bids) on the interconnections

Outputs a prepared model for solving as optimal dispatch.
"""

import logging
from pathlib import Path
import pypsa
import pandas as pd
import numpy as np

from scripts._helpers import (
    configure_logging,
)

logger = logging.getLogger(__name__)


def extract_line_limits(
    n_fp: str | Path,
    config: dict,
):
    """
    Extract the line utilisation as per unit from the network for the configured links and buses.
    """

    n = pypsa.Network(n_fp)

    # Filter for links of interest
    links_i = n.components.links.static.loc[
        (
            # Select by carrier
            n.components.links.static["carrier"].isin(config["connection_types"])
        )
        & (  # Links starting or ending (but not both) in buses of the configured bus prefix, e.g. GB
            (n.components.links.static["bus0"].str.startswith(config["to_from"]))
            ^ (n.components.links.static["bus1"].str.startswith(config["to_from"]))
        )
    ].index

    dispatch = n.components.links.dynamic["p0"][links_i]
    capacities = n.components.links.static.loc[links_i, "p_nom_opt"]

    # Calculate per unit line limits
    line_limits = dispatch.div(capacities, axis="columns")

    # Set small values that are close to 0 (negative and positive) to 0
    line_limits[line_limits.abs() < 1e-4] = 0

    # In case of 0 capacity, set line limit to 0 to avoid NaN values
    line_limits = line_limits.fillna(0)

    return line_limits


def restrict_electricity_flows(
    n: pypsa.Network,
    line_limits: pd.DataFrame,
    explicitly_allocated_lines: list[str],
    lower_bound: float = 0.95,
    upper_bound: float = 1.05,
) -> pypsa.Network:
    """
    Restrict electricity flows based on pre-calculated hourly per-unit line limits for certain links.

    The flows are restricted to an envelope defined by the lower and upper bound multipliers applied to p_min_pu and p_max_pu.

    Parameters
    ----------
    n : pypsa.Network
        PyPSA network instance
    line_limits_fp : str
        File path to CSV containing line limits
    explicitly_allocated_lines : list[str]
        List of regex patterns to match the lines for which the limits should be applied.
        Only these lines matching this pattern will be restricted.
        For each pattern at least one match must be found in the line limits file.
    lower_bound : float
        Lower bound multiplier to apply to the line limits (default: 0.95).
    upper_bound : float
        Upper bound multiplier to apply to the line limits (default: 1.05).
    """

    # Match the existing columns against the configured list
    # using regex match patterns
    matched_columns: list[str] = []
    matches_count: dict[str, int] = {}
    for regex_pattern in explicitly_allocated_lines:
        matches = line_limits.columns[line_limits.columns.str.match(regex_pattern)]
        matched_columns.extend(matches)
        matches_count[regex_pattern] = len(matches)

    # Check that each regex pattern matched at least one column
    for regex_pattern, count in matches_count.items():
        if count == 0:
            raise ValueError(
                f"The line regex pattern '{regex_pattern}' did not match any columns in the line limits file. Please check the pattern and the column names in the file."
            )

    # Load the file again, but only with the matched columns + snapshot column
    line_limits = line_limits[matched_columns]

    logger.info(
        "Restricting electricity flows based on line limits from trader forecast scenario for the following explicitly allocated lines: "
        + ", ".join(line_limits.columns)
    )

    # Patch: We cannot use .loc[index, columns] to assign to a subset of the columns in a DataFrame with a MultiIndex,
    # as this will reset the "name" attribute of the columns index, which causes issues with how pypsa exports and then loads networks
    # from netcdf. This is a known issue that is being actively worked on
    line_limits.columns.name = "name"
    line_limits = line_limits.reindex(n.components.links.dynamic["p_min_pu"].index)

    # Calculate bounds symmetrically around 0
    # For positive values: min=lower_bound*val, max=upper_bound*val
    # For negative values: min=upper_bound*val (more negative), max=lower_bound*val (less negative)
    lower_limits = lower_bound * line_limits
    upper_limits = upper_bound * line_limits

    n.components.links.dynamic["p_min_pu"][line_limits.columns] = (
        np.minimum(lower_limits, upper_limits)
    ).clip(-1, 1)
    n.components.links.dynamic["p_max_pu"][line_limits.columns] = (
        np.maximum(lower_limits, upper_limits)
    ).clip(-1, 1)

    return n


def extend_primary_fuel_sources(n):
    primary_fuel_sources = [
        "EU lignite",
        "EU coal",
        "EU oil primary",
        "EU uranium",
        "EU gas",
    ]
    n.generators.loc[primary_fuel_sources, "p_nom_extendable"] = True
    return n


def add_electrolysis_constraints(n):
    """Enforce the electrolysis dispatch to the optimal dispatch found in the solved network."""
    electrolysis_i = n.links[n.links.carrier == "H2 Electrolysis"].index
    n.links_t.p_set.loc[:, electrolysis_i] = n.links_t.p0.loc[:, electrolysis_i]
    return n


def remove_components_added_in_solve_network_py(n: pypsa.Network) -> pypsa.Network:
    """Removes components that were added in solve_network.py; we're planing on running this network through the same step again and want to avoid adding the components again."""

    logger.info("Removing components added in solve_network.py")

    # These components are not always part of the network, so
    # we check for their existence first
    if "co2_sequestration_limit" in n.global_constraints.index:
        n.remove(
            class_name="GlobalConstraint",
            name="co2_sequestration_limit",
        )

    if "load" in n.carriers.index:
        n.remove(
            class_name="Carrier",
            name="load",
        )
        gens_i = n.generators.query("`name`.str.endswith(' load')").index
        n.remove(
            class_name="Generator",
            name=gens_i,
        )

    if "curtailment" in n.carriers.index:
        n.remove(
            class_name="Carrier",
            name="curtailment",
        )
        gens_i = n.generators.query("`name`.str.endswith(' curtailment')").index
        n.remove(
            class_name="Generator",
            name=gens_i,
        )

    return n


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake(
            Path(__file__).stem,
            year=2030,
        )
    configure_logging(snakemake)

    config = snakemake.params["explicit_allocation"]

    n = pypsa.Network(snakemake.input["model"])

    # TODO check which of these steps is necessary in the new network
    n.optimize.fix_optimal_capacities()
    n = remove_components_added_in_solve_network_py(n)
    n = add_electrolysis_constraints(n)
    n = extend_primary_fuel_sources(n)

    line_limits = extract_line_limits(n_fp=snakemake.input["model_tf"], config=config)

    # For validation, save the line limits to file
    line_limits.to_csv(snakemake.output["line_limits"])

    n = restrict_electricity_flows(
        n=n,
        line_limits=line_limits,
        explicitly_allocated_lines=config["explicitly_allocated_lines"],
        lower_bound=0.95,
        upper_bound=1.05,
    )

    n.name = f"{n.name} Status Quo (SQ)"
    n.export_to_netcdf(snakemake.output["model"])
