# SPDX-FileCopyrightText: Contributors to NGV-FBMC project
#
# SPDX-License-Identifier: MIT
"""
Prepare the Trader Forecast (TF) scenario run.

Preparing the scenario run involves:
* Modifying the network according to the trader forecast error, i.e. changing demand and availability of renewables

Outputs a prepared model for solving as optimal dispatch.
"""

import logging
from pathlib import Path
import pypsa
import pandas as pd

from scripts._helpers import (
    configure_logging,
)
from scripts.prepare_scenario_SQ import add_electrolysis_constraints

logger = logging.getLogger(__name__)


def add_forecast_errors(n: pypsa.Network, error_fp: str, config: dict) -> pypsa.Network:
    """
    Creates a new network that is modified according to the trader forecast errors.

    This is a modified version of the script used in phase 01 (NGV-IEM model).

    Parameters
    ----------
    n : pypsa.Network
        The network used as a base for the trader forecast scenario.
    error_fp : str
        File path to the parquet file containing the relative errors for the "trader-forecast" scenario.
    config : dict
        The configuration dictionary containing the mapping of bus, component and carrier names between the error file and the network file.

    Returns
    -------
    pypsa.Network
        The modified network according to the specified uncertainty scenario.
    """

    # Work on a copy of the network
    n = n.copy()

    # Change name
    n.name = f"{n.name} Trader Forecast (TF)"

    # Load forecast errors
    relative_errors = pd.read_parquet(error_fp)

    # Datetimeindex contains one entry too many (8761 instead of 8760), remove the last
    relative_errors = relative_errors.iloc[:8760]

    # Realign the datetime index to be for 2009 (hourly)
    relative_errors.index = pd.date_range(
        start=f"{n.snapshots[0].year}-01-01", periods=len(relative_errors), freq="h"
    )

    # Rename index name to 'snapshot' for consistency with PyPSA
    relative_errors.index.name = "snapshot"

    # Manually curated mapping between bidding zones used
    # in the error data and the bus names used in the model
    bus_mapping = config["bus_mapping"]

    ## Restructure the error dataframe to match the bus and component names from the model
    # Duplicate the error data for each mapped node
    expanded_errors_l: list = []
    for bz, nodes in bus_mapping.items():
        bz_cols = [col for col in relative_errors.columns if col.startswith(bz)]

        for node in nodes:
            node_cols = [col.replace(bz, node) for col in bz_cols if node is not None]
            expanded_errors_l.append(
                relative_errors[bz_cols].rename(columns=dict(zip(bz_cols, node_cols)))
            )

    expanded_errors: pd.DataFrame = pd.concat(expanded_errors_l, axis=1)

    # Expand errors per component and carrier, duplicating the dataframes
    # TODO check for correct carrier/generator/load names and adjust
    cc_mapping = config["component_carrier_mapping"]

    expanded_errors_l: list = []
    for suffix_old, suffixes_new in cc_mapping.items():
        tech_cols = [col for col in expanded_errors.columns if suffix_old in col]
        for suffix_new in suffixes_new:
            new_cols = {col: col.replace(suffix_old, suffix_new) for col in tech_cols}
            expanded_errors_l.append(
                expanded_errors[tech_cols].rename(columns=new_cols)
            )

    expanded_errors: pd.DataFrame = pd.concat(expanded_errors_l, axis="columns")
    # Take the column names, split them on " " and turn the split into a multiindex
    multiindex_tuples = [col.split("|") for col in expanded_errors.columns]
    expanded_errors.columns = pd.MultiIndex.from_tuples(
        multiindex_tuples, names=["bus", "component_type", "carrier"]
    )

    ## Combining the errors with the time-series data from the TYNDP model
    # Generators
    for bus, component_type, carrier in expanded_errors.columns:
        p_col = {
            "generators": "p_max_pu",
            "loads": "p_set",
        }[component_type]

        comp = n.components[component_type].dynamic[p_col]
        cols = (
            n.components[component_type]
            .static.loc[
                (n.components[component_type].static.index.str.startswith(bus))
                & (
                    n.components[component_type]
                    .static["carrier"]
                    .str.casefold()
                    .str.contains(carrier)
                )
            ]
            .index.tolist()
        )

        if not cols:
            continue
        logger.info(f"Applying errors to {bus} {carrier} for columns: {cols}")

        # Apply the errors onto all columns from generators[col]
        new_p = comp[cols].multiply(
            1 + expanded_errors.loc[:, (bus, component_type, carrier)], axis="index"
        )

        # Errors may cause values below 0 which is unrealistic, so clip accordingly
        # We could also clip > 1, but then we need to differentiate between
        # loads (absolute timeseries) and generators (pu timeseries)
        new_p = new_p.clip(lower=0)  # , upper=max_value)

        # Assign the new values back to the generators dataframe
        # (this propagates to the network object n because it is a reference, not a copy)
        # Make sure to align snapshots first
        new_p = new_p.loc[comp.index]
        comp[cols] = new_p

    return n


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake(
            Path(__file__).stem,
            year=2030,
        )
    configure_logging(snakemake)

    config = snakemake.params["forecast_errors"]

    n = pypsa.Network(snakemake.input["model"])

    # Ensure electrolysis dispatch is fixed to the optimal dispatch in the previous run
    n = add_electrolysis_constraints(n)

    # Add forecast errors based on externally generated errors
    n = add_forecast_errors(
        n, error_fp=snakemake.input["forecast_errors"], config=config
    )

    # Doesn't hurt
    n.consistency_check()

    # Save modified network
    n.export_to_netcdf(snakemake.output["model"])

    # TODO from previous phase, probably not needed this time,
    # should already be taken care of in the prepare_scenario_IEM script
    # n.optimize.fix_optimal_capacities()
    # n = extend_primary_fuel_sources(n)
    # n = remove_components_added_in_solve_network_py(n)
