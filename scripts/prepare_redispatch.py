# SPDX-FileCopyrightText: NGV-FBMC contributors
# SPDX-FileCopyrightText: gb-dispatch-model contributors
#
# SPDX-License-Identifier: MIT

"""
Prepare network for constrained optimization.

This file is based on the gb-dispatch-model's `scripts/gb_model/redispatch/prepare_constrained_network.py` script with some modifications required for the combined model.
"""

import logging
from pathlib import Path
from typing import Literal

import pandas as pd
import pypsa

from scripts._helpers import configure_logging
from scripts.gb_model._helpers import filter_interconnectors
from scripts.gb_model.dispatch.prepare_unconstrained_network import (
    copperplate_gb,
)

logger = logging.getLogger(__name__)


def fix_dispatch(
    base_network: pypsa.Network,
    dispatch_result: pypsa.Network,
    gb_buses: pd.Index,
):
    """
    Fix dispatch of most network components based on the result of dispatch optimization

    Parameters
    ----------
    base_network: pypsa.Network
        Base network to finalize
    dispatch_result: pypsa.Network
        Result of the dispatch optimization
    gb_buses: pd.Index
        Index of GB buses to identify which components to fix
    """

    def _process_p_fix(dispatch_t: pd.DataFrame, p_nom: pd.DataFrame):
        return (dispatch_t / p_nom).round(5).fillna(0)

    for comp in dispatch_result.components[
        ["Generator", "Link", "StorageUnit", "Store"]
    ]:
        if comp.name in ["Generator", "Store", "StorageUnit"]:
            p_fix = comp.dynamic.p

        elif comp.name == "Link":
            # Do not fix the dispatch for intra-GB links
            intra_gb_links = comp.static.query(
                "`bus0` in @gb_buses and `bus1` in @gb_buses and `carrier` in ['DC']",
                local_dict={"gb_buses": gb_buses},
            ).index

            other_links = comp.static.index.difference(intra_gb_links)

            p_fix = comp.dynamic.p0.loc[:, other_links]

        base_network.components[comp.name].dynamic.p_set = p_fix

        logger.info(f"Fixed the dispatch of {comp.name}")


def _apply_multiplier(
    df: pd.DataFrame,
    multiplier: dict[str, float],
    renewable_strike_prices: pd.Series,
    direction: Literal["bid", "offer"],
) -> pd.Series:
    """
    Apply bid/offer multiplier and strike prices

    Parameters
    ----------
    df: pd.DataFrame
        Generator dataframe
    multiplier: dict[str, float]
        Mapping of conventional carrier to multiplier
    renewable_strike_prices: pd.Series
        Renewable CfD strike prices for each renewable generator
    direction: Literal["bid", "offer"]
        Direction of the multiplier, either "bid" or "offer"
    """
    new_marginal_costs = (
        (df["carrier"].map(renewable_strike_prices) - df["marginal_cost"])
        # if strike price is lower than marginal cost, then we apply zero charge for bids/offers
        .clip(lower=0)
        .mul(-1 if direction == "bid" else 1)
        .fillna(df["marginal_cost"] * df["carrier"].map(multiplier).fillna(1))
    )
    assert not (isna := new_marginal_costs.isna()).any(), (
        f"Some marginal costs are NaN after applying multipliers and strike prices: {new_marginal_costs[isna].index.tolist()}"
    )

    undefined_multipliers = set(df["carrier"].unique()) - (
        set(multiplier.keys()) | set(renewable_strike_prices.index)
    )
    logger.warning(
        f"Neither bid/offer multiplier nor strike price provided for the carriers: {undefined_multipliers}"
    )

    return new_marginal_costs


def create_up_down_plants(
    base_network: pypsa.Network,
    dispatch_result: pypsa.Network,
    bids_and_offers: dict[str, dict[str, float]],
    renewable_strike_prices: pd.Series,
    interconnector_bid_offer_profile: pd.DataFrame,
    gb_buses: pd.Index,
):
    """
    Add generators and storage units components that mimic increase / decrease in dispatch

    Parameters
    ----------
    base_network: pypsa.Network
        Base network to finalize
    dispatch_result: pypsa.Network
        Result of the dispatch optimization
    bids_and_offers: dict[str, float]
        Bid and offer multipliers for conventional carriers
    renewable_strike_prices: pd.DataFrame
        Dataframe of the renewable CfD strike prices
    interconnector_bid_offer_profile: pd.DataFrame
        Interconnectors bid/offer profile for each interconnector
    gb_buses: pd.Index
        Index of GB buses
    """
    for comp in base_network.components[["Generator", "StorageUnit", "Link"]]:
        base_network.add("Carrier", [f"{comp.name} ramp up", f"{comp.name} ramp down"])

        g = comp.static

        if comp.name in ["Generator", "StorageUnit"]:
            # Filter GB plants
            g = g.query(
                "`bus` in @gb_buses and `p_nom` != 0", local_dict={"gb_buses": gb_buses}
            )
        elif comp.name == "Link":
            # Account for different port names and only
            # add up/down plants for interconnectors that connect GB to other countries,
            # and for all generation technologies that are represented as links (e.g. OCGT)
            intra_gb_links = g.query(
                "`bus0` in @gb_buses and `bus1` in @gb_buses and `carrier` in ['DC']",
                local_dict={"gb_buses": gb_buses},
            ).index
            g = g.query(
                "`index` not in @intra_gb_links and `p_nom` != 0",
                local_dict={"intra_gb_links": intra_gb_links},
            )

        g_up = g.copy()
        g_down = g.copy()

        # Compute dispatch limits for the up and down generators
        result_component = dispatch_result.components[comp.name]
        dynamic_p = (
            result_component.dynamic.p0
            if comp.name == "Link"
            else result_component.dynamic.p
        )

        up_limit = (
            dispatch_result.get_switchable_as_dense(comp.name, "p_max_pu")
            * result_component.static.p_nom
            - dynamic_p
        ).clip(0) / result_component.static.p_nom
        if comp.name == "Generator":
            down_limit = -dynamic_p / result_component.static.p_nom
        else:
            down_limit = (
                dispatch_result.get_switchable_as_dense(comp.name, "p_min_pu")
                * result_component.static.p_nom
                - dynamic_p
            ) / result_component.static.p_nom

        prices = {}
        for direction, df in [("offer", g_up), ("bid", g_down)]:
            # Create a shared price profile for all up/down plants including interconnectors
            # the time-independent for conventional generators is casted to a time-dependent (fixed) profile
            # to have one dataframe for all cases, because we represent fossil generation assets
            # as Links rather than Generators (in the GB dispatch model)
            # Add bid/offer multipliers for conventional generators
            prices_static = _apply_multiplier(
                df=df,
                multiplier=bids_and_offers[f"{direction}_multiplier"],
                renewable_strike_prices=renewable_strike_prices,
                direction=direction,
            )

            prices_dynamic = interconnector_bid_offer_profile.filter(
                regex=f".* {direction}$"
            ).rename(columns=lambda x: x.replace(" " + direction, ""))

            # Turn prices_static into a DataFrame with the same index as prices_dynamic
            # by repeating the static prices for each timestamp in the dynamic prices
            prices[direction] = pd.DataFrame(
                [prices_static.values],
                index=[prices_dynamic.index[0]],
                columns=prices_static.index,
            ).reindex(prices_dynamic.index, method="ffill")

            # Overwrite the prices for interconnectors with the dynamic profile
            # some entries are also present in the static data, but the dynamic
            # profiles take precedence
            prices[direction].loc[:, prices_dynamic.columns] = prices_dynamic

        # Bus to connect the up/down plants to, same for _up and _down
        bus = None
        if comp.name in ["Generator", "StorageUnit"]:
            # Simple case: connect to the same bus as the original plant
            bus = g_up.bus
        elif comp.name in ["Link", "Line"]:
            # In the GB dispatch model we always connect to bus0, which is the GB bus
            # However for generating assets that are represented as Links (e.g. OCGT)
            # the relevant bus is bus1 (which is GB connected)
            # Emitting generators with bus0, bus1 and bus2 set: use bus1
            # Buses with bus0 matching "GB\s+": use bus0
            # otherwise use bus0
            bus = g_up.bus1.where(g_up.bus2 != "", g_up.bus0)

        # Add generators that can increase dispatch
        base_network.add(
            "Generator",
            g_up.index,
            suffix=" ramp up",
            carrier=f"{comp.name} ramp up",
            p_min_pu=0,
            p_max_pu=up_limit.loc[:, g_up.index],
            marginal_cost=prices["offer"].loc[:, g_up.index],
            p_nom=g_up.p_nom,
            bus=bus,
        )

        # Add generators that can decrease dispatch
        base_network.add(
            "Generator",
            g_down.index,
            suffix=" ramp down",
            carrier=f"{comp.name} ramp down",
            p_min_pu=down_limit.loc[:, g_down.index],
            p_max_pu=0,
            marginal_cost=prices["bid"].loc[:, g_down.index],
            p_nom=g_down.p_nom,
            bus=bus,
        )

        logger.info(
            f"Added {comp.name} for carriers {g_up.carrier.unique()} that can mimic increase and decrease in dispatch"
        )


def drop_existing_eur_buses(network: pypsa.Network) -> pypsa.Network:
    """
    Drop existing eur buses from the network

    Parameters
    ----------
    network: pypsa.Network
        Network to finalize
    """

    # Special buses that need to persist for the topology of the network to work
    protected_buses = [
        "EU waste",
        "EU solid biomass",
        "EU oil",
        "EU uranium",
        "EU gas",
        "co2 atmosphere",
    ]
    eur_buses = network.buses.query(
        "country != 'GB' and `index` not in @protected_buses"
    ).index
    gb_buses = network.buses.query("country == 'GB'").index
    network.remove("Bus", eur_buses)

    for comp in network.components[["Generator", "StorageUnit", "Store", "Load"]]:
        network.remove(
            comp.name,
            comp.static.query("bus in @eur_buses").index,
        )

    for comp in network.components[["Link", "Line"]]:
        # Drop all Links, except for those where bus0 or bus1 is a GB bus
        # e.g. interconnectors or generating assets represented as Links (e.g. OCGT)
        network.remove(
            comp.name,
            comp.static.query("bus0 not in @gb_buses and bus1 not in @gb_buses").index,
        )

    # Manual cleanup for some that are not easy to catch
    cleanup_components = {"Load": ["EU solid biomass final energy demand"]}
    for comp in network.components[list(cleanup_components.keys())]:
        network.remove(
            comp.name,
            comp.static.query(
                "index in @comps", local_dict={"comps": cleanup_components[comp.name]}
            ).index,
        )

    logger.info(
        f"Dropped generators, storage units, links and loads connected to {eur_buses} from the network"
    )

    return network


def add_single_eur_bus(network: pypsa.Network, unconstrained_result: pypsa.Network):
    """
    Add a single EUR bus to simplify the network structure

    Parameters
    ----------
    network: pypsa.Network
        Network to finalize
    unconstrained_result: pypsa.Network
        Result of the unconstrained optimization
    """

    network.add("Bus", "EUR", country="EUR")

    network.add(
        "Store",
        "EUR store",
        bus="EUR",
        e_nom=1e9,  # Large capacity to avoid energy constraints,
    )

    # Change bus1 of all interconnectors to EUR
    interconnectors = filter_interconnectors(
        network.links, "carrier in ['DC', 'ramp up', 'ramp down']"
    )
    network.links.loc[interconnectors.index, "bus1"] = "EUR"

    logger.info(
        "Added single EUR bus with a store and connected all interconnectors to it"
    )


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake(
            Path(__file__).stem, planning_horizons=2030, scenario="IEM"
        )

    configure_logging(snakemake)

    # Load input networks and parameters
    network = pypsa.Network(snakemake.input.network)
    dispatch_result = pypsa.Network(snakemake.input.dispatch_result)
    bids_and_offers = pd.read_csv(
        snakemake.input.bids_and_offers, index_col="carrier"
    ).to_dict()
    renewable_strike_prices = pd.read_csv(
        snakemake.input.renewable_strike_prices, index_col="carrier"
    ).squeeze()
    interconnector_bid_offer_profile = pd.read_csv(
        snakemake.input.interconnector_bid_offer, index_col="snapshot", parse_dates=True
    )

    # Currency conversion to EUR
    renewable_strike_prices *= snakemake.params["GBP_to_EUR"]
    renewable_strike_prices.name = "strike_price_EUR_per_MWh"

    # Map strike prices from original carriers to the modelled carrier equivalents
    strike_price_mapping = snakemake.params["strike_price_mapping"]
    # These mappings are partially 1:n, duplicate entries with the same strike price
    renewable_strike_prices = (
        pd.Series(strike_price_mapping)
        .explode()
        .to_frame("carrier")
        .merge(renewable_strike_prices, left_index=True, right_index=True)
        .set_index("carrier")
    )["strike_price_EUR_per_MWh"]

    # Expand the mappings for bids and offers as well (nice? No. Working? Yes!)
    bids = bids_and_offers["bid_multiplier"]
    offers = bids_and_offers["offer_multiplier"]
    bids = (
        pd.Series(strike_price_mapping)
        .explode()
        .to_frame("new_carrier")
        .merge(pd.Series(bids, name="bids"), left_index=True, right_index=True)
    ).set_index("new_carrier")["bids"]
    offers = (
        pd.Series(strike_price_mapping)
        .explode()
        .to_frame("new_carrier")
        .merge(pd.Series(offers, name="offers"), left_index=True, right_index=True)
    ).set_index("new_carrier")["offers"]
    bids_and_offers["bid_multiplier"] = bids.to_dict()
    bids_and_offers["offer_multiplier"] = offers.to_dict()

    # Select GB buses
    gb_buses = network.buses.query("country == 'GB'").index

    fix_dispatch(network, dispatch_result, gb_buses)

    create_up_down_plants(
        base_network=network,
        dispatch_result=dispatch_result,
        bids_and_offers=bids_and_offers,
        renewable_strike_prices=renewable_strike_prices,
        interconnector_bid_offer_profile=interconnector_bid_offer_profile,
        gb_buses=gb_buses,
    )

    network = drop_existing_eur_buses(network)

    add_single_eur_bus(network, unconstrained_result)

    if snakemake.params["unconstrain_lines_and_links"]:
        # Set line capacities to infinity, so only boundary capabilities bound the optimization instead of line capacities.
        copperplate_gb(network)

    network.export_to_netcdf(snakemake.output.network)
    logger.info(f"Exported network to {snakemake.output.network}")
