# SPDX-FileCopyrightText: NGV-FBMC contributors
# SPDX-FileCopyrightText: gb-dispatch-model contributors
#
# SPDX-License-Identifier: MIT

"""
Prepare network for constrained optimization.

This file is based on the gb-dispatch-model's `scripts/gb_model/redispatch/prepare_constrained_network.py` script with some modifications required for the combined model.
"""

import yaml
import logging
from pathlib import Path
from typing import Literal
import numpy as np

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
        ["Generator", "Line", "Link", "StorageUnit"]
    ]:
        if comp.name in ["Generator", "StorageUnit"]:
            p_fix = comp.dynamic.p

            # Some components are still expendable, e.g. EU-wide fuel sources; make them non-extendable
            col = [c for c in comp.static.columns if "extendable" in c][0]
            base_network.components[comp.name].static[col] = False

        elif comp.name == "Line":
            # Do not fix the dispatch for lines (intra-GB). Skip explicitly
            continue

        elif comp.name == "Link":
            # Do not fix the dispatch for intra-GB links
            # (this applies to DC links, but also to e.g. DSR links;
            # multi-links that generate electricity are not captured by this,
            # because their bus0 is a non-GB , global fuel bus)
            intra_gb_links = comp.static.query(
                "`bus0` in @gb_buses and `bus1` in @gb_buses and `carrier` in @carriers",
                local_dict={
                    "gb_buses": gb_buses,
                    "carriers": [
                        "DC",
                        "battery charger",
                        "battery discharger",
                        "home battery charger",
                        "home battery discharger",
                        "H2 electrolysis",
                        "h2-ccgt",
                    ],
                },
            ).index

            other_links = comp.static.index.difference(intra_gb_links)

            p_fix = comp.dynamic.p0.loc[:, other_links]

        base_network.components[comp.name].dynamic.p_set.loc[:, p_fix.columns] = p_fix

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


def _get_multilink_carriers(network: pypsa.Network) -> set[str]:
    """
    Identify carriers that are implemented as multi-Link components (with bus0, bus1, and bus2).

    These are typically fossil fuel plants that track both fuel input and CO2 emissions.

    Parameters
    ----------
    network: pypsa.Network
        Network to analyze

    Returns
    -------
    set[str]
        Set of carrier names that are implemented as multi-Link components
    """
    multilink_carriers = set()
    links_with_bus2 = network.links[network.links.bus2 != ""].index
    if len(links_with_bus2) > 0:
        multilink_carriers = set(network.links.loc[links_with_bus2, "carrier"].unique())
    return multilink_carriers


def create_up_down_plants(
    base_network: pypsa.Network,
    dispatch_result: pypsa.Network,
    bids_and_offers: dict[str, dict[str, float]],
    renewable_strike_prices: pd.Series,
    interconnector_bid_offer_profile: pd.DataFrame,
    gb_buses: pd.Index,
    no_redispatch_carriers: list[str],
):
    """
    Add generators and links components that mimic increase / decrease in dispatch

    For multi-Link technologies (fossil fuel plants with CO2 tracking), adds Link components
    instead of Generators to properly account for fuel input and CO2 emissions.

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
    no_redispatch_carriers: list[str]
        List of carriers to exclude from being redispatched.
    """
    # Identify which carriers are represented as multi-Link components (with bus2 for CO2 tracking)
    multilink_carriers = _get_multilink_carriers(base_network)

    for comp in base_network.components[["Generator", "StorageUnit", "Link"]]:
        base_network.add("Carrier", [f"{comp.name} ramp up", f"{comp.name} ramp down"])

        g = comp.static

        if comp.name in ["Generator", "StorageUnit"]:
            # Filter GB plants
            g = g.query(
                "`bus` in @gb_buses and `p_nom` != 0 and `carrier` not in @no_redispatch_carriers",
                local_dict={
                    "gb_buses": gb_buses,
                    "no_redispatch_carriers": no_redispatch_carriers,
                },
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
                "`index` not in @intra_gb_links and `p_nom` != 0 and `carrier` not in @no_redispatch_carriers",
                local_dict={
                    "intra_gb_links": intra_gb_links,
                    "no_redispatch_carriers": no_redispatch_carriers,
                },
            )

        # Separate multi-Link carriers from others
        if comp.name == "Link":
            g_multilink = g[g.carrier.isin(multilink_carriers)]
            g_simple = g[~g.carrier.isin(multilink_carriers)]
        else:
            g_multilink = pd.DataFrame()
            g_simple = g

        # Process simple components (generators and simple links) with Generator ramp up/down
        if not g_simple.empty:
            g_up = g_simple.copy()
            g_down = g_simple.copy()

            # Compute dispatch limits for the up and down generators
            result_component = dispatch_result.components[comp.name]
            dynamic_p = result_component.dynamic["p0" if comp.name == "Link" else "p"]

            # Up limit and down limit are calculated differently:
            # Up limit is any remaining available capacity up until the maximum available capacity
            # Down limit is any dispatch that can be reduced down until the minimum available capacity (zero or higher for some technologies, negative for interconnectors)
            up_limit = (
                dispatch_result.get_switchable_as_dense(comp.name, "p_max_pu")
                - dynamic_p / result_component.static.p_nom
            ).clip(lower=0)

            down_limit = (
                dispatch_result.get_switchable_as_dense(comp.name, "p_min_pu")
                - dynamic_p / result_component.static.p_nom
            ).clip(upper=0)

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
                #
                # Important: This attaches the up/down generators for interconnector redispatch to the GB side, not the RoE side.
                # this is beneficial for us, as we can easily remove all other components from the RoE side.
                # The redispatch generators are later moved to the right RoE bus.
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

        # Process multi-Link components with Link ramp up/down
        if not g_multilink.empty:
            result_component = dispatch_result.components[comp.name]
            dynamic_p = result_component.dynamic.p0

            # Up limit and down limit are calculated differently:
            # Up limit is any remaining available capacity up until the maximum available capacity
            # Down limit is any dispatch that can be reduced down until the minimum available capacity (zero or higher for some technologies, negative for interconnectors)
            up_limit = (
                dispatch_result.get_switchable_as_dense(comp.name, "p_max_pu")
                - dynamic_p / result_component.static.p_nom
            ).clip(lower=0)

            down_limit = (
                dispatch_result.get_switchable_as_dense(comp.name, "p_min_pu")
                - dynamic_p / result_component.static.p_nom
            ).clip(upper=0)

            # For simplicity, add the fuel and CO2 costs for multi-links to
            # the marginal cost calculation of the Links, rather than the
            # redispatch Generator components for fuel and CO2.
            # This offers the advantage of being able to use n.statistics.opex()
            # on the redispatch generators
            # To achieve this, modify the marginal cost in the static DataFrame
            g_multilink = g_multilink.copy()  # work on a copy

            # Oil is more complicated to calculate
            oil_fuel_costs = (
                # crude prices
                network.c.links.static.query("`bus0`=='EU oil primary'")[
                    "marginal_cost"
                ]
                # refining cost
                + (
                    network.c.generators.static.query("`bus`=='EU oil primary'")[
                        "marginal_cost"
                    ]
                    / network.c.generators.static.query("`bus`=='EU oil primary'")[
                        "efficiency"
                    ]
                ).item()
                # CO2 emissions for refining
                + (-1)
                * (
                    network.c.stores.static.query("`bus`=='co2 atmosphere'")[
                        "marginal_cost"
                    ].item()
                    * network.c.links.static.query("`name`=='EU oil refining'")[
                        "efficiency2"
                    ].item()
                )
            )
            oil_fuel_costs.index = oil_fuel_costs.index.map(
                {"EU oil refining": "EU oil"}
            )

            fuel_cost = (
                dispatch_result.components["Generator"].static.set_index("bus")[
                    "marginal_cost"
                ]
                / dispatch_result.components["Generator"].static.set_index("bus")[
                    "efficiency"
                ]
            )
            # Fuel costs for all including oil
            patched_fuel_cost = pd.concat(
                [
                    fuel_cost,
                    oil_fuel_costs,
                ]
            )
            patched_fuel_cost = patched_fuel_cost.loc[
                g_multilink["bus0"].unique()
            ].fillna(0)

            co2_cost = (-1) * (
                g_multilink["bus2"].map(
                    dispatch_result.components["Store"].static.set_index("bus")[
                        "marginal_cost"
                    ]
                )
                * g_multilink["efficiency2"]
            )

            # New marginal cost: old marginal cost + fuel cost + co2 cost
            new_marginal_cost = (
                g_multilink["marginal_cost"]
                + g_multilink["bus0"].map(patched_fuel_cost)
                + co2_cost
            )

            g_multilink["marginal_cost"] = new_marginal_cost

            # Calculate marginal costs for up/down links with bid/offer multipliers applied
            prices_multilink = {}
            for direction in ["offer", "bid"]:
                prices_static = _apply_multiplier(
                    df=g_multilink,
                    multiplier=bids_and_offers[f"{direction}_multiplier"],
                    renewable_strike_prices=renewable_strike_prices,
                    direction=direction,
                )

                # Prices are static in this case, but for consistency we add them to the dynamic attribute
                prices_time = pd.DataFrame(
                    [prices_static.values],
                    index=[network.snapshots[0]],
                    columns=prices_static.index,
                ).reindex(network.snapshots, method="ffill")

                prices_multilink[direction] = prices_time

            # Add ramp up links for multi-Link technologies
            # The marginal cost already includes the bid/offer multiplier via _apply_multiplier
            base_network.add(
                "Link",
                g_multilink.index,
                suffix=" ramp up",
                bus0=g_multilink.bus0,
                bus1=g_multilink.bus1,
                bus2=g_multilink.bus2,
                carrier="Link ramp up",
                efficiency=g_multilink.efficiency,
                efficiency2=g_multilink.efficiency2,
                p_min_pu=0,
                p_max_pu=up_limit.loc[:, g_multilink.index],
                marginal_cost=prices_multilink["offer"].loc[:, g_multilink.index],
                p_nom=g_multilink.p_nom,
                reversed=False,  # Special attribute required in solve_network.py - of no model relevance
            )
            logger.info(
                f"Added multi-Link ramp up components for carriers {g_multilink.carrier.unique()}"
            )

            # Add ramp down links for multi-Link technologies
            base_network.add(
                "Link",
                g_multilink.index,
                suffix=" ramp down",
                bus0=g_multilink.bus0,
                bus1=g_multilink.bus1,
                bus2=g_multilink.bus2,
                carrier="Link ramp down",
                efficiency=g_multilink.efficiency,
                efficiency2=g_multilink.efficiency2,
                p_min_pu=down_limit.loc[:, g_multilink.index],
                p_max_pu=0,
                marginal_cost=prices_multilink["bid"].loc[:, g_multilink.index],
                p_nom=g_multilink.p_nom,
                reversed=False,  # Special attribute required in solve_network.py - of no model relevance
            )
            logger.info(
                f"Added multi-Link ramp down components for carriers {g_multilink.carrier.unique()}"
            )

            # Since the fuel generators have inf capacity, we determine the
            # nominal capacity based on the maximum used capacity used by the GB part of the model
            g_multilink = g_multilink.filter(regex=r"GB\s", axis="rows")
            p_gb = dispatch_result.c.links.dynamic.p0[g_multilink.index]
            # Map column names to the bus0 to get the total transfer from that bus per snapshot
            p_gb.columns = p_gb.columns.map(g_multilink.bus0)
            p_gb = p_gb.T.groupby(level=0).sum().T

            p_nom = p_gb.max()

            # Focus on GB, drop all other global fuel buses that are not relevant for the redispatch (non-GB)
            fuel_updown_gens = dispatch_result.c["Generator"].static.query(
                "`bus` in @buses", local_dict={"buses": g_multilink["bus0"].unique()}
            )

            logger.info(
                f"Adding ramp up/down generators for the fuel input of multi-Link components for carriers {fuel_updown_gens['carrier'].unique()}"
            )

            p_nom = p_nom.loc[fuel_updown_gens.index]
            up_limit = (
                p_nom.to_frame(p_gb.index[0])
                .T.reindex(p_gb.index, axis="index")
                .ffill()
                - p_gb
            )[fuel_updown_gens.index]
            down_limit = -p_gb[fuel_updown_gens.index]

            base_network.add(
                "Generator",
                fuel_updown_gens.index,
                suffix=" ramp up",
                bus=fuel_updown_gens["bus"],
                carrier="Generator ramp up",
                p_min_pu=0,
                p_max_pu=up_limit,
                p_nom=p_nom,
                p_nom_extendable=False,
                efficiency=fuel_updown_gens.efficiency,
                marginal_cost=0.01,  # Costs are already included in the multi-link marginal cost
            )

            base_network.add(
                "Generator",
                fuel_updown_gens.index,
                suffix=" ramp down",
                bus=fuel_updown_gens["bus"],
                carrier="Generator ramp down",
                p_min_pu=down_limit,
                p_max_pu=0,
                p_nom=p_nom,
                p_nom_extendable=False,
                efficiency=fuel_updown_gens.efficiency,
                marginal_cost=-0.01,  # Costs are already included in the multi-link marginal cost
            )

            # Manually insert oil generator:
            # Since oil is represented as multi-stage multi-link, this is more complicated and not covered by the logic above
            # However we have accounted for the marginal_cost fully with the generation links, as well as their redispatch limits
            # so we simplify the process by adding corresponding unconstrained redispatch components for oil and hopefully are done with it then
            oil_generator = dispatch_result.c.generators.static.query(
                "`index` == 'EU oil primary'"
            )
            base_network.add(
                "Generator",
                oil_generator.index,
                suffix=" ramp up",
                bus=oil_generator.bus,
                carrier="Generator ramp up",
                p_min_pu=0,
                p_max_pu=1,
                p_nom=oil_generator.p_nom,
                p_nom_extendable=oil_generator.p_nom_extendable,
                efficiency=oil_generator.efficiency,
                capital_cost=0,
                marginal_cost=0.01,
            )

            base_network.add(
                "Generator",
                oil_generator.index,
                suffix=" ramp down",
                bus=oil_generator.bus,
                carrier="Generator ramp down",
                p_min_pu=-1,
                p_max_pu=0,
                p_nom=oil_generator.p_nom,
                p_nom_extendable=oil_generator.p_nom_extendable,
                efficiency=oil_generator.efficiency,
                capital_cost=0,
                marginal_cost=-0.01,
            )

            refining_link = dispatch_result.c.links.static.query(
                "`name` == 'EU oil refining'"
            )

            base_network.add(
                "Link",
                refining_link.index,
                suffix=" ramp up",
                **refining_link.assign(
                    p_min_pu=0,
                    p_max_pu=1,
                    carrier="Link ramp up",
                ).to_dict(),
            )

            base_network.add(
                "Link",
                refining_link.index,
                suffix=" ramp up",
                **refining_link.assign(
                    p_min_pu=-1,
                    p_max_pu=0,
                    carrier="Link ramp down",
                    marginal_cost=-refining_link.marginal_cost,
                ).to_dict(),
            )

            # In addition modify the fixed-dispatch of the fuel generators to only provide generation for GB
            # (The generic approach of fixing it restricts the dispatch to a *must provide* for the full EUR model)
            base_network.c.generators.dynamic.p_set.loc[:, fuel_updown_gens.index] = (
                p_gb.loc[:, fuel_updown_gens.index]
            )

            ## Need to allow for redispatch for the CO2 store at 0 marginal cost as well
            # Calculate and Fix dispatch of existing CO2 store for GB only
            idx = dispatch_result.c.links.static.query(
                "`bus1` in @gb_buses and `bus2` == 'co2 atmosphere'",
                local_dict={"gb_buses": gb_buses},
            ).index
            emission_gb = (
                dispatch_result.c.links.dynamic.p0[idx]
                * dispatch_result.c.links.static.loc[idx, "efficiency2"]
            ).sum(axis="columns")
            base_network.c.stores.dynamic.p_set.loc[:, "co2 atmosphere"] = (
                -1
            ) * emission_gb  # this is not working; need to set e_set instead below
            base_network.c.stores.dynamic.e_set.loc[:, "co2 atmosphere"] = (-1) * (
                base_network.c.stores.dynamic.p_set.loc[:, "co2 atmosphere"].mul(
                    base_network.snapshot_weightings["stores"], axis="index"
                )
            ).cumsum()

            # Add up/down plants for the CO2 store as well
            base_network.add(
                "Carrier",
                name="Store ramp up",
            )
            base_network.add(
                "Carrier",
                name="Store ramp down",
            )
            base_network.add(
                "Generator",
                name="co2 atmosphere ramp up",
                bus="co2 atmosphere",
                carrier="Store ramp up",
                p_min_pu=0,
                p_max_pu=1,
                p_nom=emission_gb.max(),
                p_nom_extendable=False,
                efficiency=1,
                marginal_cost=0.01,  # Costs are already included in the multi-link marginal cost
            )
            base_network.add(
                "Generator",
                name="co2 atmosphere ramp down",
                bus="co2 atmosphere",
                carrier="Store ramp down",
                p_min_pu=-1,
                p_max_pu=0,
                p_nom=emission_gb.max(),
                p_nom_extendable=False,
                efficiency=1,
                marginal_cost=-0.01,  # Costs are already included in the multi-link marginal cost
            )

    return base_network


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
        "country != 'GB' and `index` not in @protected_buses",
        local_dict={"protected_buses": protected_buses},
    ).index
    gb_buses = network.buses.query("country == 'GB'").index
    network.remove("Bus", eur_buses)

    for comp in network.components[["Generator", "StorageUnit", "Store", "Load"]]:
        idx = comp.static.query(
            "bus in @eur_buses", local_dict={"eur_buses": eur_buses}
        ).index
        network.remove(
            comp.name,
            idx,
        )

    for comp in network.components[["Link", "Line"]]:
        # Drop all Links, except for those where bus0 or bus1 is a GB bus
        # e.g. interconnectors or generating assets represented as Links (e.g. OCGT)
        idx = comp.static.query(
            "bus0 not in @gb_buses and bus1 not in @gb_buses",
            local_dict={"gb_buses": gb_buses},
        ).index
        network.remove(
            comp.name,
            idx,
        )

    # Cleanup dynamic p_set for the removed components
    for comp in network.components[["Generator", "StorageUnit", "Link"]]:
        idx = comp.dynamic.p_set.columns.difference(comp.static.index)
        comp.dynamic.p_set = comp.dynamic.p_set.drop(columns=idx)

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


def add_new_eur_buses(network: pypsa.Network) -> pypsa.Network:
    """
    Add end point buses for each interconnector for a simplified network structure.

    Parameters
    ----------
    network: pypsa.Network
        Network with all but GB buses removed and the interconnectors dangling.
        End points will be added to this network.
    """
    interconnectors = filter_interconnectors(
        network.c.links.static, "carrier in ['DC']"
    )

    buses = interconnectors[["bus1"]]
    buses = buses.assign(country=buses["bus1"].str[:2])
    buses = buses.drop_duplicates("bus1").set_index("bus1")

    network.add("Bus", buses.index, country=buses["country"])

    # Add bus for all interconnectors, even those with zero capacity, to have a consistent structure of the network
    network.add(
        "Carrier",
        name="interconnector dispatch",
    )

    # Only active interconnectors with non-zero capacity have up/down plants and dispatch
    interconnectors = interconnectors.query("`p_nom` > 0")

    # Add a generator with p_set that forces the dispatch of the link to be the same as in the optimal dispatch results
    # The interconnectors are GB -> non-GB, i.e. by convention we need to reverse the forced dispatch (hence the -1)
    network.add(
        "Generator",
        name=interconnectors.index,
        suffix=" dispatch",
        bus=interconnectors["bus1"],
        carrier="interconnector dispatch",
        p_min_pu=-1,
        p_max_pu=1,
        p_set=-1
        * network.get_switchable_as_dense("Link", "p_set").loc[
            :, interconnectors.index
        ],
        p_nom=np.inf,
        p_nom_extendable=False,
        marginal_cost=0,
    )

    # Remove the original dispatch constraints on the interconnectors, as they are now represented by the generators on the non-GB side
    network.c.links.dynamic.p_set = network.c.links.dynamic.p_set.drop(
        columns=interconnectors.index
    )

    logger.info(f"Added {len(buses)} buses for the endpoints of all interconnectors")

    # Move interconnector ramp up/down generators from GB buses to their respective EUR buses.
    # The up/down plants for interconnectors are initially attached to GB buses for easier
    # processing. This function moves them to the correct EUR endpoint buses.
    for interconnector_name, interconnector in interconnectors.iterrows():
        # Find corresponding ramp up/down generators for this interconnector
        ramp_gens = network.c.generators.static.query(
            "`index` in @gen_names",
            local_dict={
                "gen_names": [
                    f"{interconnector_name} ramp up",
                    f"{interconnector_name} ramp down",
                ]
            },
        )

        if len(ramp_gens) == 0:
            logger.warning(
                f"Interconnector {interconnector_name} has non-zero capacity but no ramp up/down generators attached. "
                f"This is likely an error in the input data, please check."
            )
            continue

        # Move the ramp up/down generators to the right non-GB bus (bus1 of the interconnector)
        network.generators.loc[ramp_gens.index, "bus"] = interconnector["bus1"]

        logger.info(
            f"Moved {len(ramp_gens)} interconnector ramp up/down generators from {ramp_gens['bus'].unique().item()} to {interconnector['bus1']} for interconnector {interconnector_name}"
        )

    return network


def release_annual_fuel_generation_constraints(network: pypsa.Network) -> pypsa.Network:
    """
    openTYNDP imposes some annual generation constraints on fuel-providing Generator components.

    These are needed for a different modelling scope - since we are reducing the scope to only
    GB here, we release these constraints to avoid infeasibilities in the optimization.
    """

    idx = network.generators.query(
        "`index`.str.contains('EU') and (`e_sum_min` > @neg_inf or `e_sum_max` < @pos_inf)",
        local_dict={"neg_inf": -np.inf, "pos_inf": np.inf},
    ).index
    network.c.generators.static.loc[idx, "e_sum_min"] = (
        network.c.generators.defaults.loc["e_sum_min", "default"]
    )
    network.c.generators.static.loc[idx, "e_sum_max"] = (
        network.c.generators.defaults.loc["e_sum_max", "default"]
    )

    logger.info(f"Removed annual generation constraint for generator {idx}")

    return network


def cleanup_fuel_components(
    network: pypsa.Network, dispatch_results: pypsa.Network
) -> pypsa.Network:
    """
    After the model has been merged and reduced, there remain some unused components that need to be manually removed.
    There are also some components that are of lesser importance to the model and redispatch logic, but cause
    unnecessary complexity and potential infeasibilities, so we release some constraints for them.
    """

    components = {"Generator": []}

    for comp_name, comp_indices in components.items():
        if len(comp_indices) == 0:
            continue
        if comp_indices not in network.components[comp_name].static.index:
            logger.error(
                f"Expected the following {comp_name} components to be present in the network for removal, but they are not found: {comp_indices}"
            )
        else:
            network.remove(comp_name, comp_indices)
            logger.info(
                f"Removed unused {comp_name} components with indices {comp_indices}"
            )

    # Remove restrictions on lesser used global generators and links
    # For simplicity we are not enforcing the dispatch constraints here,
    # as these are multi-stage multi-links that are not easy to handle
    network.c.generators.dynamic.p_set = network.c.generators.dynamic.p_set.drop(
        columns=[
            "EU uranium",  # No redispatch on nuclear anyways
        ]
    )

    # Fix oil links and generators missing
    oil_gen = dispatch_result.generators.query("`bus`.str.contains('EU oil')")
    oil_buses = dispatch_result.buses.query("`name`.str.contains('EU oil')")
    oil_refining = dispatch_result.links.query(
        "`bus0` == 'EU oil primary' and `bus1` == 'EU oil'"
    )
    network.add("Bus", oil_buses.index, **oil_buses.to_dict())
    network.add("Generator", oil_gen.index, **oil_gen.to_dict())
    network.add("Link", oil_refining.index, **oil_refining.to_dict())

    return network


def load_boundary_crossings_file(input_file: str) -> pd.DataFrame:
    """
    Load the boundary crossings from yaml file and return a DataFrame with the relevant information for the model.

    Parameters
    ----------
    input_file: str
        Path to the input file containing boundary crossings as yaml file.

    Returns
    -------
    pd.DataFrame
        Dataframe containing the boundary crossings.
    """

    with open(input_file) as f:
        data = yaml.safe_load(f)

    rows = []
    for group in ["etys_boundaries_lines", "etys_boundaries_links"]:
        for boundary, pairs in data.get(group, {}).items():
            for p in pairs:
                rows.append(
                    {
                        "boundary_group": group,
                        "boundary": boundary,
                        "bus0": p.get("bus0"),
                        "bus1": p.get("bus1"),
                    }
                )

    df = pd.DataFrame(rows)
    df = df.rename(columns={"boundary_group": "component", "boundary": "Boundary_n"})
    df.loc[:, "component"] = (
        df["component"].str.replace("etys_boundaries_l", "L").str[:-1]
    )  # Line or Link instead of etys_boundaries_(lines|links)

    df.loc[:, "bus0"] = "GB " + df["bus0"].str.strip()
    df.loc[:, "bus1"] = "GB " + df["bus1"].str.strip()

    return df


def convert_boundary_crossings(
    input_file: str, output_file: str, network: pypsa.Network
) -> None:
    """
    Convert boundary crossings from an external file to a csv file compatible with the original GB redispatch model logic.

    Makes for simpler processing.

    Parameters
    ----------
    input_file: str
        Path to the input file containing boundary crossings as yaml file.
    output_file: str
        Path to the output file where the converted boundary crossings will be saved as csv
    network: pypsa.Network
        The network object used to determine the line and link components from for the boundary crossings.
    """

    lines = network.components.lines.static.query("`carrier`=='AC'")[["bus0", "bus1"]]
    links = network.components.links.static.query("`carrier`=='DC'")[["bus0", "bus1"]]
    components = pd.concat(
        [
            lines.reset_index().assign(component_n="Line"),
            links.reset_index().assign(component_n="Link"),
        ]
    )
    components = components.query(
        "`bus0`.str.contains('GB ') and `bus1`.str.contains('GB ')"
    ).reset_index(drop=True)

    df = load_boundary_crossings_file(input_file)

    direct = df.merge(
        components,
        on=["bus0", "bus1"],
        how="inner",
    ).sort_values("name")

    opposite = df.merge(
        components.rename(columns={"bus0": "bus1", "bus1": "bus0"}),
        on=["bus0", "bus1"],
        how="inner",
    ).sort_values("name")

    direct = direct.assign(direction=+1)
    opposite = opposite.assign(direction=-1)
    all_connections = pd.concat(
        [direct, opposite], ignore_index=True, axis="index"
    ).sort_values(["component", "Boundary_n"])

    all_connections[
        ["component", "name", "Boundary_n", "bus0", "bus1", "direction"]
    ].to_csv(output_file, index=False)

    if all_connections["name"].isna().any():
        raise ValueError(
            f"Cannot map some boundary crossings to lines or links. Check {output_file}."
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
        no_redispatch_carriers=snakemake.params["no_redispatch_carriers"],
    )

    network = drop_existing_eur_buses(network)

    network = add_new_eur_buses(network)

    network = release_annual_fuel_generation_constraints(network)

    network = cleanup_fuel_components(network, dispatch_result)

    if snakemake.params["unconstrain_lines_and_links"]:
        # Set line capacities to infinity, so only boundary capabilities bound the optimization instead of line capacities.
        copperplate_gb(network)

    # Never hurts
    network.consistency_check(strict=None)

    network.name = f"{snakemake.wildcards.scenario} ({snakemake.wildcards.planning_horizons}) - redispatch"

    network.export_to_netcdf(snakemake.output.network)

    # Convert boundary crossings from external file to a more usable format for the solve_network script and export as csv
    convert_boundary_crossings(
        input_file=snakemake.input.boundary_crossings,
        output_file=snakemake.output.boundary_crossings,
        network=network,
    )
