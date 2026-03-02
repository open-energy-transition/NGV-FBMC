# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: MIT

import logging
import re

import pypsa

logger = logging.getLogger(__name__)


def merge_gb_tyndp(
    gb: pypsa.Network, eur: pypsa.Network, carrier_map: dict[str, str]
) -> pypsa.Network:
    """
    Combines the TYNDP (EUR) and GB-dispatch (GB) models
    """
    # prepare eur network by removing GB elements
    for comp in ["Bus", "StorageUnit", "Link", "Store", "Generator", "Load"]:
        idx = (
            eur.c[comp]
            .static[
                eur.c[comp].static.index.str.contains("GB00")
                | eur.c[comp].static.index.str.contains("GBOH")
            ]
            .index
        )
        eur.remove(comp, idx)
        cols = [col for col in eur.c[comp].static.columns if col.startswith("bus")]
        for col in cols:
            idx = eur.c[comp].static.loc[eur.c[comp].static[col] == "GB00"].index
            eur.remove(comp, idx)

    # create a mapping for the old GB names to the EUR names in TYNDP
    # note some non GB countries have multiple buses in TYNDP
    # the current assignment method only keeps buses that are connected to GB

    # for reference, the remaining buses in each bidding zone in each country
    eur_elec_buses = eur.buses[eur.buses.carrier == "AC"].index

    # prepare gb network
    non_gb_buses = gb.buses[
        (gb.buses.carrier == "AC") & ~(gb.buses.index.str.contains("GB"))
    ]
    non_gb_buses_h2 = gb.buses[
        (gb.buses.carrier == "H2") & ~(gb.buses.index.str.contains("GB"))
    ]

    gb_eur_busmap = {}
    for name, bus in non_gb_buses.iterrows():
        country_matches = [
            eur_bus for eur_bus in eur_elec_buses if eur_bus.startswith(name)
        ]
        buses_keep = ["DKW1", "NOS0", "FR00"]
        intersection = list(set(buses_keep) & set(country_matches))
        gb_eur_busmap[name] = intersection[0] if intersection else country_matches[0]

    for name, bus in non_gb_buses_h2.iterrows():
        country_matches = [
            eur_bus for eur_bus in eur_elec_buses if eur_bus.startswith(name[:-3])
        ]
        buses_keep = ["DKW1 H2", "NOS0 H2", "FR00 H2"]
        intersection = list(set(buses_keep) & set(country_matches))
        gb_eur_busmap[name] = intersection[0] if intersection else country_matches[0]

    # these buses are no longer relevant
    gb.remove("Bus", non_gb_buses.index)

    # check all carriers are accounted for
    for comp in ["Link", "Store", "StorageUnit", "Generator", "Load"]:
        gb_carriers = gb.c[comp].static.carrier.unique()
        eur_carriers = eur.c[comp].static.carrier.unique()
        for carrier in gb_carriers:
            # check for 1:1 map
            if carrier not in eur_carriers:
                # check for mapping in the carrier map
                if carrier not in carrier_map[comp].keys():
                    logger.warning(
                        f"Cannot find mapped value for carrier {carrier} component type {comp}"
                    )

    # connect to buses as named in open-tyndp
    for comp in ["Link", "Store", "StorageUnit", "Generator", "Load"]:
        cols = [col for col in gb.c[comp].static.columns if col.startswith("bus")]
        for col in cols:
            gb.c[comp].static[col] = gb.c[comp].static[col].replace(gb_eur_busmap)

        if "location" in gb.c[comp].static.columns:
            gb.c[comp].static["location"] = (
                gb.c[comp].static["location"].replace(gb_eur_map)
            )

        # leave generators for now, they are reassigned in the add_co2_multilink function
        if not comp == "Generator":
            if "carrier" in gb.c[comp].static.columns:
                gb.c[comp].static["carrier"] = (
                    gb.c[comp].static["carrier"].replace(carrier_map[comp])
                )

    # remove load shedding elements
    load_shedding_gens = gb.generators[gb.generators.carrier == "Load Shedding"]
    gb.remove("Generator", load_shedding_gens.index)

    # pypsa merge doesn't like overlapping components
    gb.remove("Carrier", gb.carriers.index.intersection(eur.carriers.index))
    gb.remove("Bus", non_gb_buses_h2.index)

    non_gb_lines = gb.lines[
        ~(gb.lines.bus0.str.contains("GB")) & ~(gb.lines.bus1.str.contains("GB"))
    ].index
    gb.remove("lines", non_gb_lines)

    res = eur.merge(gb, with_time=False)

    return res


def add_waste_element(
    n_gb: pypsa.Network,
    n_merged: pypsa.Network,
) -> pypsa.Network:
    """
    Adds a global source of waste to the TYNDP model.

    Parameters
    ----------
    n_gb : pypsa.Network
        The GB model, used to get the assumptions for waste generation and costs
    n_merged : pypsa.Network
        The merged model, to which the waste element will be added.
    """
    n_merged.add(
        "Bus",
        name="EU waste",
        carrier="waste",
        unit="MWh_LHV",
        location="EU",
    )

    ref_waste_gens = n_gb.generators[n_gb.generators.carrier == "waste"]
    # normalize cost against biomass
    cc_adjustment = (
        (
            n_merged.generators[
                n_merged.generators.carrier == "solid biomass"
            ].capital_cost.iloc[0]
        )
        / (n_gb.generators[n_gb.generators.carrier == "biomass"].capital_cost.iloc[0])
    )
    mc_adjustment = (
        (
            n_merged.generators[
                n_merged.generators.carrier == "solid biomass"
            ].marginal_cost.iloc[0]
        )
        / (n_gb.generators[n_gb.generators.carrier == "biomass"].marginal_cost.iloc[0])
    )
    normalized_cap_cost = ref_waste_gens.capital_cost[0] * (cc_adjustment)
    normalized_marginal_cost = ref_waste_gens.marginal_cost[0] * (mc_adjustment)

    n_merged.add(
        "Link",
        name="ref link: waste",
        bus0="EU waste",
        bus1="ref",
        bus2="co2 atmosphere",
        efficiency=0.2102,  # hard coded from fes_powerplants_inc_tech_data.csv
        efficiency2=0,  # EU regs consider waste to be a non-emitting renewable
        capital_cost=normalized_cap_cost,
        marginal_cost=normalized_marginal_cost,
        marginal_cost_quadratic=0,
    )
    return n_merged


def add_co2_multilink(
    n: pypsa.Network, eur: pypsa.Network, carrier_map: dict[str, str]
) -> pypsa.Network:
    """
    Replaces conventional generators of type Generator in the GB model with corresponding multilinks
    to track CO2 emissions to atmosphere. Aligns generators with the cost given by the TYNDP model
    """
    emitting_carriers = eur.links.carrier.unique()
    for gb_carrier, eur_carrier in carrier_map["Generator"].items():
        gens = n.generators[
            (n.generators.carrier == gb_carrier)
            & (n.generators.bus.str.startswith("GB"))
        ]
        ref = eur.links[
            (eur.links.carrier == eur_carrier) & (eur.links.bus1.str.startswith("GB"))
        ]

        # if there is one or more corresponding emitting generators represented as a link in the tyndp model
        if eur_carrier in emitting_carriers:
            n.add(
                "Link",
                name=gens.index,
                bus0=ref.bus0.mode().iloc[0],  # global supply bus
                bus1=gens.bus,
                bus2="co2 atmosphere",  # co2 atmosphere
                p_nom=gens.p_nom,
                efficiency=ref.efficiency.mean(),
                efficiency2=ref.efficiency2.mean(),
                capital_cost=ref.capital_cost.mean(),
                marginal_cost=ref.marginal_cost.mean(),
                marginal_cost_quadratic=ref.marginal_cost_quadratic.mean(),  # not used currently
            )

            # remove the generator after the link version is created
            n.remove("Generator", gens.index)

    return n


def align_tech_econ_assumptions(
    n: pypsa.Network, eur: pypsa.Network, carrier_map: dict[str, str]
) -> pypsa.Network:
    """
    Checks for the technical and economic assumptions of non generator based components
    """
    for comp in ["Store", "StorageUnit", "Links"]:
        # identifies params we are interested in
        cols = [
            col
            for col in eur.storage_units.columns
            if re.search("cost|efficiency|loss", col)
        ]
        carriers = carrier_map[comp]
        for carrier_gb, carrier_eur in carriers.items():
            # isolate entries in the merged network with the carrier
            carrier_mask = n.c[comp].static[n.c[comp].static.carrier == carrier_eur]
            # ref values from the original eur network
            eur_carrier_mask = eur.c[comp].static[
                eur.c[comp].static.carrier == carrier_eur
            ]
            # picks one of the reference components off the top of the list
            ref_components = eur.c[comp].static.loc[eur_carrier_mask, carrier_eur]
            ref_component = ref_components.iloc[0]
            for col in cols:
                n.c[comp].static.loc[carrier_mask, col] = ref_component[col]

    return n


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake
        from pathlib import Path

        snakemake = mock_snakemake(
            Path(__file__).stem,
            planning_horizon="2030",
        )

    carrier_map = snakemake.params.carrier_map
    n_gb = pypsa.Network(snakemake.input.gb_model)
    n_eur = pypsa.Network(snakemake.input.iem_model)

    n_merged = merge_gb_tyndp(n_gb.copy(), n_eur.copy(), carrier_map)
    n_merged = add_waste_element(n_gb, n_merged)
    n_merged = add_co2_multilink(n_merged, n_eur, carrier_map)
    # implementation TBD
    # n_merged = align_tech_econ_assumptions(n_merged, n_eur, carrier_map)

    n_merged.consistency_check()
    n_merged.export_to_netcdf(snakemake.output[0])
