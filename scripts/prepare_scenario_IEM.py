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

    res = eur.merge(gb, with_time=True)

    return res


def add_waste_element(
    n_gb: pypsa.Network,
    n_merged: pypsa.Network,
    planning_horizon: int,
) -> pypsa.Network:
    """
    Adds a global source of waste to the TYNDP model.

    Needs to be processed separately for a lack of WtE in the current openTYNDP model version.

    Parameters
    ----------
    n_gb : pypsa.Network
        The GB model, used to get the assumptions for waste generation and costs
    n_merged : pypsa.Network
        The merged model, to which the waste element will be added.
    planning_horizon : int
        The planning horizon, used to determine the cost assumptions for waste generation.
    """
    # Source for "waste" as fuel
    n_merged.add(
        "Bus",
        name="EU waste",
        carrier="waste",
        unit="MWh_LHV",
        location="EU",
    )
    n_merged.add(
        "Generator",
        name="EU waste",
        bus="EU waste",
        carrier="waste",
        p_nom_extendable=True,
        marginal_cost={
            2030: 19.0145,
            2040: 21.131,
        }[
            planning_horizon
        ],  # Costs for waste as fuel from fes_powerplants_inc_tech_data.csv
    )

    ref_waste_gens = n_gb.c["Generator"].static.loc[
        (n_gb.c["Generator"].static.carrier == "waste")
        & (n_gb.c["Generator"].static.bus.str.match(r"GB \d{1,2}"))
    ]

    # Attach the electricity from waste generator as link to all GB buses with AC carrier
    n_merged.add(
        "Link",
        name=ref_waste_gens["bus"].to_numpy(),
        suffix=" waste for electricity",
        bus0="EU waste",
        bus1=ref_waste_gens["bus"].to_numpy(),
        bus2="co2 atmosphere",
        carrier="waste",
        efficiency=0.2102,  # hard coded from fes_powerplants_inc_tech_data.csv
        efficiency2=0,  # EU regs consider waste to be a non-emitting renewable
        p_nom_extendable=ref_waste_gens["p_nom_extendable"].to_numpy(),
        p_nom=ref_waste_gens["p_nom"].to_numpy(),
        capital_cost=ref_waste_gens[
            "capital_cost"
        ].to_numpy(),  # Not normalised, as capacity expansion is off, this number will not affect the model results
        marginal_cost=3.145,  # from fes_powerplants_inc_tech_data.csv, not normalized for lack of suitable reference
        marginal_cost_quadratic=0,
    )

    # Need to transfer the dynamic constraints for the waste generators separately
    for p_lim in ["p_min_pu", "p_max_pu"]:
        mask = (
            n_gb.c["Generator"]
            .dynamic[p_lim]
            .columns.intersection(ref_waste_gens.index)
        )
        if mask.empty:
            continue

        n_merged.c["Generator"].dynamic[p_lim] = (
            n_gb.c["Generator"].dynamic[p_lim].loc[:, mask]
        )

    return n_merged


def convert_generators_to_links(
    n_merged: pypsa.Network, n_eur: pypsa.Network, carrier_map: dict[str, str]
) -> pypsa.Network:
    """
    Replaces conventional generators of type Generator in the GB model with corresponding multilinks
    to track CO2 emissions to atmosphere. Aligns generators with the cost given by the TYNDP model
    """

    # Some generation technologies in the TYNDP are represented by Link components
    # to track fuel use and emissions, rather than as Generators
    # Convert them from Generators to Links in the merged model, using the TYNDP assumptions for costs and efficiencies
    # The remaining technologies where technologies are represented as Generators in both models will be
    # aligned with their carrier names, but the components will not be converted to Links
    for gb_carrier, eur_carrier in carrier_map["Generator"].items():
        gens = n_merged.generators[
            (n_merged.generators.carrier == gb_carrier)
            & (n_merged.generators.bus.str.startswith("GB "))
        ]
        # Change the carrier name for generators
        n_merged.c["Generator"].static.loc[gens.index, "carrier"] = eur_carrier

        # Change from Generator to Link if the technology is represented as a Link in the TYNDP model
        ref = n_eur.links[
            (n_eur.links.carrier == eur_carrier)
            & (n_eur.links.bus1.str.startswith("GB "))
        ]
        if not ref.empty and not gens.empty:
            logger.info(
                f"Converting {gb_carrier} generators to links with carrier {eur_carrier}"
            )

            n_merged.add(
                "Link",
                name=gens.index,
                bus0=ref.bus0.mode().iloc[0],  # global supply bus
                bus1=gens.bus,
                bus2=ref.bus2.mode().iloc[
                    0
                ],  # co2 atmosphere for emitting generators or nothing
                carrier=eur_carrier,
                p_nom=gens.p_nom,
                p_nom_extendable=gens.p_nom_extendable,
                efficiency=ref.efficiency.mean(),
                efficiency2=ref.efficiency2.mean(),
                capital_cost=ref.capital_cost.mean(),
                marginal_cost=ref.marginal_cost.mean(),
                marginal_cost_quadratic=ref.marginal_cost_quadratic.mean(),  # not used currently
            )

            # Transfer constraints on dynamic p_min_pu and p_max_pu from the generator to the link if they exist
            for p_lim in ["p_min_pu", "p_max_pu"]:
                logger.info(
                    f"Adding dynamic constraints {p_lim} for former {gb_carrier} generators"
                )
                mask = (
                    n_gb.c["Generator"].dynamic[p_lim].columns.intersection(gens.index)
                )
                if mask.empty:
                    continue

                n_merged.c["Link"].dynamic[p_lim].loc[:, mask] = (
                    n_gb.c["Generator"].dynamic[p_lim].loc[:, mask]
                )

            # remove the generator after the link version is created
            n_merged.remove("Generator", gens.index)

    return n_merged


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


def remove_components_added_in_solve_network_py(n: pypsa.Network) -> pypsa.Network:
    """
    Removes components that are commonly added in solve_network.py.

    This is used if the same network file is reused across multiple runs of solve_network.py,
    in this case where we reuse the IEM run for additional modelling runs.
    By removing the components, `solve_network.py` can be run again without errors about components already existing in the network
    and the components can be cleanly added again with the correct assumptions for each run.

    Parameters
    ----------
    n : pypsa.Network
        The network from which components should be removed.
    """
    logger.info(f"Removing components added in solve_network.py for network {n.name}")

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


def fix_electrolysis_dispatch(n: pypsa.Network) -> pypsa.Network:
    """
    Enforce the electrolysis dispatch to the optimal dispatch found in the solved network.

    Parameters
    ----------
    n : pypsa.Network
        The network for which the electrolysis dispatch should be fixed.
    """
    logger.info(f"Fixing electrolysis dispatch for network {n.name}")
    electrolysis_i = n.links[n.links.carrier == "H2 Electrolysis"].index
    n.links_t.p_set.loc[:, electrolysis_i] = n.links_t.p0.loc[:, electrolysis_i]
    return n


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake
        from pathlib import Path

        snakemake = mock_snakemake(
            Path(__file__).stem,
            planning_horizon="2030",
        )

    # Map from configfile
    carrier_map = snakemake.params.carrier_map

    # Load networks
    n_gb = pypsa.Network(snakemake.input.gb_model)
    n_eur = pypsa.Network(snakemake.input.iem_model)

    # Preprocess networks before merging
    n_gb = remove_components_added_in_solve_network_py(n_gb)
    n_eur = remove_components_added_in_solve_network_py(n_eur)

    # Fix dispatch for electrolysis to the optimal dispatch found in the solved network for EUR
    # but not for GB - we want GB to have the freedom to find the optimal dispatch in later
    # dispatch runs as this is model internal electricity demand for the model
    n_eur = fix_electrolysis_dispatch(n_eur)

    # Merge the two networks
    n_merged = merge_gb_tyndp(n_gb.copy(), n_eur.copy(), carrier_map)

    # Convert WtE generators to links
    n_merged = add_waste_element(
        n_gb=n_gb,
        n_merged=n_merged,
        planning_horizon=int(snakemake.wildcards.planning_horizon),
    )

    # Convert generators to links for most conventional technologies
    n_merged = convert_generators_to_links(
        n_merged=n_merged, n_eur=n_eur, carrier_map=carrier_map
    )

    # TODO implementation - required?
    # n_merged = align_tech_econ_assumptions(n_merged, n_eur, carrier_map)

    # Logging for information: All remaining expandable components in the model
    logger.info("All remaining expandable components in the merged model are:")
    for c in n_merged.components:
        if "p_nom_extendable" in c.static.columns:
            col = "p_nom_extendable"
        elif "e_nom_extendable" in c.static.columns:
            col = "e_nom_extendable"
        else:
            continue

        if c.static[col].any():
            logger.info(f"{c}: {c.static.query(f'{col}').index.tolist()}")

    # Never hurts
    n_merged.consistency_check()

    # Export to file
    n_merged.export_to_netcdf(snakemake.output["model"])
