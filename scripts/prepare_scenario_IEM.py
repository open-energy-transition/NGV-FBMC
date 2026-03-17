# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: MIT

import logging
import re
import pypsa
import pandas as pd
import yaml
import numpy as np

logger = logging.getLogger(__name__)


def merge_gb_tyndp(
    gb: pypsa.Network, eur: pypsa.Network, carrier_map: dict[str, str]
) -> pypsa.Network:
    """
    Combines the TYNDP (EUR) and GB-dispatch (GB) models
    """

    # No one knows where this came from. It serves no purpose.
    eur.remove("Bus", "EU")

    # prepare eur network by removing GB elements from the openTYNDP model
    # (i.e. either GB based or offshore hub buses)
    for comp in ["Bus", "StorageUnit", "Link", "Store", "Generator", "Load"]:
        idx = (
            eur.c[comp]
            .static[
                eur.c[comp].static.index.str.contains("GB00")
                | eur.c[comp].static.index.str.contains("GB H2")
                | eur.c[comp].static.index.str.contains("GBOH")
            ]
            .index
        )
        eur.remove(comp, idx)

        # Remove also any components that are either located on these buses
        # or connected to at least with one port to any of these buses
        bus_cols = [col for col in eur.c[comp].static.columns if col.startswith("bus")]
        for col in bus_cols:
            idx = (
                eur.c[comp]
                .static.loc[(eur.c[comp].static[col].isin(["GB00", "GB H2"]))]
                .index
            )
            eur.remove(comp, idx)

    # create a mapping for the old GB names to the EUR names in TYNDP
    # note some non GB countries have multiple buses in TYNDP
    # the current assignment method only keeps buses that are connected to GB

    # for reference, the remaining buses in each bidding zone in each country
    eur_elec_buses = eur.buses[eur.buses.carrier == "AC"].index

    # prepare gb network
    non_gb_buses = gb.buses[~(gb.buses.index.str.contains("GB"))]
    non_gb_ac_buses = gb.buses[
        ~(gb.buses.index.str.contains("GB")) & (gb.buses.carrier == "AC")
    ]
    non_gb_h2_buses = gb.buses[
        ~(gb.buses.index.str.contains("GB")) & (gb.buses.carrier == "H2")
    ]

    gb_eur_busmap = {}
    for name, bus in non_gb_ac_buses.iterrows():
        # Find the bidding zones of the countries, matching to the GB model country node
        country_matches = [
            eur_bus for eur_bus in eur_elec_buses if eur_bus.startswith(name)
        ]
        # Preserve these bidding zones...
        buses_keep = ["DKW1", "NOS0", "FR00"]
        intersection = list(set(buses_keep) & set(country_matches))

        # ... for all others map to the first best match
        gb_eur_busmap[name] = intersection[0] if intersection else country_matches[0]

    for name, bus in non_gb_h2_buses.iterrows():
        # Find the bidding zones of the countries, matching to the GB model country node
        country_matches = [
            eur_bus for eur_bus in eur_elec_buses if eur_bus.startswith(name[:-3])
        ]
        # Preserve these bidding zones...
        buses_keep = ["DKW1 H2", "NOS0 H2", "FR00 H2"]
        intersection = list(set(buses_keep) & set(country_matches))
        # ... for all others map to the first best match
        gb_eur_busmap[name] = intersection[0] if intersection else country_matches[0]

    # Remove all non-GB buses from the network
    gb.remove("Bus", non_gb_buses.index)

    # remove components associated with these buses
    for comp in ["Store", "Generator", "Load", "StorageUnit"]:
        non_gb_comp_idx = (
            gb.c[comp].static[gb.c[comp].static.bus.isin(non_gb_buses.index)].index
        )
        gb.remove(comp, non_gb_comp_idx)

    # Remove all Links and Lines where no port is connected to GB
    for comp in gb.components[["Link", "Line"]]:
        # Find all "bus\d" columns
        bus_cols = [col for col in comp.static.columns if col.startswith("bus")]

        # Determine all components for which all buses are connected to non-GB buses ...
        comp_i = comp.static.loc[
            (
                (comp.static[bus_cols].isin(non_gb_buses.index))
                | (comp.static[bus_cols] == "")
            ).all(axis="columns")
        ].index

        # ... and remove those components from the GB model
        gb.remove(comp.name, comp_i)

    # check all carriers are accounted for in the mapping
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

    # Prepare to connect the interconnectors in the GB model to the buses in open-tyndp
    # Rename first, such that the connections match after merging the networks later
    # (this only affects Link components. Lines are not inter-country and all other components
    # are only attached to a single bus, which is either part of the model or not)
    bus_cols = [col for col in gb.c["Link"].static.columns if col.startswith("bus")]
    gb.c["Link"].static[bus_cols] = gb.c["Link"].static[bus_cols].replace(gb_eur_busmap)

    # Manual correction necessary, as there is not GBNI equivalent in GB model
    # and interconnectors GBNI <-> GB are connected to IE bus in GB model
    for link_name in ["Gallant", "Moyle"]:
        link = gb.links.query("`name`.str.contains(@link_name)")
        if len(link) != 1:
            logger.warning(
                f"Expected exactly one interconnector with name containing {link_name}. "
                f"Found {len(link)}. Please check the GB model for the interconnectors and their connections to buses. "
                f"Skipping manual correction for {link_name} interconnector."
            )
            continue
        if link["bus1"].iloc[0] != "IE00":
            logger.error(
                f"Interconnector {link_name} is not connected to IE00 bus in GB model as expected."
                f" Please check the GB model for the interconnectors and their connections to buses."
            )
        gb.c["Link"].static.loc[link.index, "bus1"] = "GBNI"

    # Map carriers from GB model to carrier names in the openTYNDP model
    # leave generators for now, they are reassigned in the convert_generators_to_links function
    for comp in gb.components[["Link", "Store", "StorageUnit", "Load"]]:
        comp.static["carrier"] = comp.static["carrier"].replace(carrier_map[comp.name])

    # remove load shedding elements - they are named slightly different in the GB model
    load_shedding_gens = gb.generators[gb.generators.carrier == "Load Shedding"]
    gb.remove("Generator", load_shedding_gens.index)

    # pypsa merge doesn't like overlapping components
    gb.remove("Carrier", gb.carriers.index.intersection(eur.carriers.index))

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
        p_nom_extendable=False,
        p_nom=np.inf,
        marginal_cost={
            2030: 19.0145,
            2040: 21.131,
        }[
            planning_horizon
        ],  # Costs for waste as fuel from fes_powerplants_inc_tech_data.csv
    )

    ref_waste_gens = n_gb.c["Generator"].static.loc[
        (n_gb.c["Generator"].static.carrier == "waste")
        & (n_gb.c["Generator"].static.bus.str.match(r"GB "))
    ]

    # Attach the electricity from waste generator as link to all GB buses with AC carrier
    n_merged.add(
        "Link",
        name=ref_waste_gens.index,
        bus0="EU waste",
        bus1=ref_waste_gens["bus"],
        bus2="co2 atmosphere",
        carrier="waste",
        efficiency=0.2102,  # hard coded from fes_powerplants_inc_tech_data.csv
        efficiency2=0,  # EU regs consider waste to be a non-emitting renewable
        p_nom_extendable=ref_waste_gens["p_nom_extendable"],
        p_nom=ref_waste_gens["p_nom"],
        capital_cost=ref_waste_gens[
            "capital_cost"
        ],  # Not normalised, as capacity expansion is off, this number will not affect the model results
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

    # Remove the original waste generators after the link version is created
    n_merged.remove("Generator", ref_waste_gens.index)

    return n_merged


def convert_generators_to_links(
    n_merged: pypsa.Network, n_eur: pypsa.Network, carrier_map: dict[str, str]
) -> pypsa.Network:
    """
    Replaces conventional generators of type Generator in the GB model with corresponding multilinks
    to track CO2 emissions to atmosphere. Aligns generators with the cost given by the TYNDP model
    """

    global_supply_map = n_merged.generators[
        n_merged.generators.bus.str.startswith("EU")
    ].set_index("carrier")

    # Some generation technologies in the TYNDP are represented by Link components
    # to track fuel use and emissions, rather than as Generators
    # Convert them from Generators to Links in the merged model, using the TYNDP assumptions for costs and efficiencies
    # The remaining technologies where technologies are represented as Generators in both models will be
    # aligned with their carrier names, but the components will not be converted to Links
    for gb_carrier, eur_carrier in carrier_map["Generator"].items():
        # check that the generator type isn't intended to stay as a generator (e.g. solar and other renewables)
        # for those the generator carrier is only changed, all others are convert to links
        if (
            ("solar" in eur_carrier)
            or ("wind" in eur_carrier)
            or ("geothermal" in eur_carrier)
        ):
            n_merged.c["Generator"].static.loc[
                n_merged.c["Generator"].static.carrier == gb_carrier, "carrier"
            ] = eur_carrier
        else:
            gens = n_merged.generators[
                (n_merged.generators.carrier == gb_carrier)
                & (n_merged.generators.bus.str.startswith("GB "))
            ]

            # Change the carrier name for generators
            n_merged.c["Generator"].static.loc[gens.index, "carrier"] = eur_carrier

            # Change from Generator to Link if the technology is represented as a Link in the TYNDP model
            ref = n_eur.links[
                (n_eur.links.carrier == eur_carrier)
                & (n_eur.links.bus1.str.startswith("GB"))
            ]

            # Some emitting generators have no reference links that exist (e.g. waste)
            if ref.empty and eur_carrier in global_supply_map.index:
                ref = gens
                ref = ref.assign(
                    bus0=global_supply_map.loc[eur_carrier, "bus"],
                    bus1=gens.bus,
                    # for non emitters should be nothing/nan - but doesn't matter for accounting as long as efficiency is correctly 0
                    bus2="co2 atmosphere",
                    efficiency2=0.0,
                )

            if not gens.empty:
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
                    p_nom=gens.p_nom / ref.efficiency.mean(),
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
                        n_gb.c["Generator"]
                        .dynamic[p_lim]
                        .columns.intersection(gens.index)
                    )
                    if mask.empty:
                        continue

                    n_merged.c["Link"].dynamic[p_lim].loc[:, mask] = (
                        n_gb.c["Generator"].dynamic[p_lim].loc[:, mask]
                    )

                # remove the generator after the link version is created
                n_merged.remove("Generator", gens.index)

    # Merge adds some additional attributes that all Link components need to have
    # fill in those for the GB model with bool values
    n_merged.c["Link"].static["reversed"] = (
        n_merged.c["Link"].static["reversed"].fillna(False).astype(bool)
    )
    # Drop `project_status` no longer required downstream
    n_merged.c["Link"].static = n_merged.c["Link"].static.drop(
        columns=["project_status"], errors="raise"
    )

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


def reset_network(n: pypsa.Network) -> pypsa.Network:
    """
    Removes outputs from the network added after an optimisation run.

    This prevents confusion between outputs of different iterations if not all values are overwritten and reduces the file size.

    Parameters
    ----------
    n : pypsa.Network
        The network from which dynamic outputs should be removed.
    """
    logger.info("Resetting network")

    for comp in n.components:
        # Reset static components
        defaults = comp.defaults.loc[
            (comp.defaults["status"] == "Output") & (comp.defaults["static"]), "default"
        ]
        comp.static = comp.static.assign(**defaults.to_dict())

        # Reset dynamic components
        defaults = comp.defaults.loc[
            (comp.defaults["status"] == "Output") & (comp.defaults["varying"]),
            "default",
        ]
        for attribute in defaults.index:
            if attribute in comp.dynamic:
                comp.dynamic[attribute] = comp.dynamic[attribute].drop(
                    columns=comp.dynamic[attribute].columns
                )

    return n


def cluster_network_by_time(n: pypsa.Network, time_aggregation: dict) -> pypsa.Network:
    """
    Clusters the network by time using the specified time aggregation method and parameters.

    Parameters
    ----------
    n : pypsa.Network
        The network to be clustered.
    time_aggregation : dict
        The time aggregation method and parameters to be used for clustering.

    Returns
    -------
    pypsa.Network
        The network clustered along the snapshot (time) dimension.
    """
    logger.info(
        f"Clustering network by time using method {time_aggregation['method']} and parameters {time_aggregation['parameters']}"
    )

    func = getattr(n.cluster.temporal, time_aggregation["method"])

    return func(**time_aggregation["parameters"])


def remove_unused_carriers(n: pypsa.Network) -> pypsa.Network:
    """
    Removes carriers that are not used by any component in the network after merging.
    """
    carriers = set()
    for comp in n.components[
        ["Link", "Store", "StorageUnit", "Generator", "Load", "Bus"]
    ]:
        carriers = carriers.union(comp.static.carrier)

    non_used_carriers = set(n.carriers.index) - carriers
    n.remove("Carrier", non_used_carriers)

    return n


def reorder_line_directions(
    n: pypsa.Network, manual_boundaries_fp: str
) -> pypsa.Network:
    """
    Reorders the line directions in the network to ensure that they are consistent with the specified boundaries.

    The clustering algorithm does not deterministically assign the same line directions (bus0, bus1) in each run.
    In order to align with externally calculated PTDF data, we correct the line directions at this point
    to ensure they are consistent with the specified boundaries and therefore also between runs.

    Parameters
    ----------
    n : pypsa.Network
        The network for which the line directions should be reordered.
    manual_boundaries_fp : str
        The file path to the manual boundary definitions.


    Returns
    -------
    pypsa.Network
        The network with reordered line directions.
    """
    # Load external boundary crossings file
    with open(manual_boundaries_fp) as f:
        boundaries_yaml = yaml.safe_load(f)

    # Flatten the yaml for reading as pd.DataFrame
    boundaries_flat = {}
    idx = 0
    for k, v in boundaries_yaml.items():
        c = "Line" if "line" in k else "Link"

        for boundary, entries in v.items():
            for entry in entries:
                boundaries_flat[idx] = {
                    "component": c,
                    "Boundary_n": boundary,
                    "bus0": f"GB {entry['bus0']}",
                    "bus1": f"GB {entry['bus1']}",
                }
                idx += 1

    boundaries = pd.DataFrame.from_dict(boundaries_flat, orient="index")

    logger.info("Reordering line directions")
    for comp_name, boundary, bus0, bus1 in boundaries.itertuples(index=False):
        correct = n.c[comp_name].static.query(
            "`bus0` == @bus0 and `bus1` == @bus1",
            local_dict={"bus0": bus0, "bus1": bus1},
        )
        switched = n.c[comp_name].static.query(
            "`bus0` == @bus0 and `bus1` == @bus1",
            local_dict={"bus0": bus1, "bus1": bus0},
        )

        if correct.empty and switched.empty:
            logger.error(
                f"Expected {comp_name} between {bus0} and {bus1} but None found in the network. "
                f"Check whether the {comp_name} is missing or whether the manual boundary definition is incorrect."
            )
        elif not switched.empty:
            logger.info(
                f"Switching direction of flow for {comp_name}: {switched.index.tolist()} to {bus0} -> {bus1}"
            )
            n.c[comp_name].static.loc[switched.index, ["bus0", "bus1"]] = (bus0, bus1)
        else:
            # Correctly oriented, nothing to do
            pass

    return n


def remove_erroneous_line(n: pypsa.Network) -> pypsa.Network:
    """
    Patches the network by removing an erroneously picked up line between GB EC5 and GB SC3-SC2.

    This is an offshore line connecting an offshore wind hub with onshore, but is wrongly picked up by the processing.
    Until this is fixed upstream, we remove the line manually to avoid issues with the model results and downstream processing.
    """
    idx = n.lines.query(
        "`bus0` in ['GB EC5', 'GB SC3-SC2'] and `carrier` in ['AC', 'DC']"
    ).index
    if not idx.empty:
        logger.info(
            f"Path: Removing line {idx[0]} between GB EC5 and GB SC3-SC2 which is erroneously picked up in the model."
        )
        n.remove("Line", idx)
    else:
        logger.error(
            f"Path: Expected to find one line connecting GB EC5 and GB SC3-SC2 with carrier AC or DC to remove, but found {len(idx)}. "
            f"Check for line existence/missing!"
        )

    return n


def patch_EU_fuel_generators(n: pypsa.Network) -> pypsa.Network:
    """
    Patches the bus names to have infinite capacity with p_nom_extendable=False for consistency across scenarios.

    Parameters
    ----------
    n : pypsa.Network
        The network for which the bus names should be patched.
    """

    # Fuel generators to be affected
    idx = [
        "EU lignite",
        "EU coal",
        "EU oil primary",
        "EU uranium",
        "EU gas",
        "EU biogas",
        "EU solid biomass",
        "EU waste",
    ]

    logger.info(f"Patching EU fuel generators: {idx}")

    n.c.generators.static.loc[idx, "p_nom"] = np.inf
    n.c.generators.static.loc[idx, "p_nom_extendable"] = False

    return n


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake
        from pathlib import Path

        snakemake = mock_snakemake(
            Path(__file__).stem,
            planning_horizons="2030",
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

    # Reset networks before merging
    n_gb = reset_network(n_gb)
    n_eur = reset_network(n_eur)

    # Merge the two networks
    n_merged = merge_gb_tyndp(n_gb.copy(), n_eur.copy(), carrier_map)

    # Convert WtE generators to links
    n_merged = add_waste_element(
        n_gb=n_gb,
        n_merged=n_merged,
        planning_horizon=int(snakemake.wildcards.planning_horizons),
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

    n_merged = remove_unused_carriers(n_merged)

    n_merged = patch_EU_fuel_generators(n_merged)

    # After merging we get rid of all attributes that are potential outputs from the model
    # Due to https://github.com/PyPSA/PyPSA/issues/1606 we do this on the individual networks before the merge
    # and on the merged network again
    n_merged = reset_network(n_merged)

    # Reorder line directions
    n_merged = reorder_line_directions(
        n=n_merged,
        manual_boundaries_fp=snakemake.input.external_boundary_definitions,
    )

    # Patch network: Remove erroneously picked up line between GB EC5 and GB SC3-SC2
    n_merged = remove_erroneous_line(n_merged)

    # Cluster the network by time
    # We intentionally cluster on the IEM network, rather than at a later stage, e.g. during solve_network
    # The reason is that we want the three scenarios, IEM, SQ, TF, to behave as similarly as possible.
    # If we cluster later during solve_network, the clustering might yield different results due to
    # different time-series in the scenarios.
    # By clustering before, we avoid this potential issue
    if snakemake.params.time_aggregation["enable"]:
        n_merged = cluster_network_by_time(n_merged, snakemake.params.time_aggregation)

    # Make it easier for downstream rules to identify GB buses and components by assigning a country attribute
    n_merged.buses.loc[n_merged.buses.index.str.match(r"GB\s+"), "country"] = "GB"
    n_merged.generators.loc[
        n_merged.generators.index.str.match(r"GB\s+"), "country"
    ] = "GB"
    n_merged.links.loc[n_merged.links.index.str.match(r"GB\s+"), "country"] = "GB"
    # also remove the sometimes wrongly assigned "GB" country from GBNI-components
    n_merged.buses.loc[n_merged.buses.index.str.match("GBNI"), "country"] = "GBNI"
    n_merged.generators.loc[n_merged.generators.index.str.match("GBNI"), "country"] = (
        "GBNI"
    )
    n_merged.links.loc[n_merged.links.index.str.match("GBNI"), "country"] = "GBNI"

    # Never hurts
    n_merged.consistency_check(strict=None)

    # Give the new network a proper name
    n_merged.name = (
        f"Integrated Energy Market (IEM) - {snakemake.wildcards.planning_horizons}"
    )

    # Export to file
    n_merged.export_to_netcdf(snakemake.output["model"])
