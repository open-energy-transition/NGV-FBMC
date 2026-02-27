# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: MIT

import logging
import re

import numpy as np
import pandas as pd
import pypsa

logger = logging.getLogger(__name__)

def merge_gb_tyndp(gb, eur, carrier_map):
	# prepare eur network by removing GB elements
	for comp in ["Bus", "StorageUnit", "Link", "Store", "Generator", "Load"]:
		idx = eur.c[comp].static[eur.c[comp].static.index.str.contains("GB00") | eur.c[comp].static.index.str.contains("GBOH")].index
		eur.remove(comp, idx)
		cols = [col for col in eur.c[comp].static.columns if col.startswith('bus')]
		for col in cols:
			idx = eur.c[comp].static.loc[eur.c[comp].static[col] == "GB00"].index
			eur.remove(comp, idx)

	# for reference, the remaining buses in each bidding zone in each country
	eur_elec_buses = eur.buses[eur.buses.carrier == 'AC'].index

	# prepare gb network
	non_gb_buses = gb.buses[(gb.buses.carrier == 'AC') & ~(gb.buses.index.str.contains("GB"))]
	non_gb_buses_h2 = gb.buses[(gb.buses.carrier == 'H2') & ~(gb.buses.index.str.contains("GB"))]
		
	# create a mapping for the old GB names to the EUR names in TYNDP
	# note some non GB countries have multiple buses in TYNDP
	# the current assignment method (below) is arbitary
	gb_eur_busmap = {}
	for name, bus in non_gb_buses.iterrows():
		# is it ok to rename the buses here or do i need to make a new bus using network remove/add property
		# note that the network gets merged later on
		country_matches = [eur_bus for eur_bus in eur_elec_buses if eur_bus.startswith(name)]
		buses_keep = ['DKW1', 'NOS0', 'FR00']
		intersection = list(set(buses_keep) & set(country_matches))
		gb_eur_busmap[name] = intersection[0] if intersection else country_matches[0]

	for name, bus in non_gb_buses_h2.iterrows():
		# is it ok to rename the buses here or do i need to make a new bus using network remove/add property
		# note that the network gets merged later on
		country_matches = [eur_bus for eur_bus in eur_elec_buses if eur_bus.startswith(name[:-3])]
		buses_keep = ['DKW1 H2', 'NOS0 H2', 'FR00 H2']
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
			if not carrier in eur_carriers:
				# check for mapping in the carrier map
				if not carrier in carrier_map[comp].keys():
					logger.warning(f"Cannot find mapped value for carrier {carrier} component type {comp}")

	# connect to buses as named in open-tyndp
	for comp in ["Link", "Store", "StorageUnit", "Generator", "Load"]:
		cols = [col for col in gb.c[comp].static.columns if col.startswith('bus')]
		for col in cols:
			gb.c[comp].static[col] = gb.c[comp].static[col].replace(gb_eur_busmap)

		if "location" in gb.c[comp].static.columns:
			gb.c[comp].static["location"] = gb.c[comp].static["location"].replace(gb_eur_map)

		if "carrier" in gb.c[comp].static.columns:
			gb.c[comp].static["carrier"] = gb.c[comp].static["carrier"].replace(carrier_map[comp])

	# pypsa merge doesn't like overlapping components
	gb.remove("Carrier", gb.carriers.index.intersection(eur.carriers.index))
	gb.remove("Bus", non_gb_buses_h2.index)

	non_gb_lines = gb.lines[~(gb.lines.bus0.str.contains('GB')) & ~(gb.lines.bus1.str.contains('GB'))].index
	gb.remove("lines", non_gb_lines)

	gb.set_snapshots(eur.snapshots)
	res = eur.merge(gb, with_time=True)

	return res

def add_co2_multilink(n, eur, carrier_map):
	# this is the carrier map essentially so it needs to be cleaned up
	emitting_carriers = eur.links.carrier.unique()
	for gb_carrier, eur_carrier in carrier_map["Generator"].items():
		if eur_carrier in emitting_carriers:
			gens = n.generators[(n.generators.carrier == gb_carrier) & (n.generators.bus.str.startswith('GB'))]
			ref = eur.links[(eur.links.carrier == eur_carrier)]

			# add a copy of the generator as a link - use european model as a reference for unknown efficiencies
			n.add(
				"Link",
				name = gens.index,
				bus0 = ref.bus0.mode()[0], # global supply bus
				bus1 = gens.bus,
				bus2 = ref.bus2.mode()[0], # co2 atmosphere
				p_nom = gens.p_nom,
				efficiency = gens.efficiency, #ref.efficiency.mean(),
				efficiency2 = ref.efficiency2.mean(),
				capital_cost = ref.capital_cost.mean(),
				marginal_cost = ref.marginal_cost.mean(),
				marginal_cost_quadratic = ref.marginal_cost_quadratic.mean() # not used currently
			)

			# remove the generator after the link version is created
			n.remove('Generator', gens.index)
	
	return n


if __name__ == "__main__":
	if "snakemake" not in globals():
		from scripts._helpers import mock_snakemake

		snakemake = mock_snakemake()

	carrier_map = snakemake.params.carrier_map	
	# todo: change to snakemake input 
	n_gb = pypsa.Network(snakemake.input.gb_model)
	n_eur = pypsa.Network(snakemake.input.iem_model)

	n_merged = merge_gb_tyndp(n_gb, n_eur, carrier_map)
	n_merged = add_co2_multilink(n_merged, n_eur, carrier_map)

	n_merged.export_to_netcdf(snakemake.output[0])
