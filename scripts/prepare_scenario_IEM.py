# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: MIT

import logging
import re

import numpy as np
import pandas as pd
import pypsa

def merge_gb_tyndp(gb, eur):
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
		country_matches = [eur_bus for eur_bus in eur_h2_buses if eur_bus.startswith(name[:-3])]
		buses_keep = ['DKW1 H2', 'NOS0 H2', 'FR00 H2']
		intersection = list(set(buses_keep) & set(country_matches))
		gb_eur_busmap[name] = intersection[0] if intersection else country_matches[0]

	# renaming the buses doesn't help since the str is carried to each component
	# also if you rename the buses the pypsa merge requires you to drop them
	for comp in ["Link", "Store", "StorageUnit", "Generator", "Load"]:
		cols = [col for col in gb.c[comp].static.columns if col.startswith('bus')]
		for col in cols:
			gb.c[comp].static[col] = gb.c[comp].static[col].replace(gb_eur_busmap)

	# these buses are no longer relevant
	gb.remove("Bus", non_gb_buses)

	# rename the carriers to align with the eur model
	carrier_map = {
		"Link":{
			'Baseline Electricity unmanaged load':'electricity distribution grid', 
			'EV DSR shift':'home battery charger', 
			'H2 Turbine':'h2-ccgt', 
			'Baseline Electricity (I&C) DSR reverse':'battery discharger', 
			'Baseline Electricity (Residential) DSR shift':'battery charger', 
			'EV unmanaged load':'home battery charger', 
			# 'I&C Heat DSR reverse', 
			'Baseline Electricity (I&C) DSR shift':'battery discharger', 
			'Baseline Electricity (Residential) DSR reverse':'battery charger', 
			# 'Residential Heat unmanaged load', 
			# 'Residential Heat DSR reverse', 
			'ev V2G':'home battery discharger', 
			# 'Residential Heat DSR shift', 
			'EV DSR reverse':'home battery discharger', 
			# 'I&C Heat DSR shift', 
			# 'I&C Heat unmanaged load'
		},
		"Store":{
			# 'Residential Heat DSR', 
			'ev V2G':'home battery', 
			'Baseline Electricity (Residential) DSR':'battery', 
			# 'I&C Heat DSR', 
			'Baseline Electricity (I&C) DSR':'battery', 
			'EV DSR':'home battery'
		},
		"StorageUnit":{
			'hydro':'hydro-reservoir', 
			'Battery Storage':'battery', # battery is a store carrier 
			'PHS':'hydro-phs' # hydro-phs is a store carrier 
		},
		"Generator":{
			# 'nuclear', generator in GB, link in EUR 
			'solar':'solar-pv-utility', 
			# 'waste', 
			'biomass':'solid biomass', 
			'oil':'oil-heavy', 
			# 'geothermal', 
			# 'engine', not sure what this is?
			# 'Load Shedding', 
			'offwind-dc':'offwind-dc-fl-oh', 
		},
		"Load":{
			# '':, 
			'EV':'electricity', 
			'Baseline Electricity':'electricity', 
			# 'Residential Heat', 
			# 'I&C Heat', 
			'H2':'H2 exogenous demand'
		},
	}

	# renaming the buses doesn't help since the str is carried to each component
	# also if you rename the buses the pypsa merge requires you to drop them
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
	gb.remove("Carrier", carrier_map.keys())
	res = eur.merge(gb, with_time=True)

	return res

def remove_co2_costs(n, fp):
	powerplant_list = pd.read_csv(fp)
	powerplant_list["marginal_cost_non_co2"] = powerplant_list["marginal_cost"] - powerplant_list["VOM_carbon"]
	
	# probably need to filter this by the generators we replace with multilinks
	# if we can't replace all the generators with multilinks
	n.generators['marginal_cost'] = n.generators['marginal_cost_non_co2']
	return n

def add_co2_multilink(n, eur):
	# this is the carrier map essentially so it needs to be cleaned up
	gen_types = [
	['CCGT', 'gas-ccgt'],
	['OCGT', 'gas-ocgt'],
	['nuclear', 'nuclear'],
	# ['biomass', ''], 
	# ['engine', ''],
	['oil', 'oil-heavy'], # only oil-heavy has a GB based gen in TYNDP
	# ['waste', ''],
	['coal', 'lignite'] # double check that lignite and coal are interchangeable in this context
	]
	for gen_type in gen_types:
		ccgt = n.generators[(n.generators.carrier == gen_type[0]) & (n.generators.bus.str.startswith('GB'))]
		ref = eur.generators[(eur.generators.carrier == gen_type[1])]

		# add a copy of the generator as a link - use european model as a reference for unknown efficiencies
		n.add(
			"Link",
			name = gens.index,
			bus0 = ref.bus0.mode()[0],
			bus1 = gens.bus,
			bus2 = ref.bus2.mode()[0],
			p_nom = gens.p_nom,
			efficiency = gens.efficiency, #ref.efficiency.mean(),
			efficiency2 = ref.efficiency2.mean()
		)

		# remove the generator after the link version is created
		n.remove('Generator', gens.index)
	return n


if __name__ == "__main__":
	if "snakemake" not in globals():
		from scripts._helpers import mock_snakemake

		snakemake = mock_snakemake()

	# todo: change to snakemake input 
	n_gb = pypsa.Network(snakemake.input.gb_model)
	n_gb = add_co2_multilink(n_gb)

	n_eur = pypsa.Network(snakemake.input.iem_model)

	n_merged = merge_gb_tyndp(n_gb, n_eur)
	n_merged.export_to_netcdf(snakemake.output[0])
