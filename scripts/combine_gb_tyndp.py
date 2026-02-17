# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: MIT

import logging
import re

import numpy as np
import pandas as pd
import pypsa

def combine_networks(gb, eur):
	# prepare eur network
	eur.remove("Bus","GB00")
	idx = eur.c['Bus'].static[eur.c['Bus'].static.location == "GB00"].index
	eur.remove("Bus", idx)

	# not sure all loads and generators exist 1:1 in the GB / EUR models (e.g. ror gen)
	idx = eur.c['StorageUnit'].static[eur.c['StorageUnit'].static.bus == "GB00"].index
	eur.remove("StorageUnit", idx)
	# todo: note H2 Electrolysis in GB, H2 electrolysis in EUR
	idx = eur.c["Link"].static[(eur.c['Link'].static.bus0 == "GB00") | (eur.c['Link'].static.bus1 == "GB00")].index
	eur.remove("Link", idx)

	# prepare gb network
	non_gb_buses = gb.buses[(gb.buses.carrier == 'AC') & ~(gb.buses.index.str.contains("GB"))]
	eur_elec_buses = eur.buses[eur.buses.carrier == 'AC'].index
	
	# create a mapping for the old GB names to the EUR names in TYNDP
	# note some non GB countries have multiple buses in TYNDP
	# the current assignment method (below) is arbitary
	gb_eur_busmap = {}
	for name, bus in gb_elec_buses.iterrows():
		# is it ok to rename the buses here or do i need to make a new bus using network remove/add property
		# note that the network gets merged later on
		country_matches = [eur_bus for eur_bus in eur_elec_buses if eur_bus.startswith(name)]
		gb_eur_busmap[name] = country_matches

	# renaming the buses doesn't help since the str is carried to each component
	# also if you rename the buses the pypsa merge requires you to drop them
	for comp in ["Link", "Store", "StorageUnit", "Generator", "Load"]:
		cols = [col for col in gb.c[comp].static.columns if col.startswith('bus')]
		for col in cols:
			gb.c[comp].static[col] = gb.c[comp].static[col].replace(gb_eur_busmap)

	# pypsa merge doesn't like overlapping components
	gb.remove("Carrier", gb.carriers.index.intersection(eur.carriers.index))

	res = eur.merge(gb, with_time = False)

	return res

if __name__ == "__main__":
	if "snakemake" not in globals():
		from scripts._helpers import mock_snakemake

		snakemake = mock_snakemake()

	# todo: change to snakemake input 
	gb_fp = "modules/gb-dispatch-model/resources/GB/networks/unconstrained_clustered/2035.nc"
	n_gb = pypsa.Network(gb_fp)

	eur_fp = "modules/NGV-IEM/resources/ngv-iem/fbmc-test-1H-Jan/networks/base_s_all_elec.nc"
	n_eur = pypsa.Network(eur_fp)

	combined_n = combine_networks(n_gb, n_eur)

