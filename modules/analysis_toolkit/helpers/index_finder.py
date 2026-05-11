from typing import Callable, Literal
from enum import Enum

import pypsa
import pandas as pd


class GeoOptions(str, Enum):
    NOT_IN_GB = 'not_in_gb'
    GB_ONLY = 'gb_only'
    SYSTEM_WIDE = 'system-wide'

GEO_OPTIONS_TYPE = Literal[GeoOptions.NOT_IN_GB, GeoOptions.GB_ONLY, GeoOptions.SYSTEM_WIDE]


def call_not_in_gb(n: pypsa.Network, method: Callable[[pypsa.Network, GEO_OPTIONS_TYPE], pd.Index | dict[str, pd.Index]]) -> pd.Index | dict[str, pd.Index]:
    system_wide_index = method(n, GeoOptions.SYSTEM_WIDE)
    if isinstance(system_wide_index, pd.Index):
        gb_only_index = method(n, GeoOptions.GB_ONLY)
        return system_wide_index.difference(gb_only_index)
    elif isinstance(system_wide_index, dict):
        gb_only_index = method(n, GeoOptions.GB_ONLY)
        return {
            comp: system_wide_index[comp].difference(gb_only_index[comp])
            for comp in system_wide_index
            if comp in gb_only_index
        }
    else:
        raise TypeError("Method must return either a pd.Index or a dict[str, pd.Index]")


class IndexFinder:

    @classmethod
    def get_ac_buses(cls, n: pypsa.Network, where: GEO_OPTIONS_TYPE) -> pd.Index:
        if where == GeoOptions.NOT_IN_GB:
            return call_not_in_gb(n=n, method=cls.get_ac_buses)
        query = "carrier in ['AC', 'AC_OH']"
        if where==GeoOptions.GB_ONLY:
            query += " and index.str.contains('GB ')"
        return n.buses.query(query).index

    @classmethod
    def get_interconnectors(cls, n: pypsa.Network, where: GEO_OPTIONS_TYPE) -> pd.Index:
        if where == GeoOptions.NOT_IN_GB:
            return call_not_in_gb(n=n, method=cls.get_interconnectors)
        ac_buses_in_gb = cls.get_ac_buses(n, where=GeoOptions.GB_ONLY)
        query = "carrier in ['DC', 'DC_OH'] and not (bus0 in @ac_buses_in_gb and bus1 in @ac_buses_in_gb)"  # select DC links that are not entirely within GB
        if where == GeoOptions.GB_ONLY:
            query += " and (bus0 in @ac_buses_in_gb or bus1 in @ac_buses_in_gb)"
        return n.links.query(query).index

    @classmethod
    def get_internal_dc_links(cls, n: pypsa.Network) -> pd.Index:
        ac_buses_in_gb = cls.get_ac_buses(n, where=GeoOptions.GB_ONLY)
        query_internal_gb_links = "carrier in ['DC', 'DC_OH'] and bus0 in @ac_buses_in_gb and bus1 in @ac_buses_in_gb"
        return n.links.query(query_internal_gb_links).index

    @classmethod
    def get_components_connected_to_ac(cls, n: pypsa.Network, where: GEO_OPTIONS_TYPE) -> dict[str, pd.Index]:
        if where == GeoOptions.NOT_IN_GB:
            return call_not_in_gb(n=n, method=cls.get_components_connected_to_ac)

        ac_buses = cls.get_ac_buses(n, where=where)
        components = {}
        for comp_name in n.branch_components:
            comp = n.c[comp_name].df
            components[comp_name] = comp[comp.bus0.isin(ac_buses) | comp.bus1.isin(ac_buses)].index
        for comp_name in n.one_port_components:
            comp = n.c[comp_name].df
            components[comp_name] = comp[comp.bus.isin(ac_buses)].index
        return {k: v for k,v in components.items() if len(v) > 0}

    @classmethod
    def _net_position_per_component_type(cls, n: pypsa.Network) :
        return n.statistics.energy_balance(groupby=["name", "bus"], bus_carrier=["AC", "AC_OH"])

    @classmethod
    def get_ac_consumers(cls, n: pypsa.Network, where: GEO_OPTIONS_TYPE) -> dict[str, pd.Index]:
        if where == GeoOptions.NOT_IN_GB:
            return call_not_in_gb(n=n, method=cls.get_ac_consumers)

        ac_buses = cls.get_ac_buses(n, where=where) # get all AC buses in the system, to determine the component port connected to AC

        np = cls._net_position_per_component_type(n=n)  # select only net positions at AC buses
        buses = ac_buses.intersection(np.index.get_level_values("bus"))

        ac_consumers = {}
        for comp in np.index.get_level_values("component").unique():
            np_iter = np.loc[comp, :, buses]
            if (np_iter < 0).any() and comp != 'Line':  # exclude lines as they are not producers, even if they have negative net position at the connected AC bus due to losses
                ac_consumers[comp] = np_iter.loc[np_iter < 0].index.get_level_values("name")

        ac_consumers["Link"] = ac_consumers["Link"].drop(n.links.query("carrier in ['DC', 'DC_OH']").index.intersection(ac_consumers["Link"]))  # drop all DC links which are not consumers, even if they have negative net position at the connected AC bus due to losses
        # TODO: CHECK THAT THERE ARE NO "BATTERY" OR OTHER STORAGE COMPONENTS
        return ac_consumers

    @classmethod
    def get_ac_producers(cls, n: pypsa.Network, where: GEO_OPTIONS_TYPE) -> dict[str, pd.Index]:
        if where == GeoOptions.NOT_IN_GB:
            return call_not_in_gb(n=n, method=cls.get_ac_producers)

        ac_buses = cls.get_ac_buses(n, where=where)   # get all AC buses in the system, to determine the component port connected to AC

        np = cls._net_position_per_component_type(n=n)
        buses = ac_buses.intersection(np.index.get_level_values("bus"))

        ac_producers = {}
        for comp in np.index.get_level_values("component").unique():
            np_iter = np.loc[comp, :, buses]
            if (np_iter > 0).any() and comp != 'Line':  # exclude lines as they are not producers, even if they have negative net position at the connected AC bus due to losses
                ac_producers[comp] = np_iter.loc[(np_iter > 0)].index.get_level_values("name")

        ac_producers["Link"] = ac_producers["Link"].drop(n.links.query("carrier in ['DC', 'DC_OH']").index.intersection(ac_producers["Link"]))  # drop all DC links which are not consumers, even if they have negative net position at the connected AC bus due to losses
        # TODO: CHECK THAT THERE ARE NO "BATTERY" OR OTHER STORAGE COMPONENTS
        return ac_producers

    @classmethod
    def get_ac_storage(cls, n: pypsa.Network, where: GEO_OPTIONS_TYPE) -> dict[str, pd.Index]:
        # todo: maybe some battery charger/discharger will be classified as consumer/producer,
        #  so this method might need to be adapted to look at both positive and negative power and classify
        #  as storage if it can be both
        if where == GeoOptions.NOT_IN_GB:
            return call_not_in_gb(n=n, method=cls.get_ac_producers)

        components_connected_to_ac = cls.get_components_connected_to_ac(n, where=where)
        producers = cls.get_ac_producers(n, where=where)
        consumers = cls.get_ac_consumers(n, where=where)
        ac_storage = {}

        for comp, comp_indices in components_connected_to_ac.items():
            if comp in producers:
                ac_storage[comp] = comp_indices.difference(producers[comp])
            elif comp in consumers:
                ac_storage[comp] = comp_indices.difference(consumers[comp])
            else:
                ac_storage[comp] = comp_indices

        return ac_storage


if __name__=='__main__':
    from config.filepaths import get_network_fps_for_year
    n = pypsa.Network(get_network_fps_for_year(2030)['n_iem_dispatch'])
    tst = IndexFinder.get_components_connected_to_ac(n, where=GeoOptions.NOT_IN_GB)
    print()