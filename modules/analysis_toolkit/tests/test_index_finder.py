from setup_tests import N
from modules.analysis_toolkit.helpers.index_finder import IndexFinder, GeoOptions


def test_get_ac_buses():
    test_ac_buses = IndexFinder.get_ac_buses(N, where=GeoOptions.SYSTEM_WIDE)
    test_ac_buses_gb = IndexFinder.get_ac_buses(N, where=GeoOptions.GB_ONLY)
    test_ac_buses_not_in_gb = IndexFinder.get_ac_buses(N, where=GeoOptions.NOT_IN_GB)

    all_ac_buses = N.buses.query("carrier in ['AC', 'AC_OH']")
    assert test_ac_buses.equals(all_ac_buses.index)
    assert test_ac_buses_gb.equals(all_ac_buses[all_ac_buses.country=='GB'].index)
    assert test_ac_buses_not_in_gb.equals(all_ac_buses.index.difference(test_ac_buses_gb))

def test_get_interconnectors():
    test_ics = IndexFinder.get_interconnectors(N, where=GeoOptions.SYSTEM_WIDE)
    test_ics_gb = IndexFinder.get_interconnectors(N, where=GeoOptions.GB_ONLY)
    test_ics_not_in_gb = IndexFinder.get_interconnectors(N, where=GeoOptions.NOT_IN_GB)

    all_interconnectors = N.links.query("carrier in ['DC', 'DC_OH']")
    all_interconnectors = all_interconnectors.drop(IndexFinder.get_internal_dc_links(N))  # drop relation links which are not interconnectors

    assert test_ics.equals(all_interconnectors.index)
    assert test_ics_gb.equals(all_interconnectors.query("(bus0.str.contains('GB ') or bus1.str.contains('GB '))").index)
    assert test_ics_not_in_gb.equals(all_interconnectors.index.difference(test_ics_gb))

def test_get_consumers():
    test_consumers = IndexFinder.get_ac_consumers(N, where=GeoOptions.SYSTEM_WIDE)
    test_consumers_gb = IndexFinder.get_ac_consumers(N, where=GeoOptions.GB_ONLY)
    test_consumers_not_in_gb = IndexFinder.get_ac_consumers(N, where=GeoOptions.NOT_IN_GB)

    all_consumers = N.statistics.energy_balance(groupby=["name", "carrier"], bus_carrier=["AC", "AC_OH"])
    all_consumers = all_consumers[all_consumers < 0]  # select only consumers, i.e. components with negative net position

    for comp_type in all_consumers.index.get_level_values("component").unique():
        if comp_type == 'Line':
            continue  # skip lines, as they are not consumers and their naming convention creates problems
        assert set(all_consumers.xs(comp_type, level="component").index.get_level_values("name")) == set(test_consumers[comp_type]), f"Mismatch in {comp_type} between all_consumers and test_consumers"
        assert test_consumers_gb[comp_type].str.contains("GB ").all() if len(test_consumers_gb[comp_type]) > 0 else True
        assert not test_consumers_not_in_gb[comp_type].str.contains("GB ").all() if len(test_consumers_not_in_gb[comp_type]) > 0 else True

    # TODO: CHECK THAT THERE ARE NO "BATTERY" OR OTHER STORAGE COMPONENTS

def test_get_producers():
    test_producers = IndexFinder.get_ac_producers(N, where=GeoOptions.SYSTEM_WIDE)
    test_producers_gb = IndexFinder.get_ac_producers(N, where=GeoOptions.GB_ONLY)
    test_producers_not_in_gb = IndexFinder.get_ac_producers(N, where=GeoOptions.NOT_IN_GB)

    all_producers = N.statistics.energy_balance(groupby=["name", "carrier"], bus_carrier=["AC", "AC_OH"])
    all_producers = all_producers[all_producers > 0]  # select only consumers, i.e. components with negative net position

    for comp_type in all_producers.index.get_level_values("component").unique():
        if comp_type == 'Line':
            continue  # skip lines, as they are not consumers and their naming convention creates problems
        assert set(all_producers.xs(comp_type, level="component").index.get_level_values("name")) == set(test_producers[comp_type]), f"Mismatch in {comp_type} between all_consumers and test_consumers"
        assert test_producers_gb[comp_type].str.contains("GB ").all() if len(test_producers_gb[comp_type]) > 0 else True
        assert not test_producers_not_in_gb[comp_type].str.contains("GB ").all() if len(test_producers_not_in_gb[comp_type]) > 0 else True

    # TODO: CHECK THAT THERE ARE NO "BATTERY" OR OTHER STORAGE COMPONENTS

def test_get_storage():
    ...

def test_get_all_components_connected_to_ac():
    test_ac_components = IndexFinder.get_components_connected_to_ac(N, where=GeoOptions.SYSTEM_WIDE)
    test_ac_components_gb = IndexFinder.get_components_connected_to_ac(N, where=GeoOptions.GB_ONLY)
    test_ac_components_not_in_gb = IndexFinder.get_components_connected_to_ac(N, where=GeoOptions.NOT_IN_GB)