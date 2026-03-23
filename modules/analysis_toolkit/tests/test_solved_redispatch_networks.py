from setup_tests import RC, N, PTDF, BOUNDARIES
import numpy as np


def test_boundary_limts_not_exceeded():
    pass

def test_constraint_costs_are_larger_in_sq_than_iem():
    # check that constraint costs are larger in SQ than in IEM, as SQ does not have counter-trading or load shedding
    constraint_costs_sq = RC.constraint_costs.sq_redispatch().sum().sum()
    constraint_costs_iem = RC.constraint_costs.iem_redispatch().sum().sum()
    assert (constraint_costs_sq >= constraint_costs_iem), "Constraint costs should be larger in SQ than in IEM"

def test_opex_redispatch_larger_than_dispatch():
    # TODO: redispatch network doesn't include the dispatch costs of EU components because they were removed.
    # check that opex redispatch costs are larger in redispatch than in dispatch, as redispatch should have more redispatch costs
    # opex_dispatch = RC.opex.iem_dispatch().sum()
    # opex_redispatch = RC.opex.iem_redispatch().sum()
    # assert (opex_redispatch >= opex_dispatch), "Opex redispatch costs should be larger in redispatch than in dispatch"
    pass

def test_dispatch_opex_are_all_reflected_in_redispatch_network():
    # check that all opex for dispatch carriers is reflected in redispatch network, as redispatch network should include all dispatch costs plus additional redispatch costs
    opex_dispatch = RC.opex.iem_dispatch(groupby=["name", "carrier", "bus", "country"]).to_frame()

    opex_redispatch = (RC.opex.iem_redispatch(groupby=["name", "carrier", "bus", "country"]).to_frame()
                       .query("not index.get_level_values('carrier').str.contains(r'(ramp|co2|load)') and "  # ignore ramp (only present in redispatch), load and CO2 costs (allowed to change in redispatch)
                              "not index.get_level_values('name').str.contains('EU ')"))  # ignore all Generator components producing primary EU energy carriers
    components_affecting_gb_dispatch_costs = (opex_redispatch.index.get_level_values("name").unique()).tolist()

    selection_opex_dispatch = opex_dispatch.loc[:, components_affecting_gb_dispatch_costs, :, :, :]

    #TODO: storage units have a lower opex ~11-16£, making the first assertion fail
    assert np.allclose(opex_redispatch, selection_opex_dispatch, atol=1e-3), "Opex values differ for some components in the dispatch and redispatch networks"