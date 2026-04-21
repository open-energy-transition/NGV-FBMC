from setup_tests import RC, N, PTDF, BOUNDARIES
import numpy as np


# def test_all_opex_is_captured_in_constraint_costs():  # Todo: re-think test, some opex is related to dispatch costs
#     # check that all opex for redispatch carriers is captured in constraint costs
#     opex_redispatch = N.statistics.opex(groupby=["name", "carrier", "bus"])
#     opex_component_names = opex_redispatch.index.get_level_values("name").unique()
#
#     constraint_costs = RC.constraint_costs.iem_redispatch()
#     constraint_costs_component_names = constraint_costs.index.get_level_values("name").unique()
#
#     components_in_opex_not_in_constraint_costs = opex_component_names.difference(constraint_costs_component_names)
#     assert components_in_opex_not_in_constraint_costs.empty, \
#         (f"Opex for redispatch carriers is not fully captured in constraint costs. "
#          f"Opex components not in constraint costs: {components_in_opex_not_in_constraint_costs}")
#     assert np.isclose(opex_redispatch.sum(), constraint_costs.sum().sum()), \
#         f"Total opex for redispatch carriers {opex_redispatch.sum()} does not match total constraint costs {constraint_costs.sum().sum()}"

def test_constraint_costs_non_negative(): # todo: check if passes after marginal cost is reflected on link components
    # check that constraint costs are non-negative and zero when no constraints are violated
    constraint_costs = RC.constraint_costs.iem_redispatch().sum().sum()
    assert (constraint_costs >= 0), "Constraint costs should be non-negative"

def test_countertrading_costs():
    # check that counter-trading costs are non-negative and zero when no counter-trading is needed
    countertrading_costs = RC.countertrading_costs.iem_redispatch().sum().sum()
    assert (countertrading_costs >= 0), "Counter-trading costs should be non-negative"

def test_load_shedding_costs():
    # check that load shedding costs are non-negative and zero when no load shedding is needed
    load_shedding_costs = RC.load_shedding_costs.iem_redispatch().sum().sum()
    tolerance = 1
    assert (load_shedding_costs >= 0 - tolerance), "Load shedding costs should be non-negative"

def test_redispatch_costs():  # todo: check if passes after marginal cost is reflected on link components
    # check that redispatch costs are non-negative and zero when no redispatch is needed
    redispatch_costs = RC.redispatch_costs.iem_redispatch().sum().sum()
    assert (redispatch_costs >= 0), "Redispatch costs should be non-negative"

def test_constraint_costs_components():
    # check that constraint costs are redispatch costs + counter-trading costs + load shedding costs
    constraint_costs = RC.constraint_costs.iem_redispatch().sum(axis=1)

    redispatch_costs = RC.redispatch_costs.iem_redispatch().sum(axis=1)
    countertrading_costs = RC.countertrading_costs.iem_redispatch().sum(axis=1)
    load_shedding_costs = RC.load_shedding_costs.iem_redispatch().sum(axis=1)

    indices_not_accounted_in_cost_components = constraint_costs.index.difference(
        redispatch_costs.index.union(
            countertrading_costs.index.union(
                load_shedding_costs.index
            )
        )
    )

    assert indices_not_accounted_in_cost_components.empty, \
        f"Some components were not included in any cost component: {indices_not_accounted_in_cost_components.get_level_values("name")}"

    assert np.isclose(constraint_costs.sum(), redispatch_costs.sum() + countertrading_costs.sum() + load_shedding_costs.sum()), \
        f"Constraint costs {constraint_costs} do not match sum of components {redispatch_costs + countertrading_costs + load_shedding_costs}"