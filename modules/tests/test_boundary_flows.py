from setup_tests import RC, N, PTDF, BOUNDARIES
import numpy as np


def test_net_position_gb():
    # GB's net position should be equal to the sum of its net injection (generation - load) --> RC._get_gb_net_position()
    # and the flows on its CZ interconnectors (links) --> np_gb_ics
    np_gb_ics = - N.statistics.energy_balance(bus_carrier="AC", carrier="DC", groupby=["country", "carrier", "name"], groupby_time=False) \
        .xs("GB", level="country") \
        .sum()
    assert np.allclose(np_gb_ics, RC._get_gb_net_position(N), atol=1e-3)

def test_boundary_flows_actual():
    # Boundary B6 (direct) intersects line 43 and link relation/15775538-600-DC, both in the "direct" direction.
    # Boundary B13 (opposite) intersects lines 52 and 53 in the "direct" direction, and line 33 in the "opposite" direction.
    flows_actual = RC.boundary_flows.iem_dispatch(which="actual")
    assert(
        (flows_actual.loc[:, "B6", "DIRECT"] == N.lines_t.p0.loc[:, "43"] + N.links_t.p0.loc[:, "relation/15775538-600-DC"]).all()
    )
    assert(
        (flows_actual.loc[:, "B13", "OPPOSITE"] == - N.lines_t.p0.apply(lambda r: r["52"] + r["53"] - r["33"], axis=1)).all()
    )

def test_boundary_flows_ptdf():
    # Snapshots in the PTDf must be the same as in the network.
    # Also, we compare the manual computation of B9 direct with the implementation in ResultsComputer
    flows_ptdf = RC.boundary_flows.iem_dispatch(which="ptdf").loc[:, "B9", "DIRECT"]
    net_position_gb = RC._get_gb_net_position(N)
    link_flows = N.links_t.p0
    b9_rows = PTDF.query("boundary=='B9' and direction=='DIRECT'")
    b9_rows = b9_rows.set_index("snapshot")
    assert (PTDF.snapshot.unique() == N.snapshots).all()
    assert np.allclose(
        flows_ptdf, b9_rows["f0"] + net_position_gb * b9_rows["gb"] + link_flows.mul(b9_rows).dropna(axis=1).sum(axis=1),
        atol=1e-3
    )

def test_boundary_loading():
    # check that the loading of one boundary multiplied by its capacity corresponds to its flow,
    # both in the ptdf and actual modes.
    loading_ptdf = RC.boundary_loading.iem_dispatch(which="ptdf").loc[:, "SC1.5", "OPPOSITE"]
    flow_ptdf = RC.boundary_flows.iem_dispatch(which="ptdf").loc[:, "SC1.5", "OPPOSITE"]
    assert np.allclose(loading_ptdf * BOUNDARIES["SC1.5"].capacity, flow_ptdf.mask(flow_ptdf<0, other=0), atol=1e-3)

    loading_actual = RC.boundary_loading.iem_dispatch(which="actual").loc[:, "SC1.5", "OPPOSITE"]
    flow_actual = RC.boundary_flows.iem_dispatch(which="actual").loc[:, "SC1.5", "OPPOSITE"]
    assert np.allclose(loading_actual * BOUNDARIES["SC1.5"].capacity, flow_actual.mask(flow_actual<0, other=0), atol=1e-3)
