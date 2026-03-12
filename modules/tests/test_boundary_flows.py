from setup_tests import RC, T0, N


def test_boundary_flows_actual():
    # Boundary B6 intersects line 43 and link relation/15775538-600-DC, both in the same direction.
    flows_actual = RC.boundary_flows.iem_dispatch(which="actual")
    assert(
        flows_actual.loc[T0, "B6", "DIRECT"] == N.lines_t.p0.loc[T0, "43"] + N.links_t.p0.loc[T0, "relation/15775538-600-DC"]
    )

def test_boundary_flows_ptdf():
    # TODO: check that at least one of the boundaries is correctly calculated based on the PTDF * NPs
    #   cannot be implemented right now because snapshots in network and PTDF are not aligned:
    #   network with time segmentation only has ~2190 snapshots for the year 2009
    #   ptdf has all snapshots (8760) for the year 2013
    # net_position_gb = N.statistics.energy_balance(bus_carrier="AC", groupby="country", groupby_time=False).xs("GB", level="country").loc[:, T0].sum()
    # link_flows = N.links_t.p0.loc[T0, :]
    # flows_ptdf = RC.boundary_flows.iem_dispatch(which="ptdf").loc[T0, :, "DIRECT"]
    # position_labels = PTDF

def test_boundary_loading():
    # check that the loading of one boundary corresponds to its flow divided by its capacity, both in the ptdf and actual modes.
    # loading_ptdf = RC.boundary_flows.iem_dispatch(which="ptdf")
    # loading_actual = RC.boundary_flows.iem_dispatch(which="actual")
    pass

def boundary_congestion_count():
    # check that the congestion count is correct for the actual loading of one of the boundaries
    congestion_count = RC.congestion_count.iem_dispatch(which="actual")
    pass