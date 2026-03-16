import pypsa
import pandas as pd

from typing import Literal

from modules.analysis_toolkit.helpers.results_computer_base import ResultsComputerBase
from modules.analysis_toolkit.helpers.results_computer_wrappers import metric
from modules.analysis_toolkit.helpers.boundaries import get_fb_constraints, get_link_columns_in_ptdf, Boundaries
from modules.analysis_toolkit.helpers.config.filepaths import get_etys_boundaries_geopandas_fp


class ResultsComputer(ResultsComputerBase):
    """Compact results computer: expose metrics by decorating methods with @metric.

    Example:
      @metric
      def revenue(self, n):
          return n.statistics.revenue()

    Callers can use: res.revenue.iem(**kwargs), res.revenue.diff(), res.revenue.sq(), res.revenue(n, **kwargs)
    """
    def __init__(self, year: int):
        super().__init__(year=year)

    @metric
    def consumer_surplus(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def producer_surplus(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def congestion_income(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def storage_surplus(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def border_flows(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def border_price_spreads(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def co2_emissions(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def share_of_renewables(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def net_position(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    def _get_gb_net_position(self, n: pypsa.Network):
        net_position_gb = n.statistics.energy_balance(
            bus_carrier=["AC"],
            groupby_time=self.groupby_time,
            groupby=self.groupby). \
            drop(["DC"], level="carrier") \
            .xs("GB", level="country")
        return net_position_gb.sum(axis=0)

    def _get_gb_interconnector_flows(self, n: pypsa.Network):
        link_flows = n.statistics.transmission(
            bus_carrier=["AC"],
            components=["Link"],
            groupby=self.groupby,
            groupby_time=self.groupby_time
        ).groupby("name").sum()
        link_names = get_link_columns_in_ptdf(year=self.year)
        filter_links_in_ptdf = link_flows.index.get_level_values("name").isin(link_names)
        # filter to only include links that contribute to the ptdf-based boundary loading
        return link_flows[filter_links_in_ptdf]

    def _boundary_flows_ptdf(self, n: pypsa.Network):
        """Flows on the boundary lines, which is an approximation to the actual line loading."""
        link_flows = self._get_gb_interconnector_flows(n=n)
        ptdf = get_fb_constraints(year=self.year).set_index(["snapshot", "boundary", "direction"])
        ptdf.columns.name = "name"
        # contribution from link flows to the boundary loading, based on the ptdf values
        boundary_flows = link_flows.T.mul(ptdf)
        net_position_gb = self._get_gb_net_position(n=n)
        # contribution from the net position of GB to the boundary loading, based on the ptdf values
        boundary_flows["GB"] = net_position_gb.mul(ptdf["gb"])
        # copy the maximum and initial flows
        boundary_flows.loc[:, ["fmax", "f0"]] = ptdf.loc[:, ["fmax", "f0"]]
        return boundary_flows.sort_index(level=["snapshot", "boundary", "direction"])

    def _compute_net_boundary_flows_ptdf(self, n: pypsa.Network):
        boundary_flows = self._boundary_flows_ptdf(n=n)
        all_columns_except_fmax = boundary_flows.columns.difference(["fmax"])
        net_boundary_flows = boundary_flows.loc[:, all_columns_except_fmax].sum(axis=1)
        return net_boundary_flows

    def _boundary_loading_ptdf(self, n: pypsa.Network):
        """Flow-based loading of the boundary lines, which is an approximation to the actual line loading."""
        boundary_flows = self._boundary_flows_ptdf(n=n)
        net_boundary_flows = self._compute_net_boundary_flows_ptdf(n=n)
        loading = net_boundary_flows.div(boundary_flows.loc[:, "fmax"])
        # remove the negative loadings, only one direction per border is negative per timestamp
        return loading.clip(lower=0)

    def _boundary_flows_actual(self, n: pypsa.Network):
        """Flows on the boundary lines, which is an approximation to the actual line loading."""
        boundaries = Boundaries(network=n, year=self.year)
        boundary_flows_dict = {}
        for boundary_name, boundary in boundaries.items():
            line_flows = n.lines_t.p0.loc[:, boundary.lines].apply(lambda col: col * dict(zip(boundary.lines, boundary.line_directions))[col.name], axis=0)
            link_flows = n.links_t.p0.loc[:, boundary.links].apply(lambda col: col * dict(zip(boundary.links, boundary.link_directions))[col.name], axis=0)
            line_flows = line_flows.sum(axis=1) if line_flows.ndim > 1 else line_flows
            link_flows = link_flows.sum(axis=1) if link_flows.ndim > 1 else link_flows
            boundary_flows_dict[(boundary_name, "DIRECT")] = line_flows + link_flows
            boundary_flows_dict[(boundary_name, "OPPOSITE")] = - boundary_flows_dict[(boundary_name, "DIRECT")]
        boundary_flows = pd.DataFrame(boundary_flows_dict, index=n.snapshots).T.stack()
        boundary_flows = boundary_flows.rename_axis(index=["boundary", "direction", "snapshot"])
        boundary_flows = boundary_flows.reorder_levels(["snapshot", "boundary", "direction"])
        return boundary_flows.sort_index(level=["snapshot", "boundary", "direction"])

    def _boundary_loading_actual(self, n: pypsa.Network):
        """Actual loading of the boundary lines, based on the actual flows and the sum of the line capacities."""
        boundaries = Boundaries(network=n, year=self.year)
        boundary_flows = self._boundary_flows_actual(n=n)
        capacity = boundary_flows.reset_index().apply(lambda row: boundaries[row["boundary"]].capacity, axis=1)
        capacity.index = boundary_flows.index
        loading = boundary_flows.div(capacity)
        return loading.clip(lower=0)

    @metric
    def boundary_flows(self, n: pypsa.Network, which: Literal["ptdf", "actual"], **kwargs):
        if which == "ptdf":
            return self._compute_net_boundary_flows_ptdf(n=n)
        elif which == "actual":
            return self._boundary_flows_actual(n=n)
        else:
            raise NotImplementedError

    @metric
    def boundary_loading(self, n: pypsa.Network, which: Literal["ptdf", "actual"], **kwargs):
        if which == "ptdf":
            return self._boundary_loading_ptdf(n=n)
        elif which == "actual":
            return self._boundary_loading_actual(n=n)
        else:
            raise NotImplementedError

    @metric
    def boundary_congestion_count(self, n: pypsa.Network, which: Literal["ptdf", "actual"], **kwargs):
        if which == "ptdf":
            loading = self._boundary_loading_ptdf(n=n)
        elif which == "actual":
            loading = self._boundary_loading_actual(n=n)
        else:
            raise NotImplementedError

        return (loading > 1).groupby(["boundary", "direction"]).sum()


if __name__ == "__main__":
    year=2030
    rc = ResultsComputer(year=year)
    # test metric
    rc.boundary_flows.iem_dispatch(which='actual')

    print()