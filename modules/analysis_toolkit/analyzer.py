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

    @metric(restricted_to= "redispatch")
    def constraint_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        # Constraint costs include both re-dispatch and counter-trading costs
        constraint_carriers = n.carriers.filter(
            regex=r" ramp (up|down)$", axis=0
        ).index.tolist() + ["load"]

        constraint_costs = n.statistics.opex(  # todo: not working yet, we need to wait for the marginal cost of links from OET.
            groupby_time=False,
            groupby=["name", "carrier", "bus", "country"],
            carrier=constraint_carriers,
        ).query("not bus.str.contains('EU ')")

        return constraint_costs

    @metric(restricted_to= "redispatch")
    def countertrading_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        counter_trading_carriers = n.carriers.filter(
            regex=r"Link ramp (up|down)$", axis=0
        ).index.tolist()

        counter_trading_costs = n.statistics.opex(
            comps=["Generator"],
            groupby_time=False,
            groupby=["name", "carrier", "bus", "country"],
            carrier=counter_trading_carriers
        ).query('not bus.str.contains("GB ") and carrier.str.contains("ramp")')

        return counter_trading_costs


    @metric(restricted_to= "redispatch")
    def redispatch_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        redispatch_carriers = n.carriers.filter(
            regex=r"(Generator|Link|StorageUnit) ramp (up|down)$", axis=0
        ).index.tolist()

        redispatch_costs = n.statistics.opex(
            comps=["Generator", "Link", "StorageUnit"],
            groupby_time=False,
            groupby=["name", "carrier", "bus", "country"],
            carrier=redispatch_carriers
        ).query('country=="GB" and bus!="GBNI" and carrier.str.contains("ramp")')

        return redispatch_costs

    @metric(restricted_to= "redispatch")
    def load_shedding_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        load_shedding_costs = n.statistics.opex(
            comps=["Generator"],
            groupby_time=False,
            groupby=["name", "carrier", "bus", "country"],
            carrier="load"
        )

        return load_shedding_costs

    @metric(restricted_to= "dispatch")
    def consumer_costs(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the consumer cost per component (for GB only).
        The storage units, the  DC links and the lines are excluded from the calculation.
        The demand consists of both unmanaged and DSR components.
        """
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time = False, groupby= ["name", "carrier", "bus", "country"]).xs("GB", level = "country")

        component_cashflows = n.statistics.revenue(bus_carrier=["AC", "AC_OH"], groupby = ["name", "carrier", "bus", "country"], groupby_time = False).xs("GB", level = "country")
        cashflow_of_consumption = component_cashflows[energy_balance.sum(axis=1) < 0]
        consumer_cost = cashflow_of_consumption.drop("DC", level="carrier").drop(["StorageUnit", "Line"], level="component")
        return consumer_cost

    @metric(restricted_to= "dispatch")
    def producer_costs_in_gb(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the producer cost per component (for GB only).
        Producer cost = fuel cost + co2 cost + opex (vom). The first two are extracted with the revenue command.
        """
        # Filter all components that inject or absorb power to/from the AC grid of GB.
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time = False, groupby= ["name", "carrier", "country"]).xs("GB", level = "country")

        # Note: For the opex we should not define the bus_carrier filter, as we need to include the links that are producers.
        component_opex = n.statistics.opex(groupby=["name", "carrier"],
                                           groupby_time=False)

        # Includes fuel and co2 costs, excludes electricity revenues.
        component_production_costs = n.statistics.revenue(groupby=["name", "carrier", "bus_carrier"],
                                                     groupby_time=False, at_port = True).drop("AC", level = "bus_carrier").droplevel("bus_carrier").groupby(["component","name", "carrier"]).sum()

        # DC links and storage components should be excluded.
        excluded_carriers = ["home battery discharger", "DC"]

        # --- Align both dataframes to have the exact same index ---
        energy_balance_aligned_opex, component_opex = energy_balance.align(component_opex, join='inner')
        energy_balance_aligned_revenues, component_production_costs = energy_balance.align(component_production_costs, join='inner')

        opex_of_production = component_opex[energy_balance_aligned_opex.sum(axis=1) > 0].drop(excluded_carriers, level="carrier").drop("StorageUnit", level="component")
        costs_of_production = component_production_costs[energy_balance_aligned_revenues.sum(axis=1) > 0].drop("home battery discharger", level="carrier")

        producer_costs = opex_of_production.add(costs_of_production, fill_value=0)
        return producer_costs

    @metric(restricted_to= "dispatch")
    def producer_surplus_in_gb(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the producer cost per component (for GB only).
        Producer surplus = electricity revenue -  fuel cost - co2 cost - opex (vom). The net sum of the first 3 are extracted with the revenue command.
        """
        # Filter all components that inject or absorb power to/from the AC grid of GB.
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time = False, groupby= ["name", "carrier", "country"]).xs("GB", level = "country")

        # Note: For the opex we should not define the bus_carrier filter, as we need to include the links that are producers.
        component_opex = n.statistics.opex(groupby=["name", "carrier"],
                                           groupby_time=False)

        component_revenue = n.statistics.revenue(groupby=["name", "carrier"],
                                           groupby_time=False)

        # DC links and storage components should be excluded.
        excluded_carriers = ["home battery discharger", "DC"]

        # --- Align both dataframes to have the exact same index ---
        energy_balance_aligned_opex, component_opex = energy_balance.align(component_opex, join='inner')
        energy_balance_aligned_revenues, component_revenue = energy_balance.align(component_revenue, join='inner')

        opex_of_production = component_opex[energy_balance_aligned_opex.sum(axis=1) > 0].drop(excluded_carriers, level="carrier").drop("StorageUnit", level="component")
        cashflow_of_production = component_revenue[energy_balance_aligned_revenues.sum(axis=1) > 0].drop(excluded_carriers, level="carrier").drop("StorageUnit", level="component")

        producer_surplus = cashflow_of_production.sub(opex_of_production, fill_value=0)
        return producer_surplus

    @metric(restricted_to="dispatch")
    def storage_surplus_in_gb(self, n: pypsa.Network, **kwargs):  # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the storage surplus per component (for GB only).
        Storage surplus = electricity cashflow - opex (vom).
        """
        # Filter all components that inject or absorb power to/from the AC grid of GB.
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time=False,
                                                     groupby=["name", "carrier", "country"]).xs("GB", level="country")

        # Note: For the opex we should not define the bus_carrier filter, as we need to include the links that are producers.
        component_opex = n.statistics.opex(groupby=["name", "carrier"],
                                           groupby_time=False)

        component_cashflows = n.statistics.revenue(bus_carrier=["AC", "AC_OH"], groupby = ["name", "carrier", "country"], groupby_time = False)

        # --- Align both dataframes to have the exact same index ---
        energy_balance_aligned_opex, component_opex = energy_balance.align(component_opex, join='inner')
        energy_balance_aligned_cashflows, component_cashflows = energy_balance.align(component_cashflows, join='inner')

        opex_of_storage_units = component_opex.loc["StorageUnit"]
        cashflow_of_storage_units = component_cashflows.loc["StorageUnit"]

        storage_surplus = cashflow_of_storage_units.sub(opex_of_storage_units, fill_value=0)
        return storage_surplus


if __name__ == "__main__":
    rc = ResultsComputer(year=2030)
    print()