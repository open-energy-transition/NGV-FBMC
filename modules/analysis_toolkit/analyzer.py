import pypsa
import pandas as pd
import numpy as np

import functools
from types import MethodType
from typing import Literal

from modules.analysis_toolkit.helpers.results_computer_base import ResultsComputerBase
from modules.analysis_toolkit.helpers.results_computer_wrappers import metric
from modules.analysis_toolkit.helpers.boundaries import get_fb_constraints, get_link_columns_in_ptdf, Boundaries
from modules.analysis_toolkit.helpers.config.filepaths import get_etys_boundaries_geopandas_fp
from modules.analysis_toolkit.helpers.index_finder import IndexFinder, GeoOptions
from modules.analysis_toolkit.helpers.config.constants import CAPTURE_RATE_IC


def _show_sum_by(result: pd.DataFrame, labels: list[str]):
    index_labels = [l for l in labels if l in result.index.names]
    column_labels = [l for l in labels if l in result.columns.names]
    remaining_labels = set(labels) - set(index_labels) - set(column_labels)
    if remaining_labels:
        raise ValueError(
            f"The following labels are not found in the index or columns of the result: {remaining_labels}")
    else:
        available_labels_in_df = result.index.names + result.columns.names
        print(f"Grouping by {labels} aggregates across {list(set(available_labels_in_df) - set(labels))}.")
        result = result.groupby(level=column_labels, axis=1).sum() if column_labels else result.sum(axis=1)
        result = result.groupby(level=index_labels, axis=0).sum() if index_labels else result.sum(axis=0)
        return result

def _scale(result: pd.DataFrame, factor: float):
    return result * factor

def format_result(result: pd.DataFrame, sum_by: list[str], factor: float = 1e-6):
    # Warning: be careful of what is being aggregated and if it requires an abs() before summing, or if it requires
    #   a different aggregation function. Only use this function when you are sure of the result it will give you.
    #   Also note that by default, it divides by 1 million.
    result = _show_sum_by(result, sum_by)
    result = _scale(result, factor)
    return result


class ResultsComputer(ResultsComputerBase):
    """Compact results computer: expose metrics by decorating methods with @metric.

    Example:
      @metric
      def revenue(self, n):
          return n.statistics.revenue()

    Callers can use: res.revenue.iem(**kwargs), res.revenue.diff(), res.revenue.sq(), res.revenue(n, **kwargs)
    """
    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if isinstance(attr, MethodType):
            func = attr.__func__
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if func.__qualname__.startswith("ResultsComputer.") and not name.startswith("_"):
                    print(f"Calling method: {name}")
                return attr(*args, **kwargs)
            return wrapper
        return attr

    def __init__(self, year: int, apply_snapshot_filter: bool = False):
        super().__init__(year=year, apply_snapshot_filter=apply_snapshot_filter)

    def _get_gb_net_position(self, n: pypsa.Network):
        net_position_gb = n.statistics.energy_balance(
            bus_carrier=["AC"],
            groupby_time=self.groupby_time,
            groupby=self.groupby). \
            drop(["DC"], level="carrier") \
            .xs("GB", level="country").drop("GBNI", level="bus")
        assert np.allclose(net_position_gb.sum(axis=0), -self._get_gb_interconnector_flows(n).sum(), atol=1), \
            "Generation - demand in GB should be approximately equal to the net flow on the interconnectors (with a tolerance of 1 MW, to account for small numerical differences)."
        return net_position_gb.sum(axis=0)

    def _get_gb_interconnector_flows(self, n: pypsa.Network):
        link_flows = -n.statistics.transmission(  # negative sign to have the flow direction from GB to the neighboring countries as negative, which is consistent with the net position of GB.
            bus_carrier=["AC"],
            components=["Link"],
            groupby=self.groupby,
            groupby_time=self.groupby_time
        ).groupby("name").sum()
        link_names = get_link_columns_in_ptdf(year=self.year)
        assert set(link_names) == set(IndexFinder.get_interconnectors(n, where=GeoOptions.GB_ONLY)), \
            "The list of links in the PTDF are different from those in the network. Please check the consistency of the data."
        filter_links_in_ptdf = link_flows.index.get_level_values("name").isin(link_names)
        # filter to only include links that contribute to the ptdf-based boundary loading
        return link_flows[filter_links_in_ptdf]

    def _boundary_flows_ptdf(self, n: pypsa.Network):
        """Flows on the boundary lines, which is an approximation to the actual line loading."""
        link_flows = self._get_gb_interconnector_flows(n=n)
        ptdf = get_fb_constraints(year=self.year).set_index(["snapshot", "boundary", "direction"])
        full_index = pd.MultiIndex.from_product(
            [n.snapshots,
             ptdf.index.get_level_values("boundary").unique(),
             ptdf.index.get_level_values("direction").unique()],
            names=["snapshot", "boundary", "direction"]
        )
        ptdf = (
            ptdf.reindex(full_index)
            .sort_index()
            .groupby(level=["boundary", "direction"])
            .ffill()
        )
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

    def _constraint_costs(self, n: pypsa.Network):
        # Constraint costs include both re-dispatch and counter-trading costs
        constraint_carriers = n.carriers.filter(
            regex=r" ramp (up|down)$", axis=0
        ).index.tolist() + ["load"]

        # Exclude the EU fuel buses ramp up/down and the Store ramp up/down
        constraint_costs = n.statistics.opex(
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country"],
            carrier=constraint_carriers,
            drop_zero=False
        ).query("not name.str.contains('EU gas|EU waste|EU solid biomass')").drop(["Store ramp up", "Store ramp down"], level = "carrier")

        return constraint_costs

    @metric(restricted_to= "redispatch")
    def constraint_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        return self._constraint_costs(n=n)

    def _countertrading_costs(self, n: pypsa.Network):
        """
        Counter trading in the interconnectors is modeled with the virtual generators at the GB-neighbor side
        The components we want to filter have: Component type = "Generator", Carrier type = "Link ramp up/down"
        Note: The "Link ramp down" components have negative opex (they decrease the objective function)
        """

        counter_trading_carriers = n.carriers.filter(
            regex=r"Link ramp (up|down)$", axis=0
        ).index.tolist()

        counter_trading_costs = n.statistics.opex(
            comps=["Generator"],
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country"],
            carrier=counter_trading_carriers,
            drop_zero=False
        )

        return counter_trading_costs

    @metric(restricted_to= "redispatch")
    def countertrading_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        return self._countertrading_costs(n=n)

    def _redispatch_costs(self, n: pypsa.Network):
        """
        Redispatch costs occur from changing the setpoint of generation units. Some generation technologies are modeled
        as "Generators" (i.e. Solar, Wind), while others as "Links" (i.e. gas-ccgt, biomass etc.)
        The "Generator" components we want to filter have: Component type = "Generator", Carrier type = "Generator ramp up/down"
        The "Link" components we want to filter have: Component type = "Link", Carrier type = "Link ramp up/down"
        Note: The "Link ramp down" carriers of the "Link" components have negative opex (they decrease the objective function)
        We also exclude the opex of the EU fuel buses (EU gas/waste/solid biomass ramp up/down)
        """
        redispatch_carriers_generators = n.carriers.filter(
            regex=r"Generator ramp (up|down)$", axis=0
        ).index.tolist()

        redispatch_carriers_links = n.carriers.filter(
            regex=r"Link ramp (up|down)$", axis=0
        ).index.tolist()

        redispatch_carriers_storage = n.carriers.filter(
            regex=r"StorageUnit ramp (up|down)$", axis=0
        ).index.tolist()

        redispatch_costs_generators = n.statistics.opex(
            comps=["Generator"],
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country"],
            carrier=redispatch_carriers_generators,
            drop_zero=False
        ).query("not bus.str.contains('EU ')")

        redispatch_costs_links = n.statistics.opex(
            comps=["Link"],
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country"],
            carrier=redispatch_carriers_links,
            drop_zero=False
        )

        redispatch_costs_storage = n.statistics.opex(
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country"],
            carrier=redispatch_carriers_storage,
            drop_zero=False
        )

        redispatch_costs_total = pd.concat(
            [redispatch_costs_generators, redispatch_costs_links, redispatch_costs_storage],
            axis=0
        )

        return redispatch_costs_total

    @metric(restricted_to= "redispatch")
    def redispatch_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        return self._redispatch_costs(n=n)

    def _load_shedding_costs(self, n: pypsa.Network):
        load_shedding_costs = n.statistics.opex(
            comps=["Generator"],
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country"],
            carrier="load",
            drop_zero=False
        )

        return load_shedding_costs

    @metric(restricted_to= "redispatch")
    def load_shedding_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        return self._load_shedding_costs(n=n)

    @metric(restricted_to= "dispatch")
    def consumer_costs_in_gb(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the consumer cost per component (for GB only).
        The storage units, the  DC links and the lines are excluded from the calculation.
        The demand consists of both unmanaged and DSR components.
        """
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time = False, groupby= ["name", "carrier", "bus"]).query("bus.str.contains('GB ')")

        component_cashflows = n.statistics.revenue(bus_carrier=["AC", "AC_OH"], groupby = ["name", "carrier", "bus"], groupby_time=False, at_port=True).query("bus.str.contains('GB ')")
        cashflow_of_consumption = component_cashflows[energy_balance.sum(axis=1) < 0]
        consumer_cost = cashflow_of_consumption.drop("DC", level="carrier").drop(["StorageUnit", "Line"], level="component")
        return consumer_cost

    def _consumer_costs_system(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the consumer cost per component (for GB only).
        The storage units, the  DC links and the lines are excluded from the calculation.
        The demand consists of both unmanaged and DSR components.
        """
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time = False, groupby= ["name", "carrier", "bus"])

        # Exclude DC links and storage components
        excluded_carriers = ["DC", "DC_OH", "battery charger", "hydro-phs-pump", "hydro-phs-pure-pump"]
        component_cashflows = n.statistics.revenue(bus_carrier=["AC", "AC_OH"], groupby = ["name", "carrier", "bus"], groupby_time = False, at_port=True)
        cashflow_of_consumption = component_cashflows[energy_balance.sum(axis=1) < 0]
        consumer_cost = cashflow_of_consumption.drop(excluded_carriers, level="carrier").drop(["StorageUnit", "Line"], level="component")
        return consumer_cost

    @metric(restricted_to= "dispatch")
    def producer_costs_in_gb(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the producer cost per component (for GB only).
        Producer cost = fuel cost + co2 cost + opex (vom). The first two are extracted with the revenue command.
        """
        # Filter all components that inject or absorb power to/from the AC grid of GB.
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time = self.groupby_time, groupby= ["name", "carrier", "country"]).xs("GB", level = "country")

        # Note: For the opex we should not define the bus_carrier filter, as we need to include the links that are producers.
        component_opex = n.statistics.opex(groupby=["name", "carrier"],
                                           groupby_time=self.groupby_time)

        # Includes fuel and co2 costs, excludes electricity revenues.
        component_production_costs = n.statistics.revenue(groupby=["name", "carrier", "bus_carrier"],
                                                     groupby_time=self.groupby_time, at_port = True).drop(["AC", "AC_OH"], level = "bus_carrier").droplevel("bus_carrier").groupby(["component","name", "carrier"]).sum()

        # DC links and storage components should be excluded.
        excluded_carriers = ["home battery discharger", "DC"]

        # --- Align both dataframes to have the exact same index ---
        energy_balance_aligned_opex, component_opex = energy_balance.align(component_opex, join='inner')
        energy_balance_aligned_revenues, component_production_costs = energy_balance.align(component_production_costs, join='inner')

        opex_of_production = component_opex[energy_balance_aligned_opex.sum(axis=1) > 0].drop(excluded_carriers, level="carrier").drop("StorageUnit", level="component")
        costs_of_production = component_production_costs[energy_balance_aligned_revenues.sum(axis=1) > 0].drop("home battery discharger", level="carrier")

        producer_costs = opex_of_production.add(costs_of_production, fill_value=0)
        return producer_costs

    @metric(restricted_to="dispatch")
    def producer_costs_system(self, n: pypsa.Network, **kwargs):  # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the producer cost per component (for GB only).
        Producer cost = fuel cost + co2 cost + opex (vom). The first two are extracted with the revenue command.
        """
        # Filter all components that inject or absorb power to/from the AC grid of GB.
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time=self.groupby_time,
                                                     groupby=["name", "carrier"])

        # Note: For the opex we should not define the bus_carrier filter, as we need to include the links that are producers.
        component_opex = n.statistics.opex(groupby=["name", "carrier"],
                                           groupby_time=self.groupby_time)

        # Includes fuel and co2 costs, excludes electricity revenues.
        component_production_costs = n.statistics.revenue(groupby=["name", "carrier", "bus_carrier"],
                                                          groupby_time=self.groupby_time, at_port=True).drop(["AC", "AC_OH"],
                                                                                                 level="bus_carrier").droplevel(
            "bus_carrier").groupby(["component", "name", "carrier"]).sum()

        # DC links and storage components should be excluded. Also, load shedding generators and the elec distribution grid links.
        excluded_carriers = ["home battery discharger", "DC", "DC_OH", "battery charger", "battery discharger",
                             "hydro-phs-pump",
                             "hydro-phs-pure-pump", "hydro-phs-turbine", "hydro-phs-pure-turbine", "load",
                             "electricity distribution grid"]

        # --- Align both dataframes to have the exact same index ---
        energy_balance_aligned_opex, component_opex = energy_balance.align(component_opex, join='inner')
        energy_balance_aligned_revenues, component_production_costs = energy_balance.align(component_production_costs,
                                                                                           join='inner')

        opex_of_production = component_opex[energy_balance_aligned_opex.sum(axis=1) > 0].drop(excluded_carriers,
                                                                                              level="carrier").drop(
            "StorageUnit", level="component")
        costs_of_production = component_production_costs[energy_balance_aligned_revenues.sum(axis=1) > 0].drop(
            excluded_carriers, level="carrier")

        producer_costs = costs_of_production.sub(opex_of_production, fill_value=0)
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
                                           groupby_time=self.groupby_time)

        component_revenue = n.statistics.revenue(groupby=["name", "carrier"],
                                           groupby_time=self.groupby_time)

        # DC links and storage components should be excluded.
        excluded_carriers = ["home battery discharger", "DC"]

        # --- Align both dataframes to have the exact same index ---
        energy_balance_aligned_opex, component_opex = energy_balance.align(component_opex, join='inner')
        energy_balance_aligned_revenues, component_revenue = energy_balance.align(component_revenue, join='inner')

        opex_of_production = component_opex[energy_balance_aligned_opex.sum(axis=1) > 0].drop(excluded_carriers, level="carrier").drop("StorageUnit", level="component")
        cashflow_of_production = component_revenue[energy_balance_aligned_revenues.sum(axis=1) > 0].drop(excluded_carriers, level="carrier").drop("StorageUnit", level="component")

        producer_surplus = cashflow_of_production.sub(opex_of_production, fill_value=0)
        return producer_surplus


    def _producer_surplus_system(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the producer cost per component (for the whole system).
        Producer surplus = electricity revenue -  fuel cost - co2 cost - opex (vom). The net sum of the first 3 are extracted with the revenue command.
        """
        # Filter all components that inject or absorb power to/from the AC grid of GB.
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time = self.groupby_time, groupby= ["name", "carrier"])

        # Note: For the opex we should not define the bus_carrier filter, as we need to include the links that are producers.
        component_opex = n.statistics.opex(groupby=["name", "carrier"],
                                           groupby_time=self.groupby_time)

        component_revenue = n.statistics.revenue(groupby=["name", "carrier"],
                                           groupby_time=self.groupby_time)

        # DC links and storage components should be excluded. Also, load shedding generators and the elec distribution grid links.
        excluded_carriers = ["home battery discharger", "DC", "DC_OH", "battery charger", "battery discharger", "hydro-phs-pump",
                         "hydro-phs-pure-pump", "hydro-phs-turbine", "hydro-phs-pure-turbine", "load", "electricity distribution grid"]

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
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time=self.groupby_time,
                                                     groupby=["name", "carrier", "country"]).xs("GB", level="country")

        # Note: For the opex we should not define the bus_carrier filter, as we need to include the links that are producers.
        component_opex = n.statistics.opex(groupby=["name", "carrier"],
                                           groupby_time=self.groupby_time)

        component_cashflows = n.statistics.revenue(bus_carrier=["AC", "AC_OH"], groupby = ["name", "carrier", "country"], groupby_time = self.groupby_time)

        # --- Align both dataframes to have the exact same index ---
        energy_balance_aligned_opex, component_opex = energy_balance.align(component_opex, join='inner')
        energy_balance_aligned_cashflows, component_cashflows = energy_balance.align(component_cashflows, join='inner')

        opex_of_storage_units = component_opex.loc["StorageUnit"]
        cashflow_of_storage_units = component_cashflows.loc["StorageUnit"]

        storage_surplus = cashflow_of_storage_units.sub(opex_of_storage_units, fill_value=0)
        return storage_surplus


    def _storage_surplus_system(self, n: pypsa.Network, **kwargs):  # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the storage surplus per component (for the whole system).
        Storage surplus = electricity cashflow - opex (vom).
        """

        # TODO : need to ensure that all "storage" components are included --> possibly add a test? (another test: dischargers have more surplus than chargers (who have negative surplus))
        # Filter all components that inject or absorb power to/from the AC grid of GB.
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC", "AC_OH"], groupby_time=self.groupby_time,
                                                     groupby=["name", "carrier"])

        # Note: For the opex we should not define the bus_carrier filter, as we need to include the links that are producers.
        component_opex = n.statistics.opex(groupby=["name", "carrier"],
                                           groupby_time=self.groupby_time)

        component_cashflows = n.statistics.revenue(bus_carrier=["AC", "AC_OH"], groupby=["name", "carrier"],
                                                   groupby_time=self.groupby_time)

        # --- Align both dataframes to have the exact same index ---
        energy_balance_aligned_opex, component_opex = energy_balance.align(component_opex, join='inner')
        energy_balance_aligned_cashflows, component_cashflows = energy_balance.align(component_cashflows, join='inner')

        storage_links = ["battery discharger", "battery charger", "hydro-phs-turbine",
                       "hydro-phs-pure-turbine", "hydro-phs-pump","hydro-phs-pure-pump", "home battery discharger"]

        opex_of_storage_units = component_opex.loc[["StorageUnit"]]  #double brackets to keep the "component type" level
        cashflow_of_storage_units = component_cashflows.loc[["StorageUnit"]]

        opex_of_storage_links = component_opex.loc["Link",:, storage_links]
        cashflow_of_storage_links = component_cashflows.loc["Link",:, storage_links]

        storage_surplus_units = cashflow_of_storage_units.sub(opex_of_storage_units, fill_value=0)
        storage_surplus_links = cashflow_of_storage_links.sub(opex_of_storage_links, fill_value=0)

        storage_surplus = pd.concat([storage_surplus_units, storage_surplus_links], axis = 0)
        return storage_surplus

    def _welfare_system(self, n: pypsa.Network, **kwargs):
        """Internal helper: Computes the aggregated welfare components for a single network."""

        # 1. Fetch the data from the helpers and crush them safely into single floats
        consumer_total = self._consumer_costs_system(n, **kwargs).sum().sum()/1e6
        producer_total = self._producer_surplus_system(n, **kwargs).sum().sum()/1e6
        storage_total = self._storage_surplus_system(n, **kwargs).sum().sum()/1e6

        congestion_total = float(np.nansum(self._congestion_income(n, where=GeoOptions.SYSTEM_WIDE).values))/1e6

        # 2. Calculate the grand total
        total_welfare = producer_total + consumer_total + storage_total + congestion_total

        # 3. Pack them into a Pandas Series
        welfare_summary = pd.Series({
            "producer surplus": producer_total,
            "consumer cost": consumer_total,
            "congestion income": congestion_total,
            "storage surplus": storage_total,
            "total_welfare": total_welfare
        }, name="welfare")  # Giving it a name helps Pandas name the index column later

        return welfare_summary

    @metric(restricted_to="dispatch")
    def consumer_costs_system(self, n: pypsa.Network, **kwargs):
        return self._consumer_costs_system(n, **kwargs)

    @metric(restricted_to="dispatch")
    def producer_surplus_system(self, n: pypsa.Network, **kwargs):
        return self._producer_surplus_system(n, **kwargs)

    @metric(restricted_to="dispatch")
    def storage_surplus_system(self, n: pypsa.Network, **kwargs):
        return self._storage_surplus_system(n, **kwargs)

    @metric(restricted_to="dispatch")
    def welfare_system(self, n: pypsa.Network, **kwargs):
        return self._welfare_system(n, **kwargs)

    @metric
    def interconnector_flows(self, n: pypsa.Network):
         return -self._get_gb_interconnector_flows(n=n)  # negative sign to have GB exports as positive

    @metric
    def interconnector_price_spreads(self, n: pypsa.Network):# positive price spread <--> GB is cheaper
        flows = -self._get_gb_interconnector_flows(n=n)
        congestion_income = self._congestion_income(n=n, where=GeoOptions.GB_ONLY)
        price_spreads = congestion_income.div(flows, fill_value=0)
        return price_spreads


    @metric
    def net_position_gb(self, n: pypsa.Network):
        return self._get_gb_net_position(n=n)

    def _congestion_income(self, n: pypsa.Network, where:GeoOptions, **kwargs):
        """
        The method returns a disaggregated data frame with the time series of the congestion income per interconnector (for GB only).
        Congestion income = flow on interconnector * price difference between the two sides of the interconnector.
        """
        link_names = IndexFinder.get_interconnectors(n, where=where)
        congestion_income = n.statistics.revenue(
            bus_carrier=["AC", "AC_OH"],
            groupby_time=self.groupby_time,
            carrier=["DC", "DC_OH"],
            groupby=["name"]
        ).query('name in @link_names').droplevel("component")
        return congestion_income

    @metric(restricted_to="dispatch")
    def congestion_income(self, n: pypsa.Network, where:GeoOptions, apply_capture_rates: bool=False, **kwargs):
        """Public metric wrapper for congestion income."""
        ci = self._congestion_income(n, where=where, **kwargs)
        if apply_capture_rates:
            capture_rates = pd.Series(CAPTURE_RATE_IC)
            ci = ci.mul(capture_rates, axis=0)
        return ci

    def restricted_capacity(self):
        return self.interconnector_flows.iem_dispatch().sub(self.interconnector_flows.iem_fb_dispatch(), fill_value=0)

    def lost_congestion_income(self):
        return self.congestion_income.iem_dispatch(where=GeoOptions.GB_ONLY).sub(
            self.congestion_income.iem_fb_dispatch(where=GeoOptions.GB_ONLY),
            fill_value=0
        )

    @metric(restricted_to="dispatch")
    def renewable_dispatch(self):
        # [optional] renewable production in MW that can help find patterns and correlations
        return NotImplementedError

    @metric(restricted_to="dispatch")
    def consumption(self):
        # [optional] consumption in MW that can help find patterns and correlations
        return NotImplementedError

    @metric(restricted_to="redispatch")
    def renewable_redispatch(self):
        # [optional] ramped up and down renewable production in MW that can help find patterns and correlations
        return NotImplementedError

    def _redispatched_volume(self, n: pypsa.Network):
        redispatch_carriers_generators = n.carriers.filter(
            regex=r"Generator ramp (up|down)$", axis=0
        ).index.tolist()

        redispatch_carriers_links = n.carriers.filter(
            regex=r"Link ramp (up|down)$", axis=0
        ).index.tolist()

        redispatch_carriers_storage = n.carriers.filter(
            regex=r"StorageUnit ramp (up|down)$", axis=0
        ).index.tolist()

        buses = n.buses.query("carrier in ['AC', 'AC_OH']").index.to_list()
        n.generators["ramp carrier"] = ""
        n.links["ramp carrier"] = ""
        n.storage_units["ramp carrier"] = ""

        for bus in buses:
            str_to_replace = f"{bus} "
            gen_select = n.generators.index.str.contains("ramp") & (n.generators["bus"] == bus)
            n.generators.loc[gen_select, "ramp carrier"] = n.generators.loc[gen_select].index.str.replace(str_to_replace, "")

            link_select = n.links.index.str.contains("ramp") & ((n.links["bus0"] == bus) | (n.links["bus1"] == bus))
            n.links.loc[link_select, "ramp carrier"] = n.links.loc[link_select].index.str.replace(str_to_replace, "")

            storage_select = n.storage_units.index.str.contains("ramp") & (n.storage_units["bus"] == bus)
            n.storage_units.loc[storage_select, "ramp carrier"] = n.storage_units.loc[storage_select].index.str.replace(str_to_replace, "")

        redispatch_volume_generators = n.statistics.energy_balance(
            comps=["Generator"],
            bus_carrier="AC",
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country", "ramp carrier"],
            carrier=redispatch_carriers_generators,
            drop_zero=False
        ).query("not bus.str.contains('EU ')").abs()  # take the absolute value to have a positive volume for both ramp up and ramp down

        redispatch_volume_links = n.statistics.energy_balance(
            comps=["Link"],
            bus_carrier="AC",
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country", "ramp carrier"],
            carrier=redispatch_carriers_links,
            drop_zero=False
        ).abs()  # take the absolute value to have a positive volume for both ramp up and ramp down

        redispatch_volume_storage = n.statistics.energy_balance(
            bus_carrier="AC",
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country", "ramp carrier"],
            carrier=redispatch_carriers_storage,
            drop_zero=False
        ).abs()

        redispatch_volume_total = pd.concat(
            [redispatch_volume_generators, redispatch_volume_links, redispatch_volume_storage],
            axis=0
        )
        return redispatch_volume_total

    def _countertraded_volume(self, n: pypsa.Network):
        counter_trading_carriers = n.carriers.filter(
            regex=r"Link ramp (up|down)$", axis=0
        ).index.tolist()

        counter_trading_volume = n.statistics.energy_balance(
            comps=["Generator"],
            bus_carrier="AC",
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country", "ramp carrier"],
            carrier=counter_trading_carriers,
            drop_zero=False
        ).abs()  # take the absolute value to have a positive volume for both ramp up and ramp down
        return counter_trading_volume

    def _load_shedding_volume(self, n: pypsa.Network):
        load_shedding_volume = n.statistics.energy_balance(
            comps=["Generator"],
            bus_carrier="AC",
            groupby_time=self.groupby_time,
            groupby=["name", "carrier", "bus", "country"],
            carrier="load",
            drop_zero=False
        ).abs()  # take the absolute value to have a positive volume
        return load_shedding_volume

    @metric(restricted_to="redispatch")
    def redispatched_volume(self, n: pypsa.Network, **kwargs):
        return self._redispatched_volume(n=n)

    @metric(restricted_to="redispatch")
    def countertraded_volume(self, n: pypsa.Network, **kwargs):
        return self._countertraded_volume(n=n)

    @metric(restricted_to="redispatch")
    def load_shedding_volume(self, n: pypsa.Network):
        return self._load_shedding_volume(n=n)

    @metric(restricted_to="redispatch")
    def constraint_management_volume(self, n: pypsa.Network):
        return pd.concat(
            [
                self._congestion_management_volume_without_shedding(n=n),
                self._load_shedding_volume(n=n)
            ],
            axis=0
        )

    def _congestion_management_volume_without_shedding(self, n: pypsa.Network):
        congestion_management_volume = pd.concat(
            [
                self._redispatched_volume(n=n),
                self._countertraded_volume(n=n)
            ],
            axis=0
        )
        return congestion_management_volume

    def _congestion_management_cost_without_shedding(self, n: pypsa.Network):
        congestion_management_cost = pd.concat(
            [
                self._redispatch_costs(n=n),
                self._countertrading_costs(n=n)
            ],
            axis=0
        )
        return congestion_management_cost

    @metric(restricted_to="redispatch")
    def average_ramping_costs_per_mw(self, n: pypsa.Network, **kwargs):
        cost = self._congestion_management_cost_without_shedding(n).droplevel(["bus", "country"]) # drop the bus and country level to have the same index as the volume
        volume = self._congestion_management_volume_without_shedding(n).droplevel(["bus", "country", "ramp carrier"])  # drop the bus and country level to have the same index as the volume

        def total_in_direction(df, direction):
            return df.filter(like=f"ramp {direction}", axis=0).sum().sum()

        return pd.Series(
            {
                f"avg_cost_ramp_{direction}": total_in_direction(cost, direction) / total_in_direction(volume, direction)
                for direction in ["up", "down"]
            }
        )


if __name__ == "__main__":
    from modules.analysis_toolkit.helpers.plotting import TimeSeriesPlot, HistogramPlot, BarChartPlot, WaterfallPlot

    study_years = [2030, 2040]
    rc = {
        year: ResultsComputer(
        year=year,
        apply_snapshot_filter=False
        )
        for year in [2030, 2040]
    }

    #rc[2030].constraint_costs.iem_redispatch()
    #rc[2030].congestion_income.compare_dispatch()
    final_surplus_df = rc[2030].storage_surplus_system.compare_dispatch().groupby(level = 0, axis = 1).sum().sum(axis = 0)/1e6
    congestion_income_df = rc[2030].congestion_income.compare_dispatch().groupby(level = 0, axis = 1).sum().sum(axis = 0)/1e6

    welfare_comparison = rc[2030].welfare_system.compare_dispatch()
    price_spreads = rc[2030].interconnector_price_spreads.compare_dispatch()

    print()