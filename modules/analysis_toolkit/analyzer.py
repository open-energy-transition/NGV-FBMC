import pypsa
import pandas as pd

from helpers.results_computer_base import ResultsComputerBase
from helpers.results_computer_wrappers import metric
#from helpers.boundaries import get_fb_constraints, get_link_columns_in_ptdf

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

    @metric(restricted_to= "redispatch")
    def constraint_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        # Constraint costs include both re-dispatch and counter-trading costs
        constraint_carriers = n.carriers.filter(
            regex=r" ramp (up|down)$", axis=0
        ).index.tolist() + ["Load Shedding"]

        constraint_costs = n.statistics.opex(
            comps="Generator", groupby_time = False, groupby= ["name", "carrier", "bus"], carrier=constraint_carriers
        )

        return constraint_costs

    @metric(restricted_to= "redispatch")
    def countertrading_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        counter_trading_carriers = n.carriers.filter(
            regex=r"Link ramp (up|down)$", axis=0
        ).index.tolist()

        counter_trading_costs = n.statistics.opex(
            comps="Generator", groupby_time = False, groupby= ["name", "carrier", "bus"], carrier=counter_trading_carriers
        )

        return counter_trading_costs


    @metric(restricted_to= "redispatch")
    def redispatch_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        redispatch_carriers = n.carriers.filter(
            regex=r"(Generator|StorageUnit) ramp (up|down)$", axis=0
        ).index.tolist()

        redispatch_costs = n.statistics.opex(
            comps="Generator", groupby_time = False, groupby= ["name", "carrier", "bus"], carrier=redispatch_carriers
        )

        return redispatch_costs

    @metric(restricted_to= "redispatch")
    def load_shedding_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        load_shedding_costs = n.statistics.opex(
            comps="Generator", groupby_time = False, groupby= ["name", "carrier", "bus"], carrier="Load Shedding"
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
    def producer_costs(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
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
    def producer_surplus(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
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
    def storage_surplus(self, n: pypsa.Network, **kwargs):  # To be used in the Dispatch model only
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
    rc.redispatch_costs.iem_redispatch()
    print()