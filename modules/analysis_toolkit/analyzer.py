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

    @metric
    def constraint_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        # Constraint costs include both re-dispatch and counter-trading costs
        constraint_carriers = n.carriers.filter(
            regex=r" ramp (up|down)$", axis=0
        ).index.tolist() + ["Load Shedding"]

        constraint_costs = n.statistics.opex(
            comps="Generator", groupby_time = False, groupby= ["name", "carrier", "bus"], carrier=constraint_carriers
        )

        return constraint_costs

    @metric
    def countertrading_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        counter_trading_carriers = n.carriers.filter(
            regex=r"Link ramp (up|down)$", axis=0
        ).index.tolist()

        counter_trading_costs = n.statistics.opex(
            comps="Generator", groupby_time = False, groupby= ["name", "carrier", "bus"], carrier=counter_trading_carriers
        )

        return counter_trading_costs


    @metric
    def redispatch_costs(self, n: pypsa.Network, **kwargs): # To be used in the re-dispatch model only
        redispatch_carriers = n.carriers.filter(
            regex=r"(Generator|StorageUnit) ramp (up|down)$", axis=0
        ).index.tolist()

        redispatch_costs = n.statistics.opex(
            comps="Generator", groupby_time = False, groupby= ["name", "carrier", "bus"], carrier=redispatch_carriers
        )

        return redispatch_costs

    @metric
    def consumer_costs(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the consumer cost per component.
        The storage units, the links and the lines are excluded from the calculation.
        The demand consists of both unmanaged and DSR components.
        """
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC"], groupby_time = False, groupby= ["name", "carrier", "bus", "country"]).xs("GB", level = 4)
        excluded_carriers = ["DC"]
        links_to_exclude = n.links[n.links.carrier.isin(excluded_carriers)].index

        component_cashflows = n.statistics.revenue(bus_carrier=["AC"], groupby = ["name", "carrier", "bus", "country"], groupby_time = False).xs("GB", level = 4)
        cashflow_of_consumption = component_cashflows[energy_balance.sum(axis=1) < 0]
        consumer_cost = cashflow_of_consumption.drop(links_to_exclude, level=1).drop("StorageUnit", level="component").drop("Line", level = "component")
        return consumer_cost

    @metric
    def producer_costs(self, n: pypsa.Network, **kwargs): # To be used in the Dispatch model only
        """
        The method returns a disaggregated data frame with the time series of the producer cost per component.
        The storage unit and the links are excluded from the calculation. Lines are not included in the opex data frame, so no need to exclude them.
        Note that in the GB dispatch model, all producers are modeled as generators (and not links), which makes the calculations easier.
        Also, the fuel and co2 costs are included in the marginal cost of the generators.
        """
        energy_balance = n.statistics.energy_balance(bus_carrier=["AC"], groupby_time = False, groupby= ["name", "carrier", "bus", "country"]).xs("GB", level = 4)
        excluded_carriers = ["DC"]
        links_to_exclude = n.links[n.links.carrier.isin(excluded_carriers)].index

        component_opex = n.statistics.opex(bus_carrier=["AC"], groupby = ["name", "carrier", "bus", "country"], groupby_time = False).xs("GB", level = 4)

        # --- Align both dataframes to have the exact same index --- (to avoid a warning of missing indexes)
        energy_balance, component_opex = energy_balance.align(component_opex, join='inner')

        cashflow_of_production = component_opex[energy_balance.sum(axis=1) > 0]
        producer_cost = cashflow_of_production.drop(links_to_exclude, level=1).drop("StorageUnit", level="component")
        return producer_cost


if __name__ == "__main__":
    rc = ResultsComputer(year=2030)
    rc.redispatch_costs.iem_redispatch()
    print()