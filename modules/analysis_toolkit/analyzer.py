import pypsa
import pandas as pd

from helpers.results_computer_base import ResultsComputerBase
from helpers.results_computer_wrappers import metric


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


if __name__ == "__main__":
    rc = ResultsComputer(year=2030)
    print()