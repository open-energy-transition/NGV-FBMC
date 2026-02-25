import pypsa
import pandas as pd

from .helpers.results_computer_base import ResultsComputerBase
from .helpers.results_computer_wrappers import metric


class ResultsComputer(ResultsComputerBase):
    """Compact results computer: expose metrics by decorating methods with @metric.

    Example:
      @metric
      def revenue(self, n):
          return n.statistics.revenue()

    Callers can use: res.revenue.iem(**kwargs), res.revenue.diff(), res.revenue.sq(), res.revenue(n, **kwargs)
    """
    def __init__(self, network_dict: dict[str, pypsa.Network]):
        super().__init__(network_dict)

    @metric
    def revenue(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Total component revenues for a network."""
        return n.statistics.revenue(**kwargs)

    @metric
    def prices(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Average price difference across all interconnectors for a network."""
        return n.statistics.prices(**kwargs)

    @metric
    def curtailment(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Total curtailment of renewable generation for a network."""
        return n.statistics.curtailment(**kwargs)

    @metric
    def system_cost(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Total system cost for a network."""
        return n.statistics.system_cost(**kwargs)

    @metric
    def capex(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Total system cost for a network."""
        return n.statistics.capex(**kwargs)

    @metric
    def opex(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Total system cost for a network."""
        return n.statistics.opex(**kwargs)

    @metric
    def energy_balance(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Energy balance for a network."""
        return n.statistics.energy_balance(**kwargs)

    @metric
    def capacity_factor(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Capacity factor for a network."""
        return n.statistics.capacity_factor(**kwargs)

    @metric
    def market_value(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Market value for a network."""
        return n.statistics.market_value(**kwargs)

    @metric
    def supply(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Supply for a network."""
        return n.statistics.supply(**kwargs)

    @metric
    def withdrawal(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Withdrawal for a network."""
        return n.statistics.withdrawal(**kwargs)

    @metric
    def transmission(self, n: pypsa.Network, **kwargs):
        """PyPSA.statistics - Transmission statistics for a network."""
        return n.statistics.transmission(**kwargs)

    @metric
    def consumer_surplus(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def producer_surplus(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def border_flows(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def border_price_spreads(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def congestion_income(self, n: pypsa.Network, **kwargs):
        return NotImplementedError()

    @metric
    def storage_surplus(self, n: pypsa.Network, **kwargs):
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