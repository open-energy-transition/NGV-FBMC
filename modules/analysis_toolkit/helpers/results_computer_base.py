from typing import Callable, Any
import pypsa
import pandas as pd

from modules.analysis_toolkit.helpers.config.filepaths import get_network_fps_for_year
from modules.analysis_toolkit.helpers.config.constants import GROUPBY_OPTIONS, GLOBAL_GROUPBY
from modules.analysis_toolkit.helpers.results_computer_wrappers import NetworkSelector, metric


class ResultsComputerBase:
    """Compact results computer: expose metrics by decorating methods with @metric.

    Example:
      @metric
      def revenue(self, n):
          return n.statistics.revenue()

    Callers can use: res.revenue.iem(**kwargs), res.revenue.diff(), res.revenue.sq(), res.revenue(n, **kwargs)
    """

    SNAPSHOT_FILTER_CARRIER = "AC"
    SNAPSHOT_FILTER_SQ_THRESHOLD = 2900.0
    SNAPSHOT_FILTER_IEM_THRESHOLD = 2900.0

    def __init__(self, year: int, apply_snapshot_filter: bool = False):
        self.year = year
        network_dict = self.get_network_dict()

        if apply_snapshot_filter:
            network_dict = self._apply_snapshot_filter(network_dict)

        self.ns: NetworkSelector = NetworkSelector(network_dict)
        self.groupby: list[GROUPBY_OPTIONS] = GLOBAL_GROUPBY
        self.groupby_time: bool = False

    def get_network_dict(self) -> dict[str, pypsa.Network]:
        return {name: pypsa.Network(path)
                for name, path in get_network_fps_for_year(self.year).items()
                }

    def change_computation_settings(self, groupby: list[GROUPBY_OPTIONS], groupby_time: bool):
        self.groupby = groupby
        self.groupby_time = groupby_time

    @staticmethod
    def _get_snapshots_exceeding_threshold(n: pypsa.Network, carrier: str, threshold: float) -> pd.Index:
        """Return snapshots where at least one bus with given carrier has marginal_price > threshold."""
        buses = n.buses[n.buses.carrier == carrier].index
        if buses.empty:
            return pd.DatetimeIndex([])
        prices = n.buses_t.marginal_price[buses]
        return prices.index[(prices > threshold).any(axis=1)]

    def _apply_snapshot_filter(
            self, network_dict: dict[str, pypsa.Network]
    ) -> dict[str, pypsa.Network]:
        """Apply snapshot filtering to all networks.

        Args:
            network_dict: Dictionary of loaded networks
        Returns:
            Dictionary with filtered networks (originals are copied, not modified)
        """
        carrier = self.SNAPSHOT_FILTER_CARRIER
        sq_threshold = self.SNAPSHOT_FILTER_SQ_THRESHOLD
        iem_threshold = self.SNAPSHOT_FILTER_IEM_THRESHOLD

        to_drop_sq = self._get_snapshots_exceeding_threshold(
            network_dict["n_sq_dispatch"], carrier, sq_threshold
        )
        to_drop_iem = self._get_snapshots_exceeding_threshold(
            network_dict["n_iem_dispatch"], carrier, iem_threshold
        )
        to_drop = to_drop_sq.union(to_drop_iem)

        filtered = {}
        for name, n in network_dict.items():
            m = n.copy()
            keep = m.snapshots.difference(to_drop)
            m.set_snapshots(keep)
            filtered[name] = m
        return filtered

    # small helpers used by the bound-metric object
    def _sq_dispatch(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_sq_dispatch())

    def _iem_dispatch(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_iem_dispatch())

    def _iem_fb_dispatch(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_iem_fb_dispatch())

    def _sq_redispatch(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_sq_redispatch())

    def _iem_redispatch(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_iem_redispatch())

    def _iem_fb_redispatch(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_iem_fb_redispatch())

    def _diff_sq(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_sq_redispatch()).sub(func(self.ns.get_sq_dispatch()), fill_value=0)

    def _diff_iem(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_iem_redispatch()).sub(func(self.ns.get_iem_dispatch()), fill_value=0)

    def _diff_iem_fb(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_iem_fb_redispatch()).sub(func(self.ns.get_iem_fb_dispatch()), fill_value=0)

    def _compare_dispatch(self, func: Callable[[pypsa.Network], Any]):
        return pd.concat({
            'sq': self._sq_dispatch(func),
            'iem': self._iem_dispatch(func),
            'iem_fb': self._iem_fb_dispatch(func),
            'diff: iem-sq': self._iem_dispatch(func).sub(self._sq_dispatch(func), fill_value=0),
            'diff: iemfb-iem': self._iem_fb_dispatch(func).sub(self._iem_dispatch(func), fill_value=0)
        }, axis=1, names=["scenario"])

    def _compare_redispatch(self, func: Callable[[pypsa.Network], Any]):
        return pd.concat({
            'sq': self._sq_redispatch(func),
            'iem': self._iem_redispatch(func),
            'iem_fb': self._iem_fb_redispatch(func),
            'diff: iem-sq': self._iem_redispatch(func).sub(self._sq_redispatch(func), fill_value=0),
            'diff: iemfb-iem': self._iem_fb_redispatch(func).sub(self._iem_redispatch(func), fill_value=0)
        }, axis=1, names=["scenario"])

    def _compare_diff(self, func: Callable[[pypsa.Network], Any]):
        return pd.concat({
            'sq': self._diff_sq(func),
            'iem': self._diff_iem(func),
            'iem_fb': self._diff_iem_fb(func)
        }, axis=1, names=["scenario"])

    # native statistics package functions

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