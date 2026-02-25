from typing import Callable, Any, Literal
import pypsa
import pandas as pd

from .constants import GROUPBY_OPTIONS
from .helpers.results_computer_wrappers import NetworkSelector
from .filepaths import get_networks_for_year


class ResultsComputerBase:
    """Compact results computer: expose metrics by decorating methods with @metric.

    Example:
      @metric
      def revenue(self, n):
          return n.statistics.revenue()

    Callers can use: res.revenue.iem(**kwargs), res.revenue.diff(), res.revenue.sq(), res.revenue(n, **kwargs)
    """

    def __init__(self, network_paths_dict: dict[str, pypsa.Network]):
        self.ns: NetworkSelector = NetworkSelector(network_paths_dict)
        self.groupby: list[GROUPBY_OPTIONS] = ["component", "carrier"]

    def change_computation_settings(self, groupby: list[GROUPBY_OPTIONS], groupby_time: bool):
        self.groupby = groupby
        self.groupby_time = groupby_time

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
        return func(self.ns.get_sq_redispatch()) - func(self.ns.get_sq_dispatch())

    def _diff_iem(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_iem_redispatch()) - func(self.ns.get_iem_dispatch())

    def _diff_iem_fb(self, func: Callable[[pypsa.Network], Any]):
        return func(self.ns.get_iem_fb_redispatch()) - func(self.ns.get_iem_fb_dispatch())

    def _compare_dispatch(self, func: Callable[[pypsa.Network], Any]):
        return pd.concat({
            'sq': self._sq_dispatch(func),
            'iem': self._iem_dispatch(func),
            'iem_fb': self._iem_fb_dispatch(func)
        }, axis=1)

    def _compare_redispatch(self, func: Callable[[pypsa.Network], Any]):
        return pd.concat({
            'sq': self._sq_redispatch(func),
            'iem': self._iem_redispatch(func),
            'iem_fb': self._iem_fb_redispatch(func)
        }, axis=1)

    def _compare_diff(self, func: Callable[[pypsa.Network], Any]):
        return pd.concat({
            'sq': self._diff_sq(func),
            'iem': self._diff_iem(func),
            'iem_fb': self._diff_iem_fb(func)
        }, axis=1)