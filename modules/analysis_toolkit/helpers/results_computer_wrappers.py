from typing import Callable, Any, Optional, Dict
import pypsa
import pandas as pd
from functools import wraps


class NetworkSelector:

    def __init__(self, network_dict: dict[str, pypsa.Network]):
        for key in ['n_sq_dispatch', 'n_iem_dispatch', 'n_iem_fb_dispatch', 'n_sq_redispatch', 'n_iem_redispatch', 'n_iem_fb_redispatch']:
            if key not in network_dict:
                raise ValueError(f"Missing required network '{key}' in network_dict")
        self.network_dict = network_dict

    def get_sq_dispatch(self) -> pypsa.Network:
        return self.network_dict['n_sq_dispatch']

    def get_iem_dispatch(self) -> pypsa.Network:
        return self.network_dict['n_iem_dispatch']

    def get_iem_fb_dispatch(self) -> pypsa.Network:
        return self.network_dict['n_iem_fb_dispatch']

    def get_sq_redispatch(self) -> pypsa.Network:
        return self.network_dict['n_sq_redispatch']

    def get_iem_redispatch(self) -> pypsa.Network:
        return self.network_dict['n_iem_redispatch']

    def get_iem_fb_redispatch(self) -> pypsa.Network:
        return self.network_dict['n_iem_fb_redispatch']


def metric(func: Callable[..., Any]):
    """Decorator that turns a (self, network)->value method into a property returning
    a bound metric object with .sq(), .iem(), .tf(), .diff() and callable behavior.

    Chosen usage (clear and unambiguous):
      - results.revenue.iem(**kwargs)        # pass kwargs to underlying n.statistics.* call
      - results.revenue(n, **kwargs)         # compute metric for explicit network n with kwargs
    Not supported:
      - results.revenue(**kwargs)            # ambiguous: kwargs without explicit network

    This keeps configuration explicit (kwargs provided where the computation happens).
    """

    @property
    @wraps(func)
    def _prop(instance: "ResultsComputer"):
        # bound function (n, **kwargs) -> func(instance, n, **kwargs)
        def bound_fn(n: pypsa.Network, **kwargs):
            return func(instance, n, **kwargs)

        class _BM:
            def __init__(self, rc: "ResultsComputer", f: Callable[[pypsa.Network, Any], Any],
                         saved_kwargs: Optional[Dict] = None):
                self._rc = rc
                self._f = f
                # saved_kwargs is not used by external callers in this design, but keep for internal convenience
                self._saved_kwargs = dict(saved_kwargs) if saved_kwargs else {}

            def sq(self, **kwargs):
                return self._rc._sq(lambda n: self._f(n, **kwargs))

            def iem(self, **kwargs):
                return self._rc._iem(lambda n: self._f(n, **kwargs))

            def iem_fb(self, **kwargs):
                return self._rc._tf(lambda n: self._f(n, **kwargs))

            def diff(self, **kwargs):
                return self._rc._diff(lambda n: self._f(n, **kwargs))

            def compare(self, **kwargs):
                return self._rc._compare(lambda n: self._f(n, **kwargs))

            def __call__(self, *args, **kwargs):
                # Allowed: called with a Network (optionally with kwargs) -> compute and return result
                if args:
                    n = args[0]
                    combined = self._combine(kwargs)
                    return self._f(n, **combined)
                # Disallow: kwargs without a Network -> ambiguous usage
                if kwargs:
                    raise TypeError(
                        "Passing kwargs to the metric property without a Network is not supported. "
                        "Use .{scenario}_{optimization_stage}(**kwargs) or call the metric with a Network: "
                        "results.revenue(n, **kwargs)"
                        "where scenario is one of 'sq', 'iem', 'iem_fb' "
                        "and optimization_stage is one of 'dispatch', 'redispatch'."
                    )
                # No args/kwargs -> return self (no-op), allowing chaining like results.revenue.iem()
                return self

        return _BM(instance, bound_fn)

    return _prop