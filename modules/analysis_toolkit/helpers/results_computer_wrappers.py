from typing import Callable, Any, Optional, Dict, Literal
import pypsa
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


def metric(func: Optional[Callable[..., Any]] = None, *, restricted_to: Optional[Literal["dispatch", "redispatch"]] = None):
    """Decorator (or decorator factory) that turns a (self, network)->value method into a property returning
    a bound metric object with .sq(), .iem(), .tf(), .diff() and callable behavior.

    Supports both usages:
      - @metric
      - @metric(restricted_to="dispatch")

    See the inner _make_prop for the actual implementation.
    """
    def _make_prop(func: Callable[..., Any]):
        def restricted_method(method):
            @wraps(method)
            def wrapper(self, *args, **kwargs):
                # Accept arbitrary args/kwargs because this decorator is applied both to
                # the original metric (which expects a Network as first arg) and to the
                # helper methods on the bound-metric object (which don't pass a Network).
                # Use the method's name to check whether the called helper matches the
                # requested optimization stage.
                if restricted_to and method.__name__.split('_')[-1] != restricted_to:
                    raise AttributeError(f"This metric is only available for {restricted_to} stage.")
                return method(self, *args, **kwargs)
            return wrapper

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

                @restricted_method
                def sq_dispatch(self, **kwargs):
                    return self._rc._sq_dispatch(lambda n: self._f(n, **kwargs))

                @restricted_method
                def iem_dispatch(self, **kwargs):
                    return self._rc._iem_dispatch(lambda n: self._f(n, **kwargs))

                @restricted_method
                def iem_fb_dispatch(self, **kwargs):
                    return self._rc._iem_fb_dispatch(lambda n: self._f(n, **kwargs))

                @restricted_method
                def sq_redispatch(self, **kwargs):
                    return self._rc._sq_redispatch(lambda n: self._f(n, **kwargs))

                @restricted_method
                def iem_redispatch(self, **kwargs):
                    return self._rc._iem_redispatch(lambda n: self._f(n, **kwargs))

                @restricted_method
                def iem_fb_redispatch(self, **kwargs):
                    return self._rc._iem_fb_redispatch(lambda n: self._f(n, **kwargs))

                def diff_sq(self, **kwargs):
                    return self._rc._diff_sq(lambda n: self._f(n, **kwargs))

                def diff_iem(self, **kwargs):
                    return self._rc._diff_iem(lambda n: self._f(n, **kwargs))

                def diff_iem_fb(self, **kwargs):
                    return self._rc._diff_iem_fb(lambda n: self._f(n, **kwargs))

                @restricted_method
                def compare_dispatch(self, **kwargs):
                    return self._rc._compare_dispatch(lambda n: self._f(n, **kwargs))

                @restricted_method
                def compare_redispatch(self, **kwargs):
                    return self._rc._compare_redispatch(lambda n: self._f(n, **kwargs))

                def compare_diff(self, **kwargs):
                    return self._rc._compare_diff(lambda n: self._f(n, **kwargs))

                def __call__(self, *args, **kwargs):
                    # Allowed: called with a Network (optionally with kwargs) -> compute and return result
                    if args:
                        n = args[0]
                        combined = self._rc._combine(kwargs)
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

    # If used as @metric or @metric(...)
    if func is None:
        return _make_prop
    return _make_prop(func)
