from typing import Callable

import matplotlib.pyplot as plt
import seaborn as sns
from modules.analysis_toolkit.helpers.colors import Color
from modules.analysis_toolkit.analyzer import ResultsComputer


FIG_SIZE = (10, 6)


# TODO: in these examples, we have a lot of repeated code (because I coded them quickly).
#  If you find a better way, please feel free to refactor them.
#  Let's use the same name for the plot as for the ResultsComputer method that finds the data. If you need to use more
#  than one method, it might be a sign that you should create a new method in the ResultsComputer that combines the data in the way you need for the plot.

class TimeSeriesPlot:
    @staticmethod
    def interconnector_flows(rc: ResultsComputer, interconnectors: list[str]=None) -> None:
        data = rc.interconnector_flows.compare_dispatch()
        interconnectors_to_plot = interconnectors if interconnectors is not None else data.index.get_level_values("name")
        scenarios = data.index.get_level_values(0)
        colormap = Color.get_n_colors(len(interconnectors))
        print(colormap)
        f, axes = plt.subplots(nrows=len(interconnectors_to_plot), ncols=1, figsize=FIG_SIZE)
        for k, (ic, ax) in enumerate(zip(interconnectors_to_plot, axes)):
            data.loc[ic].T.unstack(level=0).plot(ax=ax, color=Color.listed_colormap_in_color(colormap[k], len(scenarios)).colors, alpha=0.5)
            ax.legend()
            ax.set_ylabel(f"{ic}\nFlow [MW]")
        plt.xlabel("Time")
        plt.title("Dispatch Flows through Interconnectors (2030)")
        plt.tight_layout()
        plt.show()

class ScatterPlot:
    pass

class WaterfallPlot:
    pass

class BarChartPlot:
    pass

class HistogramPlot:
    @staticmethod
    def restricted_capacity(rc: ResultsComputer) -> None:
        data = rc.restricted_capacity()
        interconnectors = data.index.get_level_values("name")
        colormap = Color.get_n_colors(len(interconnectors))
        f, axes = plt.subplots(nrows=len(interconnectors), ncols=1, figsize=FIG_SIZE)
        for k, (ic, ax) in enumerate(zip(interconnectors, axes)):
            data.loc[ic].T.plot.hist(ax=ax, color=colormap(k), bins=50, sharex=True)
            ax.legend()
        plt.xlabel("Restricted Capacity [MW]")
        plt.tight_layout()
        plt.show()