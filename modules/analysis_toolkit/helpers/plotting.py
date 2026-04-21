from typing import Literal
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from modules.analysis_toolkit.helpers.colors import Color
from modules.analysis_toolkit.analyzer import ResultsComputer


FIG_SIZE = (10, 6)
OUTLIER_LOWER_QUANTILE = 0.01
OUTLIER_UPPER_QUANTILE = 0.99


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

    @staticmethod
    def boundary_loading_dispatch(rc: ResultsComputer, which: Literal["ptdf", "actual"]):
        loading = rc.boundary_loading.compare_dispatch(which=which)
        boundaries = loading.index.get_level_values(level="boundary").unique()
        for b in boundaries:
            _, axes = plt.subplots(2, 1, figsize=(10, 7))
            plt.suptitle(f"Boundary {b} - {which}")
            loading.xs(b, level="boundary").xs("DIRECT", level="direction").plot(ax=axes[0])
            axes[0].set_ylabel("loading DIRECT [pu]")
            loading.xs(b, level="boundary").xs("OPPOSITE", level="direction").plot(ax=axes[1])
            axes[1].set_ylabel("loading OPPOSITE [pu]")

    @staticmethod
    def boundary_loading_redispatch(rc: ResultsComputer, which: Literal["ptdf", "actual"]):
        loading = rc.boundary_loading.compare_redispatch(which=which)
        boundaries = loading.index.get_level_values(level="boundary").unique()
        for b in boundaries:
            _, axes = plt.subplots(2, 1, figsize=(10, 7))
            plt.suptitle(f"Boundary {b} - {which}")
            loading.xs(b, level="boundary").xs("DIRECT", level="direction").plot(ax=axes[0])
            axes[0].set_ylabel("loading DIRECT [pu]")
            loading.xs(b, level="boundary").xs("OPPOSITE", level="direction").plot(ax=axes[1])
            axes[1].set_ylabel("loading OPPOSITE [pu]")

class ScatterPlot:
    @staticmethod
    def interconnector_inefficiencies(rc: ResultsComputer): # currently works for sq_dispatch only
        flows_df = rc.interconnector_flows.sq_dispatch().T
        price_spreads_df = rc.interconnector_price_spreads.sq_dispatch().T

        for intercon in flows_df.columns:
            if flows_df[intercon].abs().max() < 0.001:  # To avoid interconnectors with 0 flow (not built)
                continue

            data = pd.DataFrame({
                'Price Difference [€/MWh]': price_spreads_df[intercon],
                'Flow [MW]': flows_df[intercon]
            })

            # Outlier Removal
            p_low = data['Price Difference [€/MWh]'].quantile(0.01)
            p_high = data['Price Difference [€/MWh]'].quantile(0.99)

            mask = (
                    (data['Price Difference [€/MWh]'] >= p_low) &
                    (data['Price Difference [€/MWh]'] <= p_high)
            )

            data_clean = data[mask]

            plt.figure(figsize=FIG_SIZE)  # Square figure is best for parity plots
            sns.set_context("talk")

            # Scatter Plot
            sns.scatterplot(
                data=data_clean,
                x='Price Difference [€/MWh]',
                y='Flow [MW]',
                s=50,  # Size of dots
                alpha=0.7,
                edgecolor='black'  # distinct borders
            )

            # Apply the limits for symmetric plot
            max_abs_val = max(
                abs(data_clean['Price Difference [€/MWh]'].min()),
                abs(data_clean['Price Difference [€/MWh]'].max())
            )
            limit = max_abs_val * 1.1
            plt.xlim(-limit, limit)
            plt.title(f'Interconnector: {intercon}')
            filename = f"interconnector_inefficiency_{rc.year}_{intercon}.png"
            output_folder = f"saved_results/plots/interconnector_inefficiencies_{rc.year}"
            filepath = Path(f"{output_folder}/{filename}")
            filepath.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(filepath, dpi=200, bbox_inches="tight")

class DurationCurvePlot:
    @staticmethod
    def price_spread_duration_curves(rc: ResultsComputer):
        price_spreads_df = rc.interconnector_price_spreads.sq_dispatch().T
        for interconnector in price_spreads_df.columns:
            if price_spreads_df[interconnector].isnull().all():
                continue
            price_spread = price_spreads_df[interconnector]
            price_spread_sorted = price_spread.sort_values(ascending=False)
            price_spread_sorted = price_spread_sorted[price_spread_sorted.abs() < 500]
            x_axis = np.linspace(0, 100, len(price_spread_sorted))
            plt.figure(figsize=FIG_SIZE)
            sns.set_context("talk")
            plt.plot(x_axis, price_spread_sorted, color='#344CAF', linewidth=2)
            plt.axhline(0, color='black', linewidth=0.8, linestyle='--')
            plt.title(f"Interconnector: {interconnector}")
            plt.xlabel("Percentage of Time [%]")
            plt.ylabel("Price Spread [€/MWh]")
            plt.ylim(-200, 200)
            filename = f"price_spread_curve_{rc.year}_{interconnector}.png"
            output_folder = f"saved_results/plots/price_spread_curves_{rc.year}"
            filepath = Path(f"{output_folder}/{filename}")
            filepath.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(filepath,bbox_inches="tight")

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
            data.loc[ic].T.plot.hist(ax=ax, color=colormap[k], bins=50, sharex=True)
            ax.legend()
        plt.xlabel("Restricted Capacity [MW]")
        plt.tight_layout()
        plt.show()