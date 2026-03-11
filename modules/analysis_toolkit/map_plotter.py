import geopandas as gpd
import pydeck as pdk
import pandas as pd

from modules.analysis_toolkit.analyzer import ResultsComputer
from modules.analysis_toolkit.helpers.config.filepaths import get_etys_boundaries_geopandas_fp
from modules.analysis_toolkit.helpers.boundaries import get_capacities_map


def add_boundaries_layer(deck: pdk.Deck, year:int) -> pdk.Deck:
    etys_gdf = gpd.read_file(get_etys_boundaries_geopandas_fp())
    etys_gdf = etys_gdf.to_crs(epsg=4326)
    capacities = get_capacities_map(year=year)
    etys_gdf["capacity"] = etys_gdf["Boundary_n"].map(capacities)

    # Use single quotes inside the f-string to avoid quoting conflicts
    etys_gdf["tooltip_html"] = etys_gdf.apply(
        lambda row: f"<b>Boundary {row['Boundary_n']}</b>"
                    f"<p>  Capacity: {row['capacity']}</p>",
        axis=1,
    )
    etys_gdf = etys_gdf[~etys_gdf["capacity"].isna()]

    geojson = etys_gdf.__geo_interface__
    etys_layer = pdk.Layer(
        "GeoJsonLayer",
        data=geojson,
        id="ETYS boundaries",
        stroked=True,
        filled=True,
        pickable=True,           # enable mouse picking
        auto_highlight=True,     # highlight on hover
        highlight_color=[255, 255, 0, 180],
        get_fill_color=[255, 0, 0, 40],  # translucent fill
        get_line_color=[255, 0, 0, 120],  # solid outline
        line_width_min_pixels=2
    )
    # Insert at position 0 so the boundaries render under the other layers
    deck.layers.insert(0, etys_layer)
    return deck


def plot_network_map(rc: ResultsComputer):
    """Plot the network map with line flows for a given snapshot."""
    n = rc.ns.get_iem_dispatch()
    year = rc.year

    allowed = ["DC", "DC_OH"]
    widths = pd.Series(0.0, index=n.links.index)
    mask = n.links.carrier.isin(allowed)
    widths.loc[mask] = 10 / 3e3 * n.links['p_nom'].loc[mask].clip(lower=0, upper = 3e3)
    deck = n.explore(
        link_width=widths,
        link_columns=["p_nom"],
        line_columns=["p_nom"]
    )

    deck = add_boundaries_layer(deck, year=year)
    deck.to_html(f"EXPLORE_IEM_DISPATCH_{year}.html")


if __name__ == "__main__":
    rc = ResultsComputer(year=2030)
    plot_network_map(rc)
