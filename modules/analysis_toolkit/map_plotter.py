import geopandas as gpd
import pydeck as pdk
import pandas as pd
import numpy as np
from html import escape

from modules.analysis_toolkit.analyzer import ResultsComputer
from modules.analysis_toolkit.helpers.config.filepaths import get_etys_boundaries_geopandas_fp
from modules.analysis_toolkit.helpers.boundaries import get_capacities_map


RING_COLORS = [
    [31, 119, 180],
    [255, 127, 14],
    [44, 160, 44],
    [214, 39, 40],
    [148, 103, 189],
    [140, 86, 75],
    [227, 119, 194],
    [127, 127, 127],
    [188, 189, 34],
    [23, 190, 207],
    [57, 106, 177],
    [218, 124, 48],
    [62, 150, 81],
    [204, 37, 41],
    [107, 76, 154],
    [146, 36, 40],
]


def _get_capacity_column(df: pd.DataFrame) -> pd.Series:
    if "p_nom_opt" in df.columns:
        capacity = df["p_nom_opt"].where(np.isfinite(df["p_nom_opt"]), df["p_nom"])
        return capacity.fillna(df["p_nom"]).fillna(0.0)
    return df["p_nom"].fillna(0.0)


def _build_generation_capacity_per_bus(network) -> pd.DataFrame:
    gb_buses = network.buses.index[
        (network.buses.carrier == "AC") | (network.buses.carrier == "AC_OH")
    ]
    gb_buses = gb_buses[gb_buses.str.startswith("GB ")]

    generator_mask = network.generators.bus.isin(gb_buses) & (network.generators.carrier != "load")
    generator_capacity = (
        network.generators.loc[generator_mask, ["bus", "carrier"]]
        .assign(capacity_mw=_get_capacity_column(network.generators))
        .groupby(["bus", "carrier"], as_index=False)["capacity_mw"]
        .sum()
    )

    link_mask = (
        network.links.bus1.isin(gb_buses)
        & (~network.links.index.str.contains("relation"))
        & (~network.links.carrier.isin(["DC", "DC_OH"]))
    )
    link_capacity = (
        network.links.loc[link_mask, ["bus1", "carrier"]]
        .rename(columns={"bus1": "bus"})
        .assign(capacity_mw=_get_capacity_column(network.links))
        .groupby(["bus", "carrier"], as_index=False)["capacity_mw"]
        .sum()
    )

    capacity = pd.concat([generator_capacity, link_capacity], ignore_index=True)
    capacity = capacity.groupby(["bus", "carrier"], as_index=False)["capacity_mw"].sum()
    capacity = capacity[np.isfinite(capacity["capacity_mw"]) & (capacity["capacity_mw"] > 1e-3)]
    return capacity


def _build_tooltip_html(bus: str, total_capacity_mw: float, bus_capacity: pd.DataFrame) -> str:
    rows = [
        "<tr><th style='text-align:left; padding-right:12px;'>Technology</th>"
        "<th style='text-align:right;'>Capacity [MW]</th></tr>"
    ]
    for row in bus_capacity.sort_values("capacity_mw", ascending=False).itertuples(index=False):
        rows.append(
            "<tr>"
            f"<td style='padding-right:12px;'>{escape(str(row.carrier))}</td>"
            f"<td style='text-align:right;'>{row.capacity_mw:,.0f}</td>"
            "</tr>"
        )

    return (
        f"<b>{escape(bus)}</b><br/>"
        f"Total production capacity: <b>{total_capacity_mw:,.0f} MW</b>"
        "<table>"
        f"{''.join(rows)}"
        "</table>"
    )


def _arc_path(longitude: float, latitude: float, radius_km: float, start_angle: float, end_angle: float, n_points: int = 24) -> list[list[float]]:
    angles = np.linspace(start_angle, end_angle, n_points)
    lat_scale = radius_km / 111.0
    lon_scale = radius_km / (111.0 * max(np.cos(np.radians(latitude)), 1e-6))
    return [
        [
            longitude + lon_scale * np.cos(angle),
            latitude + lat_scale * np.sin(angle),
        ]
        for angle in angles
    ]


def _build_generation_ring_segments(network) -> pd.DataFrame:
    capacity = _build_generation_capacity_per_bus(network)
    if capacity.empty:
        return pd.DataFrame(columns=["bus", "carrier", "capacity_mw", "total_capacity_mw", "path", "color", "tooltip_html"])

    bus_positions = network.buses.loc[:, ["x", "y"]]
    totals = capacity.groupby("bus")["capacity_mw"].sum()
    max_total = totals.max()
    color_map = {
        carrier: RING_COLORS[idx % len(RING_COLORS)]
        for idx, carrier in enumerate(sorted(capacity["carrier"].unique()))
    }

    segments: list[dict] = []
    for bus, bus_capacity in capacity.groupby("bus"):
        if bus not in bus_positions.index:
            continue

        total_capacity_mw = float(bus_capacity["capacity_mw"].sum())
        if not np.isfinite(total_capacity_mw) or total_capacity_mw <= 0:
            continue
        radius_km = 4.0 + 18.0 * np.sqrt(total_capacity_mw / max_total) if max_total > 0 else 4.0
        start_angle = -0.5 * np.pi
        tooltip_html = _build_tooltip_html(bus=bus, total_capacity_mw=total_capacity_mw, bus_capacity=bus_capacity)

        for row in bus_capacity.sort_values("capacity_mw", ascending=False).itertuples(index=False):
            share = float(row.capacity_mw / total_capacity_mw)
            end_angle = start_angle + 2.0 * np.pi * share
            segments.append(
                {
                    "bus": bus,
                    "carrier": row.carrier,
                    "capacity_mw": float(row.capacity_mw),
                    "total_capacity_mw": total_capacity_mw,
                    "path": _arc_path(
                        longitude=float(bus_positions.at[bus, "x"]),
                        latitude=float(bus_positions.at[bus, "y"]),
                        radius_km=radius_km,
                        start_angle=start_angle,
                        end_angle=end_angle,
                    ),
                    "color": color_map[row.carrier],
                    "tooltip_html": (
                        f"<div><b>Hovered technology:</b> {escape(str(row.carrier))}"
                        f" ({float(row.capacity_mw):,.0f} MW)</div>{tooltip_html}"
                    ),
                }
            )
            start_angle = end_angle

    return pd.DataFrame(segments)


def add_generation_capacity_rings_layer(deck: pdk.Deck, network) -> pdk.Deck:
    ring_segments = _build_generation_ring_segments(network)
    if ring_segments.empty:
        return deck

    ring_layer = pdk.Layer(
        "PathLayer",
        data=ring_segments,
        id="GB generation capacity rings",
        get_path="path",
        get_color="color",
        get_width=6,
        width_units="pixels",
        width_min_pixels=4,
        pickable=True,
        auto_highlight=True,
        rounded=True,
    )
    deck.layers.append(ring_layer)
    deck._tooltip = {
        "html": "{tooltip_html}",
        "style": {
            "backgroundColor": "rgba(15, 23, 42, 0.92)",
            "color": "white",
            "fontSize": "12px",
            "padding": "10px",
        },
    }
    return deck


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
    mask = n.links.carrier.isin(allowed) & ~n.links.index.str.contains('relation')
    widths.loc[mask] = 10 / 3e3 * n.links['p_nom'].loc[mask].clip(lower=0, upper = 3e3)
    deck = n.explore(
        link_width=widths,
        link_columns=["p_nom"],
    )

    deck = add_boundaries_layer(deck, year=year)
    deck.to_html(f"EXPLORE_IEM_DISPATCH_{year}.html")

def plot_only_gb_network(rc: ResultsComputer):
    """Plot the network map with line flows for a given snapshot."""
    n = rc.ns.get_iem_dispatch()
    year = rc.year

    allowed = ["DC", "DC_OH"]
    widths = pd.Series(0.0, index=n.links.index)
    mask = (
    (
        n.links.bus0.str.contains('GB ') | n.links.bus1.str.contains('GB ')
    )
    &
    (~n.links.index.str.contains('relation'))
    &
    (n.links.carrier.isin(allowed))
    )
    widths.loc[mask] = 10 / 3e3 * n.links['p_nom'].loc[mask].clip(lower=0, upper = 3e3)
    deck = n.explore(
        link_width=widths,
        link_columns=["p_nom"],
    )

    deck = add_boundaries_layer(deck, year=year)
    deck.to_html(f"GB_NETWORK_{year}.html")


def plot_only_gb_network_with_generation_capacity_per_bus(rc: ResultsComputer):
    """Plot the network map with line flows for a given snapshot."""
    n = rc.ns.get_iem_dispatch()
    year = rc.year

    allowed = ["DC", "DC_OH"]
    widths = pd.Series(0.0, index=n.links.index)
    mask = (
    (
        n.links.bus0.str.contains('GB ') | n.links.bus1.str.contains('GB ')
    )
    &
    (~n.links.index.str.contains('relation'))
    &
    (n.links.carrier.isin(allowed))
    )
    widths.loc[mask] = 10 / 3e3 * n.links['p_nom'].loc[mask].clip(lower=0, upper = 3e3)
    deck = n.explore(
        link_width=widths,
        link_columns=["p_nom"],
    )

    deck = add_generation_capacity_rings_layer(deck, network=n)
    deck = add_boundaries_layer(deck, year=year)
    deck.to_html(f"GB_NETWORK_GENERATION_CAPACITY_{year}.html")


if __name__ == "__main__":
    rc = ResultsComputer(year=2040)
    # plot_network_map(rc)
    # plot_only_gb_network(rc)
    plot_only_gb_network_with_generation_capacity_per_bus(rc)
