# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: MIT

import logging
import re

import numpy as np
import pandas as pd
import pypsa

logger = logging.getLogger(__name__)

def load_gb_fbmc_data(
    fp: str = None
):
    """
    Load the PTDF matrix given by the NGV / NSide team.
    """
    fp = "data/fbmc/flow_based_constraints_example.xlsx"

    combined_gb = pd.read_excel(fp)
    combined_gb.columns = combined_gb.columns.str.replace('^ptdf_', '', regex=True)
    combined_gb = combined_gb.melt(id_vars=['datetime', 'boundary name', 'direction', 'fref', 'f0', 'ram'], var_name='Link name', value_name='PTDF')
    ref_n = pypsa.Network("modules/gb-dispatch-model/resources/GB/networks/unconstrained_clustered/2040.nc")
    ref_n_links = ref_n.links[ref_n.links.carrier=='DC'][['bus0', 'bus1']]
    combined_gb = pd.merge(ref_n_links, combined_gb, how='right', left_on='name', right_on='Link name') # do we need to keep the relation/ links?

    ram_gb = combined_gb.drop(['fref', 'f0', 'PTDF'], axis=1)
    ptdf_gb = combined_gb.drop(['fref', 'f0', 'ram'], axis=1)
    mask = ptdf_gb.direction == 'OPPOSITE'
    ptdf_gb.loc[mask, ['bus0', 'bus1']] = ptdf_gb.loc[mask, ['bus1', 'bus0']].values
    
    # split into 2 ptdfs to mimic the tyndp data
    ptdf_ahc_sz = ptdf_gb[ptdf_gb['Link name'] != 'gb']
    ptdf_sz = ptdf_gb[ptdf_gb['Link name'] == 'gb']

    return ptdf_ahc_sz, ptdf_sz, ram   

def load_ptdf(
    fp: str,
    ptdf_type: str,
    sheet_name: str,
    drop_columns_regex: list[str] = [r".*UA.*"],
) -> pd.DataFrame:
    """
    Load PTDF matrix from Excel file.

    PTDF matrix contains the weights for each flow through each
    line/link, by each critical network element component (CNEC).

    Parameters
    ----------
    fp : str
        File path to the Excel file containing the PTDF matrix.
    ptdf_type : str
        Type of PTDF matrix to load. Corresponds to the sheet column names in the Excel file.
        Options for ERAA2023 are "PTDF_SZ", "PTDF*_AHC,SZ" or "PTDF_EvFB".
    sheet_name : str
        Name of the sheet in the Excel file to read the PTDF matrix from.
    drop_columns_regex : list[str], optional
        List of regex patterns to identify columns to drop from the PTDF matrix.
        Default is [r".*UA.*"] to drop columns related to Ukraine.
    """

    logger.info(f"Loading PTDF matrix of type '{ptdf_type}' from sheet '{sheet_name}'.")

    ptdf: pd.DataFrame = pd.read_excel(
        fp, header=[0, 1], sheet_name=sheet_name, dtype=str
    )
    ptdf = ptdf.rename(
        columns={
            "FB_ID": "FB Domain",
            "CNEC_ID": "CNEC_ID",
        }
    )

    # Select the right columns and drop multiindex level
    ptdf = ptdf.loc[
        :,
        [
            (col[0], col[1])
            for col in ptdf.columns.values
            if col[0] in ["Type", ptdf_type]
        ],
    ]
    ptdf = ptdf.droplevel(0, axis=1)

    # Replacement of not-needed columns based on regex patterns
    drop_columns = []
    for regex in drop_columns_regex:
        drop_columns.extend([col for col in ptdf.columns if re.match(regex, col)])
    ptdf = ptdf.drop(columns=drop_columns)

    # Rename columns headers as the PTDF data uses slightly different bus naming than the TYNDP model
    bus_renaming = {
        "GB00": "UK00",
        "DEOH002": "DEKF",  # TODO Check again, DEKF exists in open-TYNDP and in PTDF org data and DEOH002 does not; # uses Hub, for Kriegers Flak (KF) offshore wind park
    }

    ptdf = ptdf.rename(
        columns={
            old_col: old_col.replace(org_bus, new_bus)
            for new_bus, org_bus in bus_renaming.items()
            for old_col in ptdf.columns
            if org_bus in old_col
        }
    )

    # Turn into long format with MultiIndex
    ptdf = ptdf.melt(
        id_vars=["FB Domain", "CNEC_ID"],
        var_name="line",
        value_name="PTDF",
    )

    # Format specific to PTDF type
    if ptdf_type == "PTDF*_AHC,SZ":
        # Split "line" into "from" and "to" bus columns
        ptdf["from"] = ptdf["line"].str.split("-").str[0].str[:4]
        ptdf["to"] = ptdf["line"].str.split("-").str[1].str[:4]

        # Reorder columns
        ptdf = ptdf[["FB Domain", "CNEC_ID", "from", "to", "line", "PTDF"]]
    elif ptdf_type == "PTDF_SZ":
        # columns are per bidding zone already
        ptdf = ptdf.rename(columns={"line": "study_zone"})
    elif ptdf_type == "PTDF_EvFB":
        ptdf = ptdf.rename(columns={"line": "virtual_zone"})
    else:
        raise ValueError(f"PTDF type '{ptdf_type}' not recognized.")

    # Convert PTDF values to float
    ptdf = ptdf.astype({"PTDF": float})

    return ptdf


def load_ram(fp: str, sheet_name: str) -> pd.DataFrame:
    """
    Load RAM matrix from Excel file.

    RAM matrix defines the remaining available margin per CNEC.

    Parameters
    ----------
    fp : str
        File path to the Excel file containing the RAM matrix.
    sheet_name : str
        Name of the sheet in the Excel file to read the RAM matrix from.
    """
    breakpoint()
    logger.info(f"!!!jgjgk!!!! Loading RAM matrix from sheet '{sheet_name}'.")

    # Read forward until row with "CNEC_ID" is found in column A,
    # then move that row to be the header
    # and read the rest of the sheet as normal
    ram = pd.read_excel(
        fp,
        sheet_name=sheet_name,
        usecols="A",
        dtype=str,
    )
    header_row_index = ram.index[ram.iloc[:, 0] == "CNEC_ID"].tolist()[0] + 1

    # Read the sheet again with the correct header row set
    ram = pd.read_excel(
        fp,
        header=header_row_index,
        sheet_name=sheet_name,
        dtype=str,
    )
    ram = ram.melt(id_vars=["CNEC_ID"], var_name="FB Domain", value_name="RAM")

    ram = ram.astype({"RAM": float})

    return ram


def load_weather_assignments(
    fp: str,
    sheet_name: str = "FB Domain Assignment",
    snapshots: pd.DatetimeIndex | None = None,
    weather_scenario: str | None = None,
    weather_year: int | None = None,
    eraa_version: str | None = None,
) -> pd.Series:
    """
    Load weather assignments between FB domains and weather year/timestep from Excel file.

    The RAM values are provided for different weather situations (seasons).
    The mapping between hours of the year and weather year to the RAM values
    is stored separately in the weather assignments.
    This function loads the correct weather assignments for the specified year.

    Parameters
    ----------
    fp : str
        File path to the Excel file containing the weather assignments.
    sheet_name : str, optional
        Name of the sheet in the Excel file to read the weather assignments from.
        Default is "FB Domain Assignment".
    snapshots : pd.DatetimeIndex, optional
        DatetimeIndex of snapshots to filter the weather assignments to.
        If None, all snapshots are returned. Default is None.
    weather_scenario : str, optional
        Weather scenario to load the weather assignments for. Only relevant for ERAA2024.
        E.g. "WS1", "WS2", etc..
    weather_year : int, optional
        Year to load the weather assignments for. If not provided, all years are loaded
        and the snapshot filtering (if used) is directly applied.
        If specified, this will align the first timestamp of 'snapshots' to the specified year
        and then filter the weather assignments using the snapshots.
        The returned weather assignments will be always be aligned to the snapshots' year.
        Only used for ERAA2024. E.g. "2026".
    eraa_version: str, optional
        ERAA version format to use to load the weather assignments for.
        Determined automatically if not provided, use for overwriting automatic detection.
        Currently "ERAA2023" and "ERAA2024" are supported.

    Returns
    -------
    pd.Series
       Series containing the weather assignments for the specified year and of the specified timestep.
    """

    if eraa_version is None:
        # Determine which ERAA version is present in the file based on sheet headers
        df = pd.read_excel(fp, sheet_name=sheet_name, nrows=1)
        if df.columns[:6].tolist() == [
            "Time_step",
            "Year",
            "Month",
            "Day",
            "Hour",
            "CY_1982",
        ]:
            eraa_version = "ERAA2023"
        elif df.columns[:5].tolist() == ["Year", "Month", "Day", "Hour", "WS1"]:
            eraa_version = "ERAA2024"
        else:
            raise ValueError("ERAA version not found")

    logger.info(f"Loading weather assignments for ERAA version {eraa_version}.")

    if eraa_version == "ERAA2023":
        weather_assignments: pd.DataFrame = pd.read_excel(
            fp, sheet_name=sheet_name, dtype=str
        )

        # Drop unnecessary columns
        weather_assignments = weather_assignments.drop(columns=["Year"])

        # Rename columns from "CY_<YYYY>" to "<YYYY>" for easier access
        weather_assignments = weather_assignments.rename(
            columns={
                col: col.replace("CY_", "")
                for col in weather_assignments.columns
                if col.startswith("CY_")
            }
        )

        # Turn weather year columns into rows
        weather_assignments = weather_assignments.melt(
            id_vars=["Time_step", "Month", "Day", "Hour"],
            var_name="Year",
            value_name="FB Domain",
        )

        # Counting of hours starts at 1, adjust to start at 0 to create proper datetime index
        weather_assignments["Hour"] = weather_assignments["Hour"].astype(int) - 1

        # Turn columns into datetime index
        weather_assignments["snapshot"] = pd.to_datetime(
            weather_assignments[["Year", "Month", "Day", "Hour"]]
        )
        weather_assignments = weather_assignments.set_index("snapshot")

    elif eraa_version == "ERAA2024":
        if weather_scenario is None:
            raise ValueError(
                "weather_scenario must be specified for ERAA2024 weather assignments."
            )

        weather_assignments: pd.DataFrame = pd.read_excel(
            fp,
            sheet_name=sheet_name,
            usecols=["Year", "Month", "Day", "Hour", weather_scenario],
            dtype={
                "Year": int,
                "Month": int,
                "Day": int,
                "Hour": int,
                weather_scenario: str,
            },
        )

        weather_assignments = weather_assignments.rename(
            columns={weather_scenario: "FB Domain"}
        )

        # Counting of hours starts at 1, adjust to start at 0 to create proper datetime index
        weather_assignments["Hour"] = weather_assignments["Hour"] - 1

        # Turn columns into datetime index
        weather_assignments["snapshot"] = pd.to_datetime(
            weather_assignments[["Year", "Month", "Day", "Hour"]]
        )
        weather_assignments = weather_assignments.set_index("snapshot")

        # Calculate the offset and realign the first timestamp of the specified year
        if weather_year and snapshots is not None and not snapshots.empty:
            year_offset = pd.DateOffset(years=snapshots.min().year - weather_year)
            weather_assignments.index = weather_assignments.index + year_offset

        elif weather_year and snapshots is None:
            # Align to the specified year directly
            weather_assignments = weather_assignments.loc[
                weather_assignments.year == weather_year
            ]

    if snapshots is not None and not snapshots.empty:
        # Select requested timesteps only
        weather_assignments = weather_assignments.loc[snapshots]

    return weather_assignments["FB Domain"].to_frame().reset_index()

    
def add_fbmc_constraints(n: pypsa.Network, fp: str, config: dict) -> None:
    """
    Add the FBMC constraints to the pypsa.Network model.

    Function is currently tailored towards the PTDF matrix and RAM values from ERAA2023,
    can be downloaded from https://eepublicdownloads.blob.core.windows.net/public-cdn-container/clean-documents/sdc-documents/ERAA/2023/FB-Domain-CORE_Merged.xlsx .

    Parameters
    ----------
    n : pypsa.Network
        The pypsa.Network object to which the FBMC constraints will be added.
    fp : str, optional
        File path to the Excel file containing the FBMC data.
        Needs to contain the PTDF matrix, RAM matrix, and weather assignments.
    config : dict
        Configuration used to modify the network for FBMC implementation.
    """
    breakpoint()
    ram = load_ram(fp, sheet_name=f"RAM_{config['ram_year']}")
    wa = load_weather_assignments(
        fp,
        snapshots=n.snapshots,
        weather_scenario=config.get("weather_scenario"),
        weather_year=config.get("weather_year"),
    )

    if config["eraa_version"] == "eraa2023":
        ptdf_sheet_name = "PTDF"
    elif config["eraa_version"] == "eraa2024":
        ptdf_sheet_name = f"PTDF_{config['ptdf_year']}"

    # Map RAM values to weather seasons
    ram_snapshoted = wa.merge(
        ram,
        on="FB Domain",
        how="left",
    )

    # ----------------------------------
    # First part of the FBMC constraint:
    # Flows into and out of CORE bidding zones
    # ----------------------------------
    breakpoint()
    ptdf_ahc_sz_gb, ptdf_sz_gb, ram_gb = load_gb_fbmc_data(fp=)

    # load PTDF
    ptdf = load_ptdf(fp=fp, sheet_name=ptdf_sheet_name, ptdf_type="PTDF_SZ")

    # get links relevant for the intra-CCR FBMC constraint
    links_idx = n.components.links.static.loc[
        n.components.links.static["PTDF_type"] == "PTDF_SZ"
    ].index

    # "study_zones" are named after the bidding zones, the flows in the network
    # have a `-<CCR>` suffix, e.g. `-CORE` to indicate they are flows between the bidding zone
    # and the CCR hub. Therefore, to align the PTDF data with the flows, we need to add the
    # suffix to the study_zone names. We get the suffix from the link names and rename
    # the study zones in the PTDF rather than the linopy model
    rename_study_zones = {idx.split("-")[0]: idx for idx in links_idx}
    if len(set(rename_study_zones.values())) != len(links_idx):
        raise ValueError(
            "Renaming of study zones to match link names resulted in a non 1:1 mapping."
        )
    ptdf["study_zone"] = ptdf["study_zone"].replace(rename_study_zones)

    # TODO we do not include the offshore regions into the calculation
    # reason: Not part of the market, not interconnection to other countries,
    # consider with their transfer capacitiy rather than including them into the FBMC

    # get flow through links in CORE bidding zones

    # go from FB Domains to snapshots
    ptdf_snapshoted = wa.merge(
        ptdf,
        on="FB Domain",
        how="left",
    )

    ptdf_snapshoted.loc[ptdf_snapshoted["PTDF"].isna()]

    # do the fancy multiplication
    ds = (
        ptdf_snapshoted.rename(columns={"study_zone": "name"})
        .set_index(["CNEC_ID", "snapshot", "name"])["PTDF"]
        .to_xarray()
    )

    breakpoint()

    # Casting to xarray creates NaN values, need to fill those entries with 0
    flows = n.model["Link-p"].sel(name=links_idx)

    # Align indices of ptdf and flows
    ds = ds.reindex(snapshot=flows.coords["snapshot"], name=flows.coords["name"])

    # Calculate PTDF contribution and group by snapshot and CNEC_ID to sum up all contributions to each CNEC at each snapshot
    lhs_1 = (ds * flows).sum(dim="name")

    # -----------------------------------
    # Second part of the FBMC constraint:
    # loading from HVDC lines between CORE and outside of CORE
    # -----------------------------------
    ptdf = load_ptdf(fp=fp, sheet_name=ptdf_sheet_name, ptdf_type="PTDF*_AHC,SZ")

    # Map pypsa.Network links that are related to DC and their names (index) to PTDF line names where bus0=from and bus1=to
    links = (
        n.components.links.static.query("`carrier`.str.startswith('DC')")[
            ["bus0", "bus1"]
        ]
        .reset_index()
        .rename(columns={"name": "link_name"})
    )
    ptdf = ptdf.merge(
        links, left_on=["from", "to"], right_on=["bus0", "bus1"], how="left"
    )

    # Map PTDF values to seasonal values for RAM
    ptdf_snapshoted = wa.merge(
        ptdf,
        on="FB Domain",
        how="left",
    )

    ds = (
        ptdf_snapshoted.dropna(subset=["link_name"])  # Why necessary?)
        .drop_duplicates(subset=["CNEC_ID", "snapshot", "link_name"])  # Why necessary?
        .rename(columns={"link_name": "name"})
        .set_index(["CNEC_ID", "snapshot", "name"])["PTDF"]
        .to_xarray()
    )

    # Casting to xarray creates NaN values, need to fill those entries with 0
    flows = n.model["Link-p"].sel(name=ds["name"])

    ds = ds.reindex(snapshot=flows.coords["snapshot"], name=flows.coords["name"])

    lhs_2 = ds * flows
    # Group by snapshot and CNEC_ID to sum up all contributions to each CNEC at each snapshot
    lhs_2 = lhs_2.sum(dim="name")

    # -----------------------------------
    # Third part of the FBMC constraint:
    # loading from HVDC lines within CORE region bidding zones
    # -----------------------------------

    # Load PTDF
    ptdf = load_ptdf(fp=fp, sheet_name=ptdf_sheet_name, ptdf_type="PTDF_EvFB")

    # Map PTDF to seasonal values for RAM
    ptdf_snapshoted = wa.merge(
        ptdf,
        on="FB Domain",
        how="left",
    )

    flows = n.model["Link-p"].sel(name="EvFBA1-EvFBA2")

    ds = ptdf_snapshoted.set_index(["CNEC_ID", "snapshot"])["PTDF"].to_xarray() * flows

    ds = ds.reindex(
        snapshot=flows.coords["snapshot"],
    )

    # Casting to xarray creates NaN values, need to fill those entries with 0
    lhs_3 = ds

    rhs = ram_snapshoted.set_index(["CNEC_ID", "snapshot"])["RAM"].to_xarray()

    # Enable lhs_1 and lhs_3 when implemented
    n.model.add_constraints(
        lhs_1 + lhs_2 + lhs_3 <= rhs,
        name="PTDF-RAM-constraints",
    )


def modify_network_for_fbmc(n: pypsa.Network, config: dict) -> pypsa.Network:
    """
    Modify the pypsa.Network for the FBMC implementation.

    The methodology follows the description in ERAA2023.
    This function modified the network and adds additional components that are necessary for the
    evolved FBMC implementation.
    It also assigns some helpful, additional attributes to existing components like buses and links.

    Parameters
    ----------
    n : pypsa.Network
        The pypsa.Network object to be modified for FBMC implementation.
    config : dict
        Configuration used to modify the network for FBMC implementation.
        (TODO: Currently not used, but needed for proper ERAA2024 implementation)

    Returns
    -------
    pypsa.Network
        The modified pypsa.Network object with FBMC implementation.
    """

    # ---------------------------------------------------
    # Add the buses and links required for the Evolved FB
    # ---------------------------------------------------
    logger.info("Adding FBMC evolved FB buses and links to the network.")
    n.add(
        "Bus",
        name="EvFBA1",
    )
    n.add(
        "Bus",
        name="EvFBA2",
    )
    n.add(
        "Bus",
        name="EvFBA3",
    )

    # links between the evolved FB buses
    # capacities from PTDF file, "EvFB_capacities" sheet
    n.add(
        "Link",
        name="EvFBA1-EvFBA2",
        bus0="EvFBA1",
        bus1="EvFBA2",
        p_nom=1e3,
        efficiency=1.0,
        p_nom_extendable=False,
        p_min_pu=-1.0,
        p_max_pu=1.0,
    )
    n.add(
        "Link",
        name="EvFBA2-EvFBA3",
        bus0="EvFBA2",
        bus1="EvFBA3",
        p_nom=1e3,
        efficiency=1.0,
        p_nom_extendable=False,
        p_min_pu=-1.0,
        p_max_pu=1.0,
    )
    n.add(
        "Link",
        name="EvFBA3-EvFBA1",
        bus0="EvFBA3",
        bus1="EvFBA1",
        p_nom=1e3,
        efficiency=1.0,
        p_nom_extendable=False,
        p_min_pu=-1.0,
        p_max_pu=1.0,
    )

    # ----------------------------------------------------
    # Add details on which FBMC region each bus belongs to
    # ----------------------------------------------------
    fbmc_region_mapping = {
        "AT00": "CORE",
        "BE00": "CORE",
        "CZ00": "CORE",
        "DE00": "CORE",
        "FR00": "CORE",
        "HR00": "CORE",
        "HU00": "CORE",
        "NL00": "CORE",
        "PL00": "CORE",
        "RO00": "CORE",
        "SK00": "CORE",
        "SI00": "CORE",
        "EvFBA1": "ALEGRO",
        "EvFBA2": "ALEGRO",
        "EvFBA3": "ALEGRO",
    }

    for bus, region in fbmc_region_mapping.items():
        n.buses.loc[bus, "FBMC_region"] = region

    # Assign links an attribute to indicate which parts of the PTDF they are relevant for
    logger.info("Assigning PTDF types to network links for FBMC implementation.")
    n.components.links.static["PTDF_type"] = ""
    # 1. PTDF_SZ for intra-CORE flows
    core_buses = n.components.buses.static.query("FBMC_region == 'CORE'").index.tolist()
    idx = n.components.links.static[
        (n.components.links.static["bus0"].isin(core_buses))
        & (n.components.links.static["bus1"].isin(core_buses))
    ].index

    # Any of these links is removed and replaced with an unlimited link
    # between the study zone and a virtual CORE hub
    n.add("Carrier", name="FBMC")
    n.add("Bus", name="CORE", carrier="FBMC")
    n.remove(
        "Link",
        name=idx.tolist(),
    )
    n.add(
        "Link",
        name=core_buses,
        suffix="-CORE",
        carrier="DC",
        bus0=core_buses,
        bus1="CORE",
        p_nom=np.inf,
        efficiency=1.0,
        p_nom_extendable=False,
        p_min_pu=-1.0,
        p_max_pu=1.0,
        PTDF_type="PTDF_SZ",
        FBMC_region="CORE",
    )

    # 2. PTDF*_AHC,SZ for flows between CORE and outside of CORE
    idx = n.components.links.static[
        (
            (n.components.links.static["bus0"].isin(core_buses))
            ^ (n.components.links.static["bus1"].isin(core_buses))
        )
        & (n.components.links.static["carrier"].isin(["DC", "DC_OH", "AC"]))
        & (n.components.links.static["PTDF_type"] != "PTDF_SZ")
    ].index
    n.links.loc[idx, "PTDF_type"] = "PTDF*_AHC,SZ"
    n.links.loc[idx, "FBMC_region"] = "CORE-Outside"

    # 3. PTDF_EvFB for flows related to the evolved FB
    idx = n.components.links.static.filter(
        regex=r"^EvFBA\d-EvFBA\d$", axis="index"
    ).index
    n.links.loc[idx, "PTDF_type"] = "PTDF_EvFB"
    n.links.loc[idx, "FBMC_region"] = "ALEGRO"

    return n
