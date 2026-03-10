# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: MIT

import logging
from dataclasses import dataclass
from typing import Self

import pandas as pd
import pypsa
import xarray as xr

logger = logging.getLogger(__name__)


@dataclass
class FBMCConstraint:
    ptdf: xr.DataArray
    ram: xr.DataArray

    @classmethod
    def from_parquet(cls, fn: str) -> Self:
        df = (
            pd.read_parquet(fn)
            .set_index(["datetime", "boundary name", "direction"])
            .rename_axis(index={"datetime": "snapshot"})
        )
        ptdf = xr.DataArray.from_series(
            df.filter(like="ptdf_")  # pyright: ignore[reportArgumentType]
            .rename(columns=lambda s: s.removeprefix("ptdf_"))
            .rename_axis(columns="name")
            .stack()
        )
        ram = xr.DataArray.from_series(df["ram"])

        # Workaround 1:
        # The network interconnectors should match exactly the ptdf name dimension,
        # since this is currently not the case, we use the Gallant interconnector ptdf
        # values for the missing BritNed and we remove Gallant, Tarchon and LionLink
        # (EuroLink)
        ptdf = xr.concat(
            [
                ptdf.drop_sel(name=["Gallant", "Tarchon", "LionLink (EuroLink)"]),
                ptdf.sel(name="Gallant").assign_coords(name="BritNed"),
            ],
            dim="name",
        )

        # Workaround 2:
        # The snapshots should be exactly the same, instead the network is currently for
        # another year than the PTDF constraint data, so we assume it was actually
        # computed for the network year (2009) and since there is segmentation we only
        # take the start of the segment
        assumed_snapshots = pd.date_range("2009", freq="h", periods=8760)
        ptdf = ptdf.assign_coords(snapshot=assumed_snapshots)
        ram = ram.assign_coords(snapshot=assumed_snapshots)

        return cls(ptdf, ram)

    def to_netcdf(self, ptdf: str, ram: str) -> None:
        self.ptdf.to_netcdf(ptdf)
        self.ram.to_netcdf(ram)

    @classmethod
    def from_netcdf(cls, ptdf: str, ram: str) -> Self:
        return cls(xr.open_dataarray(ptdf), xr.open_dataarray(ram))

    def __call__(self, n: pypsa.Network, snapshots: pd.DatetimeIndex):
        """
        Add constraint to the model

        Assumptions:

        The definition of the net positions is positive for consumption:
        - power on interconnectors flowing from within GB to outside of GB is
          a positive net position on this interconnector, and similar
        - consumption in gb is a positive net position in gb.

        The two sub assumptions are consistent with one another since net positions need to sum to zero,
        but the PTDF assumption might also have been for generation rather than consumption and then all
        lhs signs need to be inverted.
        """
        ptdf = self.ptdf.sel(snapshot=snapshots)
        ram = self.ram.sel(snapshot=snapshots)

        ptdf_gb = ptdf.sel(name="gb")
        ptdf_ic = ptdf.drop_sel(name="gb")

        m = n.model

        # power flowing to outside of gb is positive (since all bus0 of interconnectors
        # are in GB, bus1 is outside of GB)
        net_positions = m["Link-p"].sel(name=ptdf_ic.indexes["name"])
        # net position of gb follows from energy balance (since sum over all must be 0,
        # since there is no other transport layer)
        net_position_gb = -net_positions.sum("name")

        m.add_constraints(
            (
                net_position_gb * ptdf_gb.sel(direction="DIRECT")
                + (net_positions * ptdf_ic.sel(direction="DIRECT")).sum("name")
                <= ram.sel(direction="DIRECT")
            ),
            name="FBMC direct",
        )

        m.add_constraints(
            (
                -net_position_gb * ptdf_gb.sel(direction="OPPOSITE")
                - (net_positions * ptdf_ic.sel(direction="OPPOSITE")).sum("name")
                <= ram.sel(direction="OPPOSITE")
            ),
            name="FBMC opposite",
        )
