# SPDX-FileCopyrightText: NGV-FBMC contributors
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

        return cls(ptdf, ram)

    def to_netcdf(self, ptdf: str, ram: str) -> None:
        self.ptdf.to_netcdf(ptdf)
        self.ram.to_netcdf(ram)

    def align_snapshots(self, snapshots: pd.DatetimeIndex) -> Self:
        ptdf = self.ptdf.reindex(snapshot=snapshots, method="ffill")
        ram = self.ram.reindex(snapshot=snapshots, method="ffill")
        return FBMCConstraint(ptdf, ram)

    @classmethod
    def from_netcdf(cls, ptdf: str, ram: str) -> Self:
        return cls(xr.open_dataarray(ptdf), xr.open_dataarray(ram))

    def __call__(self, n: pypsa.Network, snapshots: pd.DatetimeIndex):
        """
        Add FBMC constraint to the model

        Assumptions:

        The definition of the net positions is positive for generation:
        - power on interconnectors flowing from outside of GB to inside of GB is
          a positive net position on this interconnector, and similar
        - generation in gb is a positive net position in gb.

        The two sub assumptions are consistent with one another since net positions need
        to sum to zero, but the PTDF assumption might also have been for consumption
        rather than generation and then all lhs signs need to be inverted.
        """
        ptdf_gb = self.ptdf.sel(name="gb")
        ptdf_ic = self.ptdf.drop_sel(name="gb")

        m = n.model

        # power flowing to outside of gb is positive (since all bus0 of interconnectors
        # are in GB, bus1 is outside of GB)
        net_positions_ic = -m["Link-p"].sel(name=ptdf_ic.indexes["name"])
        # net position of gb follows from energy balance, which implies that sum over all must be 0
        net_position_gb = -net_positions_ic.sum("name")

        m.add_constraints(
            net_position_gb * ptdf_gb + (net_positions_ic * ptdf_ic).sum("name")
            <= self.ram,
            name="FBMC",
        )

        logger.info("Adding FBMC constraint to the network.")
