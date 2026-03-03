import pypsa
import yaml
import pandas as pd
import numpy as np
from typing import List
from dataclasses import dataclass
from modules.analysis_toolkit.helpers.config.filepaths import get_boundaries_fp, get_capacities_fp, get_fb_constraints_fp


def get_boundaries_map():
    return yaml.safe_load(open(get_boundaries_fp()))["etys_boundaries_lines"]

def get_capacities_map(year: int):
    return yaml.safe_load(open(get_capacities_fp(year=year)))["etys_boundary_capacities"]

def get_fb_constraints(year: int):
    df = pd.read_parquet(get_fb_constraints_fp(year=year))
    df.columns = df.columns.str.replace("ptdf_", "")  # remove "ptdf" from the name of the columns
    df = df.rename(columns={"datetime": "snapshot", "boundary name": "boundary"})
    return df

def get_all_boundaries(year: int) -> list[str]:
    ptdf = get_fb_constraints(year=year)
    return ptdf.unique().tolist()

def get_link_columns_in_ptdf(year: int) -> list[str]:
    ptdf = get_fb_constraints(year=year)
    np_labels = ptdf.columns[7:].to_list()
    np_labels.remove("gb")
    return np_labels


@dataclass
class Boundary:
    """Dataclass representing a ETYS boundary"""
    name: str
    lines: List[str]
    directions: List[int]
    capacity: float


class Boundaries(dict):

    def __init__(
            self,
            network: pypsa.Network,
            year: int
    ):
        boundaries = get_boundaries_map()
        capacity = get_capacities_map(year=year)
        self.set_boundaries(boundaries, capacity, network)

    @staticmethod
    def get_boundary(
            boundary_name: str,
            list_lines: List[dict],
            network: pypsa.Network,
            capacity: float,
    ) -> Boundary:
        """
        Get a Boundary object for a given boundary name and list of lines

        :param boundary_name: Name of the boundary
        :param list_lines: List of dicts with 'bus0' and 'bus1
        :param network: PyPSA Network object
        :param capacity: Capacity of the boundary
        """

        lines = []
        directions = []
        for map_bus in list_lines:
            bus0 = map_bus['bus0']
            bus1 = map_bus['bus1']

            select_direct = (network.lines.bus0 == f'GB {bus0}') & (network.lines.bus1 == f'GB {bus1}')
            select_opposite = (network.lines.bus0 == f'GB {bus1}') & (network.lines.bus1 == f'GB {bus0}')

            # allow for parallel lines -> include all matches
            if np.any(select_direct):
                line_indices= list(network.lines.index[select_direct])
                directions += [1] * len(line_indices)
            elif np.any(select_opposite):
                line_indices = list(network.lines.index[select_opposite])
                directions += [-1] * len(line_indices)
            else:
                raise ValueError(f"Line {map_bus} not found in the network for boundary {list_lines}")

            lines += line_indices

        return Boundary(boundary_name, lines, directions, capacity)


    def set_boundaries(self, boundaries: dict, capacity: dict, network: pypsa.Network):
        """
        Set the boundaries in the Boundaries dict

        :param boundaries: dict with boundary names as keys and list of lines as values
        :param capacity: dict with boundary names as keys and capacity as values
        :param network: PyPSA Network object
        """

        for boundary in boundaries:
            self[boundary] = Boundaries.get_boundary(boundary, boundaries[boundary], network, capacity[boundary])


if __name__ == "__main__":
    fb_2030 = get_fb_constraints(2030)
    print()