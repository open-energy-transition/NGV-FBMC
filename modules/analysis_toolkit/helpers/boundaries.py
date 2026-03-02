import pypsa
import yaml
import pandas as pd
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



if __name__ == "__main__":
    fb_2030 = get_fb_constraints(2030)
    print()