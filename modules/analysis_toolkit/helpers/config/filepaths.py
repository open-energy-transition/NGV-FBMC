from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent.parent.parent

RESULTS_DIR = f"{ROOT_DIR}/results/GB/dispatch_only_networks_110326"

IEM_SCENARIO = "IEM"
SQ_SCENARIO = "SQ"
TF_SCENARIO = "TF"
IEM_FB_SCENARIO = "IEM"  # Todo: update when IEM_FB scenario is available

def get_network_fps_for_year(year: int) -> dict[str, str]:
    return {
        "n_sq_dispatch": f"{RESULTS_DIR}/{SQ_SCENARIO}/{year}.nc",
        "n_iem_dispatch": f"{RESULTS_DIR}/{IEM_SCENARIO}/{year}.nc",
        "n_iem_fb_dispatch": f"{RESULTS_DIR}/{IEM_FB_SCENARIO}/{year}.nc",
        "n_sq_redispatch": f"{RESULTS_DIR}/{SQ_SCENARIO}/{year}.nc",
        "n_iem_redispatch": f"{RESULTS_DIR}/{IEM_SCENARIO}/{year}.nc",
        "n_iem_fb_redispatch": f"{RESULTS_DIR}/{IEM_FB_SCENARIO}/{year}.nc",
    }

CONFIG_DIR = f"{ROOT_DIR}/modules/analysis_toolkit/helpers/config"

get_boundaries_fp = lambda: f"{CONFIG_DIR}/boundaries.yaml"
get_capacities_fp = lambda year: f"{CONFIG_DIR}/capacities_{year}.yaml"
get_fb_constraints_fp = lambda year: f"{CONFIG_DIR}/flow_based_constraints_{year}_v20260210.parquet"
get_etys_boundaries_geopandas_fp = lambda: f"{CONFIG_DIR}/gb-etys-boundaries.zip"