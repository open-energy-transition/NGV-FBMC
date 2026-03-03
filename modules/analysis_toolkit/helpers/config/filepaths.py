from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent.parent.parent

RESULTS_DIR = f"{ROOT_DIR}/results/GB/networks"

DISPATCH_DIR_NAME = "unconstrained_clustered"
REDISPATCH_DIR_NAME = "constrained_clustered"
FES_SCENARIO = "HT"

def get_network_fps_for_year(year: int) -> dict[str, str]:
    # TODO: ADD THE LOGIC TO DIFFERENTIATE BETWEEN SQ, IEM, AND IEM_FB
    return {
        "n_sq_dispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{DISPATCH_DIR_NAME}/{year}.nc",
        "n_iem_dispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{DISPATCH_DIR_NAME}/{year}.nc",
        "n_iem_fb_dispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{DISPATCH_DIR_NAME}/{year}.nc",
        "n_sq_redispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{REDISPATCH_DIR_NAME}/{year}.nc",
        "n_iem_redispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{REDISPATCH_DIR_NAME}/{year}.nc",
        "n_iem_fb_redispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{REDISPATCH_DIR_NAME}/{year}.nc",
    }

CONFIG_DIR = f"{ROOT_DIR}/modules/analysis_toolkit/helpers/config"

get_boundaries_fp = lambda: f"{CONFIG_DIR}/boundaries.yaml"
get_capacities_fp = lambda year: f"{CONFIG_DIR}/capacities_{year}.yaml"
get_fb_constraints_fp = lambda year: f"{CONFIG_DIR}/flow_based_constraints_{year}_v20260210.parquet"