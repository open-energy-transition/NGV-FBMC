RESULTS_DIR = "results/GB/networks"

DISPATCH_DIR_NAME = "unconstrained_clustered"
REDISPATCH_DIR_NAME = "constrained_clustered"
FES_SCENARIO = "HT"

def get_networks_for_year(year: int) -> dict[str, str]:
    # TODO: ADD THE LOGIC TO DIFFERENTIATE BETWEEN SQ, IEM, AND IEM_FB
    return {
        "n_sq_dispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{DISPATCH_DIR_NAME}/{year}.nc",
        "n_iem_dispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{DISPATCH_DIR_NAME}/{year}.nc",
        "n_iem_fb_dispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{DISPATCH_DIR_NAME}/{year}.nc",
        "n_sq_redispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{REDISPATCH_DIR_NAME}/{year}.nc",
        "n_iem_redispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{REDISPATCH_DIR_NAME}/{year}.nc",
        "n_iem_fb_redispatch": f"{RESULTS_DIR}/{FES_SCENARIO}/{REDISPATCH_DIR_NAME}/{year}.nc",
    }