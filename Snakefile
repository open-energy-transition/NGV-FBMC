# SPDX-FileCopyrightText: NGV-FBMC contributors
#
# SPDX-License-Identifier: MIT


configfile: "config/config.default.yaml"
configfile: "config/plotting.default.yaml"
configfile: "config/benchmarking.default.yaml"
configfile: "config/config.tyndp.yaml"
configfile: "config/config.ngv-fbmc.yaml"


wildcard_constraints:
    planning_horizons="2030|2040",
    scenario="IEM|TF|SQ|FBMC",


# Notes about limitations in integrating existing workflows as modules:
# 1. Each module has its own configuration files that need to be loaded separately.
#    Loading from <module>/config/*.yaml does unfortunately not work,
#    the `configfile` directive in the module's Snakefile is not affected by the
#    `prefix` directive of the module, preventing Snakemake from finding the config files.
# 2. The module cannot be imported using the `github(..)` directive directly,
#    but the repository needs to be included as a `git` submodule in this repository.
#    The reason is that some scripts are loaded from `scripts/_helpers.py` into the `Snakefile`,
#    which does not work with `github(..)` imports.
# 3. The `prefix` of each module needs to match the actual path of the module.
#    If this is not the case, then some data files that are part of the submodule `data/` directory
#    cannot be found.


def _add_prefix(fn: str | list[str], prefix: str) -> str | list[str]:
    if isinstance(fn, str):
        return f"{prefix}{fn}"
    elif isinstance(fn, list):
        return [f"{prefix}{fn}" for f in fn]


def _remove_prefix(fn: str | list[str], prefix: str) -> str | list[str]:
    if isinstance(fn, str):
        return fn.replace(prefix, "")
    elif isinstance(fn, list):
        return [f.replace(prefix, "") for f in fn]


def gbdispatchmodel(
    fn: str | list[str], prefix="modules/gb-dispatch-model/", remove_prefix=False
) -> str | list[str]:
    """Prefix filenames in either str or list[str] for the GB Dispatch Model relative location."""
    if remove_prefix:
        return _remove_prefix(fn, prefix)
    else:
        return _add_prefix(fn, prefix)


def ngviemmodel(
    fn: str | list[str], prefix="modules/NGV-IEM/", remove_prefix=False
) -> str | list[str]:
    """Prefix filenames in either str or list[str] for the NGV IEM relative location."""
    if remove_prefix:
        return _remove_prefix(fn, prefix)
    else:
        return _add_prefix(fn, prefix)


# We create a rule to run this model standalone as a pixi task, rather than including it as a snakemake module
# Reason for this approach: Two models that are forked from PyPSA-Eur currently lead to collisions with snakemake
# making it too error-prone to use them both as module at the same time. This might be fixed in the future
# by this PR by us ( ), for now we use a less beautiful but more utalitarian solution.
# We list all files that we need from the other model to have them registered with the remaining snakemake workflow
rule run_phase01_model_as_rule:
    message:
        "Running parts of the phase 01 NGV-IEM model as preparation for the combined model."
    params:
        files=lambda wildcards, output: " ".join(
            ngviemmodel(output, remove_prefix=True)
        ),
    input:
        manifest=ngviemmodel("pixi.toml"),
        overwrite_configfiles=[
            "config/config.ngv-iem.yaml",
        ],
    output:
        forecast_errors=ngviemmodel(
            "data/ngv_iem_errors/archive/2025-12-04_17-05/relative_errors.parquet"
        ),
        results_noce_2030=ngviemmodel(
            "results/ngv-iem/latest/networks/base_s_all___2030_no_ce.nc"
        ),
        offshore_zone_trajectories=ngviemmodel(
            "resources/ngv-iem/latest/offshore_zone_trajectories.csv"
        ),
        # results_noce_2040=ngviemmodel(
        #     "results/ngv-iem/latest/networks/base_s_all___2040_no_ce.nc"
        # ),
    shell:
        """
        pixi run \
            --manifest-path={input.manifest} \
            --environment=ngv \
            snakemake \
                --cores all \
                --snakefile modules/NGV-IEM/Snakefile \
                --directory modules/NGV-IEM \
                --configfile {input.overwrite_configfiles} \
                --keep-going \
                --rerun-incomplete \
                {params.files}
        """


rule run_gbdispatchmodel_as_rule:
    message:
        "Running parts of the GB Dispatch Model as preparation for the combined model."
    params:
        files=lambda wildcards, output: " ".join(
            gbdispatchmodel(output, remove_prefix=True)
        ),
    input:
        manifest=gbdispatchmodel("pixi.toml"),
        overwrite_configfiles=[
            gbdispatchmodel("config/config.gb.etys-subset.yaml"),
            "config/config.gb-dispatch.yaml",
        ],
    output:
        networks_dispatch=expand(
            gbdispatchmodel(
                "resources/GB-ETYS-subset/networks/{fes_scenario}/constrained_clustered/{planning_horizons}.nc"
            ),
            fes_scenario=["HT", "CF"],
            planning_horizons=["2030", "2040"],
        ),
        networks_redispatch=expand(
            gbdispatchmodel(
                "resources/GB-ETYS-subset/networks/{fes_scenario}/unconstrained_clustered/{planning_horizons}.nc"
            ),
            fes_scenario=["HT", "CF"],
            planning_horizons=["2030", "2040"],
        ),
        renewable_strike_prices=gbdispatchmodel(
            "resources/GB-ETYS-subset/gb-model/CfD_strike_prices.csv"
        ),
        bid_offer_multipliers=expand(
            gbdispatchmodel(
                "resources/GB-ETYS-subset/gb-model/{fes_scenario}/bid_offer_multipliers.csv"
            ),
            fes_scenario=["HT", "CF"],
        ),
        current_etys_caps=gbdispatchmodel(
            "resources/GB-ETYS-subset/gb-model/etys_boundary_capabilities.csv"
        ),
        future_etys_caps=expand(
            gbdispatchmodel(
                "resources/GB-ETYS-subset/gb-model/{fes_scenario}/future_etys_boundary_capabilities.csv"
            ),
            fes_scenario=["HT", "CF"],
        ),
        boundary_crossings=gbdispatchmodel(
            "resources/GB-ETYS-subset/etys_boundary_crossings.csv"
        ),
    shell:
        """
        pixi run \
            --manifest-path={input.manifest} \
            --environment=gb-model \
            snakemake \
                --profile profiles/default \
                --snakefile modules/gb-dispatch-model/Snakefile \
                --directory modules/gb-dispatch-model \
                --configfile {input.overwrite_configfiles} \
                --keep-going \
                --rerun-incomplete \
                {params.files}
        """


# General logic for the additional steps done here to combine the models
# and create the different scenarios:

# 1. Create the networks from both models
# * GB Dispatch Model before dispatch
# * NGV-IEM before EC run
# then combine the models and run them as dispatch
# (check dispatch logic from both models and see how to combine them/which one to adapt)

# 2. Add the TF uncertainty to the GB model network
# * Use logic from the NGV-IEM model to add TF uncertainty to the combined network
# and use the logic for running as from the previous step

# 3. Create the SQ scenario based on the TF scenario as in the NGV-IEM model
# and run the combined model as dispatch again
# (check for differences in the dispatch logic between the two models and adapt as needed)

# 4. Setup the redispatch logic
# Use the logic from the GB Dispatch Model to run it on the combined networks

RESULTS = "results/"


rule all_IEM:
    message:
        "Collecting IEM related files"
    input:
        lambda w: expand(
            (RESULTS + "dispatch/networks/IEM/{planning_horizonss}.nc"),
            **config["scenario"],
        ),


rule prepare_scenario_IEM:
    message:
        "Preparing a combined model based on phase NGV-IEM model and GB Dispatch Model network for year {wildcards.planning_horizons} (scenario: IEM - integrated energy market)."
    params:
        carrier_map=config["carrier_mapping"],
        time_aggregation=config["time_aggregation"],
        capacity_multipliers=config["calibration"]["capacity_multipliers"],
    input:
        # Use inputs from both models with fixed capacities before they are passed to
        # the optimal dispatch run
        gb_model=gbdispatchmodel(
            f"resources/GB-ETYS-subset/networks/{config['fes_scenario']}/unconstrained_clustered/{{planning_horizons}}.nc"
        ),
        iem_model=ngviemmodel(
            "results/ngv-iem/latest/networks/base_s_all___{planning_horizons}_no_ce.nc",
        ),
        external_boundary_definitions="config/boundary_definitions.yaml",
    output:
        model="resources/base/networks/IEM/{planning_horizons}.nc",
    log:
        "logs/prepare_scenario_IEM/{planning_horizons}.log",
    script:
        "scripts/prepare_scenario_IEM.py"


rule prepare_scenario_TF:
    message:
        "Preparing model for uncertainty scenario based on combined model for year {wildcards.planning_horizons} (scenario: TF - trader forecast)."
    params:
        forecast_errors=config["forecast_errors"],
    input:
        model=rules.prepare_scenario_IEM.output.model,
        forecast_errors=rules.run_phase01_model_as_rule.output.forecast_errors,
    output:
        model="resources/base/networks/TF/{planning_horizons}.nc",
    log:
        "logs/prepare_scenario_TF/{planning_horizons}.log",
    script:
        "scripts/prepare_scenario_TF.py"


rule prepare_scenario_SQ:
    message:
        "Preparing model for status quo scenario based on combined model for year {wildcards.planning_horizons} (scenario: SQ - status quo)."
    params:
        explicit_allocation=config["explicit_allocation"],
    input:
        model=rules.prepare_scenario_IEM.output.model,
        model_tf="results/dispatch/networks/TF/{planning_horizons}.nc",
    output:
        model="resources/base/networks/SQ/{planning_horizons}.nc",
        line_limits="resources/base/line_limits/{planning_horizons}.csv",
    log:
        "logs/prepare_scenario_SQ/{planning_horizons}.log",
    script:
        "scripts/prepare_scenario_SQ.py"


rule retrieve_data_FBMC:
    message:
        "Retrieving data for flow-based market coupling for year {wildcards.planning_horizons} (scenario: FBMC - flow-based market coupling)."
    input:
        flow_based_constraints="data/NGV-FBMC/primary/20260326/flow_based_constraints_{planning_horizons}.parquet",
    output:
        ptdf="data/NGV-FBMC/primary/20260326/ptdf/{planning_horizons}.nc",
        ram="data/NGV-FBMC/primary/20260326/ram/{planning_horizons}.nc",
    log:
        "logs/retrieve_data_FBMC/{planning_horizons}.log",
    run:
        from scripts.fbmc import FBMCConstraint

        (
            FBMCConstraint.from_parquet(input.flow_based_constraints).to_netcdf(
                output.ptdf, output.ram
            )
        )


rule prepare_scenario_FBMC:
    message:
        "Preparing model for flow-based scenario based on combined model for year {wildcards.planning_horizons} (scenario: FBMC - flow-based market coupling)."
    input:
        model=rules.prepare_scenario_IEM.output.model,
        ptdf=rules.retrieve_data_FBMC.output.ptdf,
        ram=rules.retrieve_data_FBMC.output.ram,
    output:
        model="resources/base/networks/FBMC/{planning_horizons}.nc",
        ptdf="resources/base/fbmc/ptdf/{planning_horizons}.nc",
        ram="resources/base/fbmc/ram/{planning_horizons}.nc",
    log:
        "logs/prepare_scenario_FBMC/{planning_horizons}.log",
    script:
        "scripts/prepare_scenario_FBMC.py"


rule prepare_dispatch:
    message:
        "Preparing dispatch for year {wildcards.planning_horizons} and scenario {wildcards.scenario}."
    params:
        # Important: Disable GB model load shedding overwrite with this setting
        # Load shedding is handled elsewhere
        load_shedding_cost_above_marginal=None,
    input:
        network="resources/base/networks/{scenario}/{planning_horizons}.nc",
    output:
        network="resources/dispatch/networks/{scenario}/{planning_horizons}.nc",
    log:
        "logs/prepare_dispatch/{scenario}/{planning_horizons}.log",
    script:
        "scripts/prepare_dispatch.py"


rule solve_dispatch:
    message:
        "Running the dispatch for the combined model for year {wildcards.planning_horizons} in scenario: {wildcards.scenario}."
    params:
        solving=config["solving"],
        foresight=config["foresight"],
        co2_sequestration_potential=config["sector"]["co2_sequestration_potential"],
        renewable_carriers=config["electricity"]["renewable_carriers"],
        # Only the GB dispatch model defines custom extra functionality
        custom_extra_functionality="scripts/gb_model/dispatch/custom_constraints.py",
        # Files required for the custom extra functionality of the GB dispatch model
        # (they are read as params, not as input files for whatever reasons)
        # TODO make sure logic is in solve_network
        nuclear_max_annual_capacity_factor=config["conventional"]["nuclear"][
            "max_annual_capacity_factor"
        ],
        nuclear_min_annual_capacity_factor=config["conventional"]["nuclear"][
            "min_annual_capacity_factor"
        ],
        # openTYNDP specific: Not used (because OH trajectories are off)
        # but keeping for consistency to be able to reuse code from the openTYNDP model
        renewable_carriers_tyndp=config["electricity"]["tyndp_renewable_carriers"],
        scenario=lambda w: w.scenario,
    input:
        network="resources/dispatch/networks/{scenario}/{planning_horizons}.nc",
        ptdf=branch(
            lambda wildcards: wildcards.scenario == "FBMC",
            rules.prepare_scenario_FBMC.output.ptdf,
        ),
        ram=branch(
            lambda wildcards: wildcards.scenario == "FBMC",
            rules.prepare_scenario_FBMC.output.ram,
        ),
        # TYNDP specific
        offshore_zone_trajectories=rules.run_phase01_model_as_rule.output.offshore_zone_trajectories,
    output:
        network="results/dispatch/networks/{scenario}/{planning_horizons}.nc",
        config="results/dispatch/configs/{scenario}/{planning_horizons}.yaml",
    log:
        solver="logs/solve_dispatch/{scenario}/{planning_horizons}_solver.log",
        memory="logs/solve_dispatch/{scenario}/{planning_horizons}_memory.log",
        python="logs/solve_dispatch/{scenario}/{planning_horizons}_python.log",
    benchmark:
        "results/dispatch/benchmarks/solve_network/{scenario}/{planning_horizons}"
    threads: config["solving"]["solver_options"]["threads"]
    resources:
        mem_mb=config["solving"]["mem_mb"],
        runtime=config["solving"]["runtime"],
        parallel_solving=1,
    shadow:
        config["run"]["use_shadow_directory"]
    script:
        "scripts/solve_network.py"


rule calc_interconnector_bid_offer_profile:
    message:
        "Calculate interconnector bid/offer profiles"
    input:
        bids_and_offers=gbdispatchmodel(
            f"resources/GB-ETYS-subset/gb-model/{config['fes_scenario']}/bid_offer_multipliers.csv"
        ),
        unconstrained_result="results/dispatch/networks/{scenario}/{planning_horizons}.nc",
    output:
        bid_offer_profile="resources/redispatch/interconnector_bid_offer_profile/{scenario}/{planning_horizons}.csv",
    log:
        "logs/calc_interconnector_bid_offer_profile/{scenario}/{planning_horizons}.log",
    script:
        "scripts/gb_model/redispatch/calc_interconnector_bid_offer_profile.py"


rule prepare_redispatch:
    message:
        "Preparing redispatch for year {wildcards.planning_horizons} in scenario: {wildcards.scenario}."
    params:
        GBP_to_EUR=config["GBP_to_EUR"],
        strike_price_mapping=config["carrier_mapping"]["strike_price_mapping"],
        unconstrain_lines_and_links=config["redispatch"]["unconstrain_lines_and_links"],
        no_redispatch_carriers=config["redispatch"]["no_redispatch_carriers"],
    input:
        network="resources/base/networks/{scenario}/{planning_horizons}.nc",
        dispatch_result=rules.solve_dispatch.output.network,
        interconnector_bid_offer=rules.calc_interconnector_bid_offer_profile.output.bid_offer_profile,
        boundary_crossings="config/boundary_definitions.yaml",
        # Unchanged from GB dispatch model
        renewable_strike_prices=gbdispatchmodel(
            "resources/GB-ETYS-subset/gb-model/CfD_strike_prices.csv"
        ),
        bids_and_offers=gbdispatchmodel(
            f"resources/GB-ETYS-subset/gb-model/{config['fes_scenario']}/bid_offer_multipliers.csv"
        ),
    output:
        network="resources/redispatch/networks/{scenario}/{planning_horizons}.nc",
        boundary_crossings="resources/redispatch/boundary_crossings/{scenario}/{planning_horizons}.csv",
    log:
        "logs/prepare_redispatch/{scenario}/{planning_horizons}.log",
    script:
        "scripts/prepare_redispatch.py"


rule solve_redispatch:
    message:
        "Running the redispatch for year {wildcards.planning_horizons} in scenario: {wildcards.scenario}."
    params:
        solving=config["solving"],
        foresight=config["foresight"],
        co2_sequestration_potential=config["sector"]["co2_sequestration_potential"],
        renewable_carriers=config["electricity"]["renewable_carriers"],
        # GB dispatch model specific
        custom_extra_functionality="scripts/gb_model/redispatch/custom_constraints.py",
        manual_future_etys_caps=branch(
            config["etys"]["use_future_capacities"],
            config["etys"]["manual_future_capacities"],
            {},
        ),
        # openTYNDP specific: Not used (because OH trajectories are off)
        # but keeping for consistency to be able to reuse code from the openTYNDP model
        renewable_carriers_tyndp=config["electricity"]["tyndp_renewable_carriers"],
        scenario=lambda w: w.scenario,
    input:
        network=rules.prepare_redispatch.output.network,
        current_etys_caps=gbdispatchmodel(
            "resources/GB-ETYS-subset/gb-model/etys_boundary_capabilities.csv"
        ),
        future_etys_caps=branch(
            config["etys"]["use_future_capacities"],
            gbdispatchmodel(
                f"resources/GB-ETYS-subset/gb-model/{config['fes_scenario']}/future_etys_boundary_capabilities.csv"
            ),
            [],
        ),
        boundary_crossings=rules.prepare_redispatch.output.boundary_crossings,
        # TYNDP specific
        offshore_zone_trajectories=rules.run_phase01_model_as_rule.output.offshore_zone_trajectories,
    output:
        network="results/redispatch/networks/{scenario}/{planning_horizons}.nc",
        config="results/redispatch/configs/{scenario}/{planning_horizons}.yaml",
    log:
        solver="logs/solve_redispatch/{scenario}/{planning_horizons}_solver.log",
        memory="logs/solve_redispatch/{scenario}/{planning_horizons}_memory.log",
        python="logs/solve_redispatch/{scenario}/{planning_horizons}_python.log",
    benchmark:
        "results/redispatch/benchmarks/solve_network/{scenario}/{planning_horizons}"
    threads: config["solving"]["solver_options"]["threads"]
    resources:
        mem_mb=config["solving"]["mem_mb"],
        runtime=config["solving"]["runtime"],
        parallel_solving=1,
    shadow:
        config["run"]["use_shadow_directory"]
    script:
        "scripts/solve_network.py"


rule all:
    input:
        expand(
            "results/dispatch/networks/{scenario}/{planning_horizons}.nc",
            scenario=config["scenarios"],
            planning_horizons=config["planning_horizons"],
        ),
        expand(
            "results/redispatch/networks/{scenario}/{planning_horizons}.nc",
            scenario=config["scenarios"],
            planning_horizons=config["planning_horizons"],
        ),
    default_target: True
