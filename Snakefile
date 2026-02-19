from snakemake.common.configfile import load_configfile
from snakemake.utils import update_config


configfile: "config/config.yaml"


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
        network_2030=ngviemmodel(
            "resources/ngv-iem/latest/networks/base_s_all___2030.nc"
        ),
        network_noce_2030=ngviemmodel(
            "resources/ngv-iem/latest/networks/base_s_all___2030_no_ce.nc"
        ),
        network_lluk_2030=ngviemmodel(
            "resources/ngv-iem/latest/networks/base_s_all_lluk__2030.nc"
        ),
        network_2040=ngviemmodel(
            "resources/ngv-iem/latest/networks/base_s_all___2040.nc"
        ),
        network_noce_2040=ngviemmodel(
            "resources/ngv-iem/latest/networks/base_s_all___2040_no_ce.nc"
        ),
        network_lluk_2040=ngviemmodel(
            "resources/ngv-iem/latest/networks/base_s_all_lluk__2040.nc"
        ),
        results_2030=ngviemmodel("results/ngv-iem/latest/networks/base_s_all___2030.nc"),
        results_noce_2030=ngviemmodel(
            "results/ngv-iem/latest/networks/base_s_all___2030_no_ce.nc"
        ),
        results_lluk_2030=ngviemmodel(
            "results/ngv-iem/latest/networks/base_s_all_lluk__2030.nc"
        ),
        results_2040=ngviemmodel("results/ngv-iem/latest/networks/base_s_all___2040.nc"),
        results_noce_2040=ngviemmodel(
            "results/ngv-iem/latest/networks/base_s_all___2040_no_ce.nc"
        ),
        results_lluk_2040=ngviemmodel(
            "results/ngv-iem/latest/networks/base_s_all_lluk__2040.nc"
        ),
    shell:
        """
        pixi run \
            --manifest-path={input.manifest} \
            --environment=ngv \
            snakemake \
                --cores all  \
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
        overwrite_configfiles=["config/config.gb-dispatch.yaml"],
    output:
        network_2030=gbdispatchmodel(
            "resources/GB/networks/HT/constrained_clustered/2030.nc"
        ),
        network_2040=gbdispatchmodel(
            "resources/GB/networks/HT/constrained_clustered/2040.nc"
        ),
        results_dispatch_2030=gbdispatchmodel(
            "results/GB/networks/HT/unconstrained_clustered/2030.nc"
        ),
        results_dispatch_2040=gbdispatchmodel(
            "results/GB/networks/HT/unconstrained_clustered/2040.nc"
        ),
    shell:
        """
        pixi run \
            --manifest-path={input.manifest} \
            --environment=gb-model \
            snakemake \
                --cores 1 \
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


rule prepare_scenario_IEM:
    message:
        "Preparing a combined model based on phase NGV-IEM model and GB Dispatch Model network for year {wildcards.year} (scenario: IEM - integrated energy market)."
    input:
        # Use inputs from both models with fixed capacities before they are passed to
        # the optimal dispatch run
        gb_model=gbdispatchmodel(
            "resources/GB/networks/unconstrained_clustered/{year}.nc"
        ),
        iem_model=ngviemmodel(
            "results/ngv-iem/latest/networks/base_s_all___{year}_no_ce.nc"
        ),
    output:
        model="resources/dispatch/networks/IEM/{year}.nc",
    log:
        "logs/prepare_scenario_IEM/{year}.log",
    script:
        "scripts/prepare_scenario_IEM.py"


rule prepare_scenario_TF:
    message:
        "Preparing model for uncertainty scenario based on combined model for year {wildcards.year} (scenario: TF - trader forecast)."
    input:
        _model="resources/dispatch/networks/IEM/{year}.nc",
        forecast_errors=ngviemmodel("data/ngv_iem/relative_errors.parquet"),
    output:
        model="resources/dispatch/networks/TF/{year}.nc",
    log:
        "logs/prepare_scenario_TF/{year}.log",
    script:
        "scripts/prepare_scenario_TF.py"


rule prepare_scenario_SQ:
    message:
        "Preparing model for status quo scenario based on combined model for year {wildcards.year} (scenario: SQ - status quo)."
    input:
        model=rules.prepare_scenario_IEM.output.model,
        model_tf=rules.prepare_scenario_TF.output.model,
    output:
        model="resources/dispatch/networks/SQ/{year}.nc",
        # For validation only:
        line_limits="resources/dispatch/line_limits/{year}.csv",
    log:
        "logs/prepare_scenario_SQ/{year}.log",
    script:
        "scripts/prepare_scenario_SQ.py"


rule retrieve_data_FBMC:
    message:
        "Retrieving data for flow-based market coupling for year {wildcards.year} (scenario: FBMC - flow-based market coupling)."
    output:
        ptdf="data/NGV-FBMC/ptdf/{year}.parquet",
        ram="data/NGV-FBMC/ram/{year}.parquet",
    log:
        "logs/retrieve_data_FBMC/{year}.log",
    run:
        raise NotImplementedError("Logic not yet implemented.")


rule prepare_scenario_FBMC:
    message:
        "Preparing model for flow-based scenario based on combined model for year {wildcards.year} (scenario: FBMC - flow-based market coupling)."
    input:
        model=rules.prepare_scenario_IEM.output.model,
        ptdf="data/NGV-FBMC/ptdf/{year}.parquet",
        ram="data/NGV-FBMC/ram/{year}.parquet",
    output:
        model="resources/dispatch/networks/FBMC/{year}.nc",
    log:
        "logs/prepare_scenario_FBMC/{year}.log",
    script:
        "scripts/prepare_scenario_FBMC.py"


rule solve_dispatch:
    message:
        "Running the dispatch for the combined model for year {wildcards.year} in scenario: {wildcards.scenario}."
    input:
        model="resources/dispatch/networks/{scenario}/{year}.nc",
        ptdf=branch(
            rules.prepare_scenario_FBMC.input.ptdf,
            lambda wildcards: wildcards.scenario == "FBMC",
        ),
        ram=branch(
            rules.prepare_scenario_FBMC.input.ram,
            lambda wildcards: wildcards.scenario == "FBMC",
        ),
    output:
        dispatch_results="results/dispatch/networks/{scenario}/{year}.nc",
    log:
        "logs/solve_dispatch/{scenario}/{year}.log",
    script:
        "scripts/solve_dispatch.py"


rule prepare_redispatch:
    message:
        "Preparing redispatch for year {wildcards.year} in scenario: {wildcards.scenario}."
    input:
        dispatch_results="results/dispatch/networks/{scenario}/{year}.nc",
        model="resources/dispatch/networks/{scenario}/{year}.nc",
    output:
        redispatch_model="resources/dispatch/redispatch/{scenario}/{year}.nc",
    log:
        "logs/prepare_redispatch/{scenario}_{year}.log",
    script:
        "scripts/prepare_redispatch.py"


rule solve_redispatch:
    message:
        "Running the redispatch for year {wildcards.year} in scenario: {wildcards.scenario}."
    input:
        redispatch_model=rules.prepare_redispatch.output.redispatch_model,
    output:
        redispatch_results="results/dispatch/redispatch/{scenario}/{year}.nc",
    log:
        "logs/solve_redispatch/{scenario}/{year}.log",
    script:
        "scripts/solve_redispatch.py"
