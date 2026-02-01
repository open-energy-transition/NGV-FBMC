from snakemake.common.configfile import load_configfile
from snakemake.utils import update_config

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


# We create a rule to run this model standalone as a pixi task, rather than including it as a snakemake module
# Reason for this approach: Two models that are forked from PyPSA-Eur currently lead to collisions with snakemake
# making it too error-prone to use them both as module at the same time. This might be fixed in the future
# by this PR by us ( ), for now we use a less beautiful but more utalitarian solution.
# We list all files that we need from the other model to have them registered with the remaining snakemake workflow
rule run_ngviem_model_as_rule:
    message:
        "Running the full phase 01 NGV-IEM model."
    input:
        manifest="modules/NGV-IEM/pixi.toml",
    output:
        results_2030="modules/NGV-IEM/results/ngv-iem/latest/networks/base_s_all___2030.nc",
        results_2030_noce="modules/NGV-IEM/results/ngv-iem/latest/networks/base_s_all___2030_no_ce.nc",
        results_2030_lluk="modules/NGV-IEM/results/ngv-iem/latest/networks/base_s_all_lluk__2030.nc",
        results_2040="modules/NGV-IEM/results/ngv-iem/latest/networks/base_s_all___2040.nc",
        results_2040_noce="modules/NGV-IEM/results/ngv-iem/latest/networks/base_s_all___2040_no_ce.nc",
        results_2040_lluk="modules/NGV-IEM/results/ngv-iem/latest/networks/base_s_all_lluk__2040.nc",
    shell:
        "pixi run --manifest-path={input.manifest} ngv"


# Need to load the configuration for GB model module separately as they
# they cannot be found in `config/*` (where the `modules/gb-dispatch-model/Snakefile` looks for them),
# but in `modules/gb-dispatch-model/config/*`
configfiles_gbdispatchmodel = [
    "modules/gb-dispatch-model/config/config.default.yaml",
    "modules/gb-dispatch-model/config/plotting.default.yaml",
    "modules/gb-dispatch-model/config/config.gb.default.yaml",
]
config_gbdispatchmodel = {}
for configfile in configfiles_gbdispatchmodel:
    config_part = load_config > file(configfile)
    update_config(config_gbdispatchmodel, config_part)


module gbdispatchmodel:
    snakefile:
        "modules/gb-dispatch-model/Snakefile"
    prefix:
        "modules/gb-dispatch-model/"
    config:
        config_gbdispatchmodel


use rule * from gbdispatchmodel as GBDM_*
