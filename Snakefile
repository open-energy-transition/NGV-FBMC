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


# Need to load the configuration for IEM module separately
configfiles_iem = [
    "modules/NGV-IEM/config/config.default.yaml",
    "modules/NGV-IEM/config/plotting.default.yaml",
    "modules/NGV-IEM/config/benchmarking.default.yaml",
    "modules/NGV-IEM/config/config.tyndp.yaml",
    "modules/NGV-IEM/config/config.ngv.yaml",
]

config_iem = {}
for configfile in configfiles_iem:
    config_part = load_configfile(configfile)
    update_config(config_iem, config_part)


module iem:
    snakefile:
        "modules/NGV-IEM/Snakefile"
    prefix:
        "modules/NGV-IEM/"
    config:
        config_iem


# Import all rules from IEM module with a prefix
use rule * from iem as iem_*


# Need to load the configuration for GB model module separately
configfiles_gbmodel = [
    "modules/gb-dispatch-model/config/config.default.yaml",
    "modules/gb-dispatch-model/config/plotting.default.yaml",
    "modules/gb-dispatch-model/config/config.gb.default.yaml",
]
config_gbmodel = {}
for configfile in configfiles_gbmodel:
    config_part = load_configfile(configfile)
    update_config(config_gbmodel, config_part)


module gbmodel:
    snakefile:
        "modules/gb-dispatch-model/Snakefile"
    prefix:
        "modules/gb-dispatch-model/"
    config:
        config_gbmodel


# Import all rules from GB model module with a prefix
use rule * from gbmodel as gbmodel_*

rule call_gb_results:
    input:
        "modules/gb-dispatch-model/results/GB/networks/constrained_clustered/2040.nc"
    shell:
        "pixi run --manifest-path modules/gb-dispatch-model/pixi.toml --environment gb-model"
