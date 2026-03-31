<!--
SPDX-FileCopyrightText: NGV-FBMC contributors <https://github.com/open-energy-transition/NGV-FBMC>
SPDX-License-Identifier: MIT
-->

# FBMC implementation on GB model using openTYNDP data for RoE

1. Implements 3 (sequentially solved) scenarios to represent economic dispatch and redispatch.
   1. IEM: Assumes perfect foresight of load and renewable generation
   2. TF: Assumes imperfect forecasting which impacts load and generation.
   3. SQ: Adds cost for re-dispatch when the actual load deviates from the forecasted (TF) scenario. Limits line capacity based on calculated flows in TF scenario (+/- 5% tolerance by default)

2. Performs solves on merged network (Open-TYNDP and GB dispatch). Improves resolution of GB within the TYNDP model and/or increases scope of continental Europe within the GB model. Key implementation details:
   1. Inherits technology assumptions from the Open-TYNDP model
   2. Implements conventional generators from GB model as multilinks with tracked CO2 as in Open-TYNDP
   3. Implement corresponding components to account for ramp up and down (for generators, storage units, links, and stores).

## Setup 

1. (Optional) Install `pixi` if not installed.
2. Clone the repository recursive for submodules:
   ```bash
   git clone --recursive https://github.com/open-energy-transition/NGV-FBMC.git
   ```
3. Add ENTSO-E API key in `.env` file in the root directory:
   ```
   ENTSO_E_API_KEY=your_api_key_here
   ```
   To get an API key, register at [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html).
   Then send an email to `transparency@entsoe.eu` with subject `Restful API access` and your account email address in the body.
   After they have responded, you can generate an API key in your account settings under `Web API Access`.

## Running the model

* To run the default `gb-dispatch-model`, use:
  ```bash
  <not yet implemented>
  ```
* To run the default `NGV-IEM` model, use:
  ```bash
  <not yet implemented>
  ```
* To run the full model, use:
  ```bash
  <not yet implemented>
  ```

To run other targets from the `Snakefile` or one of the modules, use
```bash
pixi run --environment=ngv-fbmc snakemake --cores all <target>
```
or run `snakemake` inside a `pixi shell` session that is started with the `ngv-fbmc` environment:
```bash
pixi shell --environment=ngv-fbmc
# Work inside the shell from here, e.g.
snakemake --cores all <target>
```
