# FBMC implementation on GB model using openTYNDP data for RoE

> Description is currently being expanded.

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
  pixi run gb-dispatch-model
  ```
* To run the default `NGV-IEM` model, use:
  ```bash
  pixi run ngv-iem-model
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