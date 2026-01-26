# FBMC implementation on GB model using openTYNDP data for RoE

TODO

## Installation

1. Clone the repository recursive for submodules:
   ```bash
   git clone --recursive https://github.com/open-energy-transition/NGV-FBMC.git
   ```
2. Install `pixi` if not installed
3. Add ENTSO-E API key in `.env` file in the root directory:
   ```
   ENTSO_E_API_KEY=your_api_key_here
   ```
   To get an API key, register at [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html).
   Then send an email to `transparency@entsoe.eu` with subject `Restful API access` and your account email address in the body.
   After they have responded, you can generate an API key in your account settings under `Web API Access`.