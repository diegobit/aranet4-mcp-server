# aranet4-mcp-server

MCP server to manage your Aranet4 CO2 sensor.

## Features:
- Scan for nearby devices.
- Fetch new data from embedded device memory and save to a local sqlite db for tracking and later viewing.
- Ask questions about recent measurements or about a specific past date.
- *[For MCP clients that support images]* Ask data to be plotted to also have a nice visualization!
- **Assisted configuration!** 💫 After installation, just ask `init aranet4` in your client to set up everything for the mcp server to work with your device.

## Installation

1. Clone repo:

    ```
    git clone git@github.com:diegobit/aranet4-mcp-server.git`
    cd aranet4-mcp-server
    ```

2. Prepare environment:

    - **Recommended (with [uv](https://docs.astral.sh/uv/))**: Nothing to do. The provided `pyproject.toml` handles dependencied and virtual environments.
    - **Alternative (with pip)**: install with `pip install .`

3. Add to MCP client configuration:

    ```json
    "aranet4": {
      "command": "{{PATH_TO_UV}}", // run `which uv`
        "args": [
          "--directory",
          "{{PATH_TO_SRC}}/aranet4-mcp-server/",
          "run",
          "server.py"
        ]
    }
    ```

    - Claude Desktop MacOS config file path: `~/Library/Application Support/Claude/claude_desktop_config.json`
    - Cursor MacOS config file path: `~/.cursor/mcp.json`

4. Configure:

    - **Recommended (AI assisted config!):** start your client and ask `init aranet4` to get a guided configuration.
    - **Alternative (manual):** edit file `config.yaml`. You need to provide the mac address and the device name. You can get the mac address with `aranetctl --scan` from [Aranet4-Python](https://github.com/Anrijs/Aranet4-Python) (installed with this repo dependencies).

## Automatic data fetch job

If you want your local db to always be updated, you can setup a cronjob or a launch agent that fetches data automatically every few hours. In MacOS, do as follows:

1. Configure absolute paths in `com.diegobit.aranet4-fetch.plist`.
2. Install LaunchAgent:
   ```bash
   cp com.diegobit.aranet4-fetch.plist ~/Library/LaunchAgents/
   launchctl load ~/Library/LaunchAgents/com.diegobit.aranet4-fetch.plist
   ```

