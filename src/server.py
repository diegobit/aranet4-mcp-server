import asyncio
import json
import os
from datetime import datetime
from contextlib import asynccontextmanager

import aranet4
import yaml
from mcp.server.fastmcp import FastMCP, Image

from aranet import Aranet4Manager

# -------------------
# Init
# -------------------

def _load_cfg(path="config.yaml") -> dict:
    if os.path.exists(path):
        with open(path) as f:
            cfg = yaml.safe_load(f) or {}
    else:
        cfg = {}
    # override with env vars (for smithery)
    cfg.update({
        "device_name":  os.getenv("DEVICE_NAME", cfg.get("device_name")),
        "device_mac":   os.getenv("DEVICE_MAC",  cfg.get("device_mac")),
        "db_path":      os.path.expanduser(os.getenv("DB_PATH", cfg.get("db_path", "aranet4.db"))),
        "use_local_tz": os.getenv("USE_LOCAL_TZ", str(cfg.get("use_local_tz", True))).lower() == "true",
    })
    return cfg

@asynccontextmanager
async def _lifespan(app):
    cfg = _load_cfg()
    aranet4manager = Aranet4Manager(**cfg)
    app.cfg = cfg
    app.aranet4manager = aranet4manager
    yield

mcp = FastMCP("aranet4", lifespan=_lifespan)

# -------------------
# Tools
# -------------------

@mcp.tool()
async def init_aranet4_config() -> str:
    """
    Initialize the Aranet4 MCP Server configuration. This tool helps setup the db_path,
    device_name, device_mac and use_local_tz settings.

    Example questions:
    - "init aranet4"
    - "Help me configure my aranet4 co2 sensor"

    Returns descriptive information that guides the user through the configuration process.
    """
    return """
    To initialize the Aranet4 MCP Server configuration, please follow these steps:

    1. First, scan for nearby devices using the `scan_devices` tool.
    2. Check your current configuration with `get_configuration_and_db_stats` (with no arguments).
    3. Update the configuration with `set_configuration` tool by setting the new values. Always prompt the user before setting values.
    4. Suggest to the user what he/her might do after the configuration (just report the examples below):
        a. "Fetch new data from my aranet4" (This will update the local db with the data stored in the device).
        b. "Show recent measurements from my aranet4".
        c. "How was the CO2 last sunday around noon?".
        d. "Plot the CO2 of the last two weeks."

    The configuration consists of:
    - db_path: path to the local sqlite3 database
    - device_name: a memorable name for your Aranet4 device
    - device_mac: the MAC address of your Aranet4 CO2 sensor
    - use_local_tz: whether to use local timezone when plotting

    Use `scan_devices` now to begin the process.
    """


@mcp.tool()
async def scan_devices() -> str:
    """
    Scan for nearby Aranet4 devices and return their information.

    Example questions:
    - "Scan for nearby aranet4 devices"
    - "Are there co2 sensors around me now?"

    Returns the scan results.
    """
    try:
        # Collection of discovered devices to avoid duplicates
        discovered_devices = {}

        # Callback function to process scan results
        def on_device_found(advertisement):
            if advertisement.device.address not in discovered_devices:
                discovered_devices[advertisement.device.address] = advertisement
            else:
                # Update with newer data if available
                if advertisement.readings:
                    discovered_devices[advertisement.device.address] = advertisement

        # Run the scanner with our callback
        scanner = aranet4.Aranet4Scanner(on_device_found)
        await scanner.start()
        await asyncio.sleep(5)  # Scan for 5 seconds
        await scanner.stop()

        if not discovered_devices:
            return "No Aranet4 devices found nearby. Is your Aranet4 closeby? Maybe you should enable extended bluetooth range?"

        # Format results
        result = []
        for address, adv in discovered_devices.items():
            device_info = [f"Device: {adv.device.name or 'Unknown'}, MAC: {address}, RSSI: {adv.rssi or 'N/A'} dBm"]

            # Add sensor readings if available
            if adv.readings:
                readings = adv.readings
                device_info.append(f"  CO2: {readings.co2} ppm")
                device_info.append(f"  Temperature: {readings.temperature:.1f} °C")
                device_info.append(f"  Humidity: {readings.humidity}%")
                device_info.append(f"  Pressure: {readings.pressure:.1f} hPa")
                device_info.append(f"  Battery: {readings.battery}%")

            result.append("\n".join(device_info))

        return "\n\n".join(result)
    except Exception as e:
        return f"Error scanning for devices: {str(e)}"


@mcp.tool()
async def get_configuration_and_db_stats() -> str:
    """
    Get current config and get statistics about the Aranet4 sqlite database.

    Configurations:
    - device_name
    - device_mac
    - db_path
    - use_local_tz

    Database stats:
    - List of devices
    - Total number of measurements
    - Total time range (first to last measurement dates)

    Example questions:
    - "Get my aranet4 config"
    - "What's the path of the aranet4 database?"
    - "I need the mac address of aranet4"
    - "How many total measurements there are in the aranet4 db?"
    - "How many devices there are in the aranet4 database?"

    Returns:
        str: A markdown-formatted summary with the current configuration object and database statistics
    """
    return (
        "# Aranet4 current config:\n"
        f"{json.dumps(mcp.cfg, indent=4)}\n"
        "\n"
        "# Aranet4 database statistics:\n"
        f"{json.dumps(mcp.aranet4manager.get_database_stats(), indent=4)}"
    )


@mcp.tool()
async def set_configuration(db_path=None, device_name=None, device_mac=None, use_local_tz=None) -> str:
    """
    Change configuration of database or currently tracked device.

    Example questions:
    - "Change configured aranet4 db_path"
    - "Set device mac of my aranet4"

    Args:
        db_path: path to db where to store past measurements.
        device_name: name of the device, used as a first column in db.
        device_mac: MAC address of aranet4 sensor. Used for fetching with bluetooth.
        use_local_tz: if to use local timezone when plotting.

    Returns:
        str: new configuration object.
    """
    if db_path is None and device_name is None and device_mac is None and use_local_tz is None:
        return "Need to provide at least one argument."

    config = mcp.cfg
    aranet4manager = mcp.aranet4manager

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs("config_bk", exist_ok=True)
    backup_filename = f"config_bk/config_{timestamp}.yaml"
    with open(backup_filename, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    if db_path is not None:
        config['db_path'] = db_path
        aranet4manager.db_path = db_path
    if device_name is not None:
        config['device_name'] = device_name
        aranet4manager.device_name = device_name
    if device_mac is not None:
        config['device_mac'] = device_mac
        aranet4manager.device_mac = device_mac
    if use_local_tz is not None:
        config['use_local_tz'] = use_local_tz
        aranet4manager.use_local_tz = use_local_tz

    with open("config.yaml", 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    return (
        "Config Updated successfully.\n"
        "\n"
        "# New config:\n"
        f"{json.dumps(config, indent=4)}"
    )


@mcp.tool()
async def fetch_new_data() -> str:
    """
    Fetch the data stored in the embedded Aranet4 device memory, store in the local database, and return it (markdown formatted).

    Example questions:
    - "Get data from aranet4 device and save to local db."
    - "Update local database with new aranet4 data."

    Args:
        num_retries: Number of retry attempts if fetching fails. Default = 3
    """
    return await mcp.aranet4manager.fetch_new_data()


@mcp.tool()
async def get_recent_data(limit: int = 20, sensors: str = "all", output_as_plot: bool = False) -> str | Image:
    """
    Get most recent measurements of the configured 'aranet4 co2 sensor' from the local database. Defaults to returning data in markdown format; set output_as_plot=true if the user asks for a plot (or an image).

    Example questions:
    - "What's the co2 recently?"
    - "Show me the last 50 readings of co2 and temperature."
    - "Give me a plot for last 100 measurements."

    Args:
        limit: number of measurements to get (default: 20)
        sensors: comma-separated sensors to retrieve (valid options: temperature, humidity, pressure, CO2), or "all"
        output_as_plot: whether to get data as a an image of the plot (true) or markdown text description (false)
    """
    aranet4manager = mcp.aranet4manager

    sensors, all_valid = aranet4manager.validate_sensors(sensors)
    if not all_valid:
        valid_sensor_names = aranet4manager.list_sensors()
        return f"Invalid sensor type in '{sensors}'. Valid options are: {', '.join(valid_sensor_names)} or 'all'"

    try:
        data = aranet4manager.get_recent_data(
            limit=limit,
            sensors=sensors,
            format="plot" if output_as_plot else "markdown"
        )
        if not data:
            return "No data found"
        elif not isinstance(data, str):
            return "Data has wrong format"
        elif output_as_plot is True:
            return Image(path=data)
        else:
            return data

    except Exception as e:
        return f"Error retrieving data: {str(e)}"


@mcp.tool()
async def get_data_by_timerange(
    start_datetime: str,
    end_datetime: str,
    sensors: str = "all",
    limit: int = 100,
    output_as_plot: bool = False
) -> str | Image:
    """
    Get measuremens within a specifig datetime range of the configured 'aranet4 co2 sensor' from the local database.
    - Always use this when the user asks about specific dates and time ranges.
    - If the range is wide and there are too many measurements, these are dropped until below limit. Use a bigger limit if the timerange is big.
    - Defaults to returning data in markdown format; Set output_as_plot = "true" if the user asks for a plot (or an image).

    Example questions:
    - "Was the aranet4 co2 level good, last sunday around noon?"
    - "Get yesterday readings from my aranet4."
    - "Give me temperature and humidity yesterday morning."
    - "Show me with a plot the co2 from 2025-05-08 to 2025-05-12."
    - "plot this month of data from aranet4."

    Args:
        start_datetime: Start datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
        end_datetime: End datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
        sensors: comma-separated sensors to retrieve (valid options: temperature, humidity, pressure, CO2), or "all"
        limit: limit number of results. If there are more results than limit, one every two elements are dropped until below the threshold.
        output_plot: whether to get data as an image of the plot (true) or markdown text descrption (false)
    """
    aranet4manager = mcp.aranet4manager
    valid_sensors = aranet4manager.list_sensors()

    if sensors != "all" and any(True for s in sensors.split(",") if s not in valid_sensors):
        return f"Invalid sensor type in '{sensors}'. Valid options are: {', '.join(valid_sensors)} or 'all'"

    try:
        data = aranet4manager.get_data_by_timerange(
            start_time=start_datetime,
            end_time=end_datetime,
            sensors=sensors,
            limit=limit,
            format="plot" if output_as_plot else "markdown"
        )
        if not data:
            return f"No data found between {start_datetime} and {end_datetime}"
        elif not isinstance(data, str):
            return "Data has wrong format"
        elif output_as_plot is True:
            return Image(data)
        else:
            return data

    except ValueError as e:
        return f"Invalid datetime format: {str(e)}. Please use ISO format (YYYY-MM-DDTHH:MM:SS)"
    except Exception as e:
        return f"Error retrieving data: {str(e)}"


if __name__ == "__main__":
    # print("Starting Aranet4 MCP Server...")
    mcp.run()

