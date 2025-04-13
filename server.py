import asyncio
import os
from datetime import datetime
import json

import yaml
import aranet4
from mcp.server.fastmcp import FastMCP, Image

from aranet import Aranet4DB


mcp = FastMCP("aranet4")

with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

aranet4_db = Aranet4DB(
    device_name=config["device_name"],
    device_mac=config["device_mac"],
    db_path=os.path.expanduser(config["db_path"]),
    use_local_tz=config["use_local_tz"]
)

@mcp.tool()
async def init_aranet4_config() -> str:
    """
    Initialize the Aranet4 MCP Server configuration. This tool helps setup the db_path,
    device_name, device_mac and use_local_tz settings.

    Returns descriptive information that guides the user through the configuration process.
    """
    return """
    To initialize the Aranet4 MCP Server configuration, please follow these steps:

    1. First, scan for nearby devices using the `scan_devices` tool.
    2. Check your current configuration with `get_configuration` (with no arguments).
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
            return "No Aranet4 devices found nearby."

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
async def get_configuration() -> str:
    """
    Get current config.

    Returns:
        str: current configuration object.
    """
    return (
        "Current config:\n"
        f"{json.dumps(config, indent=4)}"
    )

@mcp.tool()
async def set_configuration(db_path=None, device_name=None, device_mac=None, use_local_tz=None) -> str:
    """
    Change configuration of database or currently tracked device.

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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"config_{timestamp}.yaml"
    with open(backup_filename, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    if db_path is not None:
        config['db_path'] = db_path
    if device_name is not None:
        config['device_name'] = device_name
    if device_mac is not None:
        config['device_mac'] = device_mac
    if use_local_tz is not None:
        config['use_local_tz'] = use_local_tz

    with open("config.yaml", 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    return (
        "Config Updated successfully.\n"
        "\n"
        "# New config:\n"
        f"{json.dumps(config, indent=4)}"
    )


@mcp.tool()
async def get_database_stats() -> str:
    """
    Get statistics about the Aranet4 sqlite database, including:
    - List of devices
    - Total number of measurements
    - Time range (first to last measurement dates)

    Returns:
        A markdown-formatted summary of database statistics
    """
    return aranet4_db.get_database_stats()

@mcp.tool()
async def fetch_new_data() -> str:
    """
    Fetch the data stored in the embedded Aranet4 device memory, store in the local database, and return it (markdown formatted).

    Args:
        num_retries: Number of retry attempts if fetching fails. Default = 3
    """
    return await aranet4_db.fetch_new_data()

@mcp.tool()
async def get_recent_data(limit: int = 20, sensors: str = "all", output_as_plot: bool = False) -> str | Image:
    """
    Get most recent sensor data from the Aranet4 local database. Defaults to return data in markdown format; set output_as_plot=true if the user asks for a plot (or an image).

    Args:
        limit: number of measurements to get (default: 20)
        sensors: comma-separated sensors to retrieve (valid options: temperature, humidity, pressure, CO2), or "all"
        output_as_plot: whether to get data as a an image of the plot (true) or markdown text description (false)
    """
    valid_sensors = aranet4_db.get_valid_sensors()

    if sensors != "all" and any(True for s in sensors.split(",") if s not in valid_sensors):
        return f"Invalid sensor type in '{sensors}'. Valid options are: {', '.join(valid_sensors)} or 'all'"

    try:
        format = "plot" if output_as_plot else "markdown"
        data = aranet4_db.get_recent_data(limit, sensors, format)
        if not data:
            return "No data found"
        elif not isinstance(data, str):
            return "Data has wrong format"
        elif output_as_plot == "plot":
            return Image(data)
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
    Get sensor data within a specific time range.
    - If the range is wide and there are too many measurements, these are dropped until below limit. Use a bigger limit if the timerange is big.
    - Defaults to returning data in markdown format; Set output_as_plot = "true" if the user asks for a plot (or an image).

    Args:
        start_datetime: Start datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
        end_datetime: End datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
        sensors: comma-separated sensors to retrieve (valid options: temperature, humidity, pressure, CO2), or "all"
        limit: limit number of results. If there are more results than limit, one every two elements are dropped until below the threshold.
        output_plot: whether to get data as an image of the plot (true) or markdown text descrption (false)
    """
    valid_sensors = aranet4_db.get_valid_sensors()

    if sensors != "all" and any(True for s in sensors.split(",") if s not in valid_sensors):
        return f"Invalid sensor type in '{sensors}'. Valid options are: {', '.join(valid_sensors)} or 'all'"

    try:
        data = aranet4_db.get_data_by_timerange(
            start_datetime,
            end_datetime,
            sensors,
            limit,
            format = "plot" if output_as_plot else "markdown"
        )
        if not data:
            return f"No data found between {start_datetime} and {end_datetime}"
        elif not isinstance(data, str):
            return "Data has wrong format"
        elif output_as_plot == "plot":
            return Image(data)
        else:
            return data

    except ValueError as e:
        return f"Invalid datetime format: {str(e)}. Please use ISO format (YYYY-MM-DDTHH:MM:SS)"
    except Exception as e:
        return f"Error retrieving data: {str(e)}"


if __name__ == "__main__":
    print("Starting Aranet4 MCP Server...")
    mcp.run()

