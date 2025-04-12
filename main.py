import asyncio

import aranet4
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from aranet4db import Aranet4DB


load_dotenv()
mcp = FastMCP("aranet4")
aranet4_db = Aranet4DB()


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
async def get_recent_data(limit: int = 20, sensor: str = "all", output_plot: bool = False) -> str:
    """
    Get most recent sensor data from the Aranet4 local database. Defaults to return data in markdown format; set output_plot=true if the user asks for a plot.

    Args:
        limit: number of measurements to get (default: 20)
        sensor: Sensor to retrieve (temperature, humidity, pressure, CO2, or "all")
        output_plot: whether to get data as a base64 image of the plot (true) or markdown text (false)
    """
    valid_sensors = aranet4_db.get_valid_sensors()

    if sensor != "all" and sensor not in valid_sensors:
        return f"Invalid sensor type. Valid options are: {', '.join(valid_sensors)} or 'all'"

    if output_plot:
        format = "plot_base64"
    else:
        format = "markdown"

    try:
        data = aranet4_db.get_recent_data(limit, sensor, format=format)
        if not data:
            return "No data found"
        if not isinstance(data, str):
            return "Data has wrong format"
        return data
    except Exception as e:
        return f"Error retrieving data: {str(e)}"


@mcp.tool()
async def get_data_by_timerange(
    start_datetime: str,
    end_datetime: str,
    sensor: str = "all",
    limit: int = 100,
    output_plot: bool = False
) -> str:
    """
    Get sensor data within a specific time range. Defaults to return data in markdown format; Set output_plot=true if the user asks for a plot. Increase the limit only if the timerange is big.

    Args:
        start_datetime: Start datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
        end_datetime: End datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
        sensor: Sensor to retrieve (temperature, humidity, pressure, CO2, or "all")
        limit: limit number of results. If above, makes it sparser. Set to a high number to (sort of) disable
        output_plot: whether to get data as a base64 image of the plot (true) or markdown text (false)
    """
    valid_sensors = aranet4_db.get_valid_sensors()

    if sensor != "all" and sensor not in valid_sensors:
        return f"Invalid sensor type. Valid options are: {', '.join(valid_sensors)} or 'all'"

    if output_plot:
        format = "plot_base64"
    else:
        format = "markdown"

    try:
        data = aranet4_db.get_data_by_timerange(start_datetime, end_datetime, sensor, limit, format=format)
        if not data:
            return f"No data found between {start_datetime} and {end_datetime}"
        if not isinstance(data, str):
            return "Data has wrong format"
        return data
    except ValueError as e:
        return f"Invalid datetime format: {str(e)}. Please use ISO format (YYYY-MM-DDTHH:MM:SS)"
    except Exception as e:
        return f"Error retrieving data: {str(e)}"


@mcp.tool()
async def generate_plot(sensors: str = "CO2", start_date: str = "", end_date: str = "", days: int = 3) -> str:
    """
    Generate a plot of sensor data within a date range or most recent days. Returns the local path of the image.

    Args:
        sensors: Comma-separated list of sensors to plot (temperature, humidity, pressure, CO2)
        start_date: Start date (YYYY-MM-DD) - overrides days parameter
        end_date: End date (YYYY-MM-DD) - defaults to current date
        days: Number of days to plot if start_date not provided (default: 3)
    """
    try:
        # Call the database method to generate the plot
        result = aranet4_db.generate_plot(sensors, start_date, end_date, days)

        # If result is a filepath, add a success message
        if not result.startswith("Error"):
            return f"Plot saved to: {result}"

        # Otherwise, return the error message
        return result
    except Exception as e:
        return f"Error generating plot: {str(e)}"


if __name__ == "__main__":
    print("Starting Aranet4 MCP Server...")
    mcp.run()
