import os
import sqlite3
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import yaml
import aranet4
import matplotlib.pyplot as plt
import pandas as pd
import tzlocal


class Aranet4DB:
    """Handler for Aranet4 device operations."""

    def __init__(self):
        self.sensor_plot_config = {
            "temperature": {"color": "red", "unit": "°C"},
            "humidity": {"color": "blue", "unit": "%"},
            "pressure": {"color": "green", "unit": "hPa"},
            "CO2": {"color": "purple", "unit": "ppm"},
        }

        with open("config.yaml", "r") as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)

        self.device_name = config["device_name"]
        self.device_mac = config["device_mac"]
        self.db_path = os.path.expanduser(config["db_path"])
        self.use_local_tz = config["use_local_tz"]
        if not self.device_name or not self.device_mac or not self.db_path:
            raise Exception("MCP server not configured. Edit config.yaml")

        self.local_timezone = "UTC"
        if self.use_local_tz:
            try:
                self.local_timezone = tzlocal.get_localzone_name()
            except Exception:
                pass

        self._init_database()

    def _init_database(self):
        """Initialize the database if it doesn't exist."""
        with sqlite3.connect(self.db_path) as con:
            cur = con.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS measurements(
                    device TEXT,
                    timestamp INTEGER,
                    temperature REAL,
                    humidity INTEGER,
                    pressure REAL,
                    CO2 INTEGER,
                    PRIMARY KEY(device, timestamp)
                )
            """)
            con.commit()

    def _format_data_as_markdown(self, column_data, timestamp_idx):
        """
        Format database query results as markdown table.

        Args:
            column_data: Tuple of (column_names, rows)
            timestamp_idx: index inside both column_names and rows of the timestamp

        Returns:
            Markdown formatted table as string
        """
        if not column_data:
            return "No data available."

        column_names, rows = column_data

        # Create header
        result = [" | ".join(column_names)]
        result.append('-' * (sum(len(name) for name in column_names) + 3 * (len(column_names) - 1)))

        # Format rows
        for row in rows:
            formatted_row = list(row)
            # Convert timestamp to local time
            dt = datetime.fromtimestamp(row[timestamp_idx], tz=timezone.utc)
            local_dt = dt.astimezone(ZoneInfo(self.local_timezone))
            formatted_row[timestamp_idx] = local_dt.strftime('%Y-%m-%d %H:%M:%S %z')
            result.append(" | ".join(str(value) for value in formatted_row))

        return "\n".join(result)

    def get_valid_sensors(self):
        """Return a list of valid sensor types."""
        return list(self.sensor_plot_config.keys())

    def get_database_stats(self) -> str:
        """
        Get statistics about the Aranet4 database, including:
        - List of devices
        - Total number of measurements
        - Time range (first to last measurement dates)

        Returns:
            A markdown-formatted summary of database statistics
        """
        try:
            with sqlite3.connect(self.db_path) as con:
                cur = con.cursor()

                # Get list of unique devices
                devices = cur.execute("SELECT DISTINCT device FROM measurements").fetchall()
                device_list = [device[0] for device in devices]

                # Get total measurement count
                count = cur.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]

                # Get first and last measurement timestamps
                first_ts = cur.execute("SELECT MIN(timestamp) FROM measurements").fetchone()[0]
                last_ts = cur.execute("SELECT MAX(timestamp) FROM measurements").fetchone()[0]

                # Get count per device
                device_counts = {}
                for device in device_list:
                    device_count = cur.execute(
                        "SELECT COUNT(*) FROM measurements WHERE device = ?",
                        (device,)
                    ).fetchone()[0]
                    device_counts[device] = device_count

            # Format timestamps as readable dates in local timezone
            if first_ts and last_ts:
                first_dt = datetime.fromtimestamp(first_ts, tz=timezone.utc)
                last_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc)

                first_local = first_dt.astimezone(ZoneInfo(self.local_timezone))
                last_local = last_dt.astimezone(ZoneInfo(self.local_timezone))

                first_str = first_local.strftime('%Y-%m-%d %H:%M:%S %z')
                last_str = last_local.strftime('%Y-%m-%d %H:%M:%S %z')
            else:
                first_str = "N/A"
                last_str = "N/A"

            # Build markdown output
            result = [
                "## Aranet4 Database Statistics",
                "",
                f"**Total Measurements**: {count}",
                "",
                f"**Time Range**: {first_str} to {last_str}",
                "",
                "**Devices**:"
            ]

            for device, device_count in device_counts.items():
                result.append(f"- {device}: {device_count} measurements")

            return "\n".join(result)
        except Exception as e:
            return f"Error retrieving database statistics: {str(e)}"

    def get_last_timestamp(self, device_name: str) -> (datetime | None):
        """Get the timestamp of the last recorded measurement for a specific device."""
        try:
           with sqlite3.connect(self.db_path) as con:
                cur = con.cursor()
                res = cur.execute(
                    """
                    SELECT MAX(timestamp)
                    FROM measurements
                    WHERE device = ?
                    """,
                    (device_name,),
                )
                row = res.fetchone()
                if row and row[0] is not None:
                    # Return as timezone-aware datetime object in local timezone
                    ts_utc = datetime.fromtimestamp(row[0], tz=timezone.utc)
                    return ts_utc.astimezone(ZoneInfo(self.local_timezone))
                return None
        except Exception as e:
            print(f"Error getting last timestamp for {device_name}: {e}")
            return None

    async def fetch_new_data(self, num_retries: int = 3) -> str:
        """
        Fetch the data stored in the embedded Aranet4 device memory, store in the local database, and return it.

        Args:
            num_retries: Number of retry attempts if fetching fails. Default = 3
        """
        entry_filter = {
            "end": datetime.now(timezone.utc).astimezone(ZoneInfo(self.local_timezone))
        }

        last_timestamp = self.get_last_timestamp(self.device_name)
        if last_timestamp:
            entry_filter['start'] = last_timestamp
            range_start = entry_filter['start'].isoformat()
        else:
            range_start = "beginning"
        range_end = entry_filter['end'].isoformat()

        history = None
        errors = []
        for _ in range(num_retries):
            entry_filter["end"] = datetime.now(timezone.utc).astimezone(ZoneInfo(self.local_timezone))
            try:
                history = await aranet4.client._all_records(self.device_mac, entry_filter, False)
                break
            except Exception as e:
                errors.append(str(e))
                continue
        if history is None:
            return (
                f"Failed to fetch measurements from '{self.device_name}' with mac '{self.device_mac} in range: ({range_start}, {range_end})\n"
                f"\n"
                f"# Errors\n"
                f"{'\n\n'.join(errors)}"
            )

        data = []
        columns = ["device", "timestamp", "temperature", "humidity", "pressure", "co2"]
        for entry in history.value:
            if entry.co2 < 0:
                continue

            data.append((
                self.device_name,
                entry.date.timestamp(),
                entry.temperature,
                entry.humidity,
                entry.pressure,
                entry.co2
            ))
        column_data = (columns, data)

        with sqlite3.connect(self.db_path) as con:
            cur = con.cursor()
            cur.executemany(
                'INSERT OR IGNORE INTO measurements VALUES(?, ?, ?, ?, ?, ?)', data
            )
            con.commit()

        return (
            f"Fetched {len(data)} measurements in range: ({range_start}, {range_end}) and added to local sqlite db.\n"
            f"\n"
            f"# Fetched data\n"
            f"{self._format_data_as_markdown(column_data, timestamp_idx=1)}\n"
        )


    def get_recent_data(self, limit=20, sensor="all", format="markdown") -> (tuple | str | None):
        """
        Retrieve recent data from the database. Gets textual output as default.
        Pass format=plot_path or plot_base64 to get the data plotted

        Args:
            limit: number of measurements to get
            sensor: Specific sensor or "all"
            format: output format. Default "markdown" for text. Available: "column_data": (tuple of column_names, rows); "markdown": str; "plot_base64": BASE64 encoded image; "plot_path": str path to png image.

        Returns:
            Tuple of (column_names, rows) or str (if format = markdown or plot_base64 or plot_path) or None on error
        """
        try:
            # Calculate date range
            end_time = datetime.now(timezone.utc)

            # Determine columns to select
            if sensor == "all":
                columns = "timestamp, temperature, humidity, pressure, CO2"
            else:
                columns = f"timestamp, {sensor}"

            # Connect and query
            with sqlite3.connect(self.db_path) as con:
                cur = con.cursor()

                query = f"""
                    SELECT {columns}
                    FROM measurements
                    WHERE timestamp <= ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """
                params = [int(end_time.timestamp()), limit]

                rows = cur.execute(query, params).fetchall()

            if not rows:
                return None

            column_data = (columns.split(", "), rows)

            if format == "markdown":
                return self._format_data_as_markdown(column_data, timestamp_idx=0)
            elif format.startswith("plot"):
                return self._generate_plot(column_data, format)

            return column_data
        except Exception as e:
            print(f"Database error: {str(e)}")
            return None

    def get_data_by_timerange(self, start_time, end_time, sensor="all", limit=100, format="column_data") -> (tuple | str | None):
        """
        Retrieve data from the database within a specific time range. Gets textual output as default.
        Pass format=plot_path or plot_base64 to get the data plotted.

        Args:
            start_time: datetime with timezone, start of the range
            end_time: datetime with timezone, end of the range
            sensor: Specific sensor or "all"
            limit: limit number of results. If above, makes it sparser. Set to a high number to (sort of) disable
            format: output format. Default "markdown" for text. Available: "column_data": (tuple of column_names, rows); "markdown": str; "plot_base64": BASE64 encoded image; "plot_path": str path to png image.

        Returns:
            Tuple of (column_names, rows) or str (format=markdown) or None on error
        """
        try:
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time)
            if isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time)

            # Convert datetimes to UTC for querying
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=ZoneInfo(self.local_timezone))
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=ZoneInfo(self.local_timezone))

            start_time_utc = start_time.astimezone(timezone.utc)
            end_time_utc = end_time.astimezone(timezone.utc)

            # Determine columns to fetch
            if sensor == "all":
                columns = "timestamp, temperature, humidity, pressure, CO2"
            else:
                columns = f"timestamp, {sensor}"

            # Connect and query
            with sqlite3.connect(self.db_path) as con:
                cur = con.cursor()

                query = f"""
                    SELECT {columns}
                    FROM measurements
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                    """
                params = [int(start_time_utc.timestamp()), int(end_time_utc.timestamp())]

                rows = cur.execute(query, params).fetchall()

            if not rows:
                return None

            # Skip one every two until below limit
            while len(rows) > limit:
                rows = rows[::2]

            column_data = (columns.split(", "), rows)

            if format == "markdown":
                return self._format_data_as_markdown(column_data, timestamp_idx=0)
            elif format.startswith("plot"):
                return self._generate_plot(column_data, format)
            return column_data
        except Exception as e:
            print(f"Database error: {str(e)}")
            return None

    def _generate_plot(self, column_data, format):
        """
        Generate a plot from column data.

        Args:
            column_data: Tuple of (column_names, rows)
            format: "plot_path" or "plot_base64" for output format

        Returns:
            str: Path to saved plot or base64 encoded image
        """
        try:
            column_names, rows = column_data

            # Convert rows to DataFrame for easier manipulation
            df = pd.DataFrame(rows, columns=column_names)

            # Convert timestamp column to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)

            # Convert to local timezone
            df['timestamp'] = df['timestamp'].dt.tz_convert(self.local_timezone)

            # Determine which sensors are in the data (excluding timestamp)
            selected_sensors = [col for col in column_names if col in self.sensor_plot_config and col != 'timestamp']

            if not selected_sensors:
                return "Error: No valid sensors found in the data"

            # Create plot
            num_sensors = len(selected_sensors)
            fig, axs = plt.subplots(num_sensors, 1, figsize=(12, 4 * num_sensors), sharex=True)

            # Ensure axs is always a list
            if num_sensors == 1:
                axs = [axs]

            for i, sensor in enumerate(selected_sensors):
                config = self.sensor_plot_config[sensor]
                ax = axs[i]
                ax.plot(
                    df["timestamp"],
                    df[sensor],
                    linestyle="-",
                    color=config["color"],
                    label=sensor.capitalize()
                )
                ax.set_ylabel(f"{sensor.capitalize()} ({config['unit']})")
                ax.grid(True)
                ax.legend(loc="upper left")

            # Format x-axis
            plt.xlabel(f"Time ({self.local_timezone})")
            fig.autofmt_xdate()

            # Set title
            date_range = f"{df['timestamp'].min().strftime('%Y-%m-%d')} to {df['timestamp'].max().strftime('%Y-%m-%d')}"
            plt.suptitle(f"Aranet4 {', '.join(selected_sensors)} - {date_range}")
            plt.tight_layout(rect=(0.0, 0.0, 1.0, 0.95))

            # Generate a unique filename
            unique_id = str(uuid.uuid4())[:4]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"aranet4_plot_{timestamp}_{unique_id}.png"
            filepath = os.path.join("/tmp", filename)

            # Save the figure to disk
            plt.savefig(filepath, format='png', dpi=100)
            plt.close(fig)

            if format == "plot_base64":
                import base64
                with open(filepath, "rb") as img_file:
                    return base64.b64encode(img_file.read()).decode('utf-8')
            else:  # Default to returning the file path
                return filepath
        except Exception as e:
            return f"Error generating plot: {str(e)}"


if __name__ == "__main__":
    aranet = Aranet4DB()
    
    async def f():
        return await aranet.fetch_new_data()

    import asyncio
    print(asyncio.run(f()))

    # print(aranet.get_data_by_timerange(
    #     sensor='CO2',
    #     end_time= '2025-04-06T23:59:59',
    #     start_time= '2025-04-06T00:00:00',
    #     format="plot_base64"
    # ))
