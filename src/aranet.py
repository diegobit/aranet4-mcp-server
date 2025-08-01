import asyncio
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import aranet4
import matplotlib.pyplot as plt
import pandas as pd
import tzlocal

logging.basicConfig(
    format='[%(asctime)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)


class InvalidDateFormat(Exception):
    pass

class InvalidSensorType(Exception):
    pass

class Aranet4Manager:
    """Handler for Aranet4 device operations."""

    def __init__(self, device_name, device_mac, db_path, use_local_tz):
        self.sensor_plot_config = {
            "temperature": {"color": "red",    "unit": "°C" },
            "humidity":    {"color": "blue",   "unit": "%"  },
            "pressure":    {"color": "green",  "unit": "hPa"},
            "CO2":         {"color": "purple", "unit": "ppm"},
        }
        self.device_name = device_name
        self.device_mac = device_mac
        self.use_local_tz = use_local_tz
        self.db_path = db_path

    @property
    def use_local_tz(self):
        return getattr(self, '_use_local_tz', None)

    @use_local_tz.setter
    def use_local_tz(self, value: str|bool):
        if isinstance(value, str):
            value = value.strip().lower()
            if value == 'true':
                value = True
            else:
                value = False

        self._use_local_tz = value
        self.local_timezone = "UTC"
        if value is True:
            try:
                self.local_timezone = tzlocal.get_localzone_name()
            except Exception:
                pass

    @property
    def db_path(self):
        return getattr(self, '_db_path', '')

    @db_path.setter
    def db_path(self, value: str):
        self._db_path = value
        with sqlite3.connect(value) as con:
            cur = con.cursor()
            # Use IF NOT EXISTS to be idempotent
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

    def sanitize_sensors(self, sensors: str) -> list[str]:
        """Return the cleaned string of sensors and a boolean saying if they are all valid."""
        valid_options = self.sensor_plot_config.keys()

        cleaned = [s.strip().lower().replace('co2', 'CO2') for s in sensors.split(",")]

        at_least_one_unknown = any(True for s in cleaned if s not in valid_options)
        if sensors != "all" and at_least_one_unknown:
            raise InvalidSensorType(f"Invalid sensor type in '{sensors}'. Valid options are: {', '.join(valid_options)} or 'all'")
        return cleaned

    async def scan_devices(self) -> dict:
        discovered_devices = {}

        def on_device_found(advertisement):
            if advertisement.device.address not in discovered_devices:
                discovered_devices[advertisement.device.address] = advertisement
            else:
                # Update with newer data if available
                if advertisement.readings:
                    discovered_devices[advertisement.device.address] = advertisement

        scanner = aranet4.Aranet4Scanner(on_device_found)
        await scanner.start()
        await asyncio.sleep(5)  # Scan for 5 seconds
        await scanner.stop()

        return discovered_devices

    def get_database_stats(self) -> dict:
        """
        Get statistics about the Aranet4 database, including:
        - List of devices
        - Total number of measurements
        - Time range (first to last measurement dates)

        Returns:
            A dict containing the database statistics
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

            return {
                "total_measurements": count,
                "beginning_date": first_str,
                "most_recent_date": last_str,
                "devices": device_counts
            }
        except Exception as e:
            return {"error": f"Error retrieving database statistics: {str(e)}"}

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

    async def fetch_new_data(self, num_retries: int = 3, verbose: bool = False) -> str:
        """
        Fetch the data stored in the embedded Aranet4 device memory, store in the local database, and return it.

        Args:
            num_retries: Number of retry attempts if fetching fails. Default = 3
            verbose: whether to log events. To be set for the background job
        """
        if verbose:
            logging.info(f"Start fetching from {self.device_name} into db at {self.db_path}")

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
        for attempt in range(num_retries):
            entry_filter["end"] = datetime.now(timezone.utc).astimezone(ZoneInfo(self.local_timezone))
            try:
                history = await aranet4.client._all_records(self.device_mac, entry_filter, False)  # type: ignore[attr-defined]
                break
            except Exception as e:
                if verbose:
                    logging.warning(f"Failed attempt {attempt+1}, retrying. Error: {e}")
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

        fetch_msg = f"Fetched {len(data)} measurements in range: ({range_start}, {range_end}) and added to local sqlite db."

        if verbose:
            logging.info(fetch_msg)

        return (
            f"{fetch_msg}\n"
            f"\n"
            f"# Fetched data\n"
            f"{self._format_data_as_markdown(column_data, timestamp_idx=1)}\n"
        )

    def get_recent_data(self, limit=20, sensors="all", format="markdown") -> (str | tuple | None):
        """
        Retrieve recent data from the database. Gets textual output as default.
        Pass format=plot to get the data plotted as an image.

        Args:
            limit: number of measurements to get
            sensors: comma-separated sensors to retrieve (valid options: temperature, humidity, pressure, CO2), or "all"
            format: output format. Default "markdown" for text. Available: "markdown": str; "column_data": tuple (columns, rows), "plot": filepath to png image.

        Returns:
            str if format="markdown" or format="plot"; tuple of (column_names, rows) if format="column_data"
        """
        try:
            # Calculate date range
            end_time = datetime.now(timezone.utc)

            # Determine columns to select
            sensor_list = self.sanitize_sensors(sensors)
            if sensors == "all":
                columns = "timestamp, temperature, humidity, pressure, CO2"
            else:
                columns = f"timestamp, {','.join(sensor_list)}"

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
            elif format == "plot":
                return self._generate_plot(column_data)
            else:
                return column_data

        except InvalidSensorType as e:
            raise e
        except Exception as e:
            print(f"Database error: {str(e)}")
            return None

    def get_data_by_timerange(self, start_time, end_time, sensors="all", limit=1460, format="markdown") -> (str | tuple | None):
        """
        Retrieve data from the database within a specific time range. Gets textual output as default.
        Pass format=plot to get the data plotted as an image.

        Args:
            start_time: datetime with timezone, start of the range
            end_time: datetime with timezone, end of the range
            sensors: comma-separated sensors to retrieve (valid options: temperature, humidity, pressure, CO2), or "all"
            limit: limit number of results. If above, makes it sparser. Set to a high number to (sort of) disable
            format: output format. Default "markdown" for text. Available: "markdown": str; "column_data": tuple (columns, rows), "plot": filepath to png image.

        Returns:
            str if format="markdown" or format="plot"; tuple of (column_names, rows) if format="column_data"
        """
        try:
            try:
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time)
                if isinstance(end_time, str):
                    end_time = datetime.fromisoformat(end_time)
            except ValueError as e:
                raise InvalidDateFormat(f"{str(e)}. Please use ISO format (YYYY-MM-DDTHH:MM:SS)")

            # Convert datetimes to UTC for querying
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=ZoneInfo(self.local_timezone))
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=ZoneInfo(self.local_timezone))

            start_time_utc = start_time.astimezone(timezone.utc)
            end_time_utc = end_time.astimezone(timezone.utc)

            # Determine columns to select
            sensor_list = self.sanitize_sensors(sensors)
            if sensors == "all":
                columns = "timestamp, temperature, humidity, pressure, CO2"
            else:
                columns = f"timestamp, {','.join(sensor_list)}"

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
            elif format == "plot":
                return self._generate_plot(column_data)
            else:
                return column_data

        except InvalidDateFormat as e:
            raise e
        except InvalidSensorType as e:
            raise e
        except Exception as e:
            print(f"Database error: {str(e)}")
            return None

    def _generate_plot(self, column_data):
        """
        Generate a plot from column data.

        Args:
            column_data: Tuple of (column_names, rows)

        Returns:
            str: Path to saved plot or base64 encoded image
        """
        try:
            column_names, rows = column_data
            df = pd.DataFrame(rows, columns=column_names)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
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

            return filepath
        except Exception as e:
            return f"Error generating plot: {str(e)}"


if __name__ == "__main__":
    # Quick tests

    aranet4_db = Aranet4Manager(
        device_name="camera",
        device_mac="11A2FFE6-EC4D-D53D-9695-EA19DCE33F63",
        db_path="/Users/diego/Documents/aranet4.db",
        use_local_tz=True
    )

    import asyncio
    print(asyncio.run(aranet4_db.fetch_new_data()))

    print(aranet4_db.get_recent_data(format="plot"))
