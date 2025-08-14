# DEPENDENCIES:
# pip install quixstreams
# pip install clickhouse-connect
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from typing import Dict, Any
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
import clickhouse_connect

load_dotenv()


class ClickHouseSink(BatchingSink):
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str | None,
        password: str | None,
        table_name: str = "solar_data",
        interface: str = "http",
        secure: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.interface = interface.lower().strip() if interface else "http"
        self.secure = secure
        self.client = None

    def _normalize_connection(self):
        """
        Normalize interface/port combinations to avoid HTTP/native mismatches.
        """
        http_ports = {8123, 8443}
        native_ports = {9000, 9440}

        if self.interface not in {"http", "native"}:
            print(f"Unknown CLICKHOUSE_INTERFACE={self.interface!r}; defaulting to 'http'")
            self.interface = "http"

        # Auto-correct obvious mismatches
        if self.interface == "http" and self.port in native_ports:
            print(
                f"WARNING: Using HTTP interface with native port {self.port}. "
                f"Switching to 8123 for HTTP."
            )
            self.port = 8123

        if self.interface == "native" and self.port in http_ports:
            print(
                f"WARNING: Using Native interface with HTTP port {self.port}. "
                f"Switching to 9000 for Native."
            )
            self.port = 9000

        # If port not provided, choose defaults
        if not self.port:
            self.port = 8123 if self.interface == "http" else 9000

    def setup(self):
        """Connect to ClickHouse and create table if needed"""
        try:
            self._normalize_connection()

            # Build client with the correct interface and optional TLS
            self.client = clickhouse_connect.get_client(
                interface=self.interface,
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.user,
                password=self.password,
                secure=self.secure if self.interface == "http" else False,
            )

            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                panel_id String,
                location_id String,
                location_name String,
                latitude Float64,
                longitude Float64,
                timezone Int32,
                power_output Float64,
                unit_power String,
                temperature Float64,
                unit_temp String,
                irradiance Float64,
                unit_irradiance String,
                voltage Float64,
                unit_voltage String,
                current Float64,
                unit_current String,
                inverter_status String,
                timestamp DateTime64(3),
                kafka_timestamp DateTime64(3),
                kafka_topic String,
                kafka_partition Int32,
                kafka_offset Int64
            ) ENGINE = MergeTree()
            ORDER BY (timestamp, panel_id)
            """
            self.client.command(create_table_query)
            print(f"Table {self.table_name} is ready")

        except Exception as e:
            # Add a clearer hint when we see HTTP 400s, which usually indicate wrong port/protocol
            msg = str(e)
            if "response code 400" in msg or "Bad Request" in msg:
                print(
                    "Connection failed with HTTP 400. This typically means the HTTP driver "
                    "is connecting to the native port (e.g., 9000). Verify CLICKHOUSE_INTERFACE "
                    "('http' or 'native') and CLICKHOUSE_PORT (8123 for http, 9000 for native)."
                )
            raise

    def write(self, batch: SinkBatch):
        """Write batch to ClickHouse"""
        if not self.client:
            raise RuntimeError("ClickHouse client not initialized")

        rows = []
        columns = [
            'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
            'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
            'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current',
            'unit_current', 'inverter_status', 'timestamp', 'kafka_timestamp',
            'kafka_topic', 'kafka_partition', 'kafka_offset'
        ]

        for item in batch:
            print(f"Raw message: {item}")

            # Parse value field which may be a JSON string or a dict
            if hasattr(item, 'value') and isinstance(item.value, str):
                try:
                    data = json.loads(item.value)
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON from value: {item.value}")
                    continue
            elif hasattr(item, 'value') and isinstance(item.value, dict):
                data = item.value
            else:
                print(f"Unexpected message structure: {item}")
                continue

            # Convert nanosecond timestamp to datetime
            if 'timestamp' in data and data['timestamp'] is not None:
                ts_seconds = data['timestamp'] / 1_000_000_000
                ts_datetime = datetime.fromtimestamp(ts_seconds)
            else:
                ts_datetime = datetime.now()

            # Convert Kafka timestamp (milliseconds) to datetime
            kafka_ts = datetime.fromtimestamp(item.timestamp / 1000)

            row = [
                data.get('panel_id', ''),
                data.get('location_id', ''),
                data.get('location_name', ''),
                float(data.get('latitude', 0.0)),
                float(data.get('longitude', 0.0)),
                int(data.get('timezone', 0)),
                float(data.get('power_output', 0.0)),
                data.get('unit_power', ''),
                float(data.get('temperature', 0.0)),
                data.get('unit_temp', ''),
                float(data.get('irradiance', 0.0)),
                data.get('unit_irradiance', ''),
                float(data.get('voltage', 0.0)),
                data.get('unit_voltage', ''),
                float(data.get('current', 0.0)),
                data.get('unit_current', ''),
                data.get('inverter_status', ''),
                ts_datetime,
                kafka_ts,
                batch.topic,
                batch.partition,
                item.offset
            ]
            rows.append(row)

        if rows:
            try:
                self.client.insert(self.table_name, rows, column_names=columns)
                print(f"Successfully wrote {len(rows)} records to ClickHouse")
            except Exception as e:
                print(f"Error writing to ClickHouse: {e}")
                # Use backpressure to retry
                raise SinkBackpressureError(
                    retry_after=5.0,
                    topic=batch.topic,
                    partition=batch.partition
                )


# Main application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "clickhouse-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_TIMEOUT", "1.0"))
)

# Get environment variables
input_topic_name = os.environ.get("input")

clickhouse_host = os.environ.get("CLICKHOUSE_HOST")
clickhouse_user = os.environ.get("CLICKHOUSE_USER")
# Support either CLICKHOUSE_PASSWORD or CLICKHOUSE_TOKEN_KEY
clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD") or os.environ.get("CLICKHOUSE_TOKEN_KEY")
clickhouse_database = os.environ.get("CLICKHOUSE_DATABASE")

# Interface and security
clickhouse_interface = os.environ.get("CLICKHOUSE_INTERFACE", "http").lower().strip()
use_tls = os.environ.get("CLICKHOUSE_SECURE", "false").lower() in {"1", "true", "yes"}

# Port handling
try:
    clickhouse_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
except ValueError:
    clickhouse_port = 8123 if clickhouse_interface == "http" else 9000

# Create sink
clickhouse_sink = ClickHouseSink(
    host=clickhouse_host,
    port=clickhouse_port,
    database=clickhouse_database,
    user=clickhouse_user,
    password=clickhouse_password,
    interface=clickhouse_interface,
    secure=use_tls
)

# Create topic and dataframe
input_topic = app.topic(input_topic_name)
sdf = app.dataframe(input_topic)

# Apply sink
sdf.sink(clickhouse_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)