# DEPENDENCIES:
# pip install clickhouse-driver
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from clickhouse_driver import Client
from dotenv import load_dotenv

load_dotenv()

def get_env(*keys, default=None, required=False):
    """
    Returns the first non-empty environment variable among keys.
    If required and none found, raises ValueError.
    """
    for k in keys:
        val = os.environ.get(k)
        if val is not None and str(val).strip() != "":
            return val
    if required:
        raise ValueError(f"Missing required environment variable(s): {', '.join(keys)}")
    return default

def to_bool(val, default=False):
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")

class ClickHouseSink(BatchingSink):
    def __init__(self, host, database, table, username, password, token_key=None, port=None, secure=False):
        super().__init__()
        self.host = host
        self.database = database or "default"
        self.table = table
        # Ensure username/password are strings (not None)
        self.username = username or "default"
        self.password = password or ""
        self.token_key = token_key
        self.port = int(port) if port else None
        self.secure = bool(secure)
        self.client = None

    def setup(self):
        # Validate critical params before connecting
        if not self.host:
            raise ValueError("CLICKHOUSE_HOST is required and cannot be empty")
        if not self.table:
            raise ValueError("CLICKHOUSE_TABLE is required and cannot be empty")

        try:
            client_kwargs = {
                "host": str(self.host),
                "database": str(self.database),
                "user": str(self.username),
                "password": str(self.password),
            }
            if self.port:
                client_kwargs["port"] = int(self.port)
            if self.secure:
                client_kwargs["secure"] = True

            self.client = Client(**client_kwargs)

            # Test connection
            self.client.execute("SELECT 1")

            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
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
                kafka_key String,
                kafka_timestamp DateTime64(3),
                kafka_topic String,
                kafka_partition Int32,
                kafka_offset Int64
            ) ENGINE = MergeTree()
            ORDER BY (location_id, panel_id, timestamp)
            """
            self.client.execute(create_table_sql)

        except Exception as e:
            print(f"Failed to setup ClickHouse connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        if not self.client:
            raise RuntimeError("ClickHouse client not initialized")

        rows = []
        for item in batch:
            try:
                print(f"Raw message: {item}")
                
                # Parse the JSON value from the 'value' field
                if hasattr(item, 'value') and item.value is not None:
                    if isinstance(item.value, bytes):
                        data = json.loads(item.value.decode("utf-8"))
                    elif isinstance(item.value, str):
                        data = json.loads(item.value)
                    else:
                        data = item.value or {}
                else:
                    data = {}

                # Convert epoch timestamp to datetime
                timestamp_epoch = data.get("timestamp", 0)
                if timestamp_epoch:
                    # Convert from nanoseconds to seconds
                    timestamp_dt = datetime.fromtimestamp(timestamp_epoch / 1_000_000_000)
                else:
                    timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000)

                # Convert Kafka timestamp to datetime
                kafka_timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000)

                row = (
                    data.get("panel_id", ""),
                    data.get("location_id", ""),
                    data.get("location_name", ""),
                    float(data.get("latitude", 0.0) or 0.0),
                    float(data.get("longitude", 0.0) or 0.0),
                    int(data.get("timezone", 0) or 0),
                    float(data.get("power_output", 0.0) or 0.0),
                    data.get("unit_power", ""),
                    float(data.get("temperature", 0.0) or 0.0),
                    data.get("unit_temp", ""),
                    float(data.get("irradiance", 0.0) or 0.0),
                    data.get("unit_irradiance", ""),
                    float(data.get("voltage", 0.0) or 0.0),
                    data.get("unit_voltage", ""),
                    float(data.get("current", 0.0) or 0.0),
                    data.get("unit_current", ""),
                    data.get("inverter_status", ""),
                    timestamp_dt,
                    str(item.key) if item.key is not None else "",
                    kafka_timestamp_dt,
                    str(item.topic),
                    int(item.partition),
                    int(item.offset),
                )
                rows.append(row)

            except Exception as e:
                print(f"Error processing item: {e}")
                continue

        if rows:
            insert_sql = f"""
            INSERT INTO {self.table} (
                panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status, timestamp,
                kafka_key, kafka_timestamp, kafka_topic, kafka_partition, kafka_offset
            ) VALUES
            """
            self.client.execute(insert_sql, rows)

# Read configuration with validation and sensible defaults
host = get_env("CLICKHOUSE_HOST", required=True)
database = get_env("CLICKHOUSE_DATABASE", default="default")
table = get_env("CLICKHOUSE_TABLE", required=True)
username = get_env("CLICKHOUSE_USERNAME", "CLICKHOUSE_USER", default="default")
password = get_env("CLICKHOUSE_PASSWORD", default="")
token_key = get_env("CLICKHOUSE_TOKEN_KEY", default=None)
port = get_env("CLICKHOUSE_PORT", default=None)
secure = to_bool(get_env("CLICKHOUSE_SECURE", default="false"))

# Initialize the ClickHouse sink
clickhouse_sink = ClickHouseSink(
    host=host,
    database=database,
    table=table,
    username=username,
    password=password,
    token_key=token_key,
    port=port,
    secure=secure,
)

# Initialize the Quix Streams application
try:
    buffer_size = int(os.environ.get("CLICKHOUSE_BUFFER_SIZE", "1000"))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get("CLICKHOUSE_BUFFER_TIMEOUT", "1.0"))
except ValueError:
    buffer_timeout = 1.0

consumer_group = os.environ.get("CLICKHOUSE_CONSUMER_GROUP_NAME", "clickhouse-sink")
app = Application(
    consumer_group=consumer_group,
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

topic_name = get_env("CLICKHOUSE_TOPIC", required=True)
input_topic = app.topic(topic_name)
sdf = app.dataframe(input_topic)

sdf.sink(clickhouse_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)