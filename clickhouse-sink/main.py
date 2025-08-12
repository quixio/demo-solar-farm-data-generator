# DEPENDENCIES:
# pip install clickhouse-connect
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import clickhouse_connect
from dotenv import load_dotenv
load_dotenv()


class ClickHouseSink(BatchingSink):
    def __init__(
        self,
        host: str,
        database: str,
        table: str,
        username: str | None = None,
        password: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.host = host
        self.database = database
        self.table = table
        self.username = username
        self.password = password
        self.client = None
        self.table_created = False

    def setup(self):
        try:
            connect_kwargs = {
                "host": self.host,
                "database": self.database,
            }
            if self.username:
                connect_kwargs["username"] = self.username
            if self.password:
                connect_kwargs["password"] = self.password

            self.client = clickhouse_connect.get_client(**connect_kwargs)
            self.client.ping()

            if hasattr(self, "_on_client_connect_success") and self._on_client_connect_success:
                self._on_client_connect_success()

        except Exception as e:
            if hasattr(self, "_on_client_connect_failure") and self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        if self.table_created:
            return
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            panel_id String,
            location_id String,
            location_name String,
            latitude Float64,
            longitude Float64,
            timezone Int32,
            power_output Int32,
            unit_power String,
            temperature Float64,
            unit_temp String,
            irradiance Int32,
            unit_irradiance String,
            voltage Float64,
            unit_voltage String,
            current Int32,
            unit_current String,
            inverter_status String,
            timestamp DateTime64(3),
            kafka_timestamp DateTime64(3),
            kafka_key String,
            kafka_topic String,
            kafka_partition Int32,
            kafka_offset Int64
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, panel_id)
        """
        self.client.command(create_table_sql)
        self.table_created = True

    def write(self, batch: SinkBatch):
        self._create_table_if_not_exists()

        rows = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the value field which contains JSON string
            if isinstance(item.value, str):
                payload = json.loads(item.value)
            else:
                payload = item.value

            timestamp_ns = payload.get("timestamp") or 0
            event_dt = (
                datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                if timestamp_ns
                else datetime.fromtimestamp(item.timestamp / 1000)
            )
            kafka_dt = datetime.fromtimestamp(item.timestamp / 1000)

            rows.append(
                {
                    "panel_id": payload.get("panel_id", ""),
                    "location_id": payload.get("location_id", ""),
                    "location_name": payload.get("location_name", ""),
                    "latitude": payload.get("latitude", 0.0),
                    "longitude": payload.get("longitude", 0.0),
                    "timezone": payload.get("timezone", 0),
                    "power_output": payload.get("power_output", 0),
                    "unit_power": payload.get("unit_power", ""),
                    "temperature": payload.get("temperature", 0.0),
                    "unit_temp": payload.get("unit_temp", ""),
                    "irradiance": payload.get("irradiance", 0),
                    "unit_irradiance": payload.get("unit_irradiance", ""),
                    "voltage": payload.get("voltage", 0.0),
                    "unit_voltage": payload.get("unit_voltage", ""),
                    "current": payload.get("current", 0),
                    "unit_current": payload.get("unit_current", ""),
                    "inverter_status": payload.get("inverter_status", ""),
                    "timestamp": event_dt,
                    "kafka_timestamp": kafka_dt,
                    "kafka_key": str(item.key) if item.key else "",
                    "kafka_topic": item.topic,
                    "kafka_partition": item.partition,
                    "kafka_offset": item.offset,
                }
            )

        if rows:
            self.client.insert(self.table, rows)


CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'default')
CLICKHOUSE_TABLE = os.environ.get('CLICKHOUSE_TABLE', 'solar_readings')
CLICKHOUSE_USERNAME = os.environ.get('CLICKHOUSE_USERNAME', 'default')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')  # Changed from CLICKHOUSE_TOKEN_KEY to CLICKHOUSE_PASSWORD

try:
    BUFFER_SIZE = int(os.environ.get('CLICKHOUSE_BUFFER_SIZE', '1000'))
except ValueError:
    BUFFER_SIZE = 1000

try:
    BUFFER_TIMEOUT = float(os.environ.get('CLICKHOUSE_BUFFER_TIMEOUT', '1.0'))
except ValueError:
    BUFFER_TIMEOUT = 1.0

CONSUMER_GROUP = os.environ.get('CLICKHOUSE_CONSUMER_GROUP_NAME', 'clickhouse-sink')
SOURCE_TOPIC = os.environ.get('CLICKHOUSE_TOPIC')

sink = ClickHouseSink(
    host=CLICKHOUSE_HOST,
    database=CLICKHOUSE_DATABASE,
    table=CLICKHOUSE_TABLE,
    username=CLICKHOUSE_USERNAME,
    password=CLICKHOUSE_PASSWORD,  # Use the correct environment variable for password
)

app = Application(
    consumer_group=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    commit_every=BUFFER_SIZE,
    commit_interval=BUFFER_TIMEOUT,
)

input_topic = app.topic(SOURCE_TOPIC)
sdf = app.dataframe(input_topic)
sdf.sink(sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)
