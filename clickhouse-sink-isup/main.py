# DEPENDENCIES:
# pip install clickhouse-connect
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime, timezone

from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import clickhouse_connect

from dotenv import load_dotenv
load_dotenv()

class ClickHouseSink(BatchingSink):
    def __init__(self, host, database, table, token_key, consumer_group_name, buffer_size, buffer_timeout):
        super().__init__()
        self.host = host
        self.database = database
        self.table = table
        self.token_key = token_key
        self.consumer_group_name = consumer_group_name
        self.buffer_size = buffer_size
        self.buffer_timeout = buffer_timeout
        self.client = None

        # Explicit column order to match table schema
        self.columns = [
            "panel_id",
            "location_id", 
            "location_name",
            "latitude",
            "longitude",
            "timezone",
            "power_output",
            "unit_power",
            "temperature",
            "unit_temp",
            "irradiance",
            "unit_irradiance",
            "voltage",
            "unit_voltage",
            "current",
            "unit_current",
            "inverter_status",
            "timestamp",
            "kafka_timestamp",
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        ]

    def setup(self):
        self.client = clickhouse_connect.get_client(
            host=self.host,
            database=self.database,
            username=self.token_key
        )

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
            timestamp DateTime64(6),
            kafka_timestamp DateTime64(3),
            kafka_key String,
            kafka_topic String,
            kafka_partition Int32,
            kafka_offset Int64
        ) ENGINE = MergeTree()
        ORDER BY (location_id, panel_id, timestamp)
        """
        self.client.command(create_table_sql)

    def _to_datetime_from_ns(self, ts_ns):
        """
        Convert a timestamp (assumed nanoseconds since epoch) to a UTC datetime.
        Fallback to epoch if ts_ns is invalid.
        """
        if isinstance(ts_ns, (int, float)) and ts_ns > 0:
            # nanoseconds to seconds
            return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
        return datetime.fromtimestamp(0, tz=timezone.utc)

    def _to_datetime_from_ms(self, ts_ms):
        """
        Convert milliseconds since epoch to a UTC datetime.
        """
        if isinstance(ts_ms, (int, float)) and ts_ms > 0:
            return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(0, tz=timezone.utc)

    def write(self, batch: SinkBatch):
        if not self.client:
            self.setup()

        rows = []
        for item in batch:
            print(f'Raw message: {item}')

            # Parse the JSON value from the 'value' field
            if hasattr(item, 'value') and item.value is not None:
                if isinstance(item.value, (bytes, bytearray)):
                    value_str = item.value.decode("utf-8")
                elif isinstance(item.value, str):
                    value_str = item.value
                else:
                    value_str = str(item.value)
                
                try:
                    data = json.loads(value_str)
                except (json.JSONDecodeError, TypeError):
                    data = {}
            else:
                data = {}

            # Convert timestamps to datetime (ClickHouse DateTime64 expects datetime)
            # Source event timestamp is assumed to be in nanoseconds
            event_ts_ns = data.get('timestamp', 0)
            event_dt = self._to_datetime_from_ns(event_ts_ns)

            # Kafka timestamp is milliseconds since epoch
            kafka_dt = self._to_datetime_from_ms(getattr(item, 'timestamp', 0))

            row = [
                data.get('panel_id', ''),
                data.get('location_id', ''),
                data.get('location_name', ''),
                float(data.get('latitude', 0.0) or 0.0),
                float(data.get('longitude', 0.0) or 0.0),
                int(data.get('timezone', 0) or 0),
                float(data.get('power_output', 0.0) or 0.0),
                data.get('unit_power', '') or '',
                float(data.get('temperature', 0.0) or 0.0),
                data.get('unit_temp', '') or '',
                float(data.get('irradiance', 0.0) or 0.0),
                data.get('unit_irradiance', '') or '',
                float(data.get('voltage', 0.0) or 0.0),
                data.get('unit_voltage', '') or '',
                float(data.get('current', 0.0) or 0.0),
                data.get('unit_current', '') or '',
                data.get('inverter_status', '') or '',
                event_dt,               # DateTime64(6)
                kafka_dt,               # DateTime64(3)
                str(item.key) if item.key is not None else '',
                batch.topic,
                int(batch.partition),
                int(item.offset),
            ]
            rows.append(row)

        if rows:
            # Provide column names to avoid order-related issues
            self.client.insert(self.table, rows, column_names=self.columns)

# Create the ClickHouse sink
clickhouse_sink = ClickHouseSink(
    host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
    database=os.environ.get('CLICKHOUSE_DATABASE', 'default'),
    table=os.environ.get('CLICKHOUSE_TABLE', 'solar_data'),
    token_key=os.environ.get('CLICKHOUSE_TOKEN_KEY', ''),
    consumer_group_name=os.environ.get('CLICKHOUSE_CONSUMER_GROUP_NAME', 'clickhouse-sink'),
    buffer_size=int(os.environ.get('CLICKHOUSE_BUFFER_SIZE', '100')),
    buffer_timeout=float(os.environ.get('CLICKHOUSE_BUFFER_TIMEOUT', '5.0'))
)

# Create the application
app = Application(
    consumer_group=os.environ.get('CLICKHOUSE_CONSUMER_GROUP_NAME', 'clickhouse-sink'),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get('CLICKHOUSE_BUFFER_SIZE', '100')),
    commit_interval=float(os.environ.get('CLICKHOUSE_BUFFER_TIMEOUT', '5.0'))
)

# Create input topic
input_topic = app.topic(os.environ.get('CLICKHOUSE_TOPIC', 'solar-data'))

# Create streaming dataframe and sink
sdf = app.dataframe(input_topic)
sdf.sink(clickhouse_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)