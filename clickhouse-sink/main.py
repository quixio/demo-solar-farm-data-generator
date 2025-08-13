# Cached sandbox code for clickhouse
# Generated on 2025-08-13 18:39:47
# Template: Unknown
# This is cached code - delete this file to force regeneration

# DEPENDENCIES:
# pip install clickhouse-driver
# END_DEPENDENCIES

import os
import json
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
from clickhouse_driver import Client
from datetime import datetime

class ClickHouseSink(BatchingSink):
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None, **kwargs):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
            **kwargs
        )
        self._client = None
        self._table_created = False

    def setup(self):
        try:
            port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
        except ValueError:
            port = 9000

        self._client = Client(
            host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
            port=port,
            database=os.environ.get('CLICKHOUSE_DATABASE', 'default'),
            user=os.environ.get('CLICKHOUSE_USER', 'default'),
            password=os.environ.get('CLICKHOUSE_PASSWORD', '')
        )

        # Test connection
        self._client.execute('SELECT 1')

        # Call the correct success hook used by the base class
        if callable(self._on_client_connect_success):
            self._on_client_connect_success()

    def _create_table_if_not_exists(self):
        if self._table_created:
            return

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS solar_data (
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

        self._client.execute(create_table_sql)
        self._table_created = True

    def write(self, batch: SinkBatch):
        if not self._table_created:
            self._create_table_if_not_exists()

        rows = []
        for item in batch:
            print(f'Raw message: {item}')

            # Parse the JSON value string from the nested structure
            try:
                # The message has a nested structure with 'value' field containing JSON string
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                elif hasattr(item, 'value') and isinstance(item.value, dict):
                    # Check if it's the nested structure from the schema
                    if 'value' in item.value and isinstance(item.value['value'], str):
                        data = json.loads(item.value['value'])
                    else:
                        data = item.value
                else:
                    data = item.value
            except (json.JSONDecodeError, TypeError) as e:
                print(f"Error parsing message value: {e}")
                continue

            # Convert epoch timestamp (assumed nanoseconds) to datetime
            timestamp_ns = data.get('timestamp', 0)
            timestamp_ms = timestamp_ns // 1_000_000  # ns -> ms
            timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000.0)

            # Convert Kafka timestamp (ms) to datetime
            kafka_timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000.0)

            row = (
                data.get('panel_id', ''),
                data.get('location_id', ''),
                data.get('location_name', ''),
                float(data.get('latitude', 0.0) or 0.0),
                float(data.get('longitude', 0.0) or 0.0),
                int(data.get('timezone', 0) or 0),
                float(data.get('power_output', 0.0) or 0.0),
                data.get('unit_power', ''),
                float(data.get('temperature', 0.0) or 0.0),
                data.get('unit_temp', ''),
                float(data.get('irradiance', 0.0) or 0.0),
                data.get('unit_irradiance', ''),
                float(data.get('voltage', 0.0) or 0.0),
                data.get('unit_voltage', ''),
                float(data.get('current', 0.0) or 0.0),
                data.get('unit_current', ''),
                data.get('inverter_status', ''),
                timestamp_dt,
                str(item.key) if item.key is not None else '',
                kafka_timestamp_dt,
                item.topic,
                item.partition,
                item.offset
            )
            rows.append(row)

        if rows:
            try:
                insert_sql = """
                INSERT INTO solar_data (
                    panel_id, location_id, location_name, latitude, longitude, timezone,
                    power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                    voltage, unit_voltage, current, unit_current, inverter_status, timestamp,
                    kafka_key, kafka_timestamp, kafka_topic, kafka_partition, kafka_offset
                ) VALUES
                """
                self._client.execute(insert_sql, rows)
            except Exception as e:
                print(f"Error writing to ClickHouse: {e}")
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )

def main():
    app = Application(
        consumer_group="clickhouse_sink_consumer",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )

    clickhouse_sink = ClickHouseSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    sdf = sdf.apply(lambda row: row).print(metadata=True)
    sdf.sink(clickhouse_sink)

    app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()