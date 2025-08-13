# DEPENDENCIES:
# pip install clickhouse-connect
# END_DEPENDENCIES

from quixstreams import Application
from quixstreams.sinks.base.sink import BatchingSink, SinkBatch
from quixstreams.sinks.base.exceptions import SinkBackpressureError
import clickhouse_connect
import os
import time
import json
from datetime import datetime


class ClickHouseSink(BatchingSink):
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_created = False

    def setup(self):
        # Resolve connection parameters with sensible defaults
        host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
        database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
        username = os.environ.get('CLICKHOUSE_USER', 'default')
        password = os.environ.get('CLICKHOUSE_PASSWORD', '')

        # Detect and normalize port
        port_env = os.environ.get('CLICKHOUSE_PORT', '')
        try:
            port = int(port_env) if port_env else None
        except ValueError:
            port = None

        # Determine interface: 'http' (default) or 'native'
        # - Respect CLICKHOUSE_INTERFACE if provided
        # - If port == 9000 and interface not set -> use native
        # - Otherwise default to http
        interface_env = os.environ.get('CLICKHOUSE_INTERFACE', '').strip().lower()
        if interface_env in ('http', 'native'):
            interface = interface_env
        elif port == 9000:
            interface = 'native'
        else:
            interface = 'http'

        # Default ports if none provided
        if port is None:
            port = 9000 if interface == 'native' else 8123

        try:
            self._client = clickhouse_connect.get_client(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
                interface=interface
            )

            # Test connection
            self._client.ping()
        except clickhouse_connect.driver.exceptions.DatabaseError as e:
            # Provide a clearer hint for common misconfiguration
            msg = str(e).lower()
            if 'response code 400' in msg and interface == 'http' and port == 9000:
                raise RuntimeError(
                    'ClickHouse HTTP driver cannot connect to native port 9000. '
                    'Either set CLICKHOUSE_PORT=8123 for HTTP or set CLICKHOUSE_INTERFACE=native to use port 9000.'
                ) from e
            raise

        # Create table if it doesn't exist
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        if not self._table_created:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS solar_panel_data (
                timestamp DateTime64(3),
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
                original_timestamp UInt64,
                kafka_topic String,
                kafka_partition UInt32,
                kafka_offset UInt64,
                kafka_key String
            ) ENGINE = MergeTree()
            ORDER BY (timestamp, panel_id, location_id)
            """
            self._client.command(create_table_sql)
            self._table_created = True

    def write(self, batch: SinkBatch):
        attempts_remaining = 3

        # Prepare data for insertion
        data = []
        for item in batch:
            print(f'Raw message: {item}')

            # Convert timestamp (ms) to datetime for ClickHouse DateTime64(3)
            timestamp = datetime.fromtimestamp(item.timestamp / 1000.0)

            # Handle key - convert bytes to string if needed
            key = item.key
            if isinstance(key, bytes):
                key = key.decode('utf-8', errors='ignore')
            elif key is None:
                key = ''
            else:
                key = str(key)

            # Parse the value field which contains JSON or dict
            try:
                if isinstance(item.value, dict) and 'value' in item.value:
                    solar_data = json.loads(item.value['value'])
                elif isinstance(item.value, dict):
                    solar_data = item.value
                else:
                    solar_data = json.loads(str(item.value))
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"Error parsing solar data: {e}, raw value: {item.value}")
                continue

            data.append([
                timestamp,
                solar_data.get('panel_id', ''),
                solar_data.get('location_id', ''),
                solar_data.get('location_name', ''),
                float(solar_data.get('latitude', 0.0)),
                float(solar_data.get('longitude', 0.0)),
                int(solar_data.get('timezone', 0)),
                float(solar_data.get('power_output', 0.0)),
                solar_data.get('unit_power', ''),
                float(solar_data.get('temperature', 0.0)),
                solar_data.get('unit_temp', ''),
                float(solar_data.get('irradiance', 0.0)),
                solar_data.get('unit_irradiance', ''),
                float(solar_data.get('voltage', 0.0)),
                solar_data.get('unit_voltage', ''),
                float(solar_data.get('current', 0.0)),
                solar_data.get('unit_current', ''),
                solar_data.get('inverter_status', ''),
                int(solar_data.get('timestamp', 0)),
                item.topic,
                item.partition,
                item.offset,
                key
            ])

        if not data:
            return

        while attempts_remaining:
            try:
                self._client.insert(
                    'solar_panel_data',
                    data,
                    column_names=[
                        'timestamp', 'panel_id', 'location_id', 'location_name',
                        'latitude', 'longitude', 'timezone', 'power_output', 'unit_power',
                        'temperature', 'unit_temp', 'irradiance', 'unit_irradiance',
                        'voltage', 'unit_voltage', 'current', 'unit_current',
                        'inverter_status', 'original_timestamp', 'kafka_topic',
                        'kafka_partition', 'kafka_offset', 'kafka_key'
                    ]
                )
                return
            except Exception as e:
                # Handle timeouts as backpressure
                if 'timeout' in str(e).lower() or 'timed out' in str(e).lower():
                    raise SinkBackpressureError(retry_after=30.0)
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                else:
                    raise Exception(f"Error while writing to ClickHouse: {e}") from e


def main():
    app = Application(
        consumer_group="clickhouse_sink_destination",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )

    clickhouse_sink = ClickHouseSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    sdf = sdf.apply(lambda row: row).print(metadata=True)
    sdf.sink(clickhouse_sink)

    # Demo run arguments; adjust/remove for production
    app.run(count=10, timeout=20)


if __name__ == "__main__":
    main()