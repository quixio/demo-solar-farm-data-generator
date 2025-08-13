# DEPENDENCIES:
# pip install clickhouse-connect>=0.7.0
# END_DEPENDENCIES

import os
import json
import time
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
import clickhouse_connect
from clickhouse_connect.driver.exceptions import ProgrammingError


def strtobool(val: str) -> bool:
    return str(val).strip().lower() in ("1", "true", "yes", "y")


class ClickHouseSink(BatchingSink):
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_created = False

    def setup(self):
        host = os.environ.get("CLICKHOUSE_HOST", "localhost")

        # Only use 'native' if explicitly requested; default to HTTP for compatibility
        env_interface = os.environ.get("CLICKHOUSE_INTERFACE", "").strip().lower()
        interface = "native" if env_interface == "native" else "http"

        # Ports default per interface unless explicitly set
        env_port = os.environ.get("CLICKHOUSE_PORT")
        if env_port:
            try:
                port = int(env_port)
            except ValueError:
                port = 9000 if interface == "native" else 8123
        else:
            port = 9000 if interface == "native" else 8123

        secure = strtobool(os.environ.get("CLICKHOUSE_SECURE", "false"))
        verify = strtobool(os.environ.get("CLICKHOUSE_VERIFY_TLS", "true"))

        database = os.environ.get("CLICKHOUSE_DATABASE")
        username = os.environ.get("CLICKHOUSE_USER")
        password = os.environ.get("CLICKHOUSE_PASSWORD")

        client_kwargs = {
            "host": host,
            "port": port,
            "database": database,
            "username": username,
            "password": password,
            "interface": interface,
        }

        if interface == "http":
            client_kwargs["secure"] = secure
            client_kwargs["verify"] = verify

        # Try to create client; if native is not recognized, fall back to HTTP
        try:
            self._client = clickhouse_connect.get_client(**client_kwargs)
        except ProgrammingError as e:
            msg = str(e).lower()
            if interface == "native" and "unrecognized client type native" in msg:
                # Fallback to HTTP
                fallback_port_env = os.environ.get("CLICKHOUSE_HTTP_PORT")
                try:
                    fallback_port = int(fallback_port_env) if fallback_port_env else 8123
                except ValueError:
                    fallback_port = 8123

                client_kwargs.update({
                    "interface": "http",
                    "port": fallback_port,
                    "secure": secure,
                    "verify": verify
                })
                self._client = clickhouse_connect.get_client(**client_kwargs)
            else:
                # A different ProgrammingError – re-raise
                raise

        self._client.ping()
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        if self._table_created:
            return

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS solar_panel_data (
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
            kafka_key String,
            kafka_topic String,
            kafka_partition Int32,
            kafka_offset Int64
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, timestamp)
        """

        self._client.command(create_table_sql)
        self._table_created = True

    def write(self, batch: SinkBatch):
        attempts_remaining = 3

        while attempts_remaining:
            try:
                data_rows = []

                for item in batch:
                    print(f'Raw message: {item}')

                    # Parse the nested value field from the message structure
                    if hasattr(item, 'value') and isinstance(item.value, str):
                        # The value field contains a JSON string that needs to be parsed
                        value_data = json.loads(item.value)
                    elif hasattr(item, 'value') and isinstance(item.value, dict):
                        # Check if this is the outer message structure with nested value
                        if 'value' in item.value and isinstance(item.value['value'], str):
                            value_data = json.loads(item.value['value'])
                        else:
                            value_data = item.value
                    else:
                        # Fallback - treat as direct data
                        value_data = item.value if hasattr(item, 'value') else {}

                    panel_ts_ns = value_data.get("timestamp", 0)
                    panel_timestamp = datetime.fromtimestamp(panel_ts_ns / 1_000_000_000)

                    kafka_timestamp = datetime.fromtimestamp(item.timestamp / 1000) if item.timestamp else None

                    row = [
                        value_data.get("panel_id", ""),
                        value_data.get("location_id", ""),
                        value_data.get("location_name", ""),
                        float(value_data.get("latitude", 0.0) or 0.0),
                        float(value_data.get("longitude", 0.0) or 0.0),
                        int(value_data.get("timezone", 0) or 0),
                        float(value_data.get("power_output", 0.0) or 0.0),
                        value_data.get("unit_power", ""),
                        float(value_data.get("temperature", 0.0) or 0.0),
                        value_data.get("unit_temp", ""),
                        float(value_data.get("irradiance", 0.0) or 0.0),
                        value_data.get("unit_irradiance", ""),
                        float(value_data.get("voltage", 0.0) or 0.0),
                        value_data.get("unit_voltage", ""),
                        float(value_data.get("current", 0.0) or 0.0),
                        value_data.get("unit_current", ""),
                        value_data.get("inverter_status", ""),
                        panel_timestamp,
                        kafka_timestamp,
                        str(item.key) if item.key is not None else "",
                        item.topic,
                        int(item.partition) if item.partition is not None else 0,
                        int(item.offset) if item.offset is not None else 0,
                    ]
                    data_rows.append(row)

                self._client.insert(
                    "solar_panel_data",
                    data_rows,
                    column_names=[
                        "panel_id", "location_id", "location_name", "latitude", "longitude",
                        "timezone", "power_output", "unit_power", "temperature", "unit_temp",
                        "irradiance", "unit_irradiance", "voltage", "unit_voltage", "current",
                        "unit_current", "inverter_status", "timestamp", "kafka_timestamp",
                        "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset"
                    ],
                )
                return

            except ConnectionError:
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except TimeoutError:
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )

        raise Exception("Error while writing to ClickHouse")


def main():
    app = Application(
        consumer_group="clickhouse_sink_consumer",
        auto_create_topics=True,
        auto_offset_reset="earliest",
    )

    clickhouse_sink = ClickHouseSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    sdf = sdf.apply(lambda row: row).print(metadata=True)
    sdf.sink(clickhouse_sink)

    app.run(count=10, timeout=20)


if __name__ == "__main__":
    main()