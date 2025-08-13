# DEPENDENCIES:
# pip install quixstreams
# pip install clickhouse-driver
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from typing import Any, Dict, List
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
from clickhouse_driver import Client

load_dotenv()

class ClickHouseSink(BatchingSink):
    def __init__(self, 
                 host: str,
                 port: int,
                 database: str,
                 user: str,
                 password: str,
                 table_name: str = "solar_data",
                 on_client_connect_success=None,
                 on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.client = None

    def setup(self):
        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
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
            
            self.client.execute(create_table_query)
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def write(self, batch: SinkBatch):
        if not self.client:
            raise RuntimeError("ClickHouse client not initialized")
        
        rows = []
        for item in batch:
            print(f"Raw message: {item}")
            
            # Parse the value field which contains the actual data as JSON string
            if hasattr(item, 'value') and item.value:
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                elif isinstance(item.value, dict):
                    data = item.value
                else:
                    continue
                
                # Convert nanosecond timestamp to datetime
                timestamp_ns = data.get('timestamp', 0)
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1e9)
                
                # Get Kafka metadata
                kafka_timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000.0)
                
                row = (
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
                    timestamp_dt,
                    kafka_timestamp_dt,
                    batch.topic,
                    batch.partition,
                    item.offset
                )
                rows.append(row)
        
        if rows:
            try:
                insert_query = f"""
                INSERT INTO {self.table_name} (
                    panel_id, location_id, location_name, latitude, longitude, timezone,
                    power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                    voltage, unit_voltage, current, unit_current, inverter_status, timestamp,
                    kafka_timestamp, kafka_topic, kafka_partition, kafka_offset
                ) VALUES
                """
                self.client.execute(insert_query, rows)
            except Exception as e:
                print(f"Error writing to ClickHouse: {e}")
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
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1"))
)

input_topic = app.topic(os.environ.get('input'))

# Parse port with error handling
try:
    port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
except ValueError:
    port = 9000

clickhouse_sink = ClickHouseSink(
    host=os.environ.get('CLICKHOUSE_HOST'),
    port=port,
    database=os.environ.get('CLICKHOUSE_DATABASE'),
    user=os.environ.get('CLICKHOUSE_USER'),
    password=os.environ.get('CLICKHOUSE_PASSWORD')
)

sdf = app.dataframe(input_topic)
sdf.sink(clickhouse_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)