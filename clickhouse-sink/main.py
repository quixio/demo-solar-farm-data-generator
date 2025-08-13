# DEPENDENCIES:
# pip install clickhouse-connect
# END_DEPENDENCIES

import os
import json
import time
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
import clickhouse_connect

class ClickHouseSink(BatchingSink):
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_created = False

    def setup(self):
        try:
            port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
        except ValueError:
            port = 8123
            
        self._client = clickhouse_connect.get_client(
            host=os.environ.get('CLICKHOUSE_HOST'),
            port=port,
            database=os.environ.get('CLICKHOUSE_DATABASE'),
            username=os.environ.get('CLICKHOUSE_USER'),
            password=os.environ.get('CLICKHOUSE_PASSWORD')
        )
        
        # Test connection
        self._client.ping()
        
        # Create table if it doesn't exist
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
                    
                    # Parse the JSON value field
                    if isinstance(item.value, str):
                        value_data = json.loads(item.value)
                    else:
                        value_data = item.value
                    
                    # Convert epoch timestamp to datetime
                    panel_timestamp = datetime.fromtimestamp(value_data.get('timestamp', 0) / 1000000000)
                    kafka_timestamp = datetime.fromtimestamp(item.timestamp / 1000)
                    
                    # Map message data to table schema
                    row = [
                        value_data.get('panel_id', ''),
                        value_data.get('location_id', ''),
                        value_data.get('location_name', ''),
                        value_data.get('latitude', 0.0),
                        value_data.get('longitude', 0.0),
                        value_data.get('timezone', 0),
                        value_data.get('power_output', 0.0),
                        value_data.get('unit_power', ''),
                        value_data.get('temperature', 0.0),
                        value_data.get('unit_temp', ''),
                        value_data.get('irradiance', 0.0),
                        value_data.get('unit_irradiance', ''),
                        value_data.get('voltage', 0.0),
                        value_data.get('unit_voltage', ''),
                        value_data.get('current', 0.0),
                        value_data.get('unit_current', ''),
                        value_data.get('inverter_status', ''),
                        panel_timestamp,
                        kafka_timestamp,
                        str(item.key) if item.key else '',
                        item.topic,
                        item.partition,
                        item.offset
                    ]
                    data_rows.append(row)
                
                # Insert data into ClickHouse
                self._client.insert(
                    'solar_panel_data',
                    data_rows,
                    column_names=[
                        'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
                        'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
                        'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current',
                        'unit_current', 'inverter_status', 'timestamp', 'kafka_timestamp',
                        'kafka_key', 'kafka_topic', 'kafka_partition', 'kafka_offset'
                    ]
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