# DEPENDENCIES:
# pip install clickhouse-connect
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import clickhouse_connect

from dotenv import load_dotenv
load_dotenv()

class ClickHouseSink(BatchingSink):
    def __init__(self, host, database, table, username, password):
        super().__init__()
        self.host = host
        self.database = database
        self.table = table
        self.username = username
        self.password = password
        self.client = None

    def setup(self):
        self.client = clickhouse_connect.get_client(
            host=self.host,
            database=self.database,
            username=self.username,
            password=self.password
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

    def write(self, batch: SinkBatch):
        if not self.client:
            self.setup()
            
        rows = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON value
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
            
            # Convert epoch timestamp to datetime
            timestamp_ms = data.get('timestamp', 0) / 1000000  # Convert from nanoseconds to seconds
            kafka_timestamp_ms = item.timestamp / 1000  # Convert from milliseconds to seconds
            
            row = [
                data.get('panel_id', ''),
                data.get('location_id', ''),
                data.get('location_name', ''),
                data.get('latitude', 0.0),
                data.get('longitude', 0.0),
                data.get('timezone', 0),
                data.get('power_output', 0.0),
                data.get('unit_power', ''),
                data.get('temperature', 0.0),
                data.get('unit_temp', ''),
                data.get('irradiance', 0.0),
                data.get('unit_irradiance', ''),
                data.get('voltage', 0.0),
                data.get('unit_voltage', ''),
                data.get('current', 0.0),
                data.get('unit_current', ''),
                data.get('inverter_status', ''),
                timestamp_ms,
                kafka_timestamp_ms,
                str(item.key) if item.key else '',
                batch.topic,
                batch.partition,
                item.offset
            ]
            rows.append(row)
        
        if rows:
            self.client.insert(self.table, rows)

# Create the ClickHouse sink
clickhouse_sink = ClickHouseSink(
    host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
    database=os.environ.get('CLICKHOUSE_DATABASE', 'default'),
    table=os.environ.get('CLICKHOUSE_TABLE', 'solar_data'),
    username="clickadmin",
    password="clickpass"
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