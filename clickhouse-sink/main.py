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
    def __init__(self, host, database, table, token_key=None, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(on_client_connect_success=on_client_connect_success, on_client_connect_failure=on_client_connect_failure)
        self.host = host
        self.database = database
        self.table = table
        self.token_key = token_key
        self.client = None
        self.table_created = False

    def setup(self):
        try:
            connect_kwargs = {
                'host': self.host,
                'database': self.database
            }
            
            if self.token_key:
                connect_kwargs['password'] = self.token_key
            
            self.client = clickhouse_connect.get_client(**connect_kwargs)
            
            # Test connection
            self.client.ping()
            
            if self.on_client_connect_success:
                self.on_client_connect_success()
                
        except Exception as e:
            if self.on_client_connect_failure:
                self.on_client_connect_failure(e)
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
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, panel_id)
        """
        
        self.client.command(create_table_sql)
        self.table_created = True

    def write(self, batch: SinkBatch):
        self._create_table_if_not_exists()
        
        rows = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON string in the value field
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
            
            # Convert timestamps
            timestamp_ms = data.get('timestamp', 0)
            if timestamp_ms:
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1_000_000_000)
            else:
                timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000)
            
            kafka_timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000)
            
            row = {
                'panel_id': data.get('panel_id', ''),
                'location_id': data.get('location_id', ''),
                'location_name': data.get('location_name', ''),
                'latitude': data.get('latitude', 0.0),
                'longitude': data.get('longitude', 0.0),
                'timezone': data.get('timezone', 0),
                'power_output': data.get('power_output', 0),
                'unit_power': data.get('unit_power', ''),
                'temperature': data.get('temperature', 0.0),
                'unit_temp': data.get('unit_temp', ''),
                'irradiance': data.get('irradiance', 0),
                'unit_irradiance': data.get('unit_irradiance', ''),
                'voltage': data.get('voltage', 0.0),
                'unit_voltage': data.get('unit_voltage', ''),
                'current': data.get('current', 0),
                'unit_current': data.get('unit_current', ''),
                'inverter_status': data.get('inverter_status', ''),
                'timestamp': timestamp_dt,
                'kafka_timestamp': kafka_timestamp_dt,
                'kafka_key': str(item.key) if item.key else '',
                'kafka_topic': item.topic,
                'kafka_partition': item.partition,
                'kafka_offset': item.offset
            }
            rows.append(row)
        
        if rows:
            self.client.insert(self.table, rows)

# Initialize the ClickHouse sink
clickhouse_sink = ClickHouseSink(
    host=os.environ.get('CLICKHOUSE_HOST'),
    database=os.environ.get('CLICKHOUSE_DATABASE'),
    table=os.environ.get('CLICKHOUSE_TABLE'),
    token_key=os.environ.get('CLICKHOUSE_TOKEN_KEY')
)

# Configure buffer settings with safe conversion
try:
    buffer_size = int(os.environ.get('CLICKHOUSE_BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get('CLICKHOUSE_BUFFER_TIMEOUT', '1.0'))
except ValueError:
    buffer_timeout = 1.0

app = Application(
    consumer_group=os.environ.get('CLICKHOUSE_CONSUMER_GROUP_NAME', 'clickhouse-sink'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

input_topic = app.topic(os.environ.get('CLICKHOUSE_TOPIC'))
sdf = app.dataframe(input_topic)
sdf.sink(clickhouse_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)