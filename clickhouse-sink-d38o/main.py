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
    def __init__(self, host, token, database, table, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(on_client_connect_success=on_client_connect_success, on_client_connect_failure=on_client_connect_failure)
        self.host = host
        self.token = token
        self.database = database
        self.table = table
        self.client = None
        
    def setup(self):
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                username='default',
                password=self.token,
                database=self.database,
                secure=True
            )
            
            # Test connection
            self.client.ping()
            
            # Create table if it doesn't exist
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
            ORDER BY (panel_id, timestamp)
            """
            
            self.client.command(create_table_sql)
            
            if self.on_client_connect_success:
                self.on_client_connect_success()
                
        except Exception as e:
            if self.on_client_connect_failure:
                self.on_client_connect_failure(e)
            raise
    
    def write(self, batch: SinkBatch):
        if not self.client:
            raise RuntimeError("ClickHouse client not initialized")
            
        rows = []
        for item in batch:
            try:
                # Parse the value field which contains JSON string
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert epoch nanoseconds to datetime
                timestamp_ns = data.get('timestamp', 0)
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                
                # Convert kafka timestamp to datetime
                kafka_timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000)
                
                row = [
                    data.get('panel_id', ''),
                    data.get('location_id', ''),
                    data.get('location_name', ''),
                    data.get('latitude', 0.0),
                    data.get('longitude', 0.0),
                    data.get('timezone', 0),
                    data.get('power_output', 0),
                    data.get('unit_power', ''),
                    data.get('temperature', 0.0),
                    data.get('unit_temp', ''),
                    data.get('irradiance', 0),
                    data.get('unit_irradiance', ''),
                    data.get('voltage', 0.0),
                    data.get('unit_voltage', ''),
                    data.get('current', 0),
                    data.get('unit_current', ''),
                    data.get('inverter_status', ''),
                    timestamp_dt,
                    kafka_timestamp_dt,
                    str(item.key) if item.key else '',
                    batch.topic,
                    batch.partition,
                    item.offset
                ]
                rows.append(row)
                
            except Exception as e:
                print(f"Error processing item: {e}")
                continue
        
        if rows:
            try:
                self.client.insert(self.table, rows)
            except Exception as e:
                print(f"Error inserting to ClickHouse: {e}")
                raise

# Initialize application
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

# Create ClickHouse sink
clickhouse_sink = ClickHouseSink(
    host=os.environ.get('CLICKHOUSE_HOST'),
    token=os.environ.get('CLICKHOUSE_TOKEN_KEY'),
    database=os.environ.get('CLICKHOUSE_DATABASE'),
    table=os.environ.get('CLICKHOUSE_TABLE')
)

sdf = app.dataframe(input_topic)

# Debug: Print raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

sdf.sink(clickhouse_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)