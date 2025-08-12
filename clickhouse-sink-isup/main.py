# DEPENDENCIES:
# pip install clickhouse-driver
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from clickhouse_driver import Client
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
        try:
            self.client = Client(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password
            )
            
            # Test connection
            self.client.execute('SELECT 1')
            
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
                timestamp DateTime64(3),
                kafka_key String,
                kafka_timestamp DateTime64(3),
                kafka_topic String,
                kafka_partition Int32,
                kafka_offset Int64
            ) ENGINE = MergeTree()
            ORDER BY (location_id, panel_id, timestamp)
            """
            
            self.client.execute(create_table_sql)
            
        except Exception as e:
            print(f"Failed to setup ClickHouse connection: {e}")
            raise
    
    def write(self, batch: SinkBatch):
        if not self.client:
            raise RuntimeError("ClickHouse client not initialized")
            
        rows = []
        for item in batch:
            try:
                # Parse the JSON value
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert epoch timestamp to datetime
                timestamp_epoch = data.get('timestamp', 0)
                if timestamp_epoch:
                    # Convert from nanoseconds to seconds
                    timestamp_dt = datetime.fromtimestamp(timestamp_epoch / 1_000_000_000)
                else:
                    timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000)
                
                # Convert Kafka timestamp to datetime
                kafka_timestamp_dt = datetime.fromtimestamp(item.timestamp / 1000)
                
                row = (
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
                    timestamp_dt,
                    str(item.key) if item.key else '',
                    kafka_timestamp_dt,
                    item.topic,
                    item.partition,
                    item.offset
                )
                rows.append(row)
                
            except Exception as e:
                print(f"Error processing item: {e}")
                continue
        
        if rows:
            insert_sql = f"""
            INSERT INTO {self.table} (
                panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status, timestamp,
                kafka_key, kafka_timestamp, kafka_topic, kafka_partition, kafka_offset
            ) VALUES
            """
            
            self.client.execute(insert_sql, rows)

# Initialize the ClickHouse sink
clickhouse_sink = ClickHouseSink(
    host=os.environ.get('CLICKHOUSE_HOST'),
    database=os.environ.get('CLICKHOUSE_DATABASE'),
    table=os.environ.get('CLICKHOUSE_TABLE'),
    username=os.environ.get('CLICKHOUSE_USERNAME'),
    password=os.environ.get('CLICKHOUSE_PASSWORD')
)

# Initialize the Quix Streams application
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

# Add debug print to see raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

sdf.sink(clickhouse_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)