Looking at the schema analysis and requirements, I need to create a QuestDB sink for solar panel data. The data comes as JSON strings in the `value` field that need to be parsed. I'll implement a custom BatchingSink since QuestDB doesn't have a built-in sink in Quix Streams.

# DEPENDENCIES:
# pip install quixstreams
# pip install python-dotenv
# pip install psycopg2-binary
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import psycopg2
from psycopg2.extras import execute_values

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, username, password, database, table_name, 
                 on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._table_name = table_name
        self._connection = None
        self._table_created = False

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                user=self._username,
                password=self._password,
                database=self._database
            )
            self._connection.autocommit = True
            
            # Test the connection
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        if self._table_created:
            return
            
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            panel_id STRING,
            location_id STRING,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output INT,
            unit_power STRING,
            temperature DOUBLE,
            unit_temp STRING,
            irradiance INT,
            unit_irradiance STRING,
            voltage DOUBLE,
            unit_voltage STRING,
            current INT,
            unit_current STRING,
            inverter_status STRING,
            timestamp TIMESTAMP,
            kafka_timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_sql)
        
        self._connection.commit()
        self._table_created = True

    def write(self, batch: SinkBatch):
        if not self._connection:
            raise Exception("QuestDB connection not established")
        
        records = []
        for item in batch:
            try:
                # Parse the JSON string from the value field
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert timestamp from nanoseconds to datetime
                timestamp_ns = data.get('timestamp', 0)
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                
                # Convert Kafka message timestamp
                kafka_timestamp = datetime.fromisoformat(item.headers.get('dateTime', '1970-01-01T00:00:00Z').replace('Z', '+00:00'))
                
                record = (
                    data.get('panel_id'),
                    data.get('location_id'),
                    data.get('location_name'),
                    data.get('latitude'),
                    data.get('longitude'),
                    data.get('timezone'),
                    data.get('power_output'),
                    data.get('unit_power'),
                    data.get('temperature'),
                    data.get('unit_temp'),
                    data.get('irradiance'),
                    data.get('unit_irradiance'),
                    data.get('voltage'),
                    data.get('unit_voltage'),
                    data.get('current'),
                    data.get('unit_current'),
                    data.get('inverter_status'),
                    timestamp_dt,
                    kafka_timestamp
                )
                records.append(record)
                
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
        
        if records:
            insert_sql = f"""
            INSERT INTO {self._table_name} (
                panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status, timestamp, kafka_timestamp
            ) VALUES %s
            """
            
            with self._connection.cursor() as cursor:
                execute_values(cursor, insert_sql, records)

# Get configuration from environment variables
try:
    port = int(os.environ.get('QUESTDB_QUESTDB_PORT', '8812'))
except ValueError:
    port = 8812

try:
    buffer_timeout = float(os.environ.get('QUESTDB_BUFFER_TIMEOUT', '1'))
except ValueError:
    buffer_timeout = 1.0

try:
    buffer_size = int(os.environ.get('QUESTDB_BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_QUESTDB_HOST', 'questdb'),
    port=port,
    username=os.environ.get('QUESTDB_QUESTDB_USERNAME', 'admin'),
    password=os.environ.get('QUESTDB_QUESTDB_PASSWORD_KEY'),
    database=os.environ.get('QUESTDB_QUESTDB_DATABASE', 'test_db'),
    table_name=os.environ.get('QUESTDB_QUESTDB_TABLE_NAME', 'xx')
)

# Initialize the Quix Streams application
app = Application(
    consumer_group=os.environ.get('QUESTDB_CONSUMER_GROUP_NAME', 'test_db'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

# Define the input topic
input_topic = app.topic(os.environ.get('QUESTDB_INPUT', 'solar-data'))
sdf = app.dataframe(input_topic)

# Add debugging to see raw message structure
sdf = sdf.apply(lambda msg: print(f'Raw message: {msg}') or msg)

# Sink the data to QuestDB
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)