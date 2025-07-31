# DEPENDENCIES:
# pip install quixstreams
# pip install psycopg2-binary
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from datetime import datetime

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, username, password, database, table_name):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table_name = table_name
        self.connection = None
        self._table_created = False

    def setup(self):
        """Initialize connection to QuestDB"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database
            )
            self.connection.autocommit = True
            print(f"Connected to QuestDB at {self.host}:{self.port}")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create the solar data table if it doesn't exist"""
        if self._table_created:
            return
            
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
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
            current_value INT,
            unit_current STRING,
            inverter_status STRING,
            timestamp TIMESTAMP,
            message_datetime TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
            self.connection.commit()
            print(f"Table {self.table_name} created or already exists")
            self._table_created = True
        except Exception as e:
            print(f"Failed to create table: {e}")
            raise

    def write(self, batch: SinkBatch):
        """Write batch of messages to QuestDB"""
        if not self.connection:
            raise RuntimeError("QuestDB connection not established")

        records = []
        for item in batch:
            try:
                # Parse the JSON value from the message
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                elif isinstance(item.value, dict):
                    data = item.value
                else:
                    print(f"Unexpected value type: {type(item.value)}")
                    continue

                # Convert timestamp to datetime
                timestamp_ns = data.get('timestamp', 0)
                # Convert nanoseconds to seconds for datetime
                timestamp_s = timestamp_ns / 1_000_000_000
                dt = datetime.fromtimestamp(timestamp_s)

                # Convert message datetime
                message_dt = datetime.fromisoformat(item.headers.get('dateTime', '').replace('Z', '+00:00')) if 'dateTime' in item.headers else datetime.now()

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
                    data.get('current'),  # Note: using 'current' not 'current_value'
                    data.get('unit_current'),
                    data.get('inverter_status'),
                    dt,
                    message_dt
                )
                records.append(record)
                
            except Exception as e:
                print(f"Error processing message: {e}")
                continue

        if records:
            try:
                insert_sql = f"""
                INSERT INTO {self.table_name} (
                    panel_id, location_id, location_name, latitude, longitude, timezone,
                    power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                    voltage, unit_voltage, current_value, unit_current, inverter_status,
                    timestamp, message_datetime
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                with self.connection.cursor() as cursor:
                    cursor.executemany(insert_sql, records)
                self.connection.commit()
                print(f"Successfully wrote {len(records)} records to QuestDB")
                
            except Exception as e:
                print(f"Failed to write to QuestDB: {e}")
                raise

    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            print("QuestDB connection closed")

# Parse environment variables with safe defaults
try:
    questdb_port = int(os.environ.get('QUESTDB_QUESTDB_PORT', '8812'))
except ValueError:
    questdb_port = 8812

try:
    buffer_timeout = float(os.environ.get('QUESTDB_BUFFER_TIMEOUT', '1'))
except ValueError:
    buffer_timeout = 1.0

try:
    buffer_size = int(os.environ.get('QUESTDB_BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

# Create QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_QUESTDB_HOST', 'questdb'),
    port=questdb_port,
    username=os.environ.get('QUESTDB_QUESTDB_USERNAME', 'admin'),
    password=os.environ.get('QUESTDB_QUESTDB_PASSWORD_KEY', ''),
    database=os.environ.get('QUESTDB_QUESTDB_DATABASE', 'test_db'),
    table_name=os.environ.get('QUESTDB_QUESTDB_TABLE_NAME', 'xx')
)

# Create Quix application
app = Application(
    consumer_group=os.environ.get('QUESTDB_CONSUMER_GROUP_NAME', 'test_db'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

# Get input topic
input_topic = app.topic(os.environ.get('QUESTDB_INPUT', 'solar-data'))
sdf = app.dataframe(input_topic)

# Add debugging print to show raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

# Sink data to QuestDB
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)