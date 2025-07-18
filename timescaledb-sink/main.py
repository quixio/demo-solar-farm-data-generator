import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from datetime import datetime
from typing import Dict, Any

# Load environment variables from a .env file for local development
from dotenv import load_dotenv
load_dotenv()

class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink for writing solar panel data
    """
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str, table_name: str):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self._connection = None
        self._cursor = None
        
    def setup(self):
        """Set up the connection to TimescaleDB and create table if needed"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor(cursor_factory=RealDictCursor)
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                panel_id VARCHAR(100),
                location_id VARCHAR(100),
                location_name VARCHAR(200),
                latitude FLOAT,
                longitude FLOAT,
                timezone INTEGER,
                power_output INTEGER,
                unit_power VARCHAR(10),
                temperature FLOAT,
                unit_temp VARCHAR(10),
                irradiance INTEGER,
                unit_irradiance VARCHAR(20),
                voltage FLOAT,
                unit_voltage VARCHAR(10),
                current INTEGER,
                unit_current VARCHAR(10),
                inverter_status VARCHAR(50),
                timestamp TIMESTAMPTZ,
                PRIMARY KEY (panel_id, timestamp)
            );
            """
            
            self._cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist
            try:
                hypertable_query = f"SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);"
                self._cursor.execute(hypertable_query)
            except Exception as e:
                # Hypertable might already exist or TimescaleDB extension not available
                print(f"Could not create hypertable: {e}")
                
            self._connection.commit()
            print(f"TimescaleDB connection established and table '{self.table_name}' ready")
            
        except Exception as e:
            print(f"Failed to setup TimescaleDB connection: {e}")
            raise
    
    def write(self, batch: SinkBatch):
        """Write batch of data to TimescaleDB"""
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not established")
            
        try:
            records = []
            for item in batch:
                # Parse the JSON value from the message
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert epoch timestamp to datetime
                timestamp = datetime.fromtimestamp(data['timestamp'] / 1e9)  # Convert from nanoseconds
                
                record = (
                    data['panel_id'],
                    data['location_id'],
                    data['location_name'],
                    data['latitude'],
                    data['longitude'],
                    data['timezone'],
                    data['power_output'],
                    data['unit_power'],
                    data['temperature'],
                    data['unit_temp'],
                    data['irradiance'],
                    data['unit_irradiance'],
                    data['voltage'],
                    data['unit_voltage'],
                    data['current'],
                    data['unit_current'],
                    data['inverter_status'],
                    timestamp
                )
                records.append(record)
            
            # Insert all records in batch
            insert_query = f"""
            INSERT INTO {self.table_name} (
                panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (panel_id, timestamp) DO UPDATE SET
                location_id = EXCLUDED.location_id,
                location_name = EXCLUDED.location_name,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                timezone = EXCLUDED.timezone,
                power_output = EXCLUDED.power_output,
                unit_power = EXCLUDED.unit_power,
                temperature = EXCLUDED.temperature,
                unit_temp = EXCLUDED.unit_temp,
                irradiance = EXCLUDED.irradiance,
                unit_irradiance = EXCLUDED.unit_irradiance,
                voltage = EXCLUDED.voltage,
                unit_voltage = EXCLUDED.unit_voltage,
                current = EXCLUDED.current,
                unit_current = EXCLUDED.unit_current,
                inverter_status = EXCLUDED.inverter_status
            """
            
            self._cursor.executemany(insert_query, records)
            self._connection.commit()
            print(f"Successfully wrote {len(records)} records to TimescaleDB")
            
        except Exception as e:
            print(f"Failed to write to TimescaleDB: {e}")
            self._connection.rollback()
            raise
    
    def close(self):
        """Close the connection to TimescaleDB"""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Get connection parameters from environment variables
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    database=os.environ.get('TIMESCALEDB_DATABASE', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USER', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD_SECRET_KEY', ''),
    table_name=os.environ.get('TIMESCALEDB_TABLE_NAME', 'solar_datav9')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "5")),
    commit_every=int(os.environ.get("BATCH_SIZE", "100"))
)

# Define the input topic
input_topic = app.topic(os.environ["input"], key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debugging to see raw message structure
def debug_message(item):
    print(f"Raw message: {item}")
    return item

sdf = sdf.apply(debug_message)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)