import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Load environment variables from a .env file for local development
from dotenv import load_dotenv
load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name):
        super().__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.cursor = None

    def setup(self):
        """Initialize database connection and create table if it doesn't exist."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                panel_id VARCHAR(255),
                location_id VARCHAR(255),
                location_name VARCHAR(255),
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
                datetime TIMESTAMPTZ,
                PRIMARY KEY (panel_id, timestamp)
            );
            """
            
            self.cursor.execute(create_table_query)
            self.connection.commit()
            
        except Exception as e:
            print(f"Error setting up TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        """Write batch of records to TimescaleDB."""
        if not self.connection or not self.cursor:
            raise RuntimeError("Database connection not initialized")
            
        try:
            for item in batch:
                # Parse the JSON value from the message
                data = json.loads(item.value)
                
                # Convert timestamp to datetime
                timestamp_dt = datetime.fromtimestamp(data['timestamp'] / 1_000_000_000)
                datetime_dt = datetime.fromisoformat(item.headers.get('dateTime', '').replace('Z', '+00:00')) if item.headers.get('dateTime') else timestamp_dt
                
                # Insert data into table
                insert_query = f"""
                INSERT INTO {self.table_name} (
                    panel_id, location_id, location_name, latitude, longitude, timezone,
                    power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                    voltage, unit_voltage, current, unit_current, inverter_status, timestamp, datetime
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
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
                    inverter_status = EXCLUDED.inverter_status,
                    datetime = EXCLUDED.datetime
                """
                
                self.cursor.execute(insert_query, (
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
                    timestamp_dt,
                    datetime_dt
                ))
                
            self.connection.commit()
            
        except Exception as e:
            print(f"Error writing to TimescaleDB: {e}")
            self.connection.rollback()
            raise

    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Get port with proper error handling
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USER', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav5')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Debug: Print raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)