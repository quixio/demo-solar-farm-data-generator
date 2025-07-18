import os
import json
import psycopg2
from datetime import datetime
from typing import List
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch


class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink for solar panel data
    """
    def __init__(self, host, port, dbname, user, password, table_name, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.cursor = None

    def setup(self):
        """
        Setup database connection and create table if it doesn't exist.
        """
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
                timestamp TIMESTAMPTZ NOT NULL,
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
                inverter_status VARCHAR(50)
            );
            """
            self.cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);"
                self.cursor.execute(hypertable_query)
            except psycopg2.Error as e:
                # Ignore if hypertable already exists or if not a TimescaleDB instance
                print(f"Hypertable creation note: {e}")
            
            self.connection.commit()
            print(f"TimescaleDB sink setup completed for table {self.table_name}")
            
        except Exception as e:
            print(f"Failed to setup TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of messages to TimescaleDB
        """
        if not batch:
            return
            
        insert_query = f"""
        INSERT INTO {self.table_name} (
            timestamp, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        values = []
        for item in batch:
            try:
                # Parse the JSON data from the value field
                data = json.loads(item.value)
                
                # Convert timestamp to datetime
                timestamp_ms = data['timestamp']
                timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1e9)  # Convert from nanoseconds
                
                values.append((
                    timestamp_dt,
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
                    data['inverter_status']
                ))
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing message: {e}")
                continue
                
        if values:
            self.cursor.executemany(insert_query, values)
            self.connection.commit()
            print(f"Wrote {len(values)} records to TimescaleDB")

    def close(self):
        """
        Close database connection
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


# Load environment variables
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav6'),
    on_client_connect_success=lambda: print("Connected to TimescaleDB successfully"),
    on_client_connect_failure=lambda e: print(f"Failed to connect to TimescaleDB: {e}")
)

# Initialize the application
app = Application(
    consumer_group="timescaledb-sink-consumer",
    auto_offset_reset="earliest",
    commit_interval=1.0,
    commit_every=100
)

# Define the input topic
input_topic = app.topic("input", value_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Debug: Print raw message structure
def debug_message(item):
    print(f"Raw message: {item}")
    return item

sdf = sdf.apply(debug_message)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)