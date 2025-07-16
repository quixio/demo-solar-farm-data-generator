import os
import json
import psycopg2
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from typing import Any, Dict, List, Optional

# Load environment variables from a .env file for local development
from dotenv import load_dotenv
load_dotenv()

class TimescaleDBSink(BatchingSink):
    """
    TimescaleDB sink for writing data to a TimescaleDB database
    """
    
    def __init__(self, 
                 host: str,
                 port: int,
                 database: str,
                 user: str,
                 password: str,
                 table_name: str,
                 on_client_connect_success=None,
                 on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self._connection = None
        self._cursor = None
    
    def setup(self):
        """Setup the TimescaleDB connection and create table if needed"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor()
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def _create_table_if_not_exists(self):
        """Create the TimescaleDB table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            time TIMESTAMPTZ NOT NULL,
            panel_id TEXT,
            location_id TEXT,
            location_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone INTEGER,
            power_output DOUBLE PRECISION,
            unit_power TEXT,
            temperature DOUBLE PRECISION,
            unit_temp TEXT,
            irradiance DOUBLE PRECISION,
            unit_irradiance TEXT,
            voltage DOUBLE PRECISION,
            unit_voltage TEXT,
            current DOUBLE PRECISION,
            unit_current TEXT,
            inverter_status TEXT,
            timestamp_ns BIGINT
        );
        """
        
        # Create hypertable if it doesn't exist (TimescaleDB specific)
        create_hypertable_query = f"""
        SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);
        """
        
        try:
            self._cursor.execute(create_table_query)
            self._cursor.execute(create_hypertable_query)
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise
    
    def write(self, batch: SinkBatch):
        """Write a batch of data to TimescaleDB"""
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not established")
        
        insert_query = f"""
        INSERT INTO {self.table_name} (
            time, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp_ns
        ) VALUES (
            TO_TIMESTAMP(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        records = []
        for item in batch:
            # Accept dicts directly, fall back to json.loads only if a raw string somehow slips through
            raw_value = item.value
            if isinstance(raw_value, dict):
                data = raw_value
            else:
                data = json.loads(raw_value)
            
            timestamp_ns = data.get("timestamp", 0)
            timestamp_seconds = timestamp_ns / 1_000_000_000
            
            records.append((
                timestamp_seconds,                   # for TO_TIMESTAMP(%s)
                data.get("panel_id"),
                data.get("location_id"),
                data.get("location_name"),
                data.get("latitude"),
                data.get("longitude"),
                data.get("timezone"),
                data.get("power_output"),
                data.get("unit_power"),
                data.get("temperature"),
                data.get("unit_temp"),
                data.get("irradiance"),
                data.get("unit_irradiance"),
                data.get("voltage"),
                data.get("unit_voltage"),
                data.get("current"),
                data.get("unit_current"),
                data.get("inverter_status"),
                timestamp_ns
            ))
        
        if records:
            self._cursor.executemany(insert_query, records)
            self._connection.commit()
    
    def close(self):
        """Close the TimescaleDB connection"""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()


# Get port with error handling
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
    password=os.environ.get('TIMESCALE_PASSWORD_SECRET_KEY'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solarv3')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "demo-solarfarmdatageneratordemo-prod-solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print for raw message structure
sdf = sdf.apply(lambda msg: print(f'Raw message: {msg}') or msg)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run()