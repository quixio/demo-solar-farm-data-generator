import os
import json
import psycopg2
from datetime import datetime
from typing import List
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Load environment variables from a .env file for local development
from dotenv import load_dotenv
load_dotenv()

class TimescaleDBSink(BatchingSink):
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
        self._connection = None
        self._cursor = None

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor()

            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TIMESTAMPTZ NOT NULL,
                panel_id TEXT,
                location_id TEXT,
                location_name TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                timezone INTEGER,
                power_output INTEGER,
                unit_power TEXT,
                temperature DOUBLE PRECISION,
                unit_temp TEXT,
                irradiance INTEGER,
                unit_irradiance TEXT,
                voltage DOUBLE PRECISION,
                unit_voltage TEXT,
                current INTEGER,
                unit_current TEXT,
                inverter_status TEXT
            );
            """
            self._cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist
            hypertable_query = f"""
            SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);
            """
            self._cursor.execute(hypertable_query)
            
            self._connection.commit()
            print(f"Table {self.table_name} created/verified successfully")

            # Notify Quix that the sink is ready
            self._on_client_connect_success()

        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            
            # Notify Quix about the failure
            self._on_client_connect_failure(e)
            raise

    def write(self, batch: SinkBatch):
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not initialized")

        try:
            insert_query = f"""
            INSERT INTO {self.table_name} (
                timestamp, panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            for item in batch:
                # Parse the JSON value if it's a string
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert epoch timestamp to datetime
                timestamp_epoch = data.get('timestamp', 0)
                if isinstance(timestamp_epoch, (int, float)):
                    # Convert nanoseconds to seconds
                    timestamp_seconds = timestamp_epoch / 1_000_000_000
                    timestamp_dt = datetime.fromtimestamp(timestamp_seconds)
                else:
                    timestamp_dt = datetime.now()

                values = (
                    timestamp_dt,
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
                    data.get('inverter_status')
                )

                self._cursor.execute(insert_query, values)

            self._connection.commit()
            print(f"Successfully inserted {len(batch)} records into TimescaleDB")

        except Exception as e:
            print(f"Error writing to TimescaleDB: {e}")
            self._connection.rollback()
            raise

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Get database connection details from environment variables
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
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav3')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb_sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print to see raw message structure
sdf = sdf.apply(lambda message: print(f"Raw message: {message}") or message)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)