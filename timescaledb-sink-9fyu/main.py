import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name, 
                 on_client_connect_success=None, on_client_connect_failure=None):
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
                time TIMESTAMPTZ NOT NULL,
                panel_id TEXT,
                location_id TEXT,
                location_name TEXT,
                latitude FLOAT,
                longitude FLOAT,
                timezone INTEGER,
                power_output FLOAT,
                unit_power TEXT,
                temperature FLOAT,
                unit_temp TEXT,
                irradiance FLOAT,
                unit_irradiance TEXT,
                voltage FLOAT,
                unit_voltage TEXT,
                current FLOAT,
                unit_current TEXT,
                inverter_status TEXT,
                timestamp_ns BIGINT
            );
            """
            self._cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist (TimescaleDB extension)
            try:
                hypertable_query = f"SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);"
                self._cursor.execute(hypertable_query)
            except psycopg2.Error:
                # Hypertable might already exist or TimescaleDB extension not available
                pass
            
            self._connection.commit()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def write(self, batch: SinkBatch):
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not established")
        
        insert_query = f"""
        INSERT INTO {self.table_name} (
            time, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp_ns
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        records = []
        for item in batch:
            try:
                # Parse the JSON string from the value field
                data = json.loads(item.value)
                
                # Convert timestamp from nanoseconds to ISO format for PostgreSQL
                timestamp_ns = data.get('timestamp', 0)
                timestamp_seconds = timestamp_ns / 1_000_000_000
                
                record = (
                    f'TO_TIMESTAMP({timestamp_seconds})',
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
                    timestamp_ns
                )
                records.append(record)
                
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing message: {e}")
                continue
        
        if records:
            # Use proper timestamp conversion in the query
            modified_query = f"""
            INSERT INTO {self.table_name} (
                time, panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status, timestamp_ns
            ) VALUES (
                TO_TIMESTAMP(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Modify records to pass timestamp as seconds
            modified_records = []
            for record in records:
                timestamp_ns = record[18]  # timestamp_ns is at index 18
                timestamp_seconds = timestamp_ns / 1_000_000_000
                modified_record = (timestamp_seconds,) + record[1:]
                modified_records.append(modified_record)
            
            self._cursor.executemany(modified_query, modified_records)
            self._connection.commit()

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Get port with proper error handling
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
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'timescale-sink-group'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get('BATCH_TIMEOUT', '1')),
    commit_every=int(os.environ.get('BATCH_SIZE', '1000'))
)

# Define the input topic
input_topic = app.topic(os.environ.get('input', 'input-topic'), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debugging print to show raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

# Sink the data
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)