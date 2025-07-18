import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        self._table_name = table_name
        self._connection = None
        self._cursor = None

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._dbname,
                user=self._user,
                password=self._password
            )
            self._cursor = self._connection.cursor()
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
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
            try:
                self._cursor.execute(f"SELECT create_hypertable('{self._table_name}', 'timestamp', if_not_exists => TRUE);")
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
        INSERT INTO {self._table_name} (
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
            
            # Convert timestamp to datetime
            timestamp_ns = data.get('timestamp', 0)
            timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1e9)
            
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

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Initialize TimescaleDB Sink
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav9')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ["input"], key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debugging to show raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run()