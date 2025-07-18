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
                latitude FLOAT,
                longitude FLOAT,
                timezone INTEGER,
                power_output INTEGER,
                unit_power TEXT,
                temperature FLOAT,
                unit_temp TEXT,
                irradiance INTEGER,
                unit_irradiance TEXT,
                voltage FLOAT,
                unit_voltage TEXT,
                current INTEGER,
                unit_current TEXT,
                inverter_status TEXT,
                message_datetime TIMESTAMPTZ
            );
            """
            self._cursor.execute(create_table_query)
            
            # Convert to hypertable if not already
            try:
                self._cursor.execute(f"SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);")
            except psycopg2.Error:
                # Table might already be a hypertable
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
            self.setup()
        
        insert_query = f"""
        INSERT INTO {self.table_name} (
            timestamp, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, message_datetime
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON value field
            try:
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
            except (json.JSONDecodeError, TypeError) as e:
                print(f"Error parsing JSON: {e}")
                continue
            
            # Convert epoch nanoseconds to datetime
            timestamp_ns = data.get('timestamp')
            if timestamp_ns:
                # Convert nanoseconds to seconds
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
            else:
                timestamp_dt = datetime.now()
            
            # Parse message datetime
            message_datetime = datetime.fromisoformat(item.timestamp.replace('Z', '+00:00'))
            
            row = (
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
                data.get('inverter_status'),
                message_datetime
            )
            rows.append(row)
        
        if rows:
            self._cursor.executemany(insert_query, rows)
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
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav6')
)

# Initialize the application
app = Application(
    consumer_group="timescale-sink-consumer",
    auto_offset_reset="earliest",
    commit_interval=1.0,
    commit_every=100
)

# Define the input topic
input_topic = app.topic("solar-data", value_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)