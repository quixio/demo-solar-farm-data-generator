import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Load environment variables from a .env file for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._host = host
        self._port = port
        self._database = database
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
                database=self._database,
                user=self._user,
                password=self._password
            )
            self._cursor = self._connection.cursor()
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                panel_id VARCHAR(50),
                location_id VARCHAR(50),
                location_name VARCHAR(100),
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
                inverter_status VARCHAR(20),
                timestamp TIMESTAMPTZ,
                datetime TIMESTAMPTZ
            );
            """
            self._cursor.execute(create_table_query)
            self._connection.commit()
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"SELECT create_hypertable('{self._table_name}', 'timestamp', if_not_exists => TRUE);"
                self._cursor.execute(hypertable_query)
                self._connection.commit()
            except Exception as e:
                # If hypertable creation fails, it might not be a TimescaleDB instance
                # or the table might already be a hypertable
                pass
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def write(self, batch: SinkBatch):
        if not self._connection or not self._cursor:
            raise RuntimeError("Database connection not established")
        
        insert_query = f"""
        INSERT INTO {self._table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, datetime
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        records = []
        for item in batch:
            try:
                # Parse the JSON string from the value field
                data = json.loads(item.value)
                
                # Convert epoch timestamp to datetime
                timestamp_dt = datetime.fromtimestamp(data['timestamp'] / 1_000_000_000)
                datetime_dt = datetime.fromisoformat(item.headers.get('dateTime', '').replace('Z', '+00:00')) if item.headers.get('dateTime') else timestamp_dt
                
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
                    datetime_dt
                )
                records.append(record)
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                print(f"Error parsing message: {e}")
                continue
        
        if records:
            self._cursor.executemany(insert_query, records)
            self._connection.commit()

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Get database connection parameters with proper error handling
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
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print to see raw message structure
def debug_message(row):
    print(f'Raw message: {row}')
    return row

sdf = sdf.apply(debug_message)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)