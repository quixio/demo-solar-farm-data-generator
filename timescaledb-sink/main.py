import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.cursor = None
        self.table_created = False

    def setup(self):
        """Setup the connection to TimescaleDB and create table if needed"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            self._create_table_if_not_exists()
            
            if callable(self._on_client_connect_success):
                self._on_client_connect_success()
        except Exception as e:
            if callable(self._on_client_connect_failure):
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist"""
        if not self.table_created:
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                time TIMESTAMPTZ NOT NULL,
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
                inverter_status TEXT,
                timestamp_epoch BIGINT
            );
            """
            
            # Create hypertable if it doesn't exist
            hypertable_sql = f"""
            SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);
            """
            
            self.cursor.execute(create_table_sql)
            self.cursor.execute(hypertable_sql)
            self.connection.commit()
            self.table_created = True

    def write(self, batch: SinkBatch):
        """Write batch data to TimescaleDB"""
        if not batch:
            return
            
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            time, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp_epoch
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        batch_data = []
        for item in batch:
            try:
                # Parse the JSON value field
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert epoch timestamp to datetime
                timestamp_epoch = data.get('timestamp', 0)
                if timestamp_epoch:
                    # Convert nanoseconds to seconds
                    timestamp_seconds = timestamp_epoch / 1_000_000_000
                    dt = datetime.fromtimestamp(timestamp_seconds)
                else:
                    dt = datetime.now()
                
                record = (
                    dt,  # time
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
                    timestamp_epoch
                )
                batch_data.append(record)
            except Exception as e:
                print(f"Error processing item: {e}")
                continue
        
        if batch_data:
            self.cursor.executemany(insert_sql, batch_data)
            self.connection.commit()

    def close(self):
        """Close the database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize TimescaleDB Sink
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

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

# Debug: Print raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

# Extract and transform the data
def transform_message(message):
    print(f'Processing message: {message}')
    # The message value is a JSON string that needs to be parsed
    if isinstance(message, str):
        return json.loads(message)
    return message

sdf = sdf.apply(transform_message)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run()