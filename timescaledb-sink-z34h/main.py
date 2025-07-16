import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
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
                database=self.dbname,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor(cursor_factory=RealDictCursor)
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            panel_id VARCHAR(255),
            location_id VARCHAR(255),
            location_name VARCHAR(255),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone INTEGER,
            power_output INTEGER,
            unit_power VARCHAR(10),
            temperature DOUBLE PRECISION,
            unit_temp VARCHAR(10),
            irradiance INTEGER,
            unit_irradiance VARCHAR(20),
            voltage DOUBLE PRECISION,
            unit_voltage VARCHAR(10),
            current INTEGER,
            unit_current VARCHAR(10),
            inverter_status VARCHAR(50),
            timestamp BIGINT,
            message_datetime TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create hypertable if it doesn't exist (TimescaleDB specific)
        hypertable_query = f"""
        SELECT create_hypertable('{self.table_name}', 'created_at', if_not_exists => TRUE);
        """
        
        try:
            self._cursor.execute(create_table_query)
            self._cursor.execute(hypertable_query)
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            print(f"Error creating table: {e}")

    def write(self, batch: SinkBatch):
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not established")
        
        insert_query = f"""
        INSERT INTO {self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
        ) VALUES (
            %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, %(longitude)s, %(timezone)s,
            %(power_output)s, %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s,
            %(voltage)s, %(unit_voltage)s, %(current)s, %(unit_current)s, %(inverter_status)s, %(timestamp)s, %(message_datetime)s
        )
        """
        
        records = []
        for item in batch:
            try:
                # Parse the JSON string from the value field
                data = json.loads(item.value)
                
                record = {
                    'panel_id': data.get('panel_id'),
                    'location_id': data.get('location_id'),
                    'location_name': data.get('location_name'),
                    'latitude': data.get('latitude'),
                    'longitude': data.get('longitude'),
                    'timezone': data.get('timezone'),
                    'power_output': data.get('power_output'),
                    'unit_power': data.get('unit_power'),
                    'temperature': data.get('temperature'),
                    'unit_temp': data.get('unit_temp'),
                    'irradiance': data.get('irradiance'),
                    'unit_irradiance': data.get('unit_irradiance'),
                    'voltage': data.get('voltage'),
                    'unit_voltage': data.get('unit_voltage'),
                    'current': data.get('current'),
                    'unit_current': data.get('unit_current'),
                    'inverter_status': data.get('inverter_status'),
                    'timestamp': data.get('timestamp'),
                    'message_datetime': item.timestamp
                }
                records.append(record)
            except (json.JSONDecodeError, KeyError, AttributeError) as e:
                print(f"Error parsing message: {e}")
                continue
        
        if records:
            try:
                self._cursor.executemany(insert_query, records)
                self._connection.commit()
            except Exception as e:
                self._connection.rollback()
                print(f"Error writing to TimescaleDB: {e}")
                raise

    def close(self):
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
    dbname=os.environ.get('TIMESCALEDB_DATABASE', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USER', 'tsadmin'),
    password=os.environ.get('TIMESCALE_PASSWORD_SECRET_KEY'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solarv3')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), value_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Debug: print raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    timescale_sink.setup()
    app.run(count=10, timeout=20)