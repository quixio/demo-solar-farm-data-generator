import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, 
                 host, 
                 port, 
                 database, 
                 user, 
                 password, 
                 table_name,
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

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self._connection.autocommit = True
            
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
        hypertable_query = f"""
        SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_query)
            try:
                cursor.execute(hypertable_query)
            except psycopg2.Error:
                # Hypertable creation might fail if already exists or TimescaleDB extension is not available
                pass

    def write(self, batch: SinkBatch):
        if not self._connection:
            raise RuntimeError("Connection not established")
        
        insert_query = f"""
        INSERT INTO {self.table_name} (
            time, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp_ns
        ) VALUES (
            %(time)s, %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, 
            %(longitude)s, %(timezone)s, %(power_output)s, %(unit_power)s, %(temperature)s, 
            %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s, %(voltage)s, %(unit_voltage)s, 
            %(current)s, %(unit_current)s, %(inverter_status)s, %(timestamp_ns)s
        )
        """
        
        records = []
        for item in batch:
            # Parse the JSON string in the value field
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
            
            # Convert timestamp from nanoseconds to timestamptz
            timestamp_ns = data.get('timestamp', 0)
            timestamp_s = timestamp_ns / 1_000_000_000  # Convert nanoseconds to seconds
            
            record = {
                'time': f"to_timestamp({timestamp_s})",
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
                'timestamp_ns': timestamp_ns
            }
            records.append(record)
        
        # Use raw SQL for timestamp conversion
        insert_query_raw = f"""
        INSERT INTO {self.table_name} (
            time, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp_ns
        ) VALUES (
            to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        with self._connection.cursor() as cursor:
            for record in records:
                cursor.execute(insert_query_raw, (
                    record['timestamp_ns'] / 1_000_000_000,  # Convert nanoseconds to seconds
                    record['panel_id'],
                    record['location_id'],
                    record['location_name'],
                    record['latitude'],
                    record['longitude'],
                    record['timezone'],
                    record['power_output'],
                    record['unit_power'],
                    record['temperature'],
                    record['unit_temp'],
                    record['irradiance'],
                    record['unit_irradiance'],
                    record['voltage'],
                    record['unit_voltage'],
                    record['current'],
                    record['unit_current'],
                    record['inverter_status'],
                    record['timestamp_ns']
                ))

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
    port=int(os.environ.get('TIMESCALEDB_PORT', '5432')),
    database=os.environ.get('TIMESCALEDB_DATABASE', 'postgres'),
    user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'timescale-sink-group'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get('input', 'solar-data'), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debugging print statement
def debug_message(item):
    print(f'Raw message: {item}')
    return item

sdf = sdf.apply(debug_message)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)