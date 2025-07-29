import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Load environment variables from a .env file for local development
from dotenv import load_dotenv
load_dotenv()


def _as_bool(env_var: str, default="false") -> bool:
    return os.environ.get(env_var, default).lower() == "true"


class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name, schema_name="public", schema_auto_update=True):
        super().__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.schema_name = schema_name
        self.schema_auto_update = schema_auto_update
        self._connection = None
        self._table_created = False

    def setup(self):
        """Initialize the database connection and create table if needed"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self._connection.autocommit = True
            
            # Create table if it doesn't exist
            if not self._table_created:
                self._create_table_if_not_exists()
                self._table_created = True
                
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create the solar_data table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
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
            timestamp TIMESTAMPTZ,
            PRIMARY KEY (panel_id, timestamp)
        );
        """
        
        # Create hypertable for TimescaleDB
        hypertable_sql = f"""
        SELECT create_hypertable('{self.schema_name}.{self.table_name}', 'timestamp', if_not_exists => TRUE);
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            try:
                cursor.execute(hypertable_sql)
            except Exception as e:
                # Hypertable creation might fail if TimescaleDB extension is not available
                # This is okay for regular PostgreSQL
                print(f"Warning: Could not create hypertable (TimescaleDB extension may not be available): {e}")
        
        self._connection.commit()

    def write(self, batch: SinkBatch):
        """Write a batch of messages to TimescaleDB"""
        if not self._connection:
            self.setup()
            
        insert_sql = f"""
        INSERT INTO {self.schema_name}.{self.table_name} 
        (panel_id, location_id, location_name, latitude, longitude, timezone, 
         power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
         voltage, unit_voltage, current, unit_current, inverter_status, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (panel_id, timestamp) DO UPDATE SET
            location_id = EXCLUDED.location_id,
            location_name = EXCLUDED.location_name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            timezone = EXCLUDED.timezone,
            power_output = EXCLUDED.power_output,
            unit_power = EXCLUDED.unit_power,
            temperature = EXCLUDED.temperature,
            unit_temp = EXCLUDED.unit_temp,
            irradiance = EXCLUDED.irradiance,
            unit_irradiance = EXCLUDED.unit_irradiance,
            voltage = EXCLUDED.voltage,
            unit_voltage = EXCLUDED.unit_voltage,
            current = EXCLUDED.current,
            unit_current = EXCLUDED.unit_current,
            inverter_status = EXCLUDED.inverter_status
        """
        
        records = []
        for item in batch:
            # Parse the JSON value from the message
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
            
            # Convert timestamp from nanoseconds to datetime
            timestamp_ns = data.get('timestamp', 0)
            timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
            
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
                timestamp_dt
            )
            records.append(record)
        
        with self._connection.cursor() as cursor:
            cursor.executemany(insert_sql, records)
        self._connection.commit()


# Get environment variables with proper error handling
try:
    port = int(os.environ.get('TSDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescaledb_sink = TimescaleDBSink(
    host=os.environ.get('TSDB_HOST'),
    port=port,
    dbname=os.environ.get('TSDB_DBNAME'),
    user=os.environ.get('TSDB_USER'),
    password=os.environ.get('TSDB_PASSWORD_KEY'),
    table_name=os.environ.get('TSDB_TABLE', 'solar_data'),
    schema_name=os.environ.get('TSDB_SCHEMA', 'public'),
    schema_auto_update=_as_bool('TSDB_SCHEMA_AUTO_UPDATE', 'true')
)

# Initialize the application
try:
    batch_size = int(os.environ.get('TSDB_BATCH_SIZE', '1000'))
except ValueError:
    batch_size = 1000

try:
    batch_timeout = float(os.environ.get('TSDB_BATCH_TIMEOUT', '1'))
except ValueError:
    batch_timeout = 1.0

app = Application(
    consumer_group=os.environ.get('TSDB_CONSUMER_GROUP_NAME', 'timescaledb-sink-group'),
    auto_offset_reset="earliest",
    commit_interval=batch_timeout,
    commit_every=batch_size
)

# Define the input topic
input_topic = app.topic(os.environ.get('TSDB_INPUT'), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print to show raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

# Sink data to TimescaleDB
sdf.sink(timescaledb_sink)

if __name__ == "__main__":
    app.run()