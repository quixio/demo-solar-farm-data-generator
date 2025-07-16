import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Load environment variables from a .env file for local development
from dotenv import load_dotenv
load_dotenv()

class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink for solar farm data
    """
    def __init__(self, host, port, dbname, user, password, table_name, 
                 on_client_connect_success=None, on_client_connect_failure=None):
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

    def setup(self):
        """Setup the database connection and create table if it doesn't exist"""
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._dbname,
                user=self._user,
                password=self._password
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
        """Create the solar data table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            panel_id VARCHAR(255),
            location_id VARCHAR(255),
            location_name VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            timezone INTEGER,
            power_output FLOAT,
            unit_power VARCHAR(10),
            temperature FLOAT,
            unit_temp VARCHAR(10),
            irradiance FLOAT,
            unit_irradiance VARCHAR(20),
            voltage FLOAT,
            unit_voltage VARCHAR(10),
            current_val FLOAT,
            unit_current VARCHAR(10),
            inverter_status VARCHAR(50),
            timestamp_val BIGINT,
            datetime_received TIMESTAMPTZ,
            PRIMARY KEY (panel_id, timestamp_val)
        );
        """
        
        # Create hypertable if it doesn't exist (TimescaleDB specific)
        create_hypertable_query = f"""
        SELECT create_hypertable('{self._table_name}', 'datetime_received', if_not_exists => TRUE);
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_query)
            try:
                cursor.execute(create_hypertable_query)
            except Exception as e:
                # Hypertable might already exist or TimescaleDB extension not installed
                print(f"Warning: Could not create hypertable: {e}")

    def write(self, batch: SinkBatch):
        """Write batch of data to TimescaleDB"""
        if not self._connection:
            raise RuntimeError("Database connection not established")
            
        insert_query = f"""
        INSERT INTO {self._table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current_val, unit_current, inverter_status, 
            timestamp_val, datetime_received
        ) VALUES %s
        ON CONFLICT (panel_id, timestamp_val) DO UPDATE SET
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
            current_val = EXCLUDED.current_val,
            unit_current = EXCLUDED.unit_current,
            inverter_status = EXCLUDED.inverter_status,
            datetime_received = EXCLUDED.datetime_received
        """
        
        # Prepare data for batch insert
        data_to_insert = []
        for item in batch:
            # Parse the JSON string in the value field
            if isinstance(item.value, str):
                try:
                    value_data = json.loads(item.value)
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON from value: {item.value}")
                    continue
            else:
                value_data = item.value
            
            data_tuple = (
                value_data.get('panel_id'),
                value_data.get('location_id'),
                value_data.get('location_name'),
                value_data.get('latitude'),
                value_data.get('longitude'),
                value_data.get('timezone'),
                value_data.get('power_output'),
                value_data.get('unit_power'),
                value_data.get('temperature'),
                value_data.get('unit_temp'),
                value_data.get('irradiance'),
                value_data.get('unit_irradiance'),
                value_data.get('voltage'),
                value_data.get('unit_voltage'),
                value_data.get('current'),  # Note: mapping 'current' to 'current_val'
                value_data.get('unit_current'),
                value_data.get('inverter_status'),
                value_data.get('timestamp'),
                item.timestamp  # Use the message timestamp
            )
            data_to_insert.append(data_tuple)
        
        if data_to_insert:
            try:
                from psycopg2.extras import execute_values
                with self._connection.cursor() as cursor:
                    execute_values(cursor, insert_query, data_to_insert)
                    print(f"Successfully inserted {len(data_to_insert)} records")
            except Exception as e:
                print(f"Error inserting data: {e}")
                raise

    def close(self):
        """Close the database connection"""
        if self._connection:
            self._connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
    port=int(os.environ.get('TIMESCALEDB_PORT', '5432')),
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'postgres'),
    user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data'),
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "demo-solarfarmdatageneratordemo-prod-solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debugging to see raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

# Sink the data
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)