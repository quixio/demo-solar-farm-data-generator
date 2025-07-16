import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
load_dotenv()

class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink for solar data
    """
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

    def setup(self):
        """Set up the connection to TimescaleDB"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.dbname,
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
        """Create the solar data table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
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
            unit_irradiance VARCHAR(10),
            voltage FLOAT,
            unit_voltage VARCHAR(10),
            current FLOAT,
            unit_current VARCHAR(10),
            inverter_status VARCHAR(50),
            timestamp BIGINT,
            message_datetime TIMESTAMP,
            PRIMARY KEY (panel_id, timestamp)
        );
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_query)

    def write(self, batch: SinkBatch):
        """Write batch of data to TimescaleDB"""
        if not self._connection:
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
            # Parse the JSON value from the message
            value_data = json.loads(item.value.get('value', '{}'))
            
            record = {
                'panel_id': value_data.get('panel_id'),
                'location_id': value_data.get('location_id'),
                'location_name': value_data.get('location_name'),
                'latitude': value_data.get('latitude'),
                'longitude': value_data.get('longitude'),
                'timezone': value_data.get('timezone'),
                'power_output': value_data.get('power_output'),
                'unit_power': value_data.get('unit_power'),
                'temperature': value_data.get('temperature'),
                'unit_temp': value_data.get('unit_temp'),
                'irradiance': value_data.get('irradiance'),
                'unit_irradiance': value_data.get('unit_irradiance'),
                'voltage': value_data.get('voltage'),
                'unit_voltage': value_data.get('unit_voltage'),
                'current': value_data.get('current'),
                'unit_current': value_data.get('unit_current'),
                'inverter_status': value_data.get('inverter_status'),
                'timestamp': value_data.get('timestamp'),
                'message_datetime': item.value.get('dateTime')
            }
            records.append(record)
        
        with self._connection.cursor() as cursor:
            cursor.executemany(insert_query, records)

    def close(self):
        """Close the connection"""
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
    dbname=os.environ.get('TIMESCALEDB_DATABASE', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USER', 'tsadmin'),
    password=os.environ.get('TIMESCALE_PASSWORD_SECRET_KEY'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data_v8')
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

# Debug: Print raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)