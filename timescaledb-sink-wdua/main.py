import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name, on_client_connect_success=None, on_client_connect_failure=None):
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
                panel_id VARCHAR(255),
                location_id VARCHAR(255),
                location_name VARCHAR(255),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                timezone INTEGER,
                power_output DOUBLE PRECISION,
                unit_power VARCHAR(10),
                temperature DOUBLE PRECISION,
                unit_temp VARCHAR(10),
                irradiance DOUBLE PRECISION,
                unit_irradiance VARCHAR(10),
                voltage DOUBLE PRECISION,
                unit_voltage VARCHAR(10),
                current DOUBLE PRECISION,
                unit_current VARCHAR(10),
                inverter_status VARCHAR(50),
                timestamp BIGINT,
                datetime TIMESTAMP
            );
            """
            
            self._cursor.execute(create_table_query)
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
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, datetime
        ) VALUES (
            %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, %(longitude)s, %(timezone)s,
            %(power_output)s, %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s,
            %(voltage)s, %(unit_voltage)s, %(current)s, %(unit_current)s, %(inverter_status)s, %(timestamp)s, %(datetime)s
        )
        """
        
        batch_data = []
        for item in batch:
            try:
                # Parse the value field which contains JSON string
                if hasattr(item, 'value') and isinstance(item.value, str):
                    solar_data = json.loads(item.value)
                else:
                    solar_data = item.value
                
                # Map the solar data to table columns
                record = {
                    'panel_id': solar_data.get('panel_id'),
                    'location_id': solar_data.get('location_id'),
                    'location_name': solar_data.get('location_name'),
                    'latitude': solar_data.get('latitude'),
                    'longitude': solar_data.get('longitude'),
                    'timezone': solar_data.get('timezone'),
                    'power_output': solar_data.get('power_output'),
                    'unit_power': solar_data.get('unit_power'),
                    'temperature': solar_data.get('temperature'),
                    'unit_temp': solar_data.get('unit_temp'),
                    'irradiance': solar_data.get('irradiance'),
                    'unit_irradiance': solar_data.get('unit_irradiance'),
                    'voltage': solar_data.get('voltage'),
                    'unit_voltage': solar_data.get('unit_voltage'),
                    'current': solar_data.get('current'),
                    'unit_current': solar_data.get('unit_current'),
                    'inverter_status': solar_data.get('inverter_status'),
                    'timestamp': solar_data.get('timestamp'),
                    'datetime': item.context.timestamp
                }
                batch_data.append(record)
                
            except Exception as e:
                print(f"Error processing item: {e}")
                continue
        
        if batch_data:
            self._cursor.executemany(insert_query, batch_data)
            self._connection.commit()

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Get TimescaleDB connection parameters
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
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data_v9b')
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

# Add debugging print to see raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)