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
        """Setup the database connection and create table if it doesn't exist."""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            self._cursor = self._connection.cursor()
            
            # Create table if it doesn't exist
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
                unit_irradiance VARCHAR(20),
                voltage FLOAT,
                unit_voltage VARCHAR(10),
                current FLOAT,
                unit_current VARCHAR(10),
                inverter_status VARCHAR(50),
                timestamp BIGINT,
                datetime TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
        """Write a batch of data to TimescaleDB."""
        if not self._connection or not self._cursor:
            raise RuntimeError("Database connection not established")
            
        insert_query = f"""
        INSERT INTO {self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp
        ) VALUES (
            %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, %(longitude)s, %(timezone)s,
            %(power_output)s, %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s,
            %(voltage)s, %(unit_voltage)s, %(current)s, %(unit_current)s, %(inverter_status)s, %(timestamp)s
        )
        """
        
        records = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON string from the value field
            try:
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                    
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
                    'timestamp': data.get('timestamp')
                }
                records.append(record)
                
            except (json.JSONDecodeError, AttributeError) as e:
                print(f"Error parsing message: {e}")
                continue
        
        if records:
            self._cursor.executemany(insert_query, records)
            self._connection.commit()

    def close(self):
        """Close database connection."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Parse port with error handling
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
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data_v9a')
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
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)