import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name, schema_name="public"):
        super().__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.schema_name = schema_name
        self._connection = None
        self._cursor = None

    def setup(self):
        """Setup database connection and create table if it doesn't exist"""
        self._connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.dbname,
            user=self.user,
            password=self.password
        )
        self._connection.autocommit = True
        self._cursor = self._connection.cursor(cursor_factory=RealDictCursor)
        
        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
            id SERIAL PRIMARY KEY,
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
            message_datetime TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self._cursor.execute(create_table_query)
        
        # Create hypertable if it doesn't exist (TimescaleDB specific)
        try:
            hypertable_query = f"""
            SELECT create_hypertable('{self.schema_name}.{self.table_name}', 'created_at', if_not_exists => TRUE);
            """
            self._cursor.execute(hypertable_query)
        except Exception as e:
            print(f"Note: Could not create hypertable (this is normal if TimescaleDB extension is not installed): {e}")

    def write(self, batch: SinkBatch):
        """Write batch of records to TimescaleDB"""
        if not batch:
            return
        
        insert_query = f"""
        INSERT INTO {self.schema_name}.{self.table_name} (
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
            # Parse the JSON string from the value field
            try:
                solar_data = json.loads(item.value)
                
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
                    'message_datetime': item.timestamp
                }
                records.append(record)
            except (json.JSONDecodeError, KeyError, AttributeError) as e:
                print(f"Error parsing message: {e}")
                continue
        
        if records:
            self._cursor.executemany(insert_query, records)

    def close(self):
        """Close database connection"""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
    port=int(os.environ.get('TIMESCALEDB_PORT', '5432')),
    dbname=os.environ.get('TIMESCALEDB_DATABASE', 'solar_data'),
    user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_panel_data'),
    schema_name=os.environ.get('TIMESCALEDB_SCHEMA', 'public')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "solar-timescale-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debugging to see raw message structure
def debug_message(row):
    print(f'Raw message: {row}')
    return row

sdf = sdf.apply(debug_message)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)