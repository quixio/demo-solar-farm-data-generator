import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name, schema_name="public"):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.schema_name = schema_name
        self.connection = None
        self.cursor = None

    def setup(self):
        """Setup the database connection and create table if it doesn't exist."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
                timestamp TIMESTAMPTZ NOT NULL,
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
                original_timestamp BIGINT
            );
            """
            
            self.cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"SELECT create_hypertable('{self.schema_name}.{self.table_name}', 'timestamp', if_not_exists => TRUE);"
                self.cursor.execute(hypertable_query)
            except Exception as e:
                print(f"Note: Could not create hypertable (this is normal if not using TimescaleDB): {e}")
            
            self.connection.commit()
            
        except Exception as e:
            print(f"Failed to setup TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        """Write a batch of data to TimescaleDB."""
        if not self.connection or not self.cursor:
            raise RuntimeError("Database connection not established")
        
        insert_query = f"""
        INSERT INTO {self.schema_name}.{self.table_name} (
            timestamp, panel_id, location_id, location_name, latitude, longitude,
            timezone, power_output, unit_power, temperature, unit_temp, irradiance,
            unit_irradiance, voltage, unit_voltage, current, unit_current,
            inverter_status, original_timestamp
        ) VALUES (
            %(timestamp)s, %(panel_id)s, %(location_id)s, %(location_name)s,
            %(latitude)s, %(longitude)s, %(timezone)s, %(power_output)s,
            %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s,
            %(unit_irradiance)s, %(voltage)s, %(unit_voltage)s, %(current)s,
            %(unit_current)s, %(inverter_status)s, %(original_timestamp)s
        )
        """
        
        records = []
        for item in batch:
            try:
                # Parse the JSON string in the value field
                data = json.loads(item.value)
                
                # Convert epoch timestamp to PostgreSQL timestamp
                from datetime import datetime
                timestamp = datetime.fromtimestamp(data['timestamp'] / 1000000000)  # Convert nanoseconds to seconds
                
                record = {
                    'timestamp': timestamp,
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
                    'original_timestamp': data.get('timestamp')
                }
                records.append(record)
                
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
        
        if records:
            try:
                self.cursor.executemany(insert_query, records)
                self.connection.commit()
                print(f"Successfully inserted {len(records)} records")
            except Exception as e:
                print(f"Error inserting records: {e}")
                self.connection.rollback()
                raise

    def flush(self):
        """Flush any remaining data."""
        if self.connection:
            self.connection.commit()

    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
    port=int(os.environ.get('TIMESCALEDB_PORT', '5432')),
    database=os.environ.get('TIMESCALEDB_DATABASE', 'postgres'),
    user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data'),
    schema_name=os.environ.get('TIMESCALEDB_SCHEMA', 'public')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'timescale-sink-group'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get('BATCH_TIMEOUT', '1')),
    commit_every=int(os.environ.get('BATCH_SIZE', '1000'))
)

# Define the input topic
input_topic = app.topic(os.environ.get('input', 'solar-data'), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print to see raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)