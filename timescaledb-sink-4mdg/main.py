import os
import json
import psycopg2
from datetime import datetime, timezone
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name):
        super().__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.cursor = None

    def setup(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TIMESTAMPTZ NOT NULL,
                panel_id TEXT,
                location_id TEXT,
                location_name TEXT,
                latitude FLOAT,
                longitude FLOAT,
                timezone INTEGER,
                power_output INTEGER,
                unit_power TEXT,
                temperature FLOAT,
                unit_temp TEXT,
                irradiance INTEGER,
                unit_irradiance TEXT,
                voltage FLOAT,
                unit_voltage TEXT,
                current INTEGER,
                unit_current TEXT,
                inverter_status TEXT,
                message_timestamp TIMESTAMPTZ
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);"
                self.cursor.execute(hypertable_query)
                self.connection.commit()
            except Exception as e:
                print(f"Hypertable creation failed (might already exist): {e}")
                
        except Exception as e:
            print(f"Error setting up TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        if not self.connection or not self.cursor:
            raise RuntimeError("Database connection not established")
        
        rows = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse JSON in item.value
            try:
                data = json.loads(item.value) if isinstance(item.value, str) else item.value
            except (json.JSONDecodeError, TypeError) as e:
                print(f"Error parsing JSON: {e}")
                continue
            
            # Convert sensor timestamp (inside payload) to datetime
            timestamp_ns = data.get('timestamp')
            timestamp_dt = (
                datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
                if timestamp_ns else datetime.now(tz=timezone.utc)
            )
            
            # Convert Kafka / Quix message timestamp
            tsi = item.timestamp
            if isinstance(tsi, str):
                message_datetime = datetime.fromisoformat(tsi.replace('Z', '+00:00'))
            else:
                message_datetime = datetime.fromtimestamp(tsi / 1_000_000_000, tz=timezone.utc)
            
            row = (
                timestamp_dt,
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
                message_datetime
            )
            rows.append(row)
        
        if rows:
            insert_query = f"""
            INSERT INTO {self.table_name} (
                timestamp, panel_id, location_id, location_name, latitude, longitude,
                timezone, power_output, unit_power, temperature, unit_temp, irradiance,
                unit_irradiance, voltage, unit_voltage, current, unit_current,
                inverter_status, message_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.executemany(insert_query, rows)
            self.connection.commit()
            print(f"Inserted {len(rows)} rows into {self.table_name}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize the application
app = Application(
    consumer_group="timescaledb-sink-consumer",
    auto_offset_reset="earliest",
    commit_interval=1.0,
    commit_every=1000
)

# Get environment variables with safe port handling
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav6')
)

# Define the input topic
input_topic = app.topic("input", value_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)