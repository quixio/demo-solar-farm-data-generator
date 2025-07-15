import os
import json
import psycopg2
from psycopg2.extras import execute_values
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
        self.connection = None
        self.cursor = None

    def setup(self):
        """Initialize the connection to TimescaleDB"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            self.create_table_if_not_exists()
            if self._on_client_connect_success:
                self._on_client_connect_success()
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def create_table_if_not_exists(self):
        """Create the table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            time TIMESTAMPTZ NOT NULL,
            panel_id TEXT NOT NULL,
            location_id TEXT NOT NULL,
            location_name TEXT NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            timezone INTEGER NOT NULL,
            power_output FLOAT NOT NULL,
            unit_power TEXT NOT NULL,
            temperature FLOAT NOT NULL,
            unit_temp TEXT NOT NULL,
            irradiance FLOAT NOT NULL,
            unit_irradiance TEXT NOT NULL,
            voltage FLOAT NOT NULL,
            unit_voltage TEXT NOT NULL,
            current FLOAT NOT NULL,
            unit_current TEXT NOT NULL,
            inverter_status TEXT NOT NULL,
            timestamp_raw BIGINT NOT NULL
        );
        """
        self.cursor.execute(create_table_query)
        
        # Create hypertable if it doesn't exist
        self.cursor.execute(f"""
        SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);
        """)
        
        self.connection.commit()

    def write(self, batch: SinkBatch):
        """Write a batch of data to TimescaleDB"""
        if not batch:
            return
            
        records = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # item.value can already be a dict if a JSON deserializer is used
            if isinstance(item.value, dict):
                solar_data = item.value
            elif isinstance(item.value, (bytes, bytearray)):
                solar_data = json.loads(item.value.decode())
            elif isinstance(item.value, str):
                solar_data = json.loads(item.value)
            else:
                print(f"Unknown value type {type(item.value)} – skipping")
                continue
            
            # Extract timestamp/header safely
            datetime_str = getattr(item, "timestamp", None)
            if item.headers and "datetime" in item.headers:
                datetime_str = item.headers["datetime"]
            
            # Convert nanosecond timestamp to milliseconds for PostgreSQL
            timestamp_ns = solar_data.get('timestamp', 0)
            timestamp_ms = timestamp_ns // 1_000_000
            
            # Use the datetime from message or convert timestamp
            if datetime_str:
                record_time = datetime_str
            else:
                record_time = f"to_timestamp({timestamp_ms / 1000.0})"
            
            record = (
                record_time,
                solar_data.get('panel_id', ''),
                solar_data.get('location_id', ''),
                solar_data.get('location_name', ''),
                solar_data.get('latitude', 0.0),
                solar_data.get('longitude', 0.0),
                solar_data.get('timezone', 0),
                solar_data.get('power_output', 0.0),
                solar_data.get('unit_power', 'W'),
                solar_data.get('temperature', 0.0),
                solar_data.get('unit_temp', 'C'),
                solar_data.get('irradiance', 0.0),
                solar_data.get('unit_irradiance', 'W/m²'),
                solar_data.get('voltage', 0.0),
                solar_data.get('unit_voltage', 'V'),
                solar_data.get('current', 0.0),
                solar_data.get('unit_current', 'A'),
                solar_data.get('inverter_status', 'OK'),
                timestamp_ns
            )
            records.append(record)
        
        if records:
            insert_query = f"""
            INSERT INTO {self.table_name} (
                time, panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status, timestamp_raw
            ) VALUES %s
            """
            execute_values(self.cursor, insert_query, records)
            self.connection.commit()
            print(f"Successfully inserted {len(records)} records into TimescaleDB")

    def close(self):
        """Close the connection to TimescaleDB"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
    port=int(os.environ.get('TIMESCALEDB_PORT', '5432')),
    database=os.environ.get('TIMESCALEDB_DATABASE', 'solar_data'),
    user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_measurements')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(
    os.environ.get("input", "solar-data"),
    key_deserializer="string",
    value_deserializer="json"
)

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)