import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
        """Setup database connection and create table if needed"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            self.create_table_if_not_exists()
            print("TimescaleDB connection established successfully")
        except Exception as e:
            print(f"Error setting up TimescaleDB connection: {e}")
            raise

    def create_table_if_not_exists(self):
        """Create the target table or patch it so that the expected columns are present"""
        # 1) Create the table if it does not exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            event_time      TIMESTAMPTZ NOT NULL,
            panel_id        TEXT        NOT NULL,
            location_id     TEXT        NOT NULL,
            location_name   TEXT        NOT NULL,
            latitude        DOUBLE PRECISION NOT NULL,
            longitude       DOUBLE PRECISION NOT NULL,
            timezone        INTEGER     NOT NULL,
            power_output    DOUBLE PRECISION NOT NULL,
            unit_power      TEXT        NOT NULL,
            temperature     DOUBLE PRECISION NOT NULL,
            unit_temp       TEXT        NOT NULL,
            irradiance      DOUBLE PRECISION NOT NULL,
            unit_irradiance TEXT        NOT NULL,
            voltage         DOUBLE PRECISION NOT NULL,
            unit_voltage    TEXT        NOT NULL,
            current         DOUBLE PRECISION NOT NULL,
            unit_current    TEXT        NOT NULL,
            inverter_status TEXT        NOT NULL,
            timestamp_raw   BIGINT      NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)

        # 2) Ensure the column exists if the table was created earlier
        self.cursor.execute(
            f'ALTER TABLE {self.table_name} '
            f'ADD COLUMN IF NOT EXISTS event_time TIMESTAMPTZ NOT NULL DEFAULT now();'
        )

        # 3) Create (or keep) the hypertable
        self.cursor.execute(
            f"SELECT create_hypertable('{self.table_name}', 'event_time', if_not_exists => TRUE);"
        )
        self.connection.commit()

    def write(self, batch: SinkBatch):
        """Write batch of records to TimescaleDB"""
        records = []
        
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the nested JSON from the value field
            try:
                if hasattr(item, 'value') and isinstance(item.value, str):
                    solar_data = json.loads(item.value)
                elif hasattr(item, 'value') and isinstance(item.value, dict):
                    solar_data = item.value
                else:
                    print(f"Unexpected message structure: {item}")
                    continue
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON from value field: {e}")
                continue

            # Extract timestamp and convert to datetime
            timestamp_ns = solar_data.get('timestamp', 0)
            record_time = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)

            # Create record tuple matching table schema
            record = (
                record_time,                      # event_time
                solar_data.get('panel_id', ''),
                solar_data.get('location_id', ''),
                solar_data.get('location_name', ''),
                solar_data.get('latitude', 0.0),
                solar_data.get('longitude', 0.0),
                solar_data.get('timezone', 0),
                solar_data.get('power_output', 0.0),
                solar_data.get('unit_power', ''),
                solar_data.get('temperature', 0.0),
                solar_data.get('unit_temp', ''),
                solar_data.get('irradiance', 0.0),
                solar_data.get('unit_irradiance', ''),
                solar_data.get('voltage', 0.0),
                solar_data.get('unit_voltage', ''),
                solar_data.get('current', 0.0),
                solar_data.get('unit_current', ''),
                solar_data.get('inverter_status', ''),
                timestamp_ns
            )
            records.append(record)

        if records:
            insert_sql = f"""
            INSERT INTO {self.table_name} (
                event_time, panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status, timestamp_raw
            ) VALUES %s
            """
            execute_values(self.cursor, insert_sql, records)
            self.connection.commit()
            print(f"Successfully wrote {len(records)} records to TimescaleDB")

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
    port=int(os.environ.get('TIMESCALEDB_PORT', 5432)),
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'timescaledb'),
    user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'data_sink')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "5.0")),
    commit_every=int(os.environ.get("BATCH_SIZE", "100"))
)

# Define the input topic
input_topic = app.topic(
    os.environ.get("input", "solar-data"),
    key_deserializer="string",
    value_deserializer="string"
)

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)