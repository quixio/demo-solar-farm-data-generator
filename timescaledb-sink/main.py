import os
import json
import psycopg2
from psycopg2.extras import execute_values
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, 
                 host,
                 port,
                 database,
                 user,
                 password,
                 table_name,
                 **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.cursor = None

    def setup(self):
        """Setup database connection and create table if not exists"""
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
            print(f"Connected to TimescaleDB successfully")
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            raise

    def create_table_if_not_exists(self):
        """Create table if it doesn't exist with all required columns"""
        # 1) Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            event_time      TIMESTAMPTZ     NOT NULL,
            panel_id        TEXT            NOT NULL,
            location_id     TEXT            NOT NULL,
            location_name   TEXT            NOT NULL,
            latitude        DOUBLE PRECISION NOT NULL,
            longitude       DOUBLE PRECISION NOT NULL,
            timezone        INTEGER         NOT NULL,
            power_output    DOUBLE PRECISION NOT NULL,
            unit_power      TEXT            NOT NULL,
            temperature     DOUBLE PRECISION NOT NULL,
            unit_temp       TEXT            NOT NULL,
            irradiance      DOUBLE PRECISION NOT NULL,
            unit_irradiance TEXT            NOT NULL,
            voltage         DOUBLE PRECISION NOT NULL,
            unit_voltage    TEXT            NOT NULL,
            current         DOUBLE PRECISION NOT NULL,
            unit_current    TEXT            NOT NULL,
            inverter_status TEXT            NOT NULL,
            timestamp_raw   BIGINT          NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)

        # 2) List of columns that must exist
        required_cols = {
            "event_time":     "TIMESTAMPTZ NOT NULL DEFAULT now()",
            "panel_id":       "TEXT",
            "location_id":    "TEXT",
            "location_name":  "TEXT",
            "latitude":       "DOUBLE PRECISION",
            "longitude":      "DOUBLE PRECISION",
            "timezone":       "INTEGER",
            "power_output":   "DOUBLE PRECISION",
            "unit_power":     "TEXT",
            "temperature":    "DOUBLE PRECISION",
            "unit_temp":      "TEXT",
            "irradiance":     "DOUBLE PRECISION",
            "unit_irradiance":"TEXT",
            "voltage":        "DOUBLE PRECISION",
            "unit_voltage":   "TEXT",
            "current":        "DOUBLE PRECISION",
            "unit_current":   "TEXT",
            "inverter_status":"TEXT",
            "timestamp_raw":  "BIGINT"
        }

        for col_name, col_def in required_cols.items():
            self.cursor.execute(
                f'ALTER TABLE {self.table_name} '
                f'ADD COLUMN IF NOT EXISTS {col_name} {col_def};'
            )

        # 3) Create (or confirm) hypertable
        self.cursor.execute(
            f"SELECT create_hypertable('{self.table_name}', 'event_time', if_not_exists => TRUE);"
        )
        self.connection.commit()

    def write(self, batch: SinkBatch):
        """Write batch of data to TimescaleDB"""
        if not batch:
            return

        # Prepare data for insertion
        rows = []
        for item in batch:
            try:
                # Parse the value field which contains JSON data
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value

                # Convert timestamp to datetime
                event_time = datetime.fromtimestamp(data['timestamp'] / 1_000_000_000)

                row = (
                    event_time,
                    data['panel_id'],
                    data['location_id'],
                    data['location_name'],
                    data['latitude'],
                    data['longitude'],
                    data['timezone'],
                    data['power_output'],
                    data['unit_power'],
                    data['temperature'],
                    data['unit_temp'],
                    data['irradiance'],
                    data['unit_irradiance'],
                    data['voltage'],
                    data['unit_voltage'],
                    data['current'],
                    data['unit_current'],
                    data['inverter_status'],
                    data['timestamp']
                )
                rows.append(row)
            except Exception as e:
                print(f"Error processing item: {e}")
                continue

        if rows:
            # Insert data using execute_values for better performance
            insert_sql = f"""
            INSERT INTO {self.table_name} (
                event_time, panel_id, location_id, location_name, latitude, longitude,
                timezone, power_output, unit_power, temperature, unit_temp, irradiance,
                unit_irradiance, voltage, unit_voltage, current, unit_current,
                inverter_status, timestamp_raw
            ) VALUES %s
            """
            
            execute_values(
                self.cursor,
                insert_sql,
                rows,
                template=None,
                page_size=1000
            )
            self.connection.commit()
            print(f"Inserted {len(rows)} rows into TimescaleDB")

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get("TIMESCALEDB_HOST", "localhost"),
    port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
    database=os.environ.get("TIMESCALEDB_DATABASE", "postgres"),
    user=os.environ.get("TIMESCALEDB_USER", "postgres"),
    password=os.environ.get("TIMESCALEDB_PASSWORD", "password"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "data_sink")
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), value_deserializer="json")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debugging to see raw message structure
def debug_message(item):
    print(f"Raw message: {item}")
    return item

sdf = sdf.apply(debug_message)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)