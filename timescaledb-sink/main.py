import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name):
        super().__init__()
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._table_name = table_name
        self._connection = None
        self._cursor = None

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password
            )
            self._cursor = self._connection.cursor()
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                panel_id TEXT,
                location_id TEXT,
                location_name TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                timezone INTEGER,
                power_output INTEGER,
                unit_power TEXT,
                temperature DOUBLE PRECISION,
                unit_temp TEXT,
                irradiance INTEGER,
                unit_irradiance TEXT,
                voltage DOUBLE PRECISION,
                unit_voltage TEXT,
                current INTEGER,
                unit_current TEXT,
                inverter_status TEXT,
                timestamp TIMESTAMP WITH TIME ZONE,
                datetime TIMESTAMP WITH TIME ZONE
            );
            """
            self._cursor.execute(create_table_query)
            self._connection.commit()
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"SELECT create_hypertable('{self._table_name}', 'timestamp', if_not_exists => TRUE);"
                self._cursor.execute(hypertable_query)
                self._connection.commit()
            except Exception as e:
                print(f"Note: Could not create hypertable (this is normal if not using TimescaleDB): {e}")
                
        except Exception as e:
            print(f"Error setting up TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        if not self._connection or not self._cursor:
            raise RuntimeError("Database connection not established")

        insert_query = f"""
        INSERT INTO {self._table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, datetime
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """

        records = []
        for item in batch:
            try:
                print(f'Raw message: {item}')
                
                # item.value is usually a dict already. Only decode if
                # the broker delivered raw bytes/str for some reason.
                if isinstance(item.value, dict):
                    data = item.value
                else:
                    data = json.loads(item.value)

                # nanoseconds → seconds
                timestamp_dt = datetime.fromtimestamp(data["timestamp"] / 1_000_000_000)

                # Use header dateTime if it exists, else fallback
                datetime_dt = None
                if hasattr(item, "headers") and item.headers:
                    dt_str = item.headers.get("dateTime")
                    if dt_str:
                        datetime_dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                if datetime_dt is None:
                    datetime_dt = timestamp_dt

                record = (
                    data.get("panel_id"),
                    data.get("location_id"),
                    data.get("location_name"),
                    data.get("latitude"),
                    data.get("longitude"),
                    data.get("timezone"),
                    data.get("power_output"),
                    data.get("unit_power"),
                    data.get("temperature"),
                    data.get("unit_temp"),
                    data.get("irradiance"),
                    data.get("unit_irradiance"),
                    data.get("voltage"),
                    data.get("unit_voltage"),
                    data.get("current"),
                    data.get("unit_current"),
                    data.get("inverter_status"),
                    timestamp_dt,
                    datetime_dt,
                )
                records.append(record)
            except Exception as e:
                print(f"Error preparing record: {e}")
                continue

        if records:
            self._cursor.executemany(insert_query, records)
            self._connection.commit()

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Initialize TimescaleDB Sink
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
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solarv3')
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