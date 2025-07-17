import os
import json
import psycopg2
from datetime import datetime
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
        self._connection = None
        self._cursor = None

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor()
            
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
                inverter_status TEXT
            );
            """
            
            self._cursor.execute(create_table_query)
            self._connection.commit()
            print(f"Table {self.table_name} created or already exists")
            
        except Exception as e:
            print(f"Error setting up TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not initialized")

        insert_query = f"""
        INSERT INTO {self.table_name} (
            timestamp, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        rows_in_batch = 0

        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON value if it's a string
            data = json.loads(item.value) if isinstance(item.value, str) else item.value

            # Convert epoch nanoseconds to datetime
            ts_epoch = data.get("timestamp", 0)
            ts_dt = datetime.fromtimestamp(ts_epoch / 1_000_000_000) if isinstance(ts_epoch, (int, float)) else datetime.now()

            values = (
                ts_dt,
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
                data.get('inverter_status')
            )

            self._cursor.execute(insert_query, values)
            rows_in_batch += 1

        self._connection.commit()
        print(f"Successfully inserted {rows_in_batch} records into TimescaleDB")

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
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav3')
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), value_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run()