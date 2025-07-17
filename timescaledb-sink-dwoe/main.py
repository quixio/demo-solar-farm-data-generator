import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._host = host
        self._port = port
        self._dbname = dbname
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
                dbname=self._dbname,
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
                timestamp TIMESTAMPTZ,
                PRIMARY KEY (panel_id, timestamp)
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
                # If hypertable creation fails, it might already exist or TimescaleDB extension not enabled
                print(f"Note: Could not create hypertable: {e}")
                self._connection.rollback()
            
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
        INSERT INTO {self._table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        records = []
        for item in batch:
            # Parse the JSON value from the message
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
            
            # Convert epoch timestamp to datetime
            timestamp_epoch = data.get('timestamp', 0)
            if isinstance(timestamp_epoch, (int, float)):
                # Handle nanosecond precision timestamp
                if timestamp_epoch > 1e12:  # If timestamp is in nanoseconds
                    timestamp_dt = datetime.fromtimestamp(timestamp_epoch / 1e9)
                else:  # If timestamp is in seconds
                    timestamp_dt = datetime.fromtimestamp(timestamp_epoch)
            else:
                timestamp_dt = datetime.now()
            
            record = (
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
                timestamp_dt
            )
            records.append(record)
        
        try:
            self._cursor.executemany(insert_query, records)
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

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
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav5')
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

# Add debugging to see raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)