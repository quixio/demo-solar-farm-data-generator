import os
import json
import psycopg2
from datetime import datetime, timezone
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
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._table_name = table_name
        self._connection = None

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password
            )
            self._connection.autocommit = False
            
            # Create table if it doesn't exist
            cursor = self._connection.cursor()
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                timestamp TIMESTAMPTZ NOT NULL,
                value DOUBLE PRECISION,
                PRIMARY KEY (timestamp)
            );
            """
            cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"SELECT create_hypertable('{self._table_name}', 'timestamp', if_not_exists => TRUE);"
                cursor.execute(hypertable_query)
            except psycopg2.Error:
                # If hypertable creation fails, continue anyway
                pass
            
            self._connection.commit()
            cursor.close()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def write(self, batch: SinkBatch):
        cursor = self._connection.cursor()
        try:
            for item in batch:
                print(f'Raw message: {item}')

                # 1. Extract original JSON payload
                data_raw = item.value if hasattr(item, 'value') else item
                if isinstance(data_raw, bytes):
                    data_raw = data_raw.decode()          # bytes → str
                if isinstance(data_raw, str):
                    data = json.loads(data_raw)            # str → dict
                elif isinstance(data_raw, dict):
                    data = data_raw
                else:
                    continue  # skip unexpected structures

                # 2. Timestamp handling
                ts = data.get("timestamp")                 # nanoseconds
                if ts is None:
                    ts_dt = datetime.now(timezone.utc)
                elif isinstance(ts, (int, float)):
                    # decide the unit based on magnitude
                    if ts > 1e14:             # nanoseconds
                        ts_dt = datetime.fromtimestamp(ts / 1e9, tz=timezone.utc)
                    elif ts > 1e11:           # milliseconds
                        ts_dt = datetime.fromtimestamp(ts / 1e3, tz=timezone.utc)
                    else:                     # seconds
                        ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                else:                                         # ISO-8601 string etc.
                    try:
                        ts_dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
                    except Exception:
                        ts_dt = datetime.now(timezone.utc)

                # 3. Choose the metric to store
                value = data.get("power_output")   # store power (W)

                # 4. Insert into TimescaleDB
                insert_query = f"""
                INSERT INTO {self._table_name} (timestamp, value)
                VALUES (%s, %s)
                ON CONFLICT (timestamp) DO UPDATE SET value = EXCLUDED.value;
                """
                cursor.execute(insert_query, (ts_dt, value))
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise
        finally:
            cursor.close()

    def close(self):
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
    database=os.environ.get('TIMESCALEDB_DATABASE', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USER', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD_SECRET_KEY'),
    table_name=os.environ.get('TIMESCALEDB_TABLE_NAME', 'solar_datav9')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ["input"], key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run()