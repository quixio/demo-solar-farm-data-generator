import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
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
        self._table_created = False

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._dbname,
                user=self._user,
                password=self._password
            )
            self._connection.autocommit = True
            
            # Create table if it doesn't exist
            if not self._table_created:
                self._create_table_if_not_exists()
                self._table_created = True
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            timestamp TIMESTAMPTZ NOT NULL,
            data JSONB
        );
        """
        
        # Create hypertable for TimescaleDB
        hypertable_query = f"""
        SELECT create_hypertable('{self._table_name}', 'timestamp', if_not_exists => TRUE);
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_query)
            try:
                cursor.execute(hypertable_query)
            except Exception:
                # If hypertable creation fails, continue without it
                pass
            self._connection.commit()

    def write(self, batch: SinkBatch):
        if not self._connection:
            self.setup()
            
        with self._connection.cursor() as cursor:
            for item in batch:
                # Convert timestamp to datetime if it's an epoch timestamp
                timestamp = datetime.fromtimestamp(item.timestamp / 1000.0)
                
                # Store the entire message value as JSONB
                data = item.value
                
                insert_query = f"""
                INSERT INTO {self._table_name} (timestamp, data)
                VALUES (%s, %s)
                """
                
                cursor.execute(insert_query, (timestamp, json.dumps(data)))
            
            self._connection.commit()

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
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav7')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "input-topic"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print to see raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)