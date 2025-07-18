import os
import json
import psycopg2
from datetime import datetime
from typing import List, Dict, Any
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str, 
                 table_name: str, json_column: str = "payload", 
                 on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.json_column = json_column
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
            self._create_table_if_not_exists()
            print(f"Connected to TimescaleDB at {self.host}:{self.port}")
            if self._on_client_connect_success:
                self._on_client_connect_success()
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            timestamp TIMESTAMPTZ NOT NULL,
            {self.json_column} JSONB
        );
        """
        try:
            self._cursor.execute(create_table_query)
            self._connection.commit()
            print(f"Table {self.table_name} created or already exists")
        except Exception as e:
            print(f"Error creating table: {e}")
            self._connection.rollback()
            raise

    def write(self, batch: SinkBatch):
        if not batch:
            return

        insert_query = f"""
        INSERT INTO {self.table_name} (timestamp, {self.json_column})
        VALUES (%s, %s);
        """
        
        records = []
        for item in batch:
            # Convert epoch timestamp to datetime
            timestamp = datetime.fromtimestamp(item.timestamp / 1000.0)
            
            # Handle the message value
            if isinstance(item.value, (str, bytes)):
                try:
                    value_data = json.loads(item.value)
                except (json.JSONDecodeError, TypeError):
                    value_data = item.value
            else:
                value_data = item.value
            
            records.append((timestamp, json.dumps(value_data)))
        
        try:
            self._cursor.executemany(insert_query, records)
            self._connection.commit()
            print(f"Successfully inserted {len(records)} records into {self.table_name}")
        except Exception as e:
            print(f"Error inserting data: {e}")
            self._connection.rollback()
            raise

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        print("TimescaleDB connection closed")

# Get port with error handling
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Allow overriding the JSON column through an environment variable
json_column = os.environ.get("TIMESCALEDB_JSONCOLUMN", "payload")

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'timescaledb_sink'),
    json_column=json_column
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "input-topic"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print to see raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)