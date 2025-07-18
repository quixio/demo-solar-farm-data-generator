import os
import json
import psycopg2
from psycopg2 import sql
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch


class TimescaleDBSink(BatchingSink):
    def __init__(self,
                 host: str,
                 port: int,
                 dbname: str,
                 user: str,
                 password: str,
                 table_name: str,
                 json_column: str = "payload",
                 on_client_connect_success=None,
                 on_client_connect_failure=None):
        # Ensure we always have a non-empty column name
        json_column = (json_column or "payload").strip()
        
        super().__init__(on_client_connect_success, on_client_connect_failure)
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
        """Set up the database connection and create table if needed."""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor()
            print(f"Connected to TimescaleDB at {self.host}:{self.port}")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self.on_client_connect_success:
                self.on_client_connect_success()
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            if self.on_client_connect_failure:
                self.on_client_connect_failure()
            raise

    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist."""
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                timestamp TIMESTAMPTZ NOT NULL,
                {json_col} JSONB
            );
        """).format(
            table=sql.Identifier(self.table_name),
            json_col=sql.Identifier(self.json_column)
        )
        
        self._cursor.execute(create_table_query)
        self._connection.commit()
        print(f"Table {self.table_name} created or already exists")

    def write(self, batch: SinkBatch):
        """Write a batch of records to TimescaleDB."""
        if not batch:
            return

        insert_query = sql.SQL("""
            INSERT INTO {table} (timestamp, {json_col})
            VALUES (%s, %s);
        """).format(
            table=sql.Identifier(self.table_name),
            json_col=sql.Identifier(self.json_column)
        )

        records = []
        for item in batch:
            # Convert timestamp from milliseconds to datetime
            ts = datetime.fromtimestamp(item.timestamp / 1000.0)
            
            # Handle message value - check if it's already parsed or needs parsing
            if isinstance(item.value, (str, bytes)):
                try:
                    value_data = json.loads(item.value)
                except (json.JSONDecodeError, TypeError):
                    value_data = item.value
            else:
                value_data = item.value
            
            records.append((ts, json.dumps(value_data)))

        self._cursor.executemany(insert_query, records)
        self._connection.commit()
        print(f"Successfully inserted {len(records)} records into {self.table_name}")

    def close(self):
        """Close database connection."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        print("Closed TimescaleDB connection")


# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "input"), value_deserializer="json")

# Get environment variables for TimescaleDB connection
host = os.environ.get('TIMESCALEDB_HOST', 'timescaledb')
username = os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin')
password = os.environ.get('TIMESCALEDB_PASSWORD', 'TIMESCALE_PASSWORD')
dbname = os.environ.get('TIMESCALEDB_DBNAME', 'metrics')

# Handle port with error handling
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Get table name - check both possible environment variables
table_name = os.environ.get('TIMESCALEDB_TABLENAME') or os.environ.get('TIMESCALEDB_TABLE_NAME', 'timescaledb_sink')

# Get JSON column name with fallback
json_column = os.environ.get("TIMESCALEDB_JSONCOLUMN") or "payload"

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=host,
    port=port,
    dbname=dbname,
    user=username,
    password=password,
    table_name=table_name,
    json_column=json_column
)

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print to see raw message structure
sdf = sdf.apply(lambda value: print(f'Raw message: {value}') or value)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    # Run for exactly 10 messages then stop
    app.run(count=10, timeout=20)