import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch


class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name, on_client_connect_success=None, on_client_connect_failure=None):
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

            # Notify Quix Streams that the client connected successfully
            if self._on_client_connect_success:
                self._on_client_connect_success()
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")

            # Notify Quix Streams that the client failed to connect
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            timestamp TIMESTAMPTZ NOT NULL,
            data JSONB NOT NULL
        );
        """
        
        self._cursor.execute(create_table_sql)
        self._connection.commit()
        print(f"Table {self.table_name} created or already exists")

        # Create hypertable if it doesn't exist (TimescaleDB specific)
        try:
            hypertable_sql = f"SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);"
            self._cursor.execute(hypertable_sql)
            self._connection.commit()
            print(f"Hypertable created for {self.table_name}")
        except Exception as e:
            print(f"Note: Could not create hypertable (may already exist): {e}")
            self._connection.rollback()

    def write(self, batch: SinkBatch):
        """Write a batch of data to TimescaleDB."""
        if not batch:
            return

        insert_sql = f"INSERT INTO {self.table_name} (timestamp, data) VALUES %s"
        values = []
        
        for item in batch:
            # Convert timestamp to datetime if it's an epoch timestamp
            if hasattr(item, 'timestamp') and item.timestamp is not None:
                timestamp = datetime.fromtimestamp(item.timestamp / 1000.0)
            else:
                timestamp = datetime.now()
            
            # Handle the message value - treat as already parsed dictionary
            if hasattr(item, 'value') and item.value is not None:
                data_dict = item.value
            else:
                # If no value field, use the entire item as data
                data_dict = {"message": str(item)}
            
            values.append((timestamp, json.dumps(data_dict)))

        # Use psycopg2's execute_values for efficient batch insert
        from psycopg2.extras import execute_values
        execute_values(self._cursor, insert_sql, values)
        self._connection.commit()
        print(f"Inserted {len(values)} records into {self.table_name}")

    def close(self):
        """Close the database connection."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        print("TimescaleDB connection closed")


# Initialize the application
app = Application(
    consumer_group="timescaledb-sink-consumer",
    auto_offset_reset="earliest",
    commit_interval=1.0,
    commit_every=1000
)

# Get environment variables with safe port handling
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav7')
)

# Define the input topic
input_topic = app.topic("input", value_deserializer="json")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print for raw messages
def debug_message(item):
    print(f"Raw message: {item}")
    return item

sdf = sdf.apply(debug_message)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run()