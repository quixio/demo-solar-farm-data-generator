import os
import json
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
load_dotenv()

class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink that extends BatchingSink
    """
    def __init__(self, host, port, database, user, password, table_name, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self._connection = None
        self._cursor = None

    def setup(self):
        """Establish connection to TimescaleDB"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            self._cursor = self._connection.cursor()
            self._create_table_if_not_exists()
            print(f"Connected to TimescaleDB at {self.host}:{self.port}")
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            timestamp TIMESTAMPTZ NOT NULL,
            data JSONB NOT NULL
        );
        """
        
        # Create hypertable if it doesn't exist
        hypertable_query = f"""
        SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);
        """
        
        try:
            self._cursor.execute(create_table_query)
            self._cursor.execute(hypertable_query)
            self._connection.commit()
            print(f"Table {self.table_name} created/verified as hypertable")
        except Exception as e:
            print(f"Error creating table: {e}")
            self._connection.rollback()
            raise

    def write(self, batch: SinkBatch):
        """Write batch data to TimescaleDB"""
        if not batch:
            return
            
        insert_query = f"""
        INSERT INTO {self.table_name} (timestamp, data)
        VALUES (NOW(), %s)
        """
        
        try:
            for item in batch:
                # Convert the message data to JSON string
                data_json = json.dumps(item.value)
                self._cursor.execute(insert_query, (data_json,))
            
            self._connection.commit()
            print(f"Successfully wrote {len(batch)} records to TimescaleDB")
            
        except Exception as e:
            print(f"Error writing to TimescaleDB: {e}")
            self._connection.rollback()
            raise

    def close(self):
        """Close database connection"""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        print("TimescaleDB connection closed")

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
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solarv3'),
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

# Add debugging to show raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run()