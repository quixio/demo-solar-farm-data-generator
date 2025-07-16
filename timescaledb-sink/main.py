import os
from datetime import datetime, timezone
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor, Json

class TimescaleDBSink(BatchingSink):
    """Custom TimescaleDB sink for storing data"""
    
    def __init__(self, host, port, database, user, password, table_name, 
                 on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self._connection = None
        self._cursor = None
    
    def setup(self):
        """Establish database connection and create table if needed"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor(cursor_factory=RealDictCursor)
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                data JSONB NOT NULL
            );
            """
            self._cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"""
                SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);
                """
                self._cursor.execute(hypertable_query)
            except psycopg2.Error:
                # If hypertable creation fails, continue (might be regular PostgreSQL)
                pass
                
            self._connection.commit()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def write(self, batch: SinkBatch):
        """Write batch data to TimescaleDB"""
        if not self._connection or not self._cursor:
            raise RuntimeError("Database connection not established")
        
        try:
            records = []
            for item in batch:
                # Convert nanosecond timestamp to datetime
                ts = item.timestamp
                if isinstance(ts, (int, float)):
                    ts = datetime.fromtimestamp(ts / 1e9, tz=timezone.utc)
                
                # Wrap the dict with Json() for psycopg2
                records.append((ts, Json(item.value, dumps=None)))
            
            insert_query = f"""
            INSERT INTO {self.table_name} (timestamp, data)
            VALUES %s
            """
            
            execute_values(
                self._cursor,
                insert_query,
                records,
                template=None,
                page_size=1000
            )
            
            self._connection.commit()
            
        except Exception as e:
            self._connection.rollback()
            raise
    
    def close(self):
        """Close database connection"""
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

# Parse port with error handling
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    database=os.environ.get('TIMESCALEDB_DATABASE', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USER', 'tsadmin'),
    password=os.environ.get('TIMESCALE_PASSWORD_SECRET_KEY'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solarv3')
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "input-topic"))

# Create streaming dataframe
sdf = app.dataframe(input_topic)

# Debug print to show message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)