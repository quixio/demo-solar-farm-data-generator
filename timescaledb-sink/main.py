import os
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from datetime import datetime
import json

# Load environment variables from a .env file for local development
from dotenv import load_dotenv
load_dotenv()

class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink implementation
    """
    def __init__(self, host, port, dbname, user, password, table_name, 
                 on_client_connect_success=None, on_client_connect_failure=None):
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
        """Establish connection to TimescaleDB"""
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._dbname,
                user=self._user,
                password=self._password,
                cursor_factory=RealDictCursor
            )
            self._connection.autocommit = False
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist"""
        if self._table_created:
            return
            
        cursor = self._connection.cursor()
        try:
            # Create table with schema that matches typical solar data
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                timestamp TIMESTAMPTZ NOT NULL,
                value DOUBLE PRECISION,
                PRIMARY KEY (timestamp)
            );
            """
            
            cursor.execute(create_table_query)
            
            # Convert to hypertable for TimescaleDB
            hypertable_query = f"""
            SELECT create_hypertable('{self._table_name}', 'timestamp', if_not_exists => TRUE);
            """
            
            try:
                cursor.execute(hypertable_query)
            except psycopg2.Error:
                # Table might already be a hypertable, ignore error
                pass
            
            self._connection.commit()
            self._table_created = True
            
        except Exception as e:
            self._connection.rollback()
            raise
        finally:
            cursor.close()

    def write(self, batch: SinkBatch):
        """Write batch of data to TimescaleDB"""
        if not self._connection:
            raise RuntimeError("Connection not established")
            
        cursor = self._connection.cursor()
        try:
            for item in batch:
                # Print raw message for debugging
                print(f'Raw message: {item}')
                
                # Extract data from message
                # Handle different message structures safely
                if hasattr(item, 'value') and item.value is not None:
                    data = item.value
                    if isinstance(data, (str, bytes)):
                        data = json.loads(data)
                else:
                    # Data might be directly in the message
                    data = item
                
                # Extract timestamp and value
                timestamp = None
                value = None
                
                # Try to get timestamp from various possible fields
                if isinstance(data, dict):
                    timestamp = data.get('timestamp') or data.get('time') or data.get('ts')
                    value = data.get('value') or data.get('val') or data.get('data')
                
                # If no timestamp found, use current time
                if timestamp is None:
                    timestamp = datetime.now()
                else:
                    # Convert epoch timestamp to datetime if needed
                    if isinstance(timestamp, (int, float)):
                        timestamp = datetime.fromtimestamp(timestamp)
                    elif isinstance(timestamp, str):
                        try:
                            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        except:
                            timestamp = datetime.now()
                
                # Insert data into table
                insert_query = f"""
                INSERT INTO {self._table_name} (timestamp, value)
                VALUES (%s, %s)
                ON CONFLICT (timestamp) DO UPDATE SET value = EXCLUDED.value;
                """
                
                cursor.execute(insert_query, (timestamp, value))
            
            self._connection.commit()
            
        except Exception as e:
            self._connection.rollback()
            raise
        finally:
            cursor.close()

    def close(self):
        """Close the connection"""
        if self._connection:
            self._connection.close()

# Get connection parameters from environment variables
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DATABASE', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USER', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD_SECRET_KEY'),
    table_name=os.environ.get('TIMESCALEDB_TABLE_NAME', 'solar_datav9')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
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
    app.run(count=10, timeout=20)