import os
import psycopg2
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.table_columns = set()

    def setup(self):
        """Establish connection to QuestDB and guarantee the table exists."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            self.connection.autocommit = True
            print(f"Connected to QuestDB at {self.host}:{self.port}")

            # Create the table first (no harm if it already exists)
            self._create_table_if_not_exists()

            # Now it is safe to cache its columns
            self._cache_table_columns()

        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create table with initial schema if it doesn't exist"""
        cursor = self.connection.cursor()
        try:
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TIMESTAMP,
                data_json STRING
            ) timestamp(timestamp) PARTITION BY DAY
            """
            cursor.execute(create_table_sql)
            print(f"Table {self.table_name} created or already exists")
        except Exception as e:
            print(f"Error creating table: {e}")
            raise
        finally:
            cursor.close()

    def _cache_table_columns(self):
        """
        Read column names that already exist in self.table_name
        and cache them in self.table_columns.
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                """,
                (self.table_name,),
            )
            self.table_columns = {row[0] for row in cursor.fetchall()}
            print(f"Cached columns for table {self.table_name}: {self.table_columns}")
        except Exception as e:
            print(f"Error caching table columns: {e}")
            # If we can't get columns, assume basic structure
            self.table_columns = {"timestamp", "data_json"}
        finally:
            cursor.close()

    def write(self, batch: SinkBatch):
        """Write batch of messages to QuestDB"""
        if not batch:
            return

        cursor = self.connection.cursor()
        try:
            insert_sql = f"INSERT INTO {self.table_name} (timestamp, data_json) VALUES (%s, %s)"
            
            records = []
            for item in batch:
                print(f"Raw message: {item}")
                
                # Get timestamp - use current time if not available
                timestamp = datetime.now()
                if hasattr(item, 'timestamp') and item.timestamp:
                    try:
                        # Convert from milliseconds to seconds if needed
                        ts_value = item.timestamp
                        if ts_value > 1e12:  # Likely milliseconds
                            ts_value = ts_value / 1000
                        timestamp = datetime.fromtimestamp(ts_value)
                    except:
                        timestamp = datetime.now()
                
                # Handle the message value
                data_json = "{}"
                try:
                    if hasattr(item, 'value') and item.value is not None:
                        if isinstance(item.value, (dict, list)):
                            data_json = json.dumps(item.value)
                        elif isinstance(item.value, str):
                            # Try to parse as JSON, if fails treat as plain string
                            try:
                                parsed = json.loads(item.value)
                                data_json = json.dumps(parsed)
                            except:
                                data_json = json.dumps({"message": item.value})
                        else:
                            data_json = json.dumps({"value": str(item.value)})
                    else:
                        # If no value field, try to serialize the entire item
                        data_json = json.dumps({"raw_item": str(item)})
                except Exception as e:
                    print(f"Error processing message value: {e}")
                    data_json = json.dumps({"error": f"Failed to process: {str(e)}"})
                
                records.append((timestamp, data_json))
            
            cursor.executemany(insert_sql, records)
            print(f"Successfully wrote {len(records)} records to {self.table_name}")
            
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise
        finally:
            cursor.close()

    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            print("QuestDB connection closed")

# Initialize application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_TIMEOUT", "5.0")),
)

# Get input topic
input_topic_name = os.environ.get('QUESTDB_INPUT_TOPIC')
if not input_topic_name:
    raise ValueError("QUESTDB_INPUT_TOPIC environment variable is required")

input_topic = app.topic(input_topic_name)

# Initialize QuestDB sink
try:
    port = int(os.environ.get('QUESTDB_PORT', '8812'))
except ValueError:
    port = 8812

questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST', 'localhost'),
    port=port,
    database=os.environ.get('QUESTDB_DATABASE', 'qdb'),
    user=os.environ.get('QUESTDB_USER', 'admin'),
    password=os.environ.get('QUESTDB_PASSWORD_KEY', ''),
    table_name=os.environ.get('QUESTDB_TABLE_NAME', 'data_sink')
)

# Create streaming dataframe and sink
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run()