import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name):
        super().__init__()
        self.host = host
        try:
            self.port = int(port) if port else 8812
        except (ValueError, TypeError):
            self.port = 8812
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.table_columns = set()

    def setup(self):
        """Establish connection to QuestDB and cache table columns"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.connection.autocommit = True
            print(f"Connected to QuestDB at {self.host}:{self.port}")
            self._cache_table_columns()
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise

    def _cache_table_columns(self):
        """
        Read column names that already exist in self.table_name
        and cache them in self.table_columns.
        """
        cursor = self.connection.cursor()
        try:
            # QuestDB helper function table_columns
            cursor.execute(
                "SELECT column_name FROM table_columns(%s)",
                (self.table_name,)
            )
            self.table_columns = {row[0] for row in cursor.fetchall()}
            if not self.table_columns:
                # Table does not exist yet – create it and cache again
                self._create_table_if_not_exists()
                self._cache_table_columns()
        finally:
            cursor.close()

    def _create_table_if_not_exists(self):
        """Create the table with basic schema if it doesn't exist"""
        cursor = self.connection.cursor()
        try:
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TIMESTAMP,
                temperature DOUBLE,
                humidity DOUBLE,
                pressure DOUBLE
            ) TIMESTAMP(timestamp) PARTITION BY DAY
            """
            cursor.execute(create_table_sql)
            self.connection.commit()
            print(f"Table {self.table_name} created or already exists")
        except Exception as e:
            print(f"Error creating table: {e}")
            raise
        finally:
            cursor.close()

    def write(self, batch: SinkBatch):
        """Write batch of records to QuestDB"""
        if not batch:
            return

        cursor = self.connection.cursor()
        try:
            records_to_insert = []
            
            for item in batch:
                print(f'Raw message: {item}')
                
                # Get the record data - check if it's directly in item or nested
                if hasattr(item, 'value') and item.value:
                    record = item.value if isinstance(item.value, dict) else json.loads(item.value)
                else:
                    # Try to get data directly from item
                    record = item if isinstance(item, dict) else {}
                
                # Convert timestamp if present
                if 'timestamp' in record:
                    if isinstance(record['timestamp'], (int, float)):
                        # Convert epoch timestamp to datetime
                        record['timestamp'] = datetime.fromtimestamp(record['timestamp'] / 1000.0 if record['timestamp'] > 1e10 else record['timestamp'])
                    elif isinstance(record['timestamp'], str):
                        try:
                            record['timestamp'] = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
                        except:
                            record['timestamp'] = datetime.now()
                else:
                    record['timestamp'] = datetime.now()

                # Strip fields that are NOT in the table
                filtered = {k: v for k, v in record.items() 
                           if k in self.table_columns}
                # Always keep timestamp
                filtered['timestamp'] = record['timestamp']
                records_to_insert.append(filtered)

            if not records_to_insert:
                return

            # Build insert only with allowed columns
            columns = [col for col in self.table_columns if col != 'timestamp']
            columns.insert(0, 'timestamp')  # make timestamp first
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = (f"INSERT INTO {self.table_name} "
                         f"({', '.join(columns)}) VALUES ({placeholders})")

            # Prepare values for insertion
            values_to_insert = []
            for record in records_to_insert:
                row_values = []
                for col in columns:
                    value = record.get(col)
                    if value is not None:
                        row_values.append(value)
                    else:
                        row_values.append(None)
                values_to_insert.append(tuple(row_values))

            if values_to_insert:
                cursor.executemany(insert_sql, values_to_insert)
                print(f"Inserted {len(values_to_insert)} records into {self.table_name}")

        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise
        finally:
            cursor.close()

    def teardown(self):
        """Clean up database connection"""
        if self.connection:
            self.connection.close()
            print("QuestDB connection closed")

# Create the QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST'),
    port=os.environ.get('QUESTDB_PORT'),
    database=os.environ.get('QUESTDB_DATABASE'),
    user=os.environ.get('QUESTDB_USER'),
    password=os.environ.get('QUESTDB_PASSWORD_KEY'),
    table_name=os.environ.get('QUESTDB_TABLE_NAME')
)

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_TIMEOUT", "1.0")),
)

input_topic = app.topic(os.environ.get('QUESTDB_INPUT_TOPIC'))

sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)