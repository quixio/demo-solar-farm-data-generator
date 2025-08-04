import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import psycopg2
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
        self.table_created = False

    def setup(self):
        """Establish connection to QuestDB"""
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
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create table if it doesn't exist"""
        if self.table_created:
            return
            
        cursor = self.connection.cursor()
        try:
            # Create table with basic schema - will be expanded as needed
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TIMESTAMP,
                temperature DOUBLE,
                humidity DOUBLE,
                pressure DOUBLE
            ) timestamp(timestamp) PARTITION BY DAY WAL;
            """
            cursor.execute(create_table_sql)
            self.connection.commit()
            self.table_created = True
            print(f"Table {self.table_name} created or already exists")
        except Exception as e:
            print(f"Error creating table: {e}")
            raise
        finally:
            cursor.close()

    @staticmethod
    def _to_datetime(ts_int: int) -> datetime:
        """
        Convert epoch value (seconds, micro- or nano-seconds) to UTC datetime.
        """
        # If the number is larger than the maximum second value that fits
        # into year 9999 (~2.5e11) we assume it is micro- or nano-seconds.
        if ts_int > 253402300799:               # > 9999-12-31 in seconds
            # nanoseconds have 1e9 multiplier, microseconds 1e6
            if ts_int > 1e14:                   # clearly nanoseconds
                ts_int /= 1_000_000_000
            else:                               # treat as microseconds
                ts_int /= 1_000_000
        return datetime.utcfromtimestamp(ts_int)

    def write(self, batch: SinkBatch):
        """Write batch of records to QuestDB"""
        if not self.connection:
            self.setup()
        
        self._create_table_if_not_exists()
        
        cursor = self.connection.cursor()
        records_to_insert = []
        
        try:
            for item in batch:
                try:
                    print(f'Raw message: {item}')
                    
                    # Extract the message value
                    message_data = item.value if hasattr(item, 'value') else item
                    
                    # If message_data is a string, parse it as JSON
                    if isinstance(message_data, str):
                        message_data = json.loads(message_data)
                    
                    # Build record for insertion
                    record = {}
                    
                    # Process each field in the message
                    for key, value in message_data.items():
                        if key.lower() not in ['timestamp', 'time', 'ts']:
                            record[key] = value
                        else:
                            # value can be seconds, µs, or ns – normalise it
                            if isinstance(value, (int, float)):
                                record['timestamp'] = self._to_datetime(int(value))
                            else:
                                # assume ISO-8601 or RFC-2822 string
                                record['timestamp'] = datetime.fromisoformat(value)
                    
                    # Ensure we have a timestamp
                    if 'timestamp' not in record:
                        record['timestamp'] = datetime.utcnow()
                    
                    records_to_insert.append(record)
                    
                except Exception as e:
                    print(f"Error processing item: {e}")
                    continue
            
            if records_to_insert:
                # Get all unique columns from all records
                all_columns = set()
                for record in records_to_insert:
                    all_columns.update(record.keys())
                
                # Prepare insert statement
                columns = list(all_columns)
                placeholders = ', '.join(['%s'] * len(columns))
                insert_sql = f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                
                # Prepare data for bulk insert
                data_to_insert = []
                for record in records_to_insert:
                    row = [record.get(col) for col in columns]
                    data_to_insert.append(row)
                
                # Execute bulk insert
                cursor.executemany(insert_sql, data_to_insert)
                print(f"Successfully inserted {len(records_to_insert)} records into {self.table_name}")
            
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise
        finally:
            cursor.close()

    def teardown(self):
        """Close connection to QuestDB"""
        if self.connection:
            self.connection.close()
            print("QuestDB connection closed")

# Initialize application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")) if os.environ.get("BUFFER_SIZE", "1000").isdigit() else 1000,
    commit_interval=float(os.environ.get("BUFFER_TIMEOUT", "1.0")) if os.environ.get("BUFFER_TIMEOUT", "1.0").replace(".", "").isdigit() else 1.0,
)

# Configure input topic
input_topic = app.topic(os.environ.get("QUESTDB_INPUT_TOPIC"))

# Handle port with try/except
try:
    port = int(os.environ.get('QUESTDB_PORT', '8812'))
except ValueError:
    port = 8812

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST', 'localhost'),
    port=port,
    database=os.environ.get('QUESTDB_DATABASE', 'qdb'),
    user=os.environ.get('QUESTDB_USER', 'admin'),
    password=os.environ.get('QUESTDB_PASSWORD_KEY', ''),
    table_name=os.environ.get('QUESTDB_TABLE_NAME', 'sensor_data')
)

# Create streaming dataframe and sink data
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    # Process only 10 messages then stop
    app.run(count=10, timeout=20)