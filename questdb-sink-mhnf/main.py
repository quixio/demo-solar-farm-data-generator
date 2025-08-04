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
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.table_created = False

    def setup(self):
        """Initialize connection to QuestDB"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.connection.autocommit = True
            print(f"Successfully connected to QuestDB at {self.host}:{self.port}")
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise

    def _create_table_if_not_exists(self, sample_data):
        """Create table based on sample data structure"""
        if self.table_created:
            return
            
        cursor = self.connection.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = %s
        """, (self.table_name,))
        
        if cursor.fetchone()[0] > 0:
            self.table_created = True
            cursor.close()
            return
        
        # Analyze sample data to determine column types
        columns = []
        columns.append("timestamp TIMESTAMP")  # Default timestamp column
        
        if isinstance(sample_data, dict):
            for key, value in sample_data.items():
                if key.lower() in ['timestamp', 'time', 'ts']:
                    continue  # Skip timestamp fields as we already have one
                    
                if isinstance(value, str):
                    columns.append(f"{key} STRING")
                elif isinstance(value, int):
                    columns.append(f"{key} LONG")
                elif isinstance(value, float):
                    columns.append(f"{key} DOUBLE")
                elif isinstance(value, bool):
                    columns.append(f"{key} BOOLEAN")
                else:
                    columns.append(f"{key} STRING")  # Default to string for unknown types
        
        create_table_sql = f"""
            CREATE TABLE {self.table_name} (
                {', '.join(columns)}
            ) timestamp(timestamp) PARTITION BY DAY;
        """
        
        print(f"Creating table with SQL: {create_table_sql}")
        cursor.execute(create_table_sql)
        self.connection.commit()
        cursor.close()
        self.table_created = True
        print(f"Table {self.table_name} created successfully")

    def write(self, batch: SinkBatch):
        """Write batch of data to QuestDB"""
        if not batch:
            return
            
        cursor = self.connection.cursor()
        
        try:
            # Get first item to analyze structure and create table if needed
            first_item = next(iter(batch))
            print(f"Raw message: {first_item}")
            
            # Parse the message data
            if hasattr(first_item, 'value'):
                sample_data = first_item.value
            else:
                sample_data = first_item
                
            # If it's still a string, parse it as JSON
            if isinstance(sample_data, (str, bytes)):
                sample_data = json.loads(sample_data)
            
            self._create_table_if_not_exists(sample_data)
            
            # Prepare data for insertion
            records = []
            for item in batch:
                try:
                    # Extract data from the message
                    if hasattr(item, 'value'):
                        data = item.value
                    else:
                        data = item
                        
                    # If it's still a string, parse it as JSON
                    if isinstance(data, (str, bytes)):
                        data = json.loads(data)
                    
                    # Create record with timestamp
                    record = {
                        'timestamp': datetime.utcnow()
                    }
                    
                    # Add data fields
                    if isinstance(data, dict):
                        for key, value in data.items():
                            if key.lower() not in ['timestamp', 'time', 'ts']:
                                record[key] = value
                            else:
                                # Handle timestamp fields - convert epoch to datetime if needed
                                if isinstance(value, (int, float)):
                                    record['timestamp'] = datetime.fromtimestamp(value)
                                else:
                                    record['timestamp'] = value
                    
                    records.append(record)
                    
                except Exception as e:
                    print(f"Error processing item: {e}")
                    continue
            
            if not records:
                return
                
            # Build INSERT statement
            columns = list(records[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Prepare values for bulk insert
            values = []
            for record in records:
                values.append(tuple(record[col] for col in columns))
            
            # Execute bulk insert
            cursor.executemany(insert_sql, values)
            print(f"Successfully inserted {len(values)} records into {self.table_name}")
            
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise
        finally:
            cursor.close()

# Configuration
try:
    port = int(os.environ.get('QUESTDB_PORT', '8812'))
except ValueError:
    port = 8812

try:
    buffer_size = int(os.environ.get('BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get('BUFFER_TIMEOUT', '1.0'))
except ValueError:
    buffer_timeout = 1.0

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST', 'localhost'),
    port=port,
    database=os.environ.get('QUESTDB_DATABASE', 'qdb'),
    user=os.environ.get('QUESTDB_USER', 'admin'),
    password=os.environ.get('QUESTDB_PASSWORD_KEY', 'quest'),
    table_name=os.environ.get('QUESTDB_TABLE_NAME', 'sensor_data')
)

# Initialize Quix application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

# Set up input topic
input_topic = app.topic(os.environ.get('QUESTDB_INPUT_TOPIC'))
sdf = app.dataframe(input_topic)

# Sink data to QuestDB
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)