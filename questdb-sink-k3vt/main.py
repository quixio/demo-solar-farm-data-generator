# DEPENDENCIES:
# pip install quixstreams
# pip install questdb>=3.2.0
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import questdb.ingress as qi

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, token_key, database, table, timestamp_column=None):
        super().__init__()
        self.host = host
        self.port = port
        self.token_key = token_key
        self.database = database
        self.table = table
        self.timestamp_column = timestamp_column
        self.client = None
        self.table_created = False

    def setup(self):
        """Initialize QuestDB connection"""
        try:
            conf = f'http::addr={self.host}:{self.port};token={self.token_key};'
            self.client = qi.Sender.from_conf(conf)
            print("QuestDB connection established successfully")
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist"""
        if self.table_created:
            return
            
        # Since QuestDB is time-series focused, we'll create a table optimized for solar panel data
        # QuestDB automatically creates tables when data is inserted, but we'll ensure proper structure
        self.table_created = True
        print(f"Table {self.table} will be created automatically on first insert")

    def write(self, batch: SinkBatch):
        """Write batch data to QuestDB"""
        if not self.client:
            self.setup()
            
        self._create_table_if_not_exists()
        
        try:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the value field which contains JSON string
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                elif hasattr(item, 'value') and isinstance(item.value, dict):
                    data = item.value
                else:
                    print(f"Unexpected message structure: {item}")
                    continue
                
                # Convert timestamp from custom format to datetime
                timestamp_ns = data.get('timestamp', 0)
                # Convert nanoseconds to seconds and create datetime
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                
                # Start building the row for QuestDB
                row = self.client.row(
                    table_name=self.table,
                    at=qi.TimestampNanos.from_datetime(timestamp_dt)
                )
                
                # Add all fields as columns
                row.column('panel_id', data.get('panel_id', ''))
                row.column('location_id', data.get('location_id', ''))
                row.column('location_name', data.get('location_name', ''))
                row.column('latitude', data.get('latitude', 0.0))
                row.column('longitude', data.get('longitude', 0.0))
                row.column('timezone', data.get('timezone', 0))
                row.column('power_output', data.get('power_output', 0.0))
                row.column('temperature', data.get('temperature', 0.0))
                row.column('irradiance', data.get('irradiance', 0.0))
                row.column('voltage', data.get('voltage', 0.0))
                row.column('current', data.get('current', 0.0))
                row.column('inverter_status', data.get('inverter_status', ''))
                
                # Add units as separate columns
                row.column('unit_power', data.get('unit_power', ''))
                row.column('unit_temp', data.get('unit_temp', ''))
                row.column('unit_irradiance', data.get('unit_irradiance', ''))
                row.column('unit_voltage', data.get('unit_voltage', ''))
                row.column('unit_current', data.get('unit_current', ''))
                
                # Complete the row
                row.at_now()
                
            # Flush the batch to QuestDB
            self.client.flush()
            print(f"Successfully wrote {len(batch)} records to QuestDB table {self.table}")
            
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise

    def close(self):
        """Close QuestDB connection"""
        if self.client:
            try:
                self.client.close()
                print("QuestDB connection closed")
            except Exception as e:
                print(f"Error closing QuestDB connection: {e}")

# Parse port with error handling
try:
    port = int(os.environ.get('QDB_PORT', '9000'))
except ValueError:
    port = 9000

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QDB_HOST', 'localhost'),
    port=port,
    token_key=os.environ.get('QDB_TOKEN_KEY', ''),
    database=os.environ.get('QDB_DATABASE', 'qdb'),
    table=os.environ.get('QDB_TABLE', 'solar_data'),
    timestamp_column=os.environ.get('QDB_TIMESTAMP_COLUMN', 'timestamp')
)

# Parse buffer settings with error handling
try:
    buffer_size = int(os.environ.get('QDB_BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get('QDB_BUFFER_TIMEOUT', '1.0'))
except ValueError:
    buffer_timeout = 1.0

app = Application(
    consumer_group=os.environ.get('QDB_CONSUMER_GROUP_NAME', 'questdb-solar-data-writer'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)