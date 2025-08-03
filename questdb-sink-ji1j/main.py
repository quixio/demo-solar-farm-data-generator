# DEPENDENCIES:
# pip install quixstreams
# pip install questdb
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
    def __init__(self, host, port, token_key=None, database=None, table_name="solar_data", timestamp_column=None):
        super().__init__()
        self.host = host
        self.port = port
        self.token_key = token_key
        self.database = database
        self.table_name = table_name
        self.timestamp_column = timestamp_column
        self.sender = None

    def setup(self):
        """Initialize QuestDB ILP sender (TCP port 9009 by default)"""
        conf = f'addr={self.host}:{self.port};'
        
        if self.token_key:
            conf += f'token={self.token_key};'
        
        if self.database:
            conf += f'db={self.database};'
        
        self.sender = qi.Sender.from_conf(conf)

    def write(self, batch: SinkBatch):
        """Write batch of records to QuestDB using ILP"""
        for item in batch:
            try:
                # Parse the value field which contains JSON string
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert timestamp if specified
                timestamp_ns = None
                if self.timestamp_column and self.timestamp_column in data:
                    timestamp_ns = int(data[self.timestamp_column])
                elif 'timestamp' in data:
                    timestamp_ns = int(data['timestamp'])
                else:
                    # Use current time in nanoseconds
                    timestamp_ns = int(datetime.utcnow().timestamp() * 1_000_000_000)
                
                # Start building the row
                row = self.sender.row(
                    self.table_name,
                    symbols={
                        'panel_id': str(data.get('panel_id', '')),
                        'location_id': str(data.get('location_id', '')),
                        'location_name': str(data.get('location_name', '')),
                        'inverter_status': str(data.get('inverter_status', ''))
                    },
                    columns={
                        'latitude': float(data.get('latitude', 0.0)),
                        'longitude': float(data.get('longitude', 0.0)),
                        'timezone': int(data.get('timezone', 0)),
                        'power_output': float(data.get('power_output', 0.0)),
                        'temperature': float(data.get('temperature', 0.0)),
                        'irradiance': float(data.get('irradiance', 0.0)),
                        'voltage': float(data.get('voltage', 0.0)),
                        'current': float(data.get('current', 0.0))
                    },
                    at=qi.TimestampNanos(timestamp_ns)
                )
                
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
        
        # Flush all rows
        self.sender.flush()

    def close(self):
        """Close the QuestDB sender"""
        if self.sender:
            self.sender.close()

# Parse environment variables with safe defaults
try:
    qdb_port = int(os.environ.get('QDB_PORT', '9009'))
except ValueError:
    qdb_port = 9009

try:
    buffer_size = int(os.environ.get('QDB_BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get('QDB_BUFFER_TIMEOUT', '1.0'))
except ValueError:
    buffer_timeout = 1.0

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QDB_HOST', 'localhost'),
    port=qdb_port,
    token_key=os.environ.get('QDB_TOKEN_KEY'),
    database=os.environ.get('QDB_DATABASE'),
    table_name=os.environ.get('QDB_TABLE', 'solar_data'),
    timestamp_column=os.environ.get('QDB_TIMESTAMP_COLUMN')
)

# Initialize Quix Streams Application
app = Application(
    consumer_group=os.environ.get('QDB_CONSUMER_GROUP_NAME', 'questdb-solar-data-writer'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

# Define input topic
input_topic = app.topic(os.environ["input"])

# Create streaming dataframe
sdf = app.dataframe(input_topic)

# Add debug print to see raw message structure
sdf = sdf.apply(lambda msg: print(f'Raw message: {msg}') or msg)

# Sink data to QuestDB
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)