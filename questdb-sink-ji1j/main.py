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

from dotenv import load_dotenv
load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, table_name, host, port, token_key=None, database=None):
        super().__init__()
        self.table_name = table_name
        self.host = host
        try:
            self.port = int(port)
        except (ValueError, TypeError):
            self.port = 9009
        self.token_key = token_key
        self.database = database
        self.sender = None
        self.table_created = False
        
    def setup(self):
        conf = f'http::addr={self.host}:{self.port};'
        if self.token_key:
            conf += f'token={self.token_key};'
        if self.database:
            conf += f'database={self.database};'
        self.sender = qi.Sender.from_conf(conf)
        
    def write(self, batch: SinkBatch):
        if not self.table_created:
            self._create_table_if_not_exists()
            self.table_created = True
            
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the message value - it's a JSON string that needs to be parsed
            if hasattr(item, 'value') and isinstance(item.value, str):
                try:
                    data = json.loads(item.value)
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON: {item.value}")
                    continue
            elif hasattr(item, 'value') and isinstance(item.value, dict):
                data = item.value
            else:
                print(f"Unexpected message format: {item}")
                continue
                
            # Convert timestamp to datetime
            timestamp_value = data.get('timestamp')
            if timestamp_value:
                # Convert nanosecond timestamp to datetime
                dt = datetime.fromtimestamp(timestamp_value / 1_000_000_000)
            else:
                dt = datetime.utcnow()
            
            # Map data to QuestDB row
            self.sender.row(
                self.table_name,
                symbols={
                    'panel_id': data.get('panel_id', ''),
                    'location_id': data.get('location_id', ''),
                    'location_name': data.get('location_name', ''),
                    'inverter_status': data.get('inverter_status', '')
                },
                columns={
                    'latitude': data.get('latitude', 0.0),
                    'longitude': data.get('longitude', 0.0),
                    'timezone': data.get('timezone', 0),
                    'power_output': data.get('power_output', 0.0),
                    'temperature': data.get('temperature', 0.0),
                    'irradiance': data.get('irradiance', 0.0),
                    'voltage': data.get('voltage', 0.0),
                    'current': data.get('current', 0.0),
                    'unit_power': data.get('unit_power', ''),
                    'unit_temp': data.get('unit_temp', ''),
                    'unit_irradiance': data.get('unit_irradiance', ''),
                    'unit_voltage': data.get('unit_voltage', ''),
                    'unit_current': data.get('unit_current', '')
                },
                at=dt
            )
        
        self.sender.flush()
        
    def _create_table_if_not_exists(self):
        # QuestDB creates tables automatically on first insert, so no explicit DDL needed
        pass

# Initialize the application
app = Application(
    consumer_group=os.environ.get('QDB_CONSUMER_GROUP_NAME', 'questdb-sink'),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get('QDB_BUFFER_SIZE', '1000')),
    commit_interval=float(os.environ.get('QDB_BUFFER_TIMEOUT', '1.0')),
)

# Get input topic from environment variable
input_topic = app.topic(os.environ["input"])

# Create QuestDB sink
questdb_sink = QuestDBSink(
    table_name=os.environ.get('QDB_TABLE', 'solar_data'),
    host=os.environ.get('QDB_HOST', 'localhost'),
    port=os.environ.get('QDB_PORT', '9009'),
    token_key=os.environ.get('QDB_TOKEN_KEY'),
    database=os.environ.get('QDB_DATABASE')
)

# Set up the streaming dataframe
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)