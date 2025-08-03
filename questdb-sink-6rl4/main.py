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
import questdb.ingress as qdb
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, username, password, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        try:
            self.port = int(port) if port else 9009
        except (ValueError, TypeError):
            self.port = 9009
        self.username = username
        self.password = password
        self.sender = None
        self.table_created = False

    def setup(self):
        try:
            if self.username and self.password:
                auth = (self.username, self.password)
                self.sender = qdb.Sender.from_conf(f'http::addr={self.host}:{self.port};username={self.username};password={self.password};')
            else:
                self.sender = qdb.Sender.from_conf(f'http::addr={self.host}:{self.port};')
            
            if self.on_client_connect_success:
                self.on_client_connect_success()
        except Exception as e:
            if self.on_client_connect_failure:
                self.on_client_connect_failure(e)
            raise

    def write(self, batch: SinkBatch):
        if not self.sender:
            self.setup()
        
        for item in batch:
            try:
                # Parse the value field which contains JSON string
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert timestamp to datetime if it exists
                timestamp = None
                if 'timestamp' in data:
                    # Convert nanosecond timestamp to datetime
                    timestamp_ns = int(data['timestamp'])
                    timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                
                # Create table entry with all fields
                table_name = 'solar_panel_data'
                
                self.sender.row(
                    table_name,
                    columns={
                        'panel_id': data.get('panel_id'),
                        'location_id': data.get('location_id'), 
                        'location_name': data.get('location_name'),
                        'latitude': float(data.get('latitude', 0)),
                        'longitude': float(data.get('longitude', 0)),
                        'timezone': int(data.get('timezone', 0)),
                        'power_output': float(data.get('power_output', 0)),
                        'unit_power': data.get('unit_power'),
                        'temperature': float(data.get('temperature', 0)),
                        'unit_temp': data.get('unit_temp'),
                        'irradiance': float(data.get('irradiance', 0)),
                        'unit_irradiance': data.get('unit_irradiance'),
                        'voltage': float(data.get('voltage', 0)),
                        'unit_voltage': data.get('unit_voltage'),
                        'current': float(data.get('current', 0)),
                        'unit_current': data.get('unit_current'),
                        'inverter_status': data.get('inverter_status')
                    },
                    at=timestamp
                )
                
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
        
        # Flush the batch
        self.sender.flush()

    def close(self):
        if self.sender:
            self.sender.close()

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST', 'localhost'),
    port=os.environ.get('QUESTDB_PORT', '9009'),
    username=os.environ.get('QUESTDB_USERNAME'),
    password=os.environ.get('QUESTDB_PASSWORD')
)

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-data-writer"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1")),
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)

# Debug print for raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)