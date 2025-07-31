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
from questdb.ingress import Sender
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self.host = os.environ.get('QDB_HOST', 'localhost')
        try:
            self.port = int(os.environ.get('QDB_PORT', '9000'))
        except (ValueError, TypeError):
            self.port = 9000
        self.token = os.environ.get('QDB_TOKEN_KEY', '')
        self.table = os.environ.get('QDB_TABLE', 'solar_data')
        self.timestamp_column = os.environ.get('QDB_TIMESTAMP_COLUMN', 'timestamp')
        self.sender = None

    def setup(self):
        self.sender = Sender.from_conf(
            f'http::addr={self.host}:{self.port};token={self.token};'
        )

    def write(self, batch: SinkBatch):
        if not self.sender:
            return
            
        for item in batch:
            try:
                print(f'Raw message: {item}')
                
                # Parse the nested JSON value
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                elif hasattr(item, 'value') and isinstance(item.value, dict):
                    data = item.value
                else:
                    continue
                
                # Convert timestamp to datetime if it's an epoch timestamp
                timestamp = data.get('timestamp')
                if timestamp and isinstance(timestamp, (int, float)):
                    # Convert nanosecond timestamp to datetime
                    dt = datetime.fromtimestamp(timestamp / 1_000_000_000)
                    timestamp_str = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                else:
                    timestamp_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                
                # Build QuestDB line protocol
                self.sender.row(
                    self.table,
                    symbols={
                        'panel_id': data.get('panel_id', ''),
                        'location_id': data.get('location_id', ''),
                        'location_name': data.get('location_name', ''),
                        'inverter_status': data.get('inverter_status', '')
                    },
                    columns={
                        'latitude': float(data.get('latitude', 0)),
                        'longitude': float(data.get('longitude', 0)),
                        'timezone': int(data.get('timezone', 0)),
                        'power_output': float(data.get('power_output', 0)),
                        'temperature': float(data.get('temperature', 0)),
                        'irradiance': float(data.get('irradiance', 0)),
                        'voltage': float(data.get('voltage', 0)),
                        'current': float(data.get('current', 0))
                    },
                    at=timestamp_str
                )
                
            except Exception as e:
                print(f'Error processing message: {e}')
                continue
        
        # Flush the batch
        try:
            self.sender.flush()
        except Exception as e:
            print(f'Error flushing to QuestDB: {e}')

    def close(self):
        if self.sender:
            self.sender.close()

# Create the application
app = Application(
    consumer_group=os.environ.get("QDB_CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("QDB_BUFFER_SIZE", "100")),
    commit_interval=float(os.environ.get("QDB_BUFFER_TIMEOUT", "5.0")),
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)

questdb_sink = QuestDBSink()
sdf.print(pretty=True,metadata=True)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run()