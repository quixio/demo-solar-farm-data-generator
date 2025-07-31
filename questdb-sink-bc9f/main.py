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
    def __init__(self):
        super().__init__()
        self._sender = None
        self._table_name = os.environ.get('QDB_TABLE', 'solar_data')
        self._timestamp_column = os.environ.get('QDB_TIMESTAMP_COLUMN', 'timestamp')
        
    def setup(self):
        host = os.environ.get('QDB_HOST', 'localhost')
        try:
            port = int(os.environ.get('QDB_PORT', '9009'))
        except ValueError:
            port = 9009
            
        token = os.environ.get('QDB_TOKEN_KEY')
        
        if token:
            self._sender = qi.Sender(host, port, auth=(qi.AuthTokenKey, token))
        else:
            self._sender = qi.Sender(host, port)
            
        # Create table if it doesn't exist
        self._ensure_table_exists()
        
    def _ensure_table_exists(self):
        # Since QuestDB is append-only and auto-creates tables, we don't need explicit DDL
        # The table will be created when we first write data
        pass
        
    def write(self, batch: SinkBatch):
        for item in batch:
            try:
                # Parse the value field which contains JSON string
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                    
                # Convert timestamp to datetime if it's an epoch timestamp
                timestamp_val = data.get('timestamp')
                if timestamp_val and isinstance(timestamp_val, (int, float)):
                    # Convert from nanoseconds to datetime
                    dt = datetime.fromtimestamp(timestamp_val / 1_000_000_000)
                else:
                    dt = datetime.now()
                
                # Send data to QuestDB
                self._sender.row(
                    self._table_name,
                    symbols={
                        'panel_id': data.get('panel_id', ''),
                        'location_id': data.get('location_id', ''),
                        'inverter_status': data.get('inverter_status', ''),
                    },
                    columns={
                        'location_name': data.get('location_name', ''),
                        'latitude': data.get('latitude', 0.0),
                        'longitude': data.get('longitude', 0.0),
                        'timezone': data.get('timezone', 0),
                        'power_output': data.get('power_output', 0.0),
                        'unit_power': data.get('unit_power', ''),
                        'temperature': data.get('temperature', 0.0),
                        'unit_temp': data.get('unit_temp', ''),
                        'irradiance': data.get('irradiance', 0.0),
                        'unit_irradiance': data.get('unit_irradiance', ''),
                        'voltage': data.get('voltage', 0.0),
                        'unit_voltage': data.get('unit_voltage', ''),
                        'current': data.get('current', 0.0),
                        'unit_current': data.get('unit_current', ''),
                    },
                    at=dt
                )
                
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
                
        # Flush the sender
        self._sender.flush()

app = Application(
    consumer_group=os.environ.get("QDB_CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("QDB_BUFFER_SIZE", "100")),
    commit_interval=float(os.environ.get("QDB_BUFFER_TIMEOUT", "5.0")),
)

input_topic = app.topic(os.environ["input"])
questdb_sink = QuestDBSink()

sdf = app.dataframe(input_topic)

sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)