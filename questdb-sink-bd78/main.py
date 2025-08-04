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
import questdb.ingress as sender
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        try:
            self._port = int(os.environ.get('QUESTDB_PORT', '9009'))
        except ValueError:
            self._port = 9009
        self._host = os.environ.get('QUESTDB_HOSTNAME', 'localhost')
        self._username = os.environ.get('QUESTDB_USERNAME', 'admin')
        self._password = os.environ.get('QUESTDB_PASSWORD')
        self._sender = None

    def setup(self):
        conf = f'http::addr={self._host}:{self._port};username={self._username}'
        if self._password:
            conf += f';password={self._password}'
        self._sender = sender.Sender.from_conf(conf)
        
    def write(self, batch: SinkBatch):
        try:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the value field which contains JSON string
                if hasattr(item, 'value') and item.value:
                    if isinstance(item.value, str):
                        data = json.loads(item.value)
                    else:
                        data = item.value
                else:
                    continue
                
                # Convert timestamp to datetime
                timestamp_ms = data.get('timestamp', 0) // 1000000  # Convert nanoseconds to milliseconds
                dt = datetime.fromtimestamp(timestamp_ms / 1000)
                
                # Write to QuestDB
                self._sender.row(
                    'solar_data',
                    symbols={
                        'panel_id': str(data.get('panel_id', '')),
                        'location_id': str(data.get('location_id', '')),
                        'location_name': str(data.get('location_name', '')),
                        'inverter_status': str(data.get('inverter_status', ''))
                    },
                    columns={
                        'latitude': float(data.get('latitude', 0)),
                        'longitude': float(data.get('longitude', 0)),
                        'timezone': int(data.get('timezone', 0)),
                        'power_output': int(data.get('power_output', 0)),
                        'temperature': float(data.get('temperature', 0)),
                        'irradiance': int(data.get('irradiance', 0)),
                        'voltage': float(data.get('voltage', 0)),
                        'current': int(data.get('current', 0)),
                        'unit_power': str(data.get('unit_power', '')),
                        'unit_temp': str(data.get('unit_temp', '')),
                        'unit_irradiance': str(data.get('unit_irradiance', '')),
                        'unit_voltage': str(data.get('unit_voltage', '')),
                        'unit_current': str(data.get('unit_current', ''))
                    },
                    at=dt
                )
            
            self._sender.flush()
            
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise

    def close(self):
        if self._sender:
            self._sender.close()

questdb_sink = QuestDBSink()

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-data-writer"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1")),
)

input_topic = app.topic(os.environ.get("QUESTDB_INPUT_TOPIC", "input"))
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)