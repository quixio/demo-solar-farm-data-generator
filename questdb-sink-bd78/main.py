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
from questdb import ingress as sender
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._sender = None
        try:
            self._port = int(os.environ.get('QUESTDB_PORT', '9009'))
        except ValueError:
            self._port = 9009
        self._host = os.environ.get('QUESTDB_HOSTNAME', 'localhost')
        self._username = os.environ.get('QUESTDB_USERNAME', 'admin')
        self._password = os.environ.get('QUESTDB_PASSWORD')

    def setup(self):
        conf = f'http::addr={self._host}:{self._port};username={self._username}'
        if self._password:
            conf += f';password={self._password}'
        self._sender = sender.Sender.from_conf(conf)
        self._sender.open()

    def write(self, batch: SinkBatch):
        try:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the JSON string in the value field
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                elif hasattr(item, 'value') and isinstance(item.value, dict):
                    data = item.value
                else:
                    print(f"Unexpected message structure: {item}")
                    continue
                
                # Convert timestamp to datetime
                timestamp_ns = data.get('timestamp', 0)
                dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                
                # Write to QuestDB
                self._sender.row(
                    'solar_data',
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
                        'power_output': data.get('power_output', 0),
                        'temperature': data.get('temperature', 0.0),
                        'irradiance': data.get('irradiance', 0),
                        'voltage': data.get('voltage', 0.0),
                        'current': data.get('current', 0)
                    },
                    at=dt
                )
            self._sender.flush()
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise

    def close(self):
        if self._sender:
            try:
                self._sender.flush()
            finally:
                self._sender.close()

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-data-writer"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1")),
)

input_topic = app.topic(os.environ["input"])
questdb_sink = QuestDBSink()

sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run()