# DEPENDENCIES:
# pip install quixstreams
# pip install python-dotenv
# pip install questdb
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
import questdb.ingress as qi
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, username=None, password=None, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.sender = None
        self.table_created = False

    def setup(self):
        try:
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)
            
            conf = f'http::addr={self.host}:{self.port};'
            if auth:
                conf += f'username={self.username};password={self.password};'
            
            self.sender = qi.Sender.from_conf(conf)
            
            if self.on_client_connect_success:
                self.on_client_connect_success(self)
        except Exception as e:
            if self.on_client_connect_failure:
                self.on_client_connect_failure(self, e)
            raise

    def write(self, batch: SinkBatch):
        if not self.sender:
            raise RuntimeError("QuestDB sender not initialized")

        try:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the message value if it's a JSON string
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                elif isinstance(item.value, dict):
                    data = item.value
                else:
                    print(f"Unexpected message format: {type(item.value)}")
                    continue

                # Convert timestamp from nanoseconds to datetime
                timestamp_ns = data.get('timestamp', 0)
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1e9)

                # Send data to QuestDB
                self.sender.row(
                    'solar_panel_data',
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
                        'unit_power': data.get('unit_power', ''),
                        'temperature': data.get('temperature', 0.0),
                        'unit_temp': data.get('unit_temp', ''),
                        'irradiance': data.get('irradiance', 0),
                        'unit_irradiance': data.get('unit_irradiance', ''),
                        'voltage': data.get('voltage', 0.0),
                        'unit_voltage': data.get('unit_voltage', ''),
                        'current': data.get('current', 0),
                        'unit_current': data.get('unit_current', '')
                    },
                    at=timestamp_dt
                )

            self.sender.flush()

        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise SinkBackpressureError(
                retry_after=10.0,
                topic=batch.topic,
                partition=batch.partition
            )

    def close(self):
        if self.sender:
            self.sender.close()

# Initialize QuestDB sink
try:
    questdb_port = int(os.environ.get('QUESTDB_PORT', '9000'))
except ValueError:
    questdb_port = 9000

questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST', 'localhost'),
    port=questdb_port,
    username=os.environ.get('QUESTDB_USERNAME'),
    password=os.environ.get('QUESTDB_PASSWORD')
)

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "100")),
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1"))
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)