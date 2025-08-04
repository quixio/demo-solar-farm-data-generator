# DEPENDENCIES:
# pip install quixstreams
# pip install python-dotenv
# pip install questdb
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from questdb.ingress import Sender, IngressError
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, username=None, password=None, table_name="solar_data", **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.table_name = table_name
        self.sender = None
        self.table_created = False

    def setup(self):
        try:
            self.sender = Sender(host=self.host, port=self.port, username=self.username, password=self.password)
        except Exception as e:
            if self.on_client_connect_failure:
                self.on_client_connect_failure(e)
            raise
        
        if self.on_client_connect_success:
            self.on_client_connect_success()

    def write(self, batch: SinkBatch):
        if not self.sender:
            raise RuntimeError("QuestDB sender not initialized")

        try:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the value field if it's a JSON string
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                elif isinstance(item.value, dict):
                    data = item.value
                else:
                    continue

                # Convert timestamp from nanoseconds to milliseconds for QuestDB
                timestamp_ns = data.get('timestamp', 0)
                timestamp_ms = timestamp_ns // 1_000_000 if timestamp_ns else None

                # Send data to QuestDB using line protocol
                self.sender.table(self.table_name) \
                    .symbol('panel_id', data.get('panel_id', '')) \
                    .symbol('location_id', data.get('location_id', '')) \
                    .symbol('location_name', data.get('location_name', '')) \
                    .symbol('inverter_status', data.get('inverter_status', '')) \
                    .column('latitude', data.get('latitude', 0.0)) \
                    .column('longitude', data.get('longitude', 0.0)) \
                    .column('timezone', data.get('timezone', 0)) \
                    .column('power_output', data.get('power_output', 0)) \
                    .column('temperature', data.get('temperature', 0.0)) \
                    .column('irradiance', data.get('irradiance', 0)) \
                    .column('voltage', data.get('voltage', 0.0)) \
                    .column('current', data.get('current', 0)) \
                    .at(timestamp_ms if timestamp_ms else datetime.utcnow())

            # Flush all pending writes
            self.sender.flush()

        except IngressError as e:
            raise SinkBackpressureError(
                retry_after=5.0,
                topic=batch.topic,
                partition=batch.partition,
            )
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise

    def close(self):
        if self.sender:
            try:
                self.sender.close()
            except Exception as e:
                print(f"Error closing QuestDB sender: {e}")

# Configuration
try:
    questdb_port = int(os.environ.get('QUESTDB_PORT', '9009'))
except ValueError:
    questdb_port = 9009

questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST', 'localhost'),
    port=questdb_port,
    username=os.environ.get('QUESTDB_USERNAME'),
    password=os.environ.get('QUESTDB_PASSWORD'),
    table_name=os.environ.get('QUESTDB_TABLE_NAME', 'solar_data')
)

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-data-writer"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "100")),
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1")),
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)