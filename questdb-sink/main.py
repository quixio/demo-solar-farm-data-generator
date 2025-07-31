# DEPENDENCIES:
# pip install quixstreams
# pip install questdb
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from questdb.ingress import Sender
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

def env_int(var_name, default):
    try:
        return int(os.environ.get(var_name, str(default)))
    except ValueError:
        return default

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._client = None
        self._table_name = None

    def setup(self):
        try:
            host = os.environ.get("QUESTDB_HOST", "questdb")
            port = env_int("QUESTDB_PORT", 8812)
            
            username = os.environ.get("QUESTDB_USERNAME")
            password = os.environ.get("QUESTDB_PASSWORD_KEY")
            
            # Create sender with positional arguments only
            self._client = Sender(host, port)
            self._table_name = os.environ.get("QUESTDB_TABLE_NAME", "solar_data")
            
            print(f"QuestDB sink setup completed successfully - Host: {host}, Port: {port}, Table: {self._table_name}")
        except Exception as e:
            print(f"Failed to setup QuestDB sink: {e}")
            raise

    def write(self, batch: SinkBatch):
        try:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the JSON string from the value field
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                elif hasattr(item, 'value') and isinstance(item.value, dict):
                    data = item.value
                else:
                    print(f"Unexpected item structure: {type(item)}")
                    continue
                
                # Convert timestamp to datetime
                timestamp_ns = data.get('timestamp')
                if timestamp_ns:
                    # Convert nanoseconds to datetime
                    timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                else:
                    timestamp_dt = datetime.now()
                
                # Write to QuestDB using ILP
                self._client.table(self._table_name) \
                    .symbol('panel_id', data.get('panel_id', '')) \
                    .symbol('location_id', data.get('location_id', '')) \
                    .symbol('location_name', data.get('location_name', '')) \
                    .symbol('inverter_status', data.get('inverter_status', '')) \
                    .symbol('unit_power', data.get('unit_power', '')) \
                    .symbol('unit_temp', data.get('unit_temp', '')) \
                    .symbol('unit_irradiance', data.get('unit_irradiance', '')) \
                    .symbol('unit_voltage', data.get('unit_voltage', '')) \
                    .symbol('unit_current', data.get('unit_current', '')) \
                    .column('latitude', data.get('latitude', 0.0)) \
                    .column('longitude', data.get('longitude', 0.0)) \
                    .column('timezone', data.get('timezone', 0)) \
                    .column('power_output', data.get('power_output', 0)) \
                    .column('temperature', data.get('temperature', 0.0)) \
                    .column('irradiance', data.get('irradiance', 0)) \
                    .column('voltage', data.get('voltage', 0.0)) \
                    .column('current', data.get('current', 0)) \
                    .at(timestamp_dt)
            
            # Flush the batch
            self._client.flush()
            print(f"Successfully wrote {len(batch)} records to QuestDB")
            
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise

    def flush(self):
        if self._client:
            try:
                self._client.flush()
            except Exception as e:
                print(f"Error flushing QuestDB client: {e}")
                raise

questdb_sink = QuestDBSink()

app = Application(
    consumer_group=os.environ.get("QUESTDB_CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=env_int("QUESTDB_BUFFER_SIZE", 1000),
    commit_interval=float(os.environ.get("QUESTDB_BUFFER_TIMEOUT", "1")),
)

input_topic = app.topic(os.environ.get("QUESTDB_INPUT", "solar-data"))
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)