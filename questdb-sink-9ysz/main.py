# DEPENDENCIES:
# pip install quixstreams
# pip install python-dotenv
# pip install questdb
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import questdb.ingress as qdb

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._sender = None

    def setup(self):
        try:
            host = os.environ.get('QUESTDB_HOST', 'localhost')
            try:
                port = int(os.environ.get('QUESTDB_PORT', '9009'))
            except ValueError:
                port = 9009
            
            self._sender = qdb.Sender(host=host, port=port)
            
            # Test connection by trying to flush
            self._sender.flush()
            
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise

    def write(self, batch: SinkBatch):
        if not self._sender:
            raise RuntimeError("QuestDB sender not initialized")
        
        try:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the value field which contains JSON string
                if hasattr(item, 'value') and isinstance(item.value, str):
                    try:
                        data = json.loads(item.value)
                    except json.JSONDecodeError:
                        print(f"Failed to parse JSON from value: {item.value}")
                        continue
                else:
                    # If value is already a dict or other type
                    data = item.value if hasattr(item, 'value') else item
                
                # Extract data fields safely
                panel_id = data.get('panel_id', '')
                location_id = data.get('location_id', '')
                location_name = data.get('location_name', '')
                latitude = data.get('latitude', 0.0)
                longitude = data.get('longitude', 0.0)
                timezone = data.get('timezone', 0)
                power_output = data.get('power_output', 0.0)
                temperature = data.get('temperature', 0.0)
                irradiance = data.get('irradiance', 0.0)
                voltage = data.get('voltage', 0.0)
                current = data.get('current', 0.0)
                inverter_status = data.get('inverter_status', '')
                timestamp = data.get('timestamp', 0)
                
                # Convert timestamp to datetime if it's a valid epoch timestamp
                if timestamp and isinstance(timestamp, (int, float)):
                    try:
                        # Assuming timestamp is in nanoseconds based on the large value in example
                        dt = datetime.fromtimestamp(timestamp / 1_000_000_000)
                    except (ValueError, OSError):
                        # Fallback to current time if timestamp conversion fails
                        dt = datetime.now()
                else:
                    dt = datetime.now()
                
                # Send data to QuestDB using line protocol
                self._sender.table('solar_data') \
                    .symbol('panel_id', panel_id) \
                    .symbol('location_id', location_id) \
                    .symbol('location_name', location_name) \
                    .symbol('inverter_status', inverter_status) \
                    .column('latitude', latitude) \
                    .column('longitude', longitude) \
                    .column('timezone', timezone) \
                    .column('power_output', power_output) \
                    .column('temperature', temperature) \
                    .column('irradiance', irradiance) \
                    .column('voltage', voltage) \
                    .column('current', current) \
                    .at(dt)
            
            # Flush all data to QuestDB
            self._sender.flush()
            
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise

    def close(self):
        if self._sender:
            try:
                self._sender.close()
            except Exception as e:
                print(f"Error closing QuestDB connection: {e}")

# Initialize the custom QuestDB sink
questdb_sink = QuestDBSink()

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-data-writer"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1")),
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)