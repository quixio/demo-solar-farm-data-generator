# DEPENDENCIES:
# pip install quixstreams
# pip install questdb
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import questdb.ingress as qi
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, token_key=None, database=None, table_name=None, timestamp_column=None):
        super().__init__()
        self.host = host
        self.port = port
        self.token_key = token_key
        self.database = database
        self.table_name = table_name or "solar_data"
        self.timestamp_column = timestamp_column
        self.sender = None
        self.table_created = False

    def setup(self):
        """Initialize QuestDB ILP sender with correct protocol for port 9009"""
        conf = f'ilp::addr={self.host}:{self.port};'
        
        if self.token_key:
            conf += f'token={self.token_key};'
        if self.database:
            conf += f'database={self.database};'

        self.sender = qi.Sender.from_conf(conf)

    def write(self, batch: SinkBatch):
        if not self.sender:
            raise RuntimeError("QuestDB sender not initialized")

        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the value field which contains JSON string
            try:
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                elif hasattr(item, 'value') and isinstance(item.value, dict):
                    data = item.value
                else:
                    print(f"Unexpected data structure: {type(item.value)}")
                    continue
            except (json.JSONDecodeError, AttributeError) as e:
                print(f"Error parsing message: {e}")
                continue

            # Map message fields to table columns
            try:
                self.sender.row(
                    table_name=self.table_name,
                    columns={
                        'panel_id': data.get('panel_id'),
                        'location_id': data.get('location_id'),
                        'location_name': data.get('location_name'),
                        'latitude': data.get('latitude'),
                        'longitude': data.get('longitude'),
                        'timezone': data.get('timezone'),
                        'power_output': data.get('power_output'),
                        'unit_power': data.get('unit_power'),
                        'temperature': data.get('temperature'),
                        'unit_temp': data.get('unit_temp'),
                        'irradiance': data.get('irradiance'),
                        'unit_irradiance': data.get('unit_irradiance'),
                        'voltage': data.get('voltage'),
                        'unit_voltage': data.get('unit_voltage'),
                        'current': data.get('current'),
                        'unit_current': data.get('unit_current'),
                        'inverter_status': data.get('inverter_status')
                    },
                    at=qi.TimestampNanos.from_int(data.get('timestamp', 0))
                )
            except Exception as e:
                print(f"Error writing row to QuestDB: {e}")
                continue

        # Flush data to QuestDB
        try:
            self.sender.flush()
        except Exception as e:
            print(f"Error flushing to QuestDB: {e}")
            raise

# Initialize QuestDB Sink
try:
    port = int(os.environ.get('QDB_PORT', '9009'))
except ValueError:
    port = 9009

questdb_sink = QuestDBSink(
    host=os.environ.get('QDB_HOST', 'localhost'),
    port=port,
    token_key=os.environ.get('QDB_TOKEN_KEY'),
    database=os.environ.get('QDB_DATABASE'),
    table_name=os.environ.get('QDB_TABLE', 'solar_data'),
    timestamp_column=os.environ.get('QDB_TIMESTAMP_COLUMN')
)

# Initialize Application
try:
    buffer_size = int(os.environ.get('QDB_BUFFER_SIZE', '100'))
except ValueError:
    buffer_size = 100

try:
    buffer_timeout = float(os.environ.get('QDB_BUFFER_TIMEOUT', '1.0'))
except ValueError:
    buffer_timeout = 1.0

app = Application(
    consumer_group=os.environ.get('QDB_CONSUMER_GROUP_NAME', 'questdb-sink'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)