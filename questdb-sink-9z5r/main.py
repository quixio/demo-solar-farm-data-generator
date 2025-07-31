# DEPENDENCIES:
# pip install questdb
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import questdb

from dotenv import load_dotenv
load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, username, password, database, table_name, timestamp_column):
        super().__init__()
        self.host = host
        try:
            self.port = int(port)
        except (ValueError, TypeError):
            self.port = 9009
        self.username = username
        self.password = password
        self.database = database
        self.table_name = table_name
        self.timestamp_column = timestamp_column
        self.sender = None
        self.table_created = False

    def setup(self):
        self.sender = questdb.Sender.from_conf(f'http::addr={self.host}:{self.port};username={self.username};token={self.password};')
        
    def _create_table_if_not_exists(self):
        if not self.table_created:
            try:
                self.sender.table(self.table_name)
                self.table_created = True
            except Exception as e:
                print(f"Error checking/creating table: {e}")
        
    def write(self, batch: SinkBatch):
        self._create_table_if_not_exists()
        
        for item in batch:
            print(f'Raw message: {item}')
            
            try:
                if hasattr(item, 'value') and item.value:
                    if isinstance(item.value, str):
                        data = json.loads(item.value)
                    else:
                        data = item.value
                elif hasattr(item, 'key') and item.key:
                    if isinstance(item.key, str):
                        data = json.loads(item.key)
                    else:
                        data = item.key
                else:
                    print(f"Warning: No data found in message: {item}")
                    continue
                
                if not data:
                    continue
                    
                print(f"Parsed data: {data}")
                
                # Map solar farm data to QuestDB
                row = self.sender.table(self.table_name)
                
                # Add symbol columns
                row.symbol('panel_id', data.get('panel_id', ''))
                row.symbol('location_id', data.get('location_id', ''))
                row.symbol('location_name', data.get('location_name', ''))
                row.symbol('inverter_status', data.get('inverter_status', ''))
                
                # Add numeric columns
                if 'latitude' in data:
                    row.float64_column('latitude', float(data['latitude']))
                if 'longitude' in data:
                    row.float64_column('longitude', float(data['longitude']))
                if 'timezone' in data:
                    row.int64_column('timezone', int(data['timezone']))
                if 'power_output' in data:
                    row.float64_column('power_output', float(data['power_output']))
                if 'temperature' in data:
                    row.float64_column('temperature', float(data['temperature']))
                if 'irradiance' in data:
                    row.float64_column('irradiance', float(data['irradiance']))
                if 'voltage' in data:
                    row.float64_column('voltage', float(data['voltage']))
                if 'current' in data:
                    row.float64_column('current', float(data['current']))
                
                # Add unit columns as strings
                if 'unit_power' in data:
                    row.symbol('unit_power', data['unit_power'])
                if 'unit_temp' in data:
                    row.symbol('unit_temp', data['unit_temp'])
                if 'unit_irradiance' in data:
                    row.symbol('unit_irradiance', data['unit_irradiance'])
                if 'unit_voltage' in data:
                    row.symbol('unit_voltage', data['unit_voltage'])
                if 'unit_current' in data:
                    row.symbol('unit_current', data['unit_current'])
                
                # Handle timestamp
                if self.timestamp_column and self.timestamp_column in data:
                    timestamp_value = data[self.timestamp_column]
                    if isinstance(timestamp_value, (int, float)):
                        if timestamp_value > 1e12:
                            timestamp_value = timestamp_value / 1000000
                        row.at_now()
                    else:
                        row.at_now()
                else:
                    row.at_now()
                
                print(f"Successfully processed record for panel: {data.get('panel_id', 'unknown')}")
                
            except Exception as e:
                print(f"Error processing record: {e}")
                print(f"Item content: {item}")
                continue
        
        try:
            self.sender.flush()
            print(f"Successfully wrote {len(batch)} records to QuestDB")
        except Exception as e:
            print(f"Error flushing to QuestDB: {e}")

    def close(self):
        if self.sender:
            self.sender.close()

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QDB_HOST', 'localhost'),
    port=os.environ.get('QDB_PORT', '9009'),
    username='admin',
    password=os.environ.get('QDB_TOKEN_KEY', ''),
    database=os.environ.get('QDB_DATABASE', 'qdb'),
    table_name=os.environ.get('QDB_TABLE', 'solar_data'),
    timestamp_column=os.environ.get('QDB_TIMESTAMP_COLUMN', 'timestamp')
)

try:
    buffer_size = int(os.environ.get('QDB_BUFFER_SIZE', '100'))
except (ValueError, TypeError):
    buffer_size = 100

try:
    buffer_timeout = float(os.environ.get('QDB_BUFFER_TIMEOUT', '5.0'))
except (ValueError, TypeError):
    buffer_timeout = 5.0

app = Application(
    consumer_group=os.environ.get('QDB_CONSUMER_GROUP_NAME', 'questdb-solar-data-writer'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)