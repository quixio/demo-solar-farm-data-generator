# DEPENDENCIES:
# pip install quixstreams
# pip install python-dotenv
# pip install questdb[pandas]
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
    def __init__(self, host, port, username=None, password=None, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(on_client_connect_success=on_client_connect_success, on_client_connect_failure=on_client_connect_failure)
        self._host = host
        try:
            self._port = int(port)
        except (ValueError, TypeError):
            self._port = 9009
        self._username = username
        self._password = password
        self._sender = None
        
    def setup(self):
        try:
            auth = None
            if self._username and self._password:
                auth = (self._username, self._password)
            
            self._sender = sender.Sender(self._host, self._port, auth=auth)
            
            # Create table if it doesn't exist
            ddl = """
            CREATE TABLE IF NOT EXISTS solar_data (
                panel_id SYMBOL,
                location_id SYMBOL,  
                location_name STRING,
                latitude DOUBLE,
                longitude DOUBLE,
                timezone INT,
                power_output DOUBLE,
                unit_power SYMBOL,
                temperature DOUBLE,
                unit_temp SYMBOL,
                irradiance DOUBLE,
                unit_irradiance SYMBOL,
                voltage DOUBLE,
                unit_voltage SYMBOL,
                current DOUBLE,
                unit_current SYMBOL,
                inverter_status SYMBOL,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY DAY;
            """
            
            self._sender.query(ddl)
            self._sender.flush()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def write(self, batch: SinkBatch):
        try:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the JSON value field
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert timestamp to datetime
                timestamp_ns = data.get('timestamp', 0)
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                
                # Map message data to table schema
                self._sender.row(
                    'solar_data',
                    symbols={
                        'panel_id': data.get('panel_id', ''),
                        'location_id': data.get('location_id', ''),
                        'unit_power': data.get('unit_power', ''),
                        'unit_temp': data.get('unit_temp', ''),
                        'unit_irradiance': data.get('unit_irradiance', ''),
                        'unit_voltage': data.get('unit_voltage', ''),
                        'unit_current': data.get('unit_current', ''),
                        'inverter_status': data.get('inverter_status', ''),
                    },
                    columns={
                        'location_name': data.get('location_name', ''),
                        'latitude': float(data.get('latitude', 0)),
                        'longitude': float(data.get('longitude', 0)),
                        'timezone': int(data.get('timezone', 0)),
                        'power_output': float(data.get('power_output', 0)),
                        'temperature': float(data.get('temperature', 0)),
                        'irradiance': float(data.get('irradiance', 0)),
                        'voltage': float(data.get('voltage', 0)),
                        'current': float(data.get('current', 0)),
                    },
                    at=timestamp_dt
                )
            
            self._sender.flush()
            
        except Exception as e:
            print(f"Error writing to QuestDB: {e}")
            raise
    
    def close(self):
        if self._sender:
            self._sender.close()

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST', 'localhost'),
    port=os.environ.get('QUESTDB_PORT', '9009'),
    username=os.environ.get('QUESTDB_USERNAME'),
    password=os.environ.get('QUESTDB_PASSWORD')
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