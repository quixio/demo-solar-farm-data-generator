# DEPENDENCIES:
# pip install quixstreams
# pip install questdb
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import time
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BaseSink
import questdb.ingress as qdb_ingress
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BaseSink):
    def __init__(self):
        super().__init__()
        try:
            self.port = int(os.environ.get('QDB_PORT', '9009'))
        except ValueError:
            self.port = 9009
        
        self.host = os.environ.get('QDB_HOST', 'localhost')
        self.auth_key = os.environ.get('QDB_AUTH_TOKEN_KEY')
        self.database = os.environ.get('QDB_DATABASE', 'qdb')
        self.table_name = os.environ.get('QDB_TABLE_NAME', 'solar_data')
        self.sender = None
        self.table_created = False

    def setup(self):
        try:
            conf = f'http::addr={self.host}:{self.port};'
            if self.auth_key:
                conf += f'token={self.auth_key};'
            
            self.sender = qdb_ingress.Sender.from_conf(conf)
            print(f"Connected to QuestDB at {self.host}:{self.port}")
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise

    def create_table_if_not_exists(self):
        if not self.table_created:
            try:
                # Create table with all solar data fields
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    panel_id SYMBOL,
                    location_id SYMBOL,
                    location_name STRING,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    timezone INT,
                    power_output DOUBLE,
                    unit_power STRING,
                    temperature DOUBLE,
                    unit_temp STRING,
                    irradiance DOUBLE,
                    unit_irradiance STRING,
                    voltage DOUBLE,
                    unit_voltage STRING,
                    current DOUBLE,
                    unit_current STRING,
                    inverter_status STRING,
                    stream_id SYMBOL,
                    message_timestamp TIMESTAMP,
                    ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY;
                """
                
                # QuestDB doesn't support DDL through the ILP protocol
                # Table will be created automatically on first insert
                self.table_created = True
                print(f"Table {self.table_name} will be created on first insert")
            except Exception as e:
                print(f"Warning: Could not pre-create table: {e}")
                self.table_created = True

    def add(self, value, key, timestamp, headers, topic, partition, offset):
        if not self.table_created:
            self.create_table_if_not_exists()
        
        print(f'Raw message: {value}')
        
        try:
            # Parse the nested JSON value
            if isinstance(value.get('value'), str):
                solar_data = json.loads(value['value'])
            else:
                solar_data = value.get('value', {})
            
            # Convert timestamp to datetime
            if 'timestamp' in solar_data:
                # Convert nanosecond timestamp to datetime
                ts_ns = solar_data['timestamp']
                dt = datetime.fromtimestamp(ts_ns / 1_000_000_000)
            else:
                dt = datetime.now()
            
            # Convert message datetime
            msg_timestamp = datetime.fromisoformat(value.get('dateTime', '').replace('Z', '+00:00'))
            
            # Send data to QuestDB using ILP
            self.sender.row(
                self.table_name,
                symbols={
                    'panel_id': solar_data.get('panel_id', ''),
                    'location_id': solar_data.get('location_id', ''),
                    'stream_id': value.get('streamId', ''),
                },
                columns={
                    'location_name': solar_data.get('location_name', ''),
                    'latitude': float(solar_data.get('latitude', 0)),
                    'longitude': float(solar_data.get('longitude', 0)),
                    'timezone': int(solar_data.get('timezone', 0)),
                    'power_output': float(solar_data.get('power_output', 0)),
                    'unit_power': solar_data.get('unit_power', ''),
                    'temperature': float(solar_data.get('temperature', 0)),
                    'unit_temp': solar_data.get('unit_temp', ''),
                    'irradiance': float(solar_data.get('irradiance', 0)),
                    'unit_irradiance': solar_data.get('unit_irradiance', ''),
                    'voltage': float(solar_data.get('voltage', 0)),
                    'unit_voltage': solar_data.get('unit_voltage', ''),
                    'current': float(solar_data.get('current', 0)),
                    'unit_current': solar_data.get('unit_current', ''),
                    'inverter_status': solar_data.get('inverter_status', ''),
                    'message_timestamp': msg_timestamp,
                },
                at=dt
            )
            
        except Exception as e:
            print(f"Error processing message: {e}")
            print(f"Message value: {value}")

    def flush(self, topic, partition):
        if self.sender:
            self.sender.flush()

    def close(self):
        if self.sender:
            self.sender.close()

# Create application
try:
    buffer_size = int(os.environ.get('QDB_BUFFER_SIZE', '100'))
except ValueError:
    buffer_size = 100

try:
    buffer_timeout = float(os.environ.get('QDB_BUFFER_TIMEOUT', '5.0'))
except ValueError:
    buffer_timeout = 5.0

app = Application(
    consumer_group=os.environ.get('QDB_CONSUMER_GROUP_NAME', 'questdb-sink'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

input_topic = app.topic(os.environ.get('QDB_INPUT_TOPIC'))
sdf = app.dataframe(input_topic)

questdb_sink = QuestDBSink()
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)