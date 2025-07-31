# DEPENDENCIES:
# pip install quixstreams
# pip install psycopg2-binary
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import psycopg2
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self.connection = None
        
    def setup(self):
        try:
            port = int(os.environ.get('QUESTDB_PORT', '8812'))
        except ValueError:
            port = 8812
            
        self.connection = psycopg2.connect(
            host=os.environ.get('QUESTDB_HOST'),
            port=port,
            user=os.environ.get('QUESTDB_USERNAME'),
            password=os.environ.get('QUESTDB_PASSWORD_KEY'),
            database=os.environ.get('QUESTDB_DATABASE')
        )
        self.connection.autocommit = True
        
        # Create table if it doesn't exist
        table_name = os.environ.get('QUESTDB_TABLE_NAME')
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            panel_id STRING,
            location_id STRING,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output INT,
            unit_power STRING,
            temperature DOUBLE,
            unit_temp STRING,
            irradiance INT,
            unit_irradiance STRING,
            voltage DOUBLE,
            unit_voltage STRING,
            current INT,
            unit_current STRING,
            inverter_status STRING,
            timestamp TIMESTAMP,
            ts TIMESTAMP
        ) timestamp(ts) PARTITION BY DAY
        """
        
        cursor = self.connection.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        
    def write(self, batch: SinkBatch):
        table_name = os.environ.get('QUESTDB_TABLE_NAME')
        cursor = self.connection.cursor()
        
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON value string
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
                
            # Convert timestamp to proper format
            timestamp_ns = data.get('timestamp', 0)
            timestamp_ms = timestamp_ns // 1000000  # Convert nanoseconds to milliseconds
            
            insert_sql = f"""
            INSERT INTO {table_name} (
                panel_id, location_id, location_name, latitude, longitude, 
                timezone, power_output, unit_power, temperature, unit_temp,
                irradiance, unit_irradiance, voltage, unit_voltage, current,
                unit_current, inverter_status, timestamp, ts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), systimestamp())
            """
            
            cursor.execute(insert_sql, (
                data.get('panel_id'),
                data.get('location_id'),
                data.get('location_name'),
                data.get('latitude'),
                data.get('longitude'),
                data.get('timezone'),
                data.get('power_output'),
                data.get('unit_power'),
                data.get('temperature'),
                data.get('unit_temp'),
                data.get('irradiance'),
                data.get('unit_irradiance'),
                data.get('voltage'),
                data.get('unit_voltage'),
                data.get('current'),
                data.get('unit_current'),
                data.get('inverter_status'),
                timestamp_ms,
                timestamp_ms
            ))
            
        cursor.close()

questdb_sink = QuestDBSink()

try:
    buffer_size = int(os.environ.get('QUESTDB_BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get('QUESTDB_BUFFER_TIMEOUT', '1'))
except ValueError:
    buffer_timeout = 1.0

app = Application(
    consumer_group=os.environ.get('QUESTDB_CONSUMER_GROUP_NAME', 'questdb-consumer'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

input_topic = app.topic(os.environ.get('QUESTDB_INPUT'))
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)