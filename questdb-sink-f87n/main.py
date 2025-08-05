import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, user, password, database, table_name, port=8812, timestamp_column=None):
        super().__init__()
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table_name = table_name
        self.port = port
        self.timestamp_column = timestamp_column
        self.connection = None
        
    def setup(self):
        try:
            port = int(self.port)
        except (ValueError, TypeError):
            port = 8812
            
        self.connection = psycopg2.connect(
            host=self.host,
            port=port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.connection.autocommit = False
        
        # Create table if it doesn't exist
        cursor = self.connection.cursor()
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
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
            timestamp TIMESTAMP,
            message_datetime TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        cursor.execute(create_table_sql)
        self.connection.commit()
        cursor.close()
        
    def write(self, batch: SinkBatch):
        if not self.connection:
            self.setup()
            
        cursor = self.connection.cursor()
        
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the value field which contains JSON string
            if hasattr(item, 'value') and isinstance(item.value, str):
                try:
                    data = json.loads(item.value)
                except json.JSONDecodeError:
                    continue
            elif hasattr(item, 'value') and isinstance(item.value, dict):
                data = item.value
            else:
                continue
                
            # Convert timestamp from nanoseconds to datetime
            timestamp_ns = data.get('timestamp', 0)
            timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
            
            # Get message datetime
            message_datetime = None
            if hasattr(item, 'timestamp') and item.timestamp:
                message_datetime = datetime.fromtimestamp(item.timestamp / 1000)
            
            insert_sql = f"""
            INSERT INTO {self.table_name} (
                panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status,
                timestamp, message_datetime
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
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
                timestamp_dt,
                message_datetime
            )
            
            cursor.execute(insert_sql, values)
        
        self.connection.commit()
        cursor.close()

# Initialize QuestDB sink
try:
    port = int(os.environ.get('QUESTDB_PORT', '8812'))
except ValueError:
    port = 8812

questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST'),
    user=os.environ.get('QUESTDB_USER'),
    password=os.environ.get('QUESTDB_PASSWORD'),
    database=os.environ.get('QUESTDB_DATABASE'),
    table_name=os.environ.get('QUESTDB_TABLE_NAME', 'solar_data'),
    port=port,
    timestamp_column=os.environ.get('QUESTDB_TIMESTAMP_COLUMN')
)

# Initialize Quix Streams application
try:
    buffer_size = int(os.environ.get('QUESTDB_BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get('QUESTDB_BUFFER_TIMEOUT', '1.0'))
except ValueError:
    buffer_timeout = 1.0

app = Application(
    consumer_group=os.environ.get('QUESTDB_CONSUMER_GROUP_NAME', 'questdb-sink'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

input_topic = app.topic(os.environ.get('QUESTDB_INPUT_TOPIC'))
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run()