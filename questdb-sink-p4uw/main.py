import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._connection = None
        self._table_name = os.environ.get('QDB_TABLE_NAME', 'solar_data')
        self._timestamp_column = os.environ.get('QDB_TIMESTAMP_COLUMN', 'timestamp')
        
    def setup(self):
        try:
            port = int(os.environ.get('QDB_PORT', '8812'))
        except ValueError:
            port = 8812
            
        self._connection = psycopg2.connect(
            host=os.environ.get('QDB_HOST'),
            port=port,
            user='admin',
            password=os.environ.get('QDB_AUTH_TOKEN_KEY'),
            database='qdb'
        )
        self._connection.autocommit = True
        
        # Create table if it doesn't exist
        cursor = self._connection.cursor()
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
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
            {self._timestamp_column} TIMESTAMP
        ) TIMESTAMP({self._timestamp_column}) PARTITION BY DAY;
        """
        cursor.execute(create_table_sql)
        cursor.close()
        
    def write(self, batch: SinkBatch):
        cursor = self._connection.cursor()
        
        for item in batch:
            # Parse the JSON string from the value field if it's a string
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
                
            # Convert epoch timestamp to datetime
            timestamp_value = data.get('timestamp')
            if timestamp_value:
                # Assuming nanosecond timestamp, convert to datetime
                dt = datetime.fromtimestamp(timestamp_value / 1_000_000_000)
                
            insert_sql = f"""
            INSERT INTO {self._table_name} (
                panel_id, location_id, location_name, latitude, longitude,
                timezone, power_output, unit_power, temperature, unit_temp,
                irradiance, unit_irradiance, voltage, unit_voltage, current,
                unit_current, inverter_status, {self._timestamp_column}
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                dt
            ))
            
        cursor.close()

# Initialize the application
app = Application(
    consumer_group=os.environ.get('QDB_CONSUMER_GROUP_NAME', 'questdb-solar-data-writer'),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get('QDB_BATCH_SIZE', '100')),
    commit_interval=float(os.environ.get('QDB_BATCH_TIMEOUT', '5.0')),
)

# Get input topic
input_topic = app.topic(os.environ.get('QDB_INPUT_TOPIC'))

# Create the sink
questdb_sink = QuestDBSink()

# Create streaming dataframe and add sink
sdf = app.dataframe(input_topic)

# Add debugging print
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)