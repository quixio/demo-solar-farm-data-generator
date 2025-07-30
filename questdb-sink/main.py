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
        self._table_name = os.environ.get('QUESTDB_TABLE_NAME', 'solar_data')

    def setup(self):
        try:
            port = int(os.environ.get('QUESTDB_PORT', '8812'))
        except ValueError:
            port = 8812
            
        # Connect to QuestDB using PostgreSQL wire protocol
        self._connection = psycopg2.connect(
            host=os.environ.get('QUESTDB_HOST'),
            port=port,
            database=os.environ.get('QUESTDB_DATABASE', 'qdb'),
            user=os.environ.get('QUESTDB_API_KEY'),
            password=os.environ.get('QUESTDB_API_KEY')
        )
        self._connection.autocommit = True
        
        # Create table if it doesn't exist
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        cursor = self._connection.cursor()
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            panel_id SYMBOL,
            location_id SYMBOL,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output INT,
            unit_power SYMBOL,
            temperature DOUBLE,
            unit_temp SYMBOL,
            irradiance INT,
            unit_irradiance SYMBOL,
            voltage DOUBLE,
            unit_voltage SYMBOL,
            current INT,
            unit_current SYMBOL,
            inverter_status SYMBOL,
            timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        
        cursor.execute(create_table_sql)
        cursor.close()

    def write(self, batch: SinkBatch):
        cursor = self._connection.cursor()
        
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON value from the message
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
            
            # Convert timestamp to datetime
            timestamp_ms = data.get('timestamp', 0)
            # Convert nanoseconds to milliseconds if needed
            if timestamp_ms > 1e12:  # If timestamp is in nanoseconds
                timestamp_ms = timestamp_ms // 1000000
            dt = datetime.fromtimestamp(timestamp_ms / 1000.0)
            
            # Insert data into QuestDB
            insert_sql = f"""
            INSERT INTO {self._table_name} (
                panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status, timestamp
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

# Initialize the Quix application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "100")),
    commit_interval=float(os.environ.get("BUFFER_TIMEOUT", "5.0")),
)

# Set up input topic
input_topic = app.topic(os.environ.get('INPUT_TOPIC'))

# Create the custom QuestDB sink
questdb_sink = QuestDBSink()

# Create streaming dataframe and sink data
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)