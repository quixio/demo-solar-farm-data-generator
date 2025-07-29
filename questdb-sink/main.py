import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, table_name, timestamp_column=None, **kwargs):
        super().__init__(**kwargs)
        self._host = host
        self._table_name = table_name
        self._timestamp_column = timestamp_column
        self._connection = None
        
    def setup(self):
        """Initialize connection to QuestDB"""
        try:
            port = int(os.environ.get('QUESTDB_PORT', '8812'))
        except ValueError:
            port = 8812
            
        self._connection = psycopg2.connect(
            host=self._host,
            port=port,
            database='qdb'
        )
        self._connection.autocommit = True
        
        # Create table if it doesn't exist
        self._create_table_if_not_exists()
        
    def _create_table_if_not_exists(self):
        """Create table with appropriate schema"""
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
            timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_sql)
        self._connection.commit()
        
    def write(self, batch: SinkBatch):
        """Write batch of messages to QuestDB"""
        if not batch:
            return
            
        insert_sql = f"""
        INSERT INTO {self._table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        records = []
        for item in batch:
            # Parse the JSON string from the value field
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
                
            # Convert timestamp to datetime
            timestamp_ns = data.get('timestamp', 0)
            timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
            
            record = (
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
                timestamp_dt
            )
            records.append(record)
        
        with self._connection.cursor() as cursor:
            cursor.executemany(insert_sql, records)

# Application setup
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-solar-data-writer"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "10")),
    commit_interval=float(os.environ.get("BUFFER_TIMEOUT", "1.0")),
)

input_topic = app.topic(os.environ.get("INPUT_TOPIC"))
sdf = app.dataframe(input_topic)

# Debug: Print raw message structure
sdf.print(metadata=True)

# Create QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST'),
    table_name=os.environ.get('QUESTDB_TABLE_NAME', 'solar_data'),
    timestamp_column=os.environ.get('TIMESTAMP_COLUMN')
)

sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)