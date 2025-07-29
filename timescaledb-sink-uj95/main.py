import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

def _as_bool(env_var: str, default="false") -> bool:
    return os.environ.get(env_var, default).lower() == "true"

class TimescaleDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._connection = None
        self._cursor = None
        
    def setup(self):
        # Get connection parameters
        host = os.environ.get('TSDB_HOST')
        try:
            port = int(os.environ.get('TSDB_PORT', '5432'))
        except ValueError:
            port = 5432
        dbname = os.environ.get('TSDB_DBNAME')
        user = os.environ.get('TSDB_USER')
        password = os.environ.get('TSDB_PASSWORD_KEY')
        
        # Connect to TimescaleDB
        self._connection = psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password
        )
        self._cursor = self._connection.cursor()
        
        # Create table if it doesn't exist
        schema_name = os.environ.get('TSDB_SCHEMA', 'public')
        table_name = os.environ.get('TSDB_TABLE', 'solar_data')
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            panel_id TEXT,
            location_id TEXT,
            location_name TEXT,
            latitude FLOAT,
            longitude FLOAT,
            timezone INTEGER,
            power_output INTEGER,
            unit_power TEXT,
            temperature FLOAT,
            unit_temp TEXT,
            irradiance INTEGER,
            unit_irradiance TEXT,
            voltage FLOAT,
            unit_voltage TEXT,
            current INTEGER,
            unit_current TEXT,
            inverter_status TEXT,
            timestamp TIMESTAMPTZ,
            PRIMARY KEY (panel_id, timestamp)
        );
        """
        
        self._cursor.execute(create_table_sql)
        self._connection.commit()
        
    def write(self, batch: SinkBatch):
        schema_name = os.environ.get('TSDB_SCHEMA', 'public')
        table_name = os.environ.get('TSDB_TABLE', 'solar_data')
        
        insert_sql = f"""
        INSERT INTO {schema_name}.{table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (panel_id, timestamp) DO UPDATE SET
            location_id = EXCLUDED.location_id,
            location_name = EXCLUDED.location_name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            timezone = EXCLUDED.timezone,
            power_output = EXCLUDED.power_output,
            unit_power = EXCLUDED.unit_power,
            temperature = EXCLUDED.temperature,
            unit_temp = EXCLUDED.unit_temp,
            irradiance = EXCLUDED.irradiance,
            unit_irradiance = EXCLUDED.unit_irradiance,
            voltage = EXCLUDED.voltage,
            unit_voltage = EXCLUDED.unit_voltage,
            current = EXCLUDED.current,
            unit_current = EXCLUDED.unit_current,
            inverter_status = EXCLUDED.inverter_status
        """
        
        data_to_insert = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON string from the value field
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
            
            # Convert timestamp to datetime
            timestamp_ms = data.get('timestamp', 0)
            # Convert nanoseconds to seconds
            timestamp_seconds = timestamp_ms / 1_000_000_000
            dt = datetime.fromtimestamp(timestamp_seconds)
            
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
                dt
            )
            data_to_insert.append(record)
        
        # Execute batch insert
        self._cursor.executemany(insert_sql, data_to_insert)
        self._connection.commit()
        
    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Initialize the TimescaleDB sink
timescale_sink = TimescaleDBSink()

# Initialize the application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'timescale-sink-group'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get('BATCH_TIMEOUT', '1')),
    commit_every=int(os.environ.get('BATCH_SIZE', '1000'))
)

# Define the input topic
input_topic = app.topic(os.environ.get('input'), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)