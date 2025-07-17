import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self.connection = None
        self.cursor = None
        
    def setup(self):
        # Get connection parameters
        try:
            port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
        except ValueError:
            port = 5432
            
        # Connect to TimescaleDB
        self.connection = psycopg2.connect(
            host=os.environ.get('TIMESCALEDB_HOST'),
            port=port,
            database=os.environ.get('TIMESCALEDB_DBNAME'),
            user=os.environ.get('TIMESCALEDB_USERNAME'),
            password=os.environ.get('TIMESCALEDB_PASSWORD')
        )
        self.cursor = self.connection.cursor()
        
        # Create table if it doesn't exist
        table_name = os.environ.get('TIMESCALEDB_TABLE_NAME', 'solar_datav2')
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            panel_id VARCHAR(50),
            location_id VARCHAR(50),
            location_name VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            timezone INTEGER,
            power_output INTEGER,
            unit_power VARCHAR(10),
            temperature FLOAT,
            unit_temp VARCHAR(10),
            irradiance INTEGER,
            unit_irradiance VARCHAR(20),
            voltage FLOAT,
            unit_voltage VARCHAR(10),
            current INTEGER,
            unit_current VARCHAR(10),
            inverter_status VARCHAR(20),
            timestamp TIMESTAMPTZ,
            PRIMARY KEY (panel_id, timestamp)
        );
        """
        
        self.cursor.execute(create_table_query)
        self.connection.commit()
        
        # Create hypertable if it doesn't exist
        try:
            self.cursor.execute(f"SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE);")
            self.connection.commit()
        except psycopg2.Error:
            # Hypertable might already exist or TimescaleDB extension not available
            pass
    
    def write(self, batch: SinkBatch):
        table_name = os.environ.get('TIMESCALEDB_TABLE_NAME', 'solar_datav2')
        
        insert_query = f"""
        INSERT INTO {table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        for item in batch:
            # Parse the JSON string from the value field
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
                
            # Convert timestamp to datetime
            timestamp_ns = data.get('timestamp', 0)
            timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1e9)
            
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
                timestamp_dt
            )
            
            self.cursor.execute(insert_query, values)
        
        self.connection.commit()

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")

# Create TimescaleDB sink
timescale_sink = TimescaleDBSink()

# Process and sink data
sdf = app.dataframe(input_topic)

# Debug: Print raw messages
sdf = sdf.apply(lambda msg: print(f'Raw message: {msg}') or msg)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)