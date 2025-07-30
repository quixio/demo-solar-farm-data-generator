import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, database, username, api_key, table_name):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.api_key = api_key
        self.table_name = table_name
        self.connection = None
        
    def setup(self):
        """Setup the database connection and create table if needed"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.api_key
            )
            self.connection.autocommit = True
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the table with proper schema if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
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
            message_datetime TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            self.connection.commit()
            print(f"Table {self.table_name} created or already exists")
    
    def write(self, batch: SinkBatch):
        """Write batch of messages to QuestDB"""
        if not self.connection:
            raise RuntimeError("Database connection not established")
            
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        with self.connection.cursor() as cursor:
            for item in batch:
                print(f"Raw message: {item}")
                
                # Parse the JSON string from the value field
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert timestamp to datetime
                timestamp_ns = data.get('timestamp', 0)
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                
                # Parse message datetime
                message_dt = datetime.fromisoformat(item.headers.get('dateTime', '').replace('Z', '+00:00')) if item.headers.get('dateTime') else datetime.now()
                
                # Map data to table schema
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
                    message_dt
                )
                
                cursor.execute(insert_sql, values)
            
            self.connection.commit()
            print(f"Successfully wrote {len(batch)} records to QuestDB")

# Configuration
try:
    port = int(os.environ.get('QUESTDB_PORT', '8812'))
except ValueError:
    port = 8812

try:
    buffer_size = int(os.environ.get('BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get('BUFFER_TIMEOUT', '1.0'))
except ValueError:
    buffer_timeout = 1.0

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST', 'localhost'),
    port=port,
    database=os.environ.get('QUESTDB_DATABASE', 'qdb'),
    username="admin",  # Hardcoded as requested
    api_key=os.environ.get('QUESTDB_API_KEY'),
    table_name=os.environ.get('QUESTDB_TABLE_NAME', 'solar_data')
)

# Initialize application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'questdb-solar-data-writer'),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

# Set up input topic
input_topic = app.topic(os.environ.get('INPUT_TOPIC'))
sdf = app.dataframe(input_topic)

# Sink data to QuestDB
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)