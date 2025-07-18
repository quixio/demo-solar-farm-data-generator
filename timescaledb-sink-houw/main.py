import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

# Load environment variables for local development
load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name):
        super().__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.cursor = None
        
    def setup(self):
        """Initialize database connection and create table if it doesn't exist."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TIMESTAMPTZ,
                panel_id VARCHAR(255),
                location_id VARCHAR(255),
                location_name VARCHAR(255),
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
                inverter_status VARCHAR(50)
            );
            """
            
            self.cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);"
                self.cursor.execute(hypertable_query)
            except psycopg2.errors.InvalidObjectName:
                # Hypertable already exists or TimescaleDB extension not available
                pass
            
            self.connection.commit()
            
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise e
    
    def write(self, batch: SinkBatch):
        """Write batch of messages to TimescaleDB."""
        if not self.connection:
            raise RuntimeError("Database connection not initialized")
        
        insert_query = f"""
        INSERT INTO {self.table_name} (
            timestamp, panel_id, location_id, location_name, latitude, longitude,
            timezone, power_output, unit_power, temperature, unit_temp, irradiance,
            unit_irradiance, voltage, unit_voltage, current, unit_current, inverter_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        batch_data = []
        for item in batch:
            try:
                # Parse the JSON string from the value field
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert epoch timestamp to datetime
                timestamp_epoch = data.get('timestamp', 0)
                if isinstance(timestamp_epoch, (int, float)):
                    # Convert nanoseconds to seconds if needed
                    if timestamp_epoch > 1e12:  # Likely nanoseconds
                        timestamp_epoch = timestamp_epoch / 1e9
                    timestamp_dt = datetime.fromtimestamp(timestamp_epoch)
                else:
                    timestamp_dt = datetime.now()
                
                row_data = (
                    timestamp_dt,
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
                    data.get('inverter_status')
                )
                
                batch_data.append(row_data)
                
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
        
        if batch_data:
            try:
                self.cursor.executemany(insert_query, batch_data)
                self.connection.commit()
                print(f"Successfully inserted {len(batch_data)} records")
            except Exception as e:
                self.connection.rollback()
                print(f"Error inserting batch: {e}")
                raise
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Get database configuration from environment variables
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'metrics'),
    user=os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav6')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), value_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debugging to show raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)