import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self._connection = None
        self._cursor = None
        
    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor()
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TIMESTAMPTZ NOT NULL,
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
            self._cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                self._cursor.execute(f"SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);")
            except psycopg2.errors.DuplicateTable:
                pass  # Table is already a hypertable
            except Exception as e:
                print(f"Warning: Could not create hypertable: {e}")
                
            self._connection.commit()
            print(f"Table {self.table_name} created/verified successfully")
            
            if self.on_client_connect_success:
                self.on_client_connect_success()
                
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            if self.on_client_connect_failure:
                self.on_client_connect_failure(e)
            raise
    
    def write(self, batch: SinkBatch):
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not established")
            
        insert_query = f"""
        INSERT INTO {self.table_name} (
            timestamp, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        data_to_insert = []
        for item in batch:
            try:
                # Parse the JSON value from the message
                parsed_data = json.loads(item.value)
                
                # Convert epoch timestamp to datetime
                timestamp_ms = parsed_data.get('timestamp')
                if timestamp_ms:
                    # Convert nanoseconds to seconds
                    timestamp_seconds = timestamp_ms / 1_000_000_000
                    dt = datetime.fromtimestamp(timestamp_seconds)
                else:
                    dt = datetime.now()
                
                # Map message fields to table columns
                row_data = (
                    dt,
                    parsed_data.get('panel_id'),
                    parsed_data.get('location_id'),
                    parsed_data.get('location_name'),
                    parsed_data.get('latitude'),
                    parsed_data.get('longitude'),
                    parsed_data.get('timezone'),
                    parsed_data.get('power_output'),
                    parsed_data.get('unit_power'),
                    parsed_data.get('temperature'),
                    parsed_data.get('unit_temp'),
                    parsed_data.get('irradiance'),
                    parsed_data.get('unit_irradiance'),
                    parsed_data.get('voltage'),
                    parsed_data.get('unit_voltage'),
                    parsed_data.get('current'),
                    parsed_data.get('unit_current'),
                    parsed_data.get('inverter_status')
                )
                
                data_to_insert.append(row_data)
                
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON from message: {e}")
                continue
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
        
        if data_to_insert:
            try:
                self._cursor.executemany(insert_query, data_to_insert)
                self._connection.commit()
                print(f"Successfully inserted {len(data_to_insert)} records")
            except Exception as e:
                print(f"Failed to insert data: {e}")
                self._connection.rollback()
                raise
    
    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Get port with safe conversion
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
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav3')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Add debug print to see raw message structure
sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)