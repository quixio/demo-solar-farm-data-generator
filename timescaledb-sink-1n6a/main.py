import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, dbname, user, password, table_name, schema_name="public"):
        super().__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.schema_name = schema_name
        self.connection = None
        self.cursor = None

    def setup(self):
        """Set up the connection to TimescaleDB and create table if needed."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
                id SERIAL PRIMARY KEY,
                panel_id VARCHAR(255),
                location_id VARCHAR(255),
                location_name VARCHAR(255),
                latitude FLOAT,
                longitude FLOAT,
                timezone INTEGER,
                power_output FLOAT,
                unit_power VARCHAR(10),
                temperature FLOAT,
                unit_temp VARCHAR(10),
                irradiance FLOAT,
                unit_irradiance VARCHAR(10),
                voltage FLOAT,
                unit_voltage VARCHAR(10),
                current FLOAT,
                unit_current VARCHAR(10),
                inverter_status VARCHAR(50),
                timestamp BIGINT,
                datetime_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()
            
            # Create hypertable if it doesn't exist (TimescaleDB extension)
            try:
                hypertable_query = f"SELECT create_hypertable('{self.schema_name}.{self.table_name}', 'datetime_created', if_not_exists => TRUE);"
                self.cursor.execute(hypertable_query)
                self.connection.commit()
            except psycopg2.Error:
                # If hypertable creation fails, continue without it
                pass
                
        except psycopg2.Error as e:
            print(f"Error setting up TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        """Write a batch of records to TimescaleDB."""
        if not self.connection or not self.cursor:
            raise RuntimeError("TimescaleDB connection not established")
            
        records = []
        for item in batch:
            try:
                # Parse the JSON string from the value field
                if hasattr(item, 'value') and isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Map message fields to table columns
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
                    data.get('timestamp')
                )
                records.append(record)
                
            except (json.JSONDecodeError, KeyError, AttributeError) as e:
                print(f"Error processing record: {e}")
                continue
        
        if records:
            try:
                insert_query = f"""
                INSERT INTO {self.schema_name}.{self.table_name} 
                (panel_id, location_id, location_name, latitude, longitude, timezone, 
                 power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                 voltage, unit_voltage, current, unit_current, inverter_status, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.cursor.executemany(insert_query, records)
                self.connection.commit()
                print(f"Successfully inserted {len(records)} records into TimescaleDB")
                
            except psycopg2.Error as e:
                print(f"Error inserting records: {e}")
                self.connection.rollback()
                raise

    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
    port=int(os.environ.get('TIMESCALEDB_PORT', '5432')),
    dbname=os.environ.get('TIMESCALEDB_DATABASE', 'timescaledb'),
    user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data'),
    schema_name=os.environ.get('TIMESCALEDB_SCHEMA', 'public')
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

# Add debugging to see raw message structure
sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)