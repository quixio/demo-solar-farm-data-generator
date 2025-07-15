import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
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
        """Set up TimescaleDB connection and create table if it doesn't exist"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            # Create table if it doesn't exist
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
                    panel_id VARCHAR(255),
                    location_id VARCHAR(255),
                    location_name VARCHAR(255),
                    latitude FLOAT,
                    longitude FLOAT,
                    timezone INTEGER,
                    power_output FLOAT,
                    unit_power VARCHAR(50),
                    temperature FLOAT,
                    unit_temp VARCHAR(50),
                    irradiance FLOAT,
                    unit_irradiance VARCHAR(50),
                    voltage FLOAT,
                    unit_voltage VARCHAR(50),
                    current FLOAT,
                    unit_current VARCHAR(50),
                    inverter_status VARCHAR(50),
                    timestamp BIGINT,
                    datetime TIMESTAMPTZ DEFAULT NOW()
                );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()
            
            # Try to create hypertable (if TimescaleDB is available)
            try:
                hypertable_query = f"""
                    SELECT create_hypertable('{self.schema_name}.{self.table_name}', 'datetime', if_not_exists => TRUE);
                """
                self.cursor.execute(hypertable_query)
                self.connection.commit()
            except Exception as e:
                print(f"Note: Could not create hypertable (TimescaleDB may not be available): {e}")
                
        except Exception as e:
            print(f"Error setting up TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        """Write batch of messages to TimescaleDB"""
        if not self.connection or not self.cursor:
            raise RuntimeError("TimescaleDB connection not established")
        
        try:
            for item in batch:
                print(f"Raw message: {item}")
                
                # Parse the JSON data from the value field
                solar_data = json.loads(item.value)
                
                # Map message fields to table columns
                insert_query = f"""
                    INSERT INTO {self.schema_name}.{self.table_name} (
                        panel_id, location_id, location_name, latitude, longitude, timezone,
                        power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                        voltage, unit_voltage, current, unit_current, inverter_status, timestamp
                    ) VALUES (
                        %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, %(longitude)s, %(timezone)s,
                        %(power_output)s, %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s,
                        %(voltage)s, %(unit_voltage)s, %(current)s, %(unit_current)s, %(inverter_status)s, %(timestamp)s
                    )
                """
                
                # Execute insert with safe field access
                self.cursor.execute(insert_query, {
                    'panel_id': solar_data.get('panel_id'),
                    'location_id': solar_data.get('location_id'),
                    'location_name': solar_data.get('location_name'),
                    'latitude': solar_data.get('latitude'),
                    'longitude': solar_data.get('longitude'),
                    'timezone': solar_data.get('timezone'),
                    'power_output': solar_data.get('power_output'),
                    'unit_power': solar_data.get('unit_power'),
                    'temperature': solar_data.get('temperature'),
                    'unit_temp': solar_data.get('unit_temp'),
                    'irradiance': solar_data.get('irradiance'),
                    'unit_irradiance': solar_data.get('unit_irradiance'),
                    'voltage': solar_data.get('voltage'),
                    'unit_voltage': solar_data.get('unit_voltage'),
                    'current': solar_data.get('current'),
                    'unit_current': solar_data.get('unit_current'),
                    'inverter_status': solar_data.get('inverter_status'),
                    'timestamp': solar_data.get('timestamp')
                })
            
            # Commit the transaction
            self.connection.commit()
            
        except Exception as e:
            print(f"Error writing to TimescaleDB: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST'),
    port=int(os.environ.get('TIMESCALEDB_PORT', 5432)),
    dbname=os.environ.get('TIMESCALEDB_DATABASE'),
    user=os.environ.get('TIMESCALEDB_USER'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data'),
    schema_name=os.environ.get('TIMESCALEDB_SCHEMA', 'public')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'timescale-sink-group'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic with string deserializer to keep value as JSON string
input_topic = app.topic(
    os.environ.get("input", "solar-data"),
    key_deserializer="string",
    value_deserializer="string"
)

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)