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
    """
    Custom TimescaleDB sink for solar farm data
    """
    def __init__(self, host, port, dbname, user, password, table_name, schema_name="public"):
        super().__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.schema_name = schema_name
        self._connection = None
        self._cursor = None

    def setup(self):
        """Setup the TimescaleDB connection and create table if it doesn't exist"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self._cursor = self._connection.cursor(cursor_factory=RealDictCursor)
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
                panel_id VARCHAR(255),
                location_id VARCHAR(255),
                location_name VARCHAR(255),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                timezone INTEGER,
                power_output DOUBLE PRECISION,
                unit_power VARCHAR(10),
                temperature DOUBLE PRECISION,
                unit_temp VARCHAR(10),
                irradiance DOUBLE PRECISION,
                unit_irradiance VARCHAR(10),
                voltage DOUBLE PRECISION,
                unit_voltage VARCHAR(10),
                current DOUBLE PRECISION,
                unit_current VARCHAR(10),
                inverter_status VARCHAR(50),
                timestamp BIGINT,
                message_datetime TIMESTAMPTZ,
                PRIMARY KEY (panel_id, timestamp)
            );
            """
            self._cursor.execute(create_table_query)
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = f"SELECT create_hypertable('{self.schema_name}.{self.table_name}', 'message_datetime', if_not_exists => TRUE);"
                self._cursor.execute(hypertable_query)
            except psycopg2.Error:
                # Hypertable might already exist or TimescaleDB extension not available
                pass
            
            self._connection.commit()
            
        except Exception as e:
            print(f"Error setting up TimescaleDB connection: {e}")
            raise

    def write(self, batch: SinkBatch):
        """Write batch of data to TimescaleDB"""
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not initialized")
        
        try:
            records = []
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the JSON string from the value field
                if hasattr(item, 'value') and item.value:
                    try:
                        # The value field contains a JSON string that needs to be parsed
                        if isinstance(item.value, str):
                            data = json.loads(item.value)
                        else:
                            data = item.value
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"Error parsing JSON from value field: {e}")
                        continue
                else:
                    print("No value field found in message")
                    continue
                
                # Extract message datetime
                message_datetime = getattr(item, 'dateTime', None)
                if not message_datetime:
                    message_datetime = getattr(item, 'datetime', None)
                
                # Map the data to table schema
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
                    data.get('timestamp'),
                    message_datetime
                )
                records.append(record)
            
            if records:
                # Insert records
                insert_query = f"""
                INSERT INTO {self.schema_name}.{self.table_name} (
                    panel_id, location_id, location_name, latitude, longitude, timezone,
                    power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                    voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    inverter_status = EXCLUDED.inverter_status,
                    message_datetime = EXCLUDED.message_datetime
                """
                
                self._cursor.executemany(insert_query, records)
                self._connection.commit()
                print(f"Successfully wrote {len(records)} records to TimescaleDB")
            
        except Exception as e:
            print(f"Error writing to TimescaleDB: {e}")
            if self._connection:
                self._connection.rollback()
            raise

    def close(self):
        """Close the TimescaleDB connection"""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Initialize TimescaleDB Sink
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
    port=port,
    dbname=os.environ.get('TIMESCALEDB_DBNAME', 'solar_data'),
    user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
    password=os.environ.get('TIMESCALEDB_PASSWORD', ''),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_panel_data'),
    schema_name=os.environ.get('TIMESCALEDB_SCHEMA', 'public')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "100"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)