import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

from dotenv import load_dotenv
load_dotenv()

class TimescaleDBSink(BatchingSink):
    """Custom TimescaleDB sink implementation"""
    
    def __init__(self, host, port, database, username, password, table_name, schema_name="public"):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.table_name = table_name
        self.schema_name = schema_name
        self._connection = None
    
    def setup(self):
        """Establish database connection and create table if needed"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            
            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
                panel_id VARCHAR(255),
                location_id VARCHAR(255),
                location_name VARCHAR(255),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                timezone INTEGER,
                power_output DOUBLE PRECISION,
                unit_power VARCHAR(50),
                temperature DOUBLE PRECISION,
                unit_temp VARCHAR(50),
                irradiance DOUBLE PRECISION,
                unit_irradiance VARCHAR(50),
                voltage DOUBLE PRECISION,
                unit_voltage VARCHAR(50),
                current DOUBLE PRECISION,
                unit_current VARCHAR(50),
                inverter_status VARCHAR(50),
                timestamp BIGINT,
                message_datetime TIMESTAMPTZ,
                PRIMARY KEY (panel_id, timestamp, message_datetime)
            );
            """
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            hypertable_sql = f"""
            SELECT create_hypertable('{self.schema_name}.{self.table_name}', 'message_datetime', if_not_exists => TRUE);
            """
            
            with self._connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                try:
                    cursor.execute(hypertable_sql)
                except Exception as e:
                    print(f"Note: Could not create hypertable (this is normal if not using TimescaleDB): {e}")
                
                self._connection.commit()
                
            print(f"Connected to TimescaleDB and ensured table {self.schema_name}.{self.table_name} exists")
            
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            raise
    
    def write(self, batch: SinkBatch):
        """Write a batch of data to TimescaleDB"""
        if not self._connection:
            raise RuntimeError("Database connection not established")

        insert_sql = f"""
        INSERT INTO {self.schema_name}.{self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
        ) VALUES (
            %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, %(longitude)s, %(timezone)s,
            %(power_output)s, %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s,
            %(voltage)s, %(unit_voltage)s, %(current)s, %(unit_current)s, %(inverter_status)s, %(timestamp)s, %(message_datetime)s
        )
        """

        try:
            with self._connection.cursor() as cursor:
                record_count = 0

                for item in batch:
                    print(f'Raw message: {item}')
                    
                    if isinstance(item.value, dict):
                        data = item.value
                    else:
                        try:
                            data = json.loads(item.value)
                        except Exception as e:
                            print(f"Skipping message – cannot decode JSON: {e}")
                            continue

                    record = {
                        'panel_id':        data.get('panel_id'),
                        'location_id':     data.get('location_id'),
                        'location_name':   data.get('location_name'),
                        'latitude':        data.get('latitude'),
                        'longitude':       data.get('longitude'),
                        'timezone':        data.get('timezone'),
                        'power_output':    data.get('power_output'),
                        'unit_power':      data.get('unit_power'),
                        'temperature':     data.get('temperature'),
                        'unit_temp':       data.get('unit_temp'),
                        'irradiance':      data.get('irradiance'),
                        'unit_irradiance': data.get('unit_irradiance'),
                        'voltage':         data.get('voltage'),
                        'unit_voltage':    data.get('unit_voltage'),
                        'current':         data.get('current'),
                        'unit_current':    data.get('unit_current'),
                        'inverter_status': data.get('inverter_status'),
                        'timestamp':       data.get('timestamp'),
                        'message_datetime': item.timestamp
                    }

                    cursor.execute(insert_sql, record)
                    record_count += 1

                self._connection.commit()
                print(f"Successfully wrote {record_count} records to TimescaleDB")

        except Exception as e:
            print(f"Error writing to TimescaleDB: {e}")
            self._connection.rollback()
            raise
    
    def close(self):
        """Close database connection"""
        if self._connection:
            self._connection.close()
            print("Closed TimescaleDB connection")


# Get port with error handling
try:
    port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
except ValueError:
    port = 5432

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
    port=port,
    database=os.environ.get('TIMESCALEDB_DATABASE', 'metrics'),
    username=os.environ.get('TIMESCALEDB_USER', 'tsadmin'),
    password=os.environ.get('TIMESCALE_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data_v3b'),
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
sdf.sink(timescale_sink)

if __name__ == "__main__":
    timescale_sink.setup()
    app.run(count=10, timeout=20)
