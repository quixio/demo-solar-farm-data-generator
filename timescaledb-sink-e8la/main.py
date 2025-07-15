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
        
    def setup(self):
        """Setup the database connection and create table if it doesn't exist"""
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.connection.autocommit = True
        self._create_table_if_not_exists()
    
    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist"""
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
            datetime TIMESTAMPTZ,
            PRIMARY KEY (panel_id, datetime)
        );
        """
        
        # Create hypertable if it doesn't exist (TimescaleDB specific)
        create_hypertable_query = f"""
        SELECT create_hypertable('{self.schema_name}.{self.table_name}', 'datetime', if_not_exists => TRUE);
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
            try:
                cursor.execute(create_hypertable_query)
            except Exception:
                # Ignore if hypertable already exists or TimescaleDB extension not available
                pass
    
    def write(self, batch: SinkBatch):
        """Write batch of records to TimescaleDB"""
        if not batch:
            return
            
        insert_query = f"""
        INSERT INTO {self.schema_name}.{self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, datetime
        ) VALUES (
            %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, %(longitude)s, %(timezone)s,
            %(power_output)s, %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s,
            %(voltage)s, %(unit_voltage)s, %(current)s, %(unit_current)s, %(inverter_status)s, %(timestamp)s, %(datetime)s
        )
        """
        
        records = []
        for item in batch:
            # Parse the JSON value from the message
            value_data = json.loads(item.value["value"])
            
            # Map the message schema to table schema
            record = {
                "panel_id": value_data["panel_id"],
                "location_id": value_data["location_id"],
                "location_name": value_data["location_name"],
                "latitude": value_data["latitude"],
                "longitude": value_data["longitude"],
                "timezone": value_data["timezone"],
                "power_output": value_data["power_output"],
                "unit_power": value_data["unit_power"],
                "temperature": value_data["temperature"],
                "unit_temp": value_data["unit_temp"],
                "irradiance": value_data["irradiance"],
                "unit_irradiance": value_data["unit_irradiance"],
                "voltage": value_data["voltage"],
                "unit_voltage": value_data["unit_voltage"],
                "current": value_data["current"],
                "unit_current": value_data["unit_current"],
                "inverter_status": value_data["inverter_status"],
                "timestamp": value_data["timestamp"],
                "datetime": item.value["dateTime"]
            }
            records.append(record)
        
        with self.connection.cursor() as cursor:
            cursor.executemany(insert_query, records)

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get("TIMESCALEDB_HOST"),
    port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
    dbname=os.environ.get("TIMESCALEDB_DATABASE"),
    user=os.environ.get("TIMESCALEDB_USER"),
    password=os.environ.get("TIMESCALEDB_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_data"),
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public")
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
    app.run(count=10, timeout=20)