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
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
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
            message_datetime TIMESTAMPTZ,
            PRIMARY KEY (panel_id, timestamp)
        );
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def write(self, batch: SinkBatch):
        if not batch:
            return

        insert_query = f"""
        INSERT INTO {self.schema_name}.{self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
        ) VALUES %s
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

        values = []
        for item in batch:
            try:
                # Parse the JSON value from the message
                if isinstance(item.value['value'], str):
                    solar_data = json.loads(item.value['value'])
                else:
                    solar_data = item.value['value']
                
                # Map the message value schema to the table schema
                row = (
                    solar_data.get('panel_id'),
                    solar_data.get('location_id'),
                    solar_data.get('location_name'),
                    solar_data.get('latitude'),
                    solar_data.get('longitude'),
                    solar_data.get('timezone'),
                    solar_data.get('power_output'),
                    solar_data.get('unit_power'),
                    solar_data.get('temperature'),
                    solar_data.get('unit_temp'),
                    solar_data.get('irradiance'),
                    solar_data.get('unit_irradiance'),
                    solar_data.get('voltage'),
                    solar_data.get('unit_voltage'),
                    solar_data.get('current'),
                    solar_data.get('unit_current'),
                    solar_data.get('inverter_status'),
                    solar_data.get('timestamp'),
                    item.value.get('dateTime')
                )
                values.append(row)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"Error processing message: {e}")
                continue

        if values:
            from psycopg2.extras import execute_values
            execute_values(self.cursor, insert_query, values)
            self.connection.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST'),
    port=int(os.environ.get('TIMESCALEDB_PORT', 5432)),
    dbname=os.environ.get('TIMESCALEDB_DBNAME'),
    user=os.environ.get('TIMESCALEDB_USER'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLE'),
    schema_name=os.environ.get('TIMESCALEDB_SCHEMA', 'public')
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)