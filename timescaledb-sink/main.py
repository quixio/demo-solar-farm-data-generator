import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._table_name = table_name
        self._connection = None

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password
            )
            self._connection.autocommit = True
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
        except Exception as e:
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
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
            message_timestamp TIMESTAMPTZ DEFAULT NOW()
        );
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_query)

    def write(self, batch: SinkBatch):
        if not self._connection:
            raise RuntimeError("Connection not established")
            
        insert_query = f"""
        INSERT INTO {self._table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        with self._connection.cursor() as cursor:
            for item in batch:
                try:
                    # Parse the JSON string from the value field
                    if isinstance(item.value, str):
                        data = json.loads(item.value)
                    else:
                        data = item.value
                    
                    # Extract values with safe access
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
                        data.get('timestamp')
                    )
                    
                    cursor.execute(insert_query, values)
                except Exception as e:
                    print(f"Error processing item: {e}")
                    print(f"Item value: {item.value}")
                    raise

    def close(self):
        if self._connection:
            self._connection.close()

def main():
    # Initialize TimescaleDB Sink
    try:
        port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
    except ValueError:
        port = 5432
    
    timescale_sink = TimescaleDBSink(
        host=os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
        port=port,
        database=os.environ.get('TIMESCALEDB_DATABASE', 'metrics'),
        user=os.environ.get('TIMESCALEDB_USER', 'tsadmn'),
        password=os.environ.get('TIMESCALE_PASSWORD_SECRET_KEY'),
        table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data_v1')
    )

    # Initialize the application
    app = Application(
        consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'timescale_sink_group'),
        auto_offset_reset="earliest",
        commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
        commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
    )

    # Define the input topic
    input_topic = app.topic(os.environ.get('input', 'solar-data'), key_deserializer="string")

    # Process and sink data
    sdf = app.dataframe(input_topic)
    
    # Add debug logging to see raw message structure
    def debug_message(item):
        print(f'Raw message: {item}')
        return item
    
    sdf = sdf.apply(debug_message)
    sdf.sink(timescale_sink)

    if __name__ == "__main__":
        app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()