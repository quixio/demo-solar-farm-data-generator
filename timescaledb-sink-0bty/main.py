import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class TimescaleDBSink(BatchingSink):
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._connection = None
        self._cursor = None
        self._table_name = os.environ.get('TIMESCALEDB_TABLE', 'solar_data')

    def setup(self):
        try:
            self._connection = psycopg2.connect(
                host=os.environ.get('TIMESCALEDB_HOST'),
                port=int(os.environ.get('TIMESCALEDB_PORT', '5432')),
                database=os.environ.get('TIMESCALEDB_DATABASE'),
                user=os.environ.get('TIMESCALEDB_USER'),
                password=os.environ.get('TIMESCALEDB_PASSWORD')
            )
            self._cursor = self._connection.cursor(cursor_factory=RealDictCursor)
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
            id SERIAL PRIMARY KEY,
            topic_id TEXT,
            topic_name TEXT,
            stream_id TEXT,
            panel_id TEXT,
            location_id TEXT,
            location_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone INTEGER,
            power_output DOUBLE PRECISION,
            unit_power TEXT,
            temperature DOUBLE PRECISION,
            unit_temp TEXT,
            irradiance DOUBLE PRECISION,
            unit_irradiance TEXT,
            voltage DOUBLE PRECISION,
            unit_voltage TEXT,
            current DOUBLE PRECISION,
            unit_current TEXT,
            inverter_status TEXT,
            timestamp BIGINT,
            datetime TIMESTAMP,
            partition_num INTEGER,
            offset_num BIGINT
        );
        """
        
        self._cursor.execute(create_table_query)
        
        # Create hypertable if it doesn't exist (TimescaleDB specific)
        hypertable_query = f"""
        SELECT create_hypertable('{self._table_name}', 'datetime', if_not_exists => TRUE);
        """
        
        try:
            self._cursor.execute(hypertable_query)
        except Exception:
            # If hypertable creation fails, continue without it
            pass
        
        self._connection.commit()

    def write(self, batch: SinkBatch):
        insert_query = f"""
        INSERT INTO {self._table_name} (
            topic_id, topic_name, stream_id, panel_id, location_id, location_name,
            latitude, longitude, timezone, power_output, unit_power, temperature,
            unit_temp, irradiance, unit_irradiance, voltage, unit_voltage, current,
            unit_current, inverter_status, timestamp, datetime, partition_num, offset_num
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        batch_data = []
        for item in batch:
            message = item.value
            
            # Parse the value field if it's a JSON string
            if isinstance(message.get('value'), str):
                try:
                    value_data = json.loads(message['value'])
                except json.JSONDecodeError:
                    value_data = {}
            else:
                value_data = message.get('value', {})
            
            # Map message schema to table schema
            row_data = (
                message.get('topicId'),
                message.get('topicName'),
                message.get('streamId'),
                value_data.get('panel_id'),
                value_data.get('location_id'),
                value_data.get('location_name'),
                value_data.get('latitude'),
                value_data.get('longitude'),
                value_data.get('timezone'),
                value_data.get('power_output'),
                value_data.get('unit_power'),
                value_data.get('temperature'),
                value_data.get('unit_temp'),
                value_data.get('irradiance'),
                value_data.get('unit_irradiance'),
                value_data.get('voltage'),
                value_data.get('unit_voltage'),
                value_data.get('current'),
                value_data.get('unit_current'),
                value_data.get('inverter_status'),
                value_data.get('timestamp'),
                message.get('dateTime'),
                message.get('partition'),
                message.get('offset')
            )
            batch_data.append(row_data)
        
        self._cursor.executemany(insert_query, batch_data)
        self._connection.commit()

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink()

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ["input"], key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)