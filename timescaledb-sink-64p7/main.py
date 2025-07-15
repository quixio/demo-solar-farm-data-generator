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
        self._connection = None
        self._cursor = None

    def setup(self):
        self._connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self._cursor = self._connection.cursor(cursor_factory=RealDictCursor)
        self._ensure_table_exists()

    def _ensure_table_exists(self):
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
            stream_id VARCHAR(255),
            topic_id VARCHAR(255),
            topic_name VARCHAR(255),
            partition_num INTEGER,
            offset_num BIGINT
        );
        """
        self._cursor.execute(create_table_query)
        self._connection.commit()

    def write(self, batch: SinkBatch):
        if not batch:
            return

        insert_query = f"""
        INSERT INTO {self.schema_name}.{self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp,
            message_datetime, stream_id, topic_id, topic_name, partition_num, offset_num
        ) VALUES (
            %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, %(longitude)s, %(timezone)s,
            %(power_output)s, %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s,
            %(voltage)s, %(unit_voltage)s, %(current)s, %(unit_current)s, %(inverter_status)s, %(timestamp)s,
            %(message_datetime)s, %(stream_id)s, %(topic_id)s, %(topic_name)s, %(partition_num)s, %(offset_num)s
        )
        """

        records = []
        for item in batch:
            # Parse the JSON value field
            value_data = json.loads(item.value["value"])
            
            record = {
                "panel_id": value_data.get("panel_id"),
                "location_id": value_data.get("location_id"),
                "location_name": value_data.get("location_name"),
                "latitude": value_data.get("latitude"),
                "longitude": value_data.get("longitude"),
                "timezone": value_data.get("timezone"),
                "power_output": value_data.get("power_output"),
                "unit_power": value_data.get("unit_power"),
                "temperature": value_data.get("temperature"),
                "unit_temp": value_data.get("unit_temp"),
                "irradiance": value_data.get("irradiance"),
                "unit_irradiance": value_data.get("unit_irradiance"),
                "voltage": value_data.get("voltage"),
                "unit_voltage": value_data.get("unit_voltage"),
                "current": value_data.get("current"),
                "unit_current": value_data.get("unit_current"),
                "inverter_status": value_data.get("inverter_status"),
                "timestamp": value_data.get("timestamp"),
                "message_datetime": item.value.get("dateTime"),
                "stream_id": item.value.get("streamId"),
                "topic_id": item.value.get("topicId"),
                "topic_name": item.value.get("topicName"),
                "partition_num": item.value.get("partition"),
                "offset_num": item.value.get("offset")
            }
            records.append(record)

        self._cursor.executemany(insert_query, records)
        self._connection.commit()

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get("TIMESCALEDB_HOST"),
    port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
    dbname=os.environ.get("TIMESCALEDB_DBNAME"),
    user=os.environ.get("TIMESCALEDB_USER"),
    password=os.environ.get("TIMESCALEDB_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_data"),
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public")
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink"),
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