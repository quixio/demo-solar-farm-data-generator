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
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
            topic_id TEXT,
            topic_name TEXT,
            stream_id TEXT,
            panel_id TEXT,
            location_id TEXT,
            location_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone INTEGER,
            power_output INTEGER,
            unit_power TEXT,
            temperature DOUBLE PRECISION,
            unit_temp TEXT,
            irradiance INTEGER,
            unit_irradiance TEXT,
            voltage DOUBLE PRECISION,
            unit_voltage TEXT,
            current INTEGER,
            unit_current TEXT,
            inverter_status TEXT,
            timestamp BIGINT,
            date_time TIMESTAMP WITH TIME ZONE,
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
            topic_id, topic_name, stream_id, panel_id, location_id, location_name,
            latitude, longitude, timezone, power_output, unit_power, temperature,
            unit_temp, irradiance, unit_irradiance, voltage, unit_voltage, current,
            unit_current, inverter_status, timestamp, date_time, partition_num, offset_num
        ) VALUES (
            %(topic_id)s, %(topic_name)s, %(stream_id)s, %(panel_id)s, %(location_id)s, %(location_name)s,
            %(latitude)s, %(longitude)s, %(timezone)s, %(power_output)s, %(unit_power)s, %(temperature)s,
            %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s, %(voltage)s, %(unit_voltage)s, %(current)s,
            %(unit_current)s, %(inverter_status)s, %(timestamp)s, %(date_time)s, %(partition_num)s, %(offset_num)s
        )
        """

        records = []
        for item in batch:
            # Parse the JSON value field
            value_data = json.loads(item.value["value"])
            
            record = {
                "topic_id": item.value["topicId"],
                "topic_name": item.value["topicName"],
                "stream_id": item.value["streamId"],
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
                "date_time": item.value["dateTime"],
                "partition_num": item.value["partition"],
                "offset_num": item.value["offset"]
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
    dbname=os.environ.get("TIMESCALEDB_DATABASE"),
    user=os.environ.get("TIMESCALEDB_USER"),
    password=os.environ.get("TIMESCALEDB_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_data_v2"),
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