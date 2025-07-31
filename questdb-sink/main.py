# DEPENDENCIES:
# pip install quixstreams
# pip install psycopg2-binary
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

def env_int(key, default):
    try:
        return int(os.environ.get(key, str(default)))
    except ValueError:
        return default

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._connection = None
        self._table_name = None

    def setup(self):
        host = os.environ.get("QUESTDB_HOST", "questdb")
        port = env_int("QUESTDB_PORT", 8812)
        username = os.environ.get("QUESTDB_USERNAME", "admin")
        password = os.environ.get("QUESTDB_PASSWORD_KEY", "")
        database = os.environ.get("QUESTDB_DATABASE", "qdb")
        self._table_name = os.environ.get("QUESTDB_TABLE_NAME", "solar_data")

        # Connect to QuestDB via PostgreSQL interface
        self._connection = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
            cursor_factory=RealDictCursor
        )
        self._connection.autocommit = True
        
        # Create table if it doesn't exist
        self._create_table_if_not_exists()
        
        print(f"QuestDB sink setup completed successfully - Host: {host}, Port: {port}, Table: {self._table_name}")

    def _create_table_if_not_exists(self):
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            panel_id SYMBOL,
            location_id SYMBOL,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output INT,
            unit_power SYMBOL,
            temperature DOUBLE,
            unit_temp SYMBOL,
            irradiance INT,
            unit_irradiance SYMBOL,
            voltage DOUBLE,
            unit_voltage SYMBOL,
            current INT,
            unit_current SYMBOL,
            inverter_status SYMBOL,
            record_timestamp TIMESTAMP,
            ts TIMESTAMP
        ) TIMESTAMP(ts) PARTITION BY DAY;
        """
        
        with self._connection.cursor() as cursor:
            cursor.execute(create_table_sql)
        self._connection.commit()
        print(f"Table {self._table_name} created or verified")

    def write(self, batch: SinkBatch):
        if not batch:
            return

        records = []
        for item in batch:
            try:
                # Parse the JSON value string
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value

                # Convert timestamp to datetime
                timestamp_ns = data.get('timestamp', 0)
                if timestamp_ns:
                    # Convert nanoseconds to seconds
                    timestamp_seconds = timestamp_ns / 1_000_000_000
                    record_timestamp = datetime.fromtimestamp(timestamp_seconds)
                else:
                    record_timestamp = datetime.utcnow()

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
                    record_timestamp,
                    datetime.utcnow()  # ts column for QuestDB partitioning
                )
                records.append(record)
            except Exception as e:
                print(f"Error processing record: {e}")
                continue

        if records:
            insert_sql = f"""
            INSERT INTO {self._table_name} (
                panel_id, location_id, location_name, latitude, longitude,
                timezone, power_output, unit_power, temperature, unit_temp,
                irradiance, unit_irradiance, voltage, unit_voltage, current,
                unit_current, inverter_status, record_timestamp, ts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            with self._connection.cursor() as cursor:
                cursor.executemany(insert_sql, records)
            
            print(f"Successfully wrote {len(records)} records to QuestDB")

    def teardown(self):
        if self._connection:
            self._connection.close()

# Create the QuestDB sink
questdb_sink = QuestDBSink()

app = Application(
    consumer_group=os.environ.get("QUESTDB_CONSUMER_GROUP_NAME", "questdb-consumer-group"),
    auto_offset_reset="earliest",
    commit_every=env_int("QUESTDB_BUFFER_SIZE", 1000),
    commit_interval=float(os.environ.get("QUESTDB_BUFFER_TIMEOUT", "1")),
)

input_topic = app.topic(os.environ.get("QUESTDB_INPUT", "solar-data"))
sdf = app.dataframe(input_topic)

# Debug: Print raw message structure
sdf = sdf.apply(lambda msg: print(f'Raw message: {msg}') or msg)

sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)