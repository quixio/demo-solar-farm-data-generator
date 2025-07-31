# DEPENDENCIES:
# pip install quixstreams
# pip install psycopg2-binary
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import psycopg2
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self.connection = None
        
    def setup(self):
        try:
            port = int(os.environ.get('QUESTDB_PORT', '8812'))
        except ValueError:
            port = 8812
            
        self.connection = psycopg2.connect(
            host=os.environ.get('QUESTDB_HOST'),
            port=port,
            user=os.environ.get('QUESTDB_USERNAME'),
            password=os.environ.get('QUESTDB_PASSWORD_KEY'),
            database=os.environ.get('QUESTDB_DATABASE')
        )
        self.connection.autocommit = True
        
        # Create table if it doesn't exist
        table_name = os.environ.get('QUESTDB_TABLE_NAME')
        cursor = self.connection.cursor()
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            panel_id STRING,
            location_id STRING,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output INT,
            unit_power STRING,
            temperature DOUBLE,
            unit_temp STRING,
            irradiance INT,
            unit_irradiance STRING,
            voltage DOUBLE,
            unit_voltage STRING,
            current INT,
            unit_current STRING,
            inverter_status STRING,
            timestamp TIMESTAMP,
            ts TIMESTAMP
        ) timestamp(ts)
        """
        
        cursor.execute(create_table_sql)
        cursor.close()
        
    def write(self, batch: SinkBatch):
        table_name = os.environ.get('QUESTDB_TABLE_NAME')
        cursor = self.connection.cursor()

        for item in batch:
            print(f'Raw message: {item.value}')
            
            # item.value is the Kafka "envelope". Decode it first.
            envelope = json.loads(item.value if isinstance(item.value, str) else item.value)

            # Decode the real sensor payload that sits in envelope["value"]
            payload = (json.loads(envelope["value"])
                       if isinstance(envelope.get("value"), str)
                       else envelope.get("value", {}))

            # Convert nanoseconds to milliseconds (QuestDB's to_timestamp(ms))
            ts_ms = payload.get("timestamp", 0) // 1_000_000

            insert_sql = f"""
            INSERT INTO {table_name} (
                panel_id, location_id, location_name, latitude, longitude,
                timezone, power_output, unit_power, temperature, unit_temp,
                irradiance, unit_irradiance, voltage, unit_voltage, current,
                unit_current, inverter_status, timestamp, ts
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, to_timestamp(%s), to_timestamp(%s)
            )
            """

            cursor.execute(insert_sql, (
                payload.get("panel_id"),
                payload.get("location_id"),
                payload.get("location_name"),
                payload.get("latitude"),
                payload.get("longitude"),
                payload.get("timezone"),
                payload.get("power_output"),
                payload.get("unit_power"),
                payload.get("temperature"),
                payload.get("unit_temp"),
                payload.get("irradiance"),
                payload.get("unit_irradiance"),
                payload.get("voltage"),
                payload.get("unit_voltage"),
                payload.get("current"),
                payload.get("unit_current"),
                payload.get("inverter_status"),
                ts_ms,
                ts_ms
            ))

        cursor.close()
        
    def close(self):
        if self.connection:
            self.connection.close()

app = Application(
    consumer_group=os.environ.get("QUESTDB_CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("QUESTDB_BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("QUESTDB_BUFFER_TIMEOUT", "1")),
)

input_topic = app.topic(os.environ.get("QUESTDB_INPUT"))
questdb_sink = QuestDBSink()

sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)