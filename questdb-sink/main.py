import os
import json
import psycopg2
from datetime import datetime, timezone
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._connection = None
        self._table_name = os.environ.get('QUESTDB_TABLE_NAME', 'solar_data')
        
    def setup(self):
        try:
            port = int(os.environ.get('QUESTDB_PORT', '8812'))
        except ValueError:
            port = 8812
            
        self._connection = psycopg2.connect(
            host=os.environ.get('QUESTDB_HOST'),
            port=port,
            database=os.environ.get('QUESTDB_DATABASE'),
            user='admin',
            password=os.environ.get('QUESTDB_API_KEY')
        )
        self._connection.autocommit = True
        
        # Create table if it doesn't exist
        cursor = self._connection.cursor()
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
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
            data_timestamp TIMESTAMP,
            message_datetime TIMESTAMP
        ) TIMESTAMP(message_datetime) PARTITION BY DAY;
        """
        cursor.execute(create_table_sql)
        cursor.close()
        
    def write(self, batch: SinkBatch):
        cursor = self._connection.cursor()
        
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON value
            if isinstance(item.value, str):
                data = json.loads(item.value)
            elif isinstance(item.value, dict):
                data = item.value
            else:
                print(f"Unexpected value type: {type(item.value)}")
                continue
            
            # Handle message datetime from headers
            headers = getattr(item, "headers", None) or {}
            date_str = headers.get("dateTime")
            
            if date_str:
                # Parse header time, normalize to UTC, then make it tz-naive
                msg_dt = (
                    datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    .astimezone(timezone.utc)
                    .replace(tzinfo=None)  # Remove timezone info for QuestDB
                )
            else:
                msg_dt = datetime.utcnow()
            
            # Convert data timestamp to datetime (assuming it's in nanoseconds)
            data_timestamp = datetime.fromtimestamp(data.get('timestamp', 0) / 1_000_000_000)
            
            insert_sql = f"""
            INSERT INTO {self._table_name} (
                panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status,
                data_timestamp, message_datetime
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
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
                data_timestamp,
                msg_dt
            ))
        
        cursor.close()

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_TIMEOUT", "1")),
)

input_topic = app.topic(os.environ.get('INPUT_TOPIC'))

questdb_sink = QuestDBSink()

sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run()