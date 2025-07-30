import os
import json
import psycopg2
from datetime import datetime, timezone
from quixstreams import Application
from quixstreams.sinks.base.sink import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, port, database, api_key, table_name):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.api_key = api_key
        self.table_name = table_name
        self.connection = None

    def setup(self):
        """Establish connection to QuestDB and create table if it doesn't exist"""
        try:
            self.port = int(self.port)
        except (ValueError, TypeError):
            self.port = 8812
            
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user='admin',
            password=self.api_key
        )
        
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            panel_id SYMBOL,
            location_id SYMBOL,
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
            message_datetime TIMESTAMP
        ) timestamp(timestamp) PARTITION BY DAY;
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            self.connection.commit()
            print(f"Table {self.table_name} created or already exists")

    def write(self, batch: SinkBatch):
        if not self.connection:
            raise RuntimeError("Database connection not established")

        insert_sql = f"""
        INSERT INTO {self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        with self.connection.cursor() as cursor:
            for item in batch:
                print(f'Raw message: {item}')
                
                # Parse the payload
                payload = json.loads(item.value) if isinstance(item.value, str) else item.value

                # Convert QuestDB timestamp (ns) to Python datetime
                ts_ns = payload.get("timestamp", 0)
                ts_dt = datetime.fromtimestamp(ts_ns / 1_000_000_000) if ts_ns else None

                # Get message creation time from headers (if present)
                headers = getattr(item, "headers", None) or {}
                date_str = headers.get("dateTime")
                if date_str:
                    msg_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    msg_dt = datetime.now(timezone.utc)

                # Cast numeric fields to the types required by QuestDB
                power_output = int(round(payload["power_output"])) if "power_output" in payload else None
                irradiance = int(round(payload["irradiance"])) if "irradiance" in payload else None
                current = int(round(payload["current"])) if "current" in payload else None
                timezone_off = int(payload["timezone"]) if "timezone" in payload else None

                values = (
                    payload.get("panel_id"),
                    payload.get("location_id"),
                    payload.get("location_name"),
                    payload.get("latitude"),
                    payload.get("longitude"),
                    timezone_off,
                    power_output,
                    payload.get("unit_power"),
                    payload.get("temperature"),
                    payload.get("unit_temp"),
                    irradiance,
                    payload.get("unit_irradiance"),
                    payload.get("voltage"),
                    payload.get("unit_voltage"),
                    current,
                    payload.get("unit_current"),
                    payload.get("inverter_status"),
                    ts_dt,
                    msg_dt
                )

                cursor.execute(insert_sql, values)

            self.connection.commit()
            print(f"Successfully wrote {len(batch)} records to QuestDB")

    def close(self):
        if self.connection:
            self.connection.close()

# Initialize QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get('QUESTDB_HOST'),
    port=os.environ.get('QUESTDB_PORT', '8812'),
    database=os.environ.get('QUESTDB_DATABASE'),
    api_key=os.environ.get('QUESTDB_API_KEY'),
    table_name=os.environ.get('QUESTDB_TABLE_NAME', 'solar_data')
)

# Initialize Quix Streams Application
try:
    buffer_size = int(os.environ.get('BUFFER_SIZE', '1000'))
except ValueError:
    buffer_size = 1000

try:
    buffer_timeout = float(os.environ.get('BUFFER_TIMEOUT', '1.0'))
except ValueError:
    buffer_timeout = 1.0

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-solar-data-sink"),
    auto_offset_reset="earliest",
    commit_every=buffer_size,
    commit_interval=buffer_timeout,
)

input_topic = app.topic(os.environ.get('INPUT_TOPIC'))
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)