# DEPENDENCIES:
# pip install quixstreams
# pip install questdb
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import questdb

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

# Helper functions to handle empty environment variables
def env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    return int(raw) if raw and raw.strip() else default

def env_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    return float(raw) if raw and raw.strip() else default

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._client = None
        self._table_created = False
        
    def setup(self):
        try:
            port = env_int("QUESTDB_PORT", 8812)
        except ValueError:
            port = 8812
            
        self._client = questdb.Sender(
            host=os.environ.get('QUESTDB_HOST', 'questdb'),
            port=port,
            username=os.environ.get('QUESTDB_USERNAME', 'admin'),
            password=os.environ.get('QUESTDB_PASSWORD_KEY', ''),
            database=os.environ.get('QUESTDB_DATABASE', 'test_db')
        )
        
    def write(self, batch: SinkBatch):
        if not self._table_created:
            self._create_table_if_not_exists()
            self._table_created = True
            
        table_name = os.environ.get('QUESTDB_TABLE_NAME', 'xx')
        
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the JSON string from the value field
            if hasattr(item, 'value') and isinstance(item.value, str):
                try:
                    data = json.loads(item.value)
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON: {item.value}")
                    continue
            else:
                data = item.value
                
            # Convert timestamp to datetime
            timestamp_epoch = data.get('timestamp', 0)
            if timestamp_epoch:
                # Assuming nanosecond timestamp
                timestamp_dt = datetime.fromtimestamp(timestamp_epoch / 1_000_000_000)
            else:
                timestamp_dt = datetime.utcnow()
                
            # Map data to table schema
            row_data = {
                'panel_id': data.get('panel_id', ''),
                'location_id': data.get('location_id', ''),
                'location_name': data.get('location_name', ''),
                'latitude': data.get('latitude', 0.0),
                'longitude': data.get('longitude', 0.0),
                'timezone': data.get('timezone', 0),
                'power_output': data.get('power_output', 0),
                'unit_power': data.get('unit_power', ''),
                'temperature': data.get('temperature', 0.0),
                'unit_temp': data.get('unit_temp', ''),
                'irradiance': data.get('irradiance', 0),
                'unit_irradiance': data.get('unit_irradiance', ''),
                'voltage': data.get('voltage', 0.0),
                'unit_voltage': data.get('unit_voltage', ''),
                'current': data.get('current', 0),
                'unit_current': data.get('unit_current', ''),
                'inverter_status': data.get('inverter_status', ''),
                'timestamp': timestamp_dt
            }
            
            # Build insert statement
            columns = ', '.join(row_data.keys())
            placeholders = ', '.join(['?' for _ in row_data])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            try:
                self._client.execute(query, list(row_data.values()))
            except Exception as e:
                print(f"Error inserting data: {e}")
                
        # Commit the transaction
        self._client.commit()
        
    def _create_table_if_not_exists(self):
        table_name = os.environ.get('QUESTDB_TABLE_NAME', 'xx')
        
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
            timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY
        """
        
        try:
            self._client.execute(create_table_sql)
            self._client.commit()
            print(f"Table {table_name} created or already exists")
        except Exception as e:
            print(f"Error creating table: {e}")

# Initialize QuestDB sink
questdb_sink = QuestDBSink()

app = Application(
    consumer_group=os.environ.get("QUESTDB_CONSUMER_GROUP_NAME", "test_db"),
    auto_offset_reset="earliest",
    commit_every=env_int("QUESTDB_BUFFER_SIZE", 1000),
    commit_interval=env_float("QUESTDB_BUFFER_TIMEOUT", 1.0),
)

input_topic = app.topic(os.environ.get("QUESTDB_INPUT", "solar-data"))
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)