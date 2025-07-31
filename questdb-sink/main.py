# DEPENDENCIES:
# pip install quixstreams
# pip install questdb
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from typing import Optional
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from questdb.ingress import Sender
from dotenv import load_dotenv

load_dotenv()

def env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)))
    except ValueError:
        return default

class QuestDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._client: Optional[Sender] = None
        self._table_name: str = ""

    def setup(self):
        try:
            host = os.environ.get("QUESTDB_HOST", "questdb")
            port = env_int("QUESTDB_PORT", 8812)
            
            # Handle authentication - check for both token and username/password
            auth_token = os.environ.get("QUESTDB_AUTH_TOKEN")
            if not auth_token:
                username = os.environ.get("QUESTDB_USERNAME", "")
                password = os.environ.get("QUESTDB_PASSWORD_KEY", "")
                auth_token = f"{username}:{password}" if username or password else None
            
            # Pass all three required positional arguments
            self._client = Sender(host, port, auth_token)
            self._table_name = os.environ.get("QUESTDB_TABLE_NAME", "solar_data")
            
            print(f"QuestDB sink setup completed successfully - Host: {host}, Port: {port}, Table: {self._table_name}")
        except Exception as e:
            print(f"Failed to setup QuestDB sink: {e}")
            raise

    def write(self, batch: SinkBatch):
        if not self._client:
            raise RuntimeError("QuestDB client not initialized")
        
        try:
            for item in batch:
                print(f"Raw message: {item}")
                
                # Parse the JSON string in the value field
                try:
                    if isinstance(item.value, str):
                        data = json.loads(item.value)
                    else:
                        data = item.value
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"Failed to parse message value as JSON: {e}")
                    continue
                
                # Extract fields from parsed data
                panel_id = data.get("panel_id", "")
                location_id = data.get("location_id", "")
                location_name = data.get("location_name", "")
                latitude = data.get("latitude", 0.0)
                longitude = data.get("longitude", 0.0)
                timezone = data.get("timezone", 0)
                power_output = data.get("power_output", 0)
                unit_power = data.get("unit_power", "")
                temperature = data.get("temperature", 0.0)
                unit_temp = data.get("unit_temp", "")
                irradiance = data.get("irradiance", 0)
                unit_irradiance = data.get("unit_irradiance", "")
                voltage = data.get("voltage", 0.0)
                unit_voltage = data.get("unit_voltage", "")
                current = data.get("current", 0)
                unit_current = data.get("unit_current", "")
                inverter_status = data.get("inverter_status", "")
                timestamp = data.get("timestamp", 0)
                
                # Send data to QuestDB using ILP
                self._client.table(self._table_name) \
                    .symbol("panel_id", panel_id) \
                    .symbol("location_id", location_id) \
                    .symbol("location_name", location_name) \
                    .symbol("unit_power", unit_power) \
                    .symbol("unit_temp", unit_temp) \
                    .symbol("unit_irradiance", unit_irradiance) \
                    .symbol("unit_voltage", unit_voltage) \
                    .symbol("unit_current", unit_current) \
                    .symbol("inverter_status", inverter_status) \
                    .column("latitude", latitude) \
                    .column("longitude", longitude) \
                    .column("timezone", timezone) \
                    .column("power_output", power_output) \
                    .column("temperature", temperature) \
                    .column("irradiance", irradiance) \
                    .column("voltage", voltage) \
                    .column("current", current) \
                    .at(timestamp)
            
            # Flush the batch to QuestDB
            self._client.flush()
            print(f"Successfully wrote batch of {len(batch)} records to QuestDB")
            
        except Exception as e:
            print(f"Failed to write batch to QuestDB: {e}")
            raise

    def teardown(self):
        if self._client:
            try:
                self._client.close()
                print("QuestDB client closed successfully")
            except Exception as e:
                print(f"Error closing QuestDB client: {e}")

def main():
    app = Application(
        consumer_group=os.environ.get("QUESTDB_CONSUMER_GROUP_NAME", "questdb-sink"),
        auto_offset_reset="earliest",
        commit_every=env_int("QUESTDB_BUFFER_SIZE", 1000),
        commit_interval=float(os.environ.get("QUESTDB_BUFFER_TIMEOUT", "1.0"))
    )
    
    input_topic = app.topic(os.environ.get("QUESTDB_INPUT", "solar-data"))
    
    questdb_sink = QuestDBSink()
    
    sdf = app.dataframe(input_topic)
    sdf.sink(questdb_sink)
    
    print("Starting QuestDB sink application...")
    app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()