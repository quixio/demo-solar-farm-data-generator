# DEPENDENCIES:
# pip install quixstreams
# pip install questdb
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
from questdb.ingress import Sender, IngressError
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
        self._client = None

    def setup(self):
        try:
            ilp_port = env_int("QUESTDB_PORT", 9009)
            self._client = Sender(
                host=os.environ.get("QUESTDB_HOST", "questdb"),
                port=ilp_port,
                username=os.environ.get("QUESTDB_USERNAME", "admin"),
                password=os.environ.get("QUESTDB_PASSWORD_KEY", "")
            )
            print("QuestDB sink setup completed successfully")
        except Exception as e:
            print(f"Failed to setup QuestDB sink: {e}")
            raise

    def write(self, batch: SinkBatch):
        table = os.environ.get("QUESTDB_TABLE_NAME", "solar_data")
        
        for item in batch:
            print(f'Raw message: {item}')
            
            payload = item.value
            try:
                data = json.loads(payload) if isinstance(payload, str) else payload
            except (TypeError, json.JSONDecodeError):
                print(f"Skip malformed message: {payload}")
                continue

            ts_ns = data.get("timestamp", 0) or int(datetime.utcnow().timestamp() * 1e9)

            symbols = {
                "panel_id": data.get("panel_id", ""),
                "location_id": data.get("location_id", ""),
                "location_name": data.get("location_name", "")
            }

            try:
                with self._client.row(table, symbols=symbols, at=ts_ns) as r:
                    r["latitude"] = float(data.get("latitude", 0.0))
                    r["longitude"] = float(data.get("longitude", 0.0))
                    r["timezone"] = int(data.get("timezone", 0))
                    r["power_output"] = float(data.get("power_output", 0))
                    r["temperature"] = float(data.get("temperature", 0.0))
                    r["irradiance"] = float(data.get("irradiance", 0))
                    r["voltage"] = float(data.get("voltage", 0.0))
                    r["current"] = float(data.get("current", 0))
                    r["inverter_status"] = data.get("inverter_status", "")
                    
            except Exception as e:
                print(f"Error writing row to QuestDB: {e}")
                continue

        try:
            self._client.flush()
            print(f"Successfully flushed {len(batch)} messages to QuestDB")
        except IngressError as e:
            print(f"QuestDB ingress error: {e}")
            raise SinkBackpressureError(
                retry_after=5.0,
                topic=batch.topic,
                partition=batch.partition
            )

    def close(self):
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                print(f"Error closing QuestDB client: {e}")

questdb_sink = QuestDBSink()

app = Application(
    consumer_group=os.environ.get("QUESTDB_CONSUMER_GROUP_NAME", "questdb-data-writer"),
    auto_offset_reset="earliest",
    commit_every=env_int("QUESTDB_BUFFER_SIZE", 1000),
    commit_interval=float(os.environ.get("QUESTDB_BUFFER_TIMEOUT", "1")),
)

input_topic = app.topic(os.environ.get("QUESTDB_INPUT", "solar-data"))
sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)