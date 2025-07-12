import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
load_dotenv()

# Initialize TimescaleDB (PostgreSQL-compatible) Sink
timescaledb_sink = PostgreSQLSink(
    host=os.getenv("TIMESCALEDB_HOST", "timescaledb"),
    port=int(os.getenv("TIMESCALEDB_PORT", "5432")),
    dbname=os.getenv("TIMESCALEDB_DBNAME", "metrics"),
    user=os.getenv("TIMESCALEDB_USER", "tsadmin"),
    password=os.getenv("TIMESCALEDB_PASSWORD", ""),
    table_name=os.getenv("TIMESCALEDB_TABLE", "solar_panel_data"),
    schema_name=os.getenv("TIMESCALEDB_SCHEMA", "public"),
    schema_auto_update=os.getenv("SCHEMA_AUTO_UPDATE", "true").lower() == "true",
)

# Initialize the application
app = Application(
    consumer_group=os.getenv("CONSUMER_GROUP_NAME", "solar-timescaledb-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.getenv("BATCH_TIMEOUT", "1")),
    commit_every=int(os.getenv("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.getenv("input", "solar-data"), key_deserializer="string")

def transform_solar_data(row):
    """
    Transform the solar panel data for TimescaleDB storage.
    Converts timestamp from nanoseconds to proper datetime format.
    """
    # Parse the JSON value from the Kafka message
    if isinstance(row["value"], str):
        data = json.loads(row["value"])
    else:
        data = row["value"]
    
    # Convert nanosecond timestamp to datetime
    timestamp_ns = data["timestamp"]
    timestamp_seconds = timestamp_ns / 1_000_000_000
    data["timestamp"] = datetime.fromtimestamp(timestamp_seconds)
    
    return data

# Process and sink data
sdf = app.dataframe(input_topic)
sdf = sdf.apply(transform_solar_data)
sdf.sink(timescaledb_sink)

if __name__ == "__main__":
    app.run(sdf)