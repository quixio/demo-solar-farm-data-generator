import os
import json
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from dotenv import load_dotenv
load_dotenv()

# Initialize TimescaleDB (PostgreSQL) Sink
timescale_sink = PostgreSQLSink(
    host=os.getenv("TIMESCALE_HOST", "timescaledb"),
    port=int(os.getenv("TIMESCALE_PORT", "5432")),
    dbname=os.getenv("TIMESCALE_DBNAME", "metrics"),
    user=os.getenv("TIMESCALE_USER", "tsadmin"),
    password=os.getenv("TIMESCALE_PASSWORD"),
    table_name=os.getenv("TIMESCALE_TABLE", "solar_panel_data"),
    schema_name=os.getenv("TIMESCALE_SCHEMA", "public"),
    schema_auto_update=os.getenv("SCHEMA_AUTO_UPDATE", "true").lower() == "true",
)

# Initialize the application
app = Application(
    consumer_group="timescale-solar-sink-group",
    auto_offset_reset="earliest",
    commit_interval=float(os.getenv("BATCH_TIMEOUT", "1")),
    commit_every=int(os.getenv("BATCH_SIZE", "1000"))
)

# Define the input topic - solar-data
input_topic = app.topic(os.getenv("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Print the entire message body for debugging
sdf = sdf.update(lambda row: print(f"Processing solar panel data: {json.dumps(row, indent=2)}"))

# Convert timestamp from nanoseconds to proper timestamp for TimescaleDB
sdf = sdf.update(lambda row: {
    **row,
    "timestamp": int(row["timestamp"] / 1000000)  # Convert nanoseconds to milliseconds
})

sdf.sink(timescale_sink)

if __name__ == "__main__":
    # Use stop conditions to prevent infinite running during testing
    app.run(sdf, count=10, timeout=20)