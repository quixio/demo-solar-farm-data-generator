import json
import os

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink

# Load variables from a .env file when running locally
load_dotenv()

# ---------------------------------------------------------------------------
# TimescaleDB (PostgreSQL) Sink
# ---------------------------------------------------------------------------
timescaledb_sink = PostgreSQLSink(
    host=os.environ["TIMESCALEDB_HOST"],
    port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
    dbname=os.environ["TIMESCALEDB_DBNAME"],
    user=os.environ["TIMESCALEDB_USER"],
    password=os.environ["TIMESCALEDB_PASSWORD"],
    table_name=os.environ["TIMESCALEDB_TABLE"],
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public"),
    schema_auto_update=os.environ.get("SCHEMA_AUTO_UPDATE", "true").lower() == "true",
)

# ---------------------------------------------------------------------------
# Quix Streams Application
# ---------------------------------------------------------------------------
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000")),
)

# Define the input topic (leave out value deserializer; defaults to JSON)
input_topic = app.topic(os.environ["input"], key_deserializer="string")

# ---------------------------------------------------------------------------
# Data Processing
# ---------------------------------------------------------------------------
sdf = app.dataframe(input_topic)


def _extract_solar_data(value: dict) -> dict:
    """
    The incoming Kafka message value is expected to be a dictionary that
    contains a 'value' field holding the solar panel data as a JSON-encoded
    string. This function extracts and decodes that payload.
    """
    if isinstance(value, dict) and "value" in value:
        try:
            return json.loads(value["value"])
        except (TypeError, json.JSONDecodeError):
            # If decoding fails, keep the raw payload for inspection.
            return {"raw_value": value["value"]}
    # Fallback for unexpected formats
    return {"raw_value": value}


processed_sdf = sdf.map_values(_extract_solar_data)

# Sink the processed data to TimescaleDB
processed_sdf.sink(timescaledb_sink)

# ---------------------------------------------------------------------------
# Run the application
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Stop after processing 10 messages or 20 seconds, whichever comes first.
    app.run(processed_sdf, count=10, timeout=20)