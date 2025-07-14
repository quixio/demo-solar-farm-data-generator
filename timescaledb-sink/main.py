import os
import json
from typing import Any

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink

# ----------------------------------------------------------------------
# Environment
# ----------------------------------------------------------------------
# For local development, automatically load variables from a .env file
load_dotenv()

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def solar_message_deserializer(value: bytes) -> Any:
    """
    Deserialize incoming Kafka message bytes into a dictionary that
    contains only the solar-panel measurement fields.

    The producer sends messages in one of two known formats:
    1.   Raw JSON of the solar-panel data                     (dict-like)
    2.   A wrapper object with metadata and a `value` field
         which itself is a JSON-encoded string of the data.   (dict-like)

    This helper normalises both formats so that the returned object is
    always the *solar data dictionary* expected by the TimescaleDB sink.
    """
    try:
        outer = json.loads(value.decode("utf-8"))
    except Exception:
        # In the unlikely event that the payload is already decoded.
        outer = value

    # Case 1 – payload is the solar data itself
    if isinstance(outer, dict) and "panel_id" in outer:
        return outer

    # Case 2 – payload is a wrapper with the data in `value`
    if isinstance(outer, dict) and "value" in outer:
        inner_raw = outer["value"]
        if isinstance(inner_raw, str):
            try:
                return json.loads(inner_raw)
            except json.JSONDecodeError:
                pass  # If it's not JSON, fall through
        elif isinstance(inner_raw, dict):
            return inner_raw

    # Fallback – return whatever we got; the sink may reject it if invalid.
    return outer


# ----------------------------------------------------------------------
# TimescaleDB (PostgreSQL) Sink
# ----------------------------------------------------------------------
timescale_sink = PostgreSQLSink(
    host=os.environ.get("TIMESCALEDB_HOST"),
    port=int(os.environ.get("TIMESCALEDB_PORT", 5432)),
    dbname=os.environ.get("TIMESCALEDB_DBNAME"),
    user=os.environ.get("TIMESCALEDB_USER"),
    password=os.environ.get("TIMESCALEDB_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_panel_measurements"),
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public"),
    schema_auto_update=os.environ.get("SCHEMA_AUTO_UPDATE", "true").lower() == "true",
)

# ----------------------------------------------------------------------
# Quix Application
# ----------------------------------------------------------------------
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000")),
)

input_topic_name = (
    os.environ.get("INPUT_TOPIC") or os.environ.get("input") or "solar-data-topic"
)
input_topic = app.topic(
    input_topic_name,
    key_deserializer="string",
    value_deserializer=solar_message_deserializer,
)

sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

# ----------------------------------------------------------------------
# Entry-point
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Process exactly 10 messages (max 20 s) then shut down gracefully.
    app.run(count=10, timeout=20)