import os
import json
import logging
import datetime
from typing import Any, Dict

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink

load_dotenv()  # Load local .env if present

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("timescaledb-sink")


def _parse_message(raw_value: Any) -> Dict[str, Any]:
    """
    Transform the raw Kafka message value into a dictionary suitable for
    TimescaleDB / PostgreSQL insertion.

    The incoming `raw_value` is expected to be a JSON string that represents
    the *outer* message structure described in the schema documentation,
    including a field named `value` which itself is a JSON string containing
    the sensor payload.

    Returns
    -------
    dict
        A flattened dictionary containing the inner sensor payload plus a few
        helpful metadata columns.
    """
    if raw_value is None:
        logger.debug("Received null message value – skipping.")
        return {}

    # Decode the outer JSON layer
    try:
        outer = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        logger.warning("Failed to decode outer JSON: %s", raw_value)
        return {}

    # Decode the inner `value` field, which should be a JSON string
    inner_str = outer.get("value")
    if isinstance(inner_str, str):
        try:
            inner = json.loads(inner_str)
        except json.JSONDecodeError:
            logger.warning("Failed to decode inner JSON: %s", inner_str)
            inner = {}
    elif isinstance(inner_str, dict):
        inner = inner_str
    else:
        inner = {}

    # Attach producer timestamp (outer)
    if outer.get("dateTime"):
        inner["datetime_produced"] = outer["dateTime"]

    # Convert nanosecond epoch -> datetime if present
    ts_ns = inner.get("timestamp")
    if ts_ns is not None:
        try:
            inner["timestamp"] = datetime.datetime.utcfromtimestamp(float(ts_ns) / 1e9)
        except Exception:
            # If conversion fails, leave the original value
            logger.debug("Failed to convert timestamp %s", ts_ns)

    return inner


# --------------------------------------------------------------------------- #
# Initialise the TimescaleDB/PostgreSQL sink
# --------------------------------------------------------------------------- #
timescaledb_sink = PostgreSQLSink(
    host=os.environ.get("TIMESCALEDB_HOST"),
    port=int(os.environ.get("TIMESCALEDB_PORT", 5432)),
    dbname=os.environ.get("TIMESCALEDB_DBNAME"),
    user=os.environ.get("TIMESCALEDB_USER"),
    password=os.environ.get("TIMESCALEDB_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_data"),
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public"),
    schema_auto_update=os.environ.get("SCHEMA_AUTO_UPDATE", "true").lower() == "true",
)

# --------------------------------------------------------------------------- #
# Build the Quix Streams application
# --------------------------------------------------------------------------- #
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000")),
)

input_topic_name = os.environ.get(
    "input", "demo-solarfarmdatageneratordemo-prod-solar-data"
)
input_topic = app.topic(input_topic_name, key_deserializer="string")

# Stream processing pipeline
sdf = app.dataframe(input_topic)
processed_sdf = sdf.apply(_parse_message)  # Transform each message value
processed_sdf.sink(timescaledb_sink)       # Write to TimescaleDB

# --------------------------------------------------------------------------- #
# Entry-point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    # Process exactly 10 messages or exit after 20 seconds (whichever happens first)
    app.run(count=10, timeout=20)