import json
import logging
import os
from typing import Any

from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.models.serializers.deserializer import Deserializer
from quixstreams.sinks.community.postgresql import PostgreSQLSink

# ----------------------------------------------------------------------
# Environment
# ----------------------------------------------------------------------
load_dotenv()
logging.basicConfig(level=logging.INFO)

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def solar_message_deserializer(value: bytes) -> Any:
    """
    Decode the incoming Kafka message value.

    The incoming value is expected to be a JSON object that contains a field
    called ``value`` which itself is a JSON-encoded string with the actual
    solar-panel measurements.

    Returns a plain Python dict with the solar-panel data so that it can be
    written straight to TimescaleDB via the PostgreSQL sink.
    """
    try:
        message = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logging.error("Failed to decode incoming message: %s", value)
        return {}

    # If the payload follows the documented schema, the solar data lives in
    # the "value" field and is itself JSON-encoded.
    if isinstance(message, dict) and "value" in message:
        raw_payload = message["value"]
        if isinstance(raw_payload, str):
            try:
                return json.loads(raw_payload)
            except json.JSONDecodeError:
                logging.error("Failed to decode nested solar data JSON: %s", raw_payload)
                return {}
        # If it's already a dict (unexpected but safe) just return it.
        if isinstance(raw_payload, dict):
            return raw_payload

    # Fallback: return the whole decoded message.
    return message


# ----------------------------------------------------------------------
# Custom deserializer required by Quix
# ----------------------------------------------------------------------
class SolarMessageDeserializer(Deserializer):
    """Adapter that lets Quix treat our helper as a real deserializer."""

    def deserialize(self, value: bytes):
        return solar_message_deserializer(value)


# ----------------------------------------------------------------------
# TimescaleDB (PostgreSQL) Sink
# ----------------------------------------------------------------------
timescale_sink = PostgreSQLSink(
    host=os.environ.get("TIMESCALEDB_HOST"),
    port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
    dbname=os.environ.get("TIMESCALEDB_DBNAME"),
    user=os.environ.get("TIMESCALEDB_USER"),
    password=os.environ.get("TIMESCALEDB_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE"),
    schema_name=os.getenv("TIMESCALEDB_SCHEMA", "public"),
    schema_auto_update=os.environ.get("SCHEMA_AUTO_UPDATE", "true").lower() == "true",
)

# ----------------------------------------------------------------------
# Quix Application
# ----------------------------------------------------------------------
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "solar-timescale-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000")),
)

input_topic_name = os.environ.get("input")
input_topic = app.topic(
    input_topic_name,
    key_deserializer="string",
    value_deserializer=SolarMessageDeserializer(),
)

# Build processing pipeline
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

# ----------------------------------------------------------------------
# Main entry-point
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # The application must stop after processing 10 messages or 20 seconds,
    # whichever comes first.
    app.run(count=10, timeout=20)