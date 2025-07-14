import json
import os
from datetime import datetime
from typing import List, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError

# -----------------------------------------------------------------------------#
#  Load environment variables (use a .env file for local development)          #
# -----------------------------------------------------------------------------#
load_dotenv()

# -----------------------------------------------------------------------------#
#  TimescaleDB Sink implementation                                             #
# -----------------------------------------------------------------------------#
class TimescaleDBSink(BatchingSink):
    """
    A simple batching sink that writes solar-panel messages into a TimescaleDB
    hypertable. The sink creates the table and hypertable if they do not exist.
    """

    COLUMNS: Tuple[str, ...] = (
        "date_time",
        "panel_id",
        "location_id",
        "location_name",
        "latitude",
        "longitude",
        "timezone",
        "power_output",
        "unit_power",
        "temperature",
        "unit_temp",
        "irradiance",
        "unit_irradiance",
        "voltage",
        "unit_voltage",
        "current",
        "unit_current",
        "inverter_status",
        "measurement_timestamp",
    )

    CREATE_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {{schema}}.{{table}} (
        date_time              TIMESTAMPTZ      NOT NULL,
        panel_id               TEXT             NOT NULL,
        location_id            TEXT,
        location_name          TEXT,
        latitude               DOUBLE PRECISION,
        longitude              DOUBLE PRECISION,
        timezone               INTEGER,
        power_output           INTEGER,
        unit_power             TEXT,
        temperature            DOUBLE PRECISION,
        unit_temp              TEXT,
        irradiance             INTEGER,
        unit_irradiance        TEXT,
        voltage                DOUBLE PRECISION,
        unit_voltage           TEXT,
        current                INTEGER,
        unit_current           TEXT,
        inverter_status        TEXT,
        measurement_timestamp  BIGINT,
        PRIMARY KEY (date_time, panel_id)
    );
    """

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        table_name: str,
        schema_name: str = "public",
        on_client_connect_success=None,
        on_client_connect_failure=None,
    ):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._conn_params = dict(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        self._table_name = table_name
        self._schema_name = schema_name
        self._connection = None

    # ---------------------------------------------------------------------#
    #  Quix Streams required overrides                                     #
    # ---------------------------------------------------------------------#
    def setup(self):
        """Establish DB connection and ensure table / hypertable exist."""
        try:
            self._connection = psycopg2.connect(**self._conn_params)
            self._connection.autocommit = True
            self._ensure_table()
            if self.on_client_connect_success:
                self.on_client_connect_success()
        except Exception as exc:  # pragma: no cover
            if self.on_client_connect_failure:
                self.on_client_connect_failure(exc)
            raise

    def write(self, batch: SinkBatch):
        """
        Transform the batch of Kafka messages and persist to TimescaleDB.
        """
        records: List[Tuple] = []
        for row in batch:
            try:
                record = self._transform_row(row.value)
                if record:
                    records.append(record)
            except Exception as exc:  # Ignore malformed rows but log if needed
                # In production, consider using proper logging
                print(f"Skipping malformed record: {exc}")

        if not records:
            return

        insert_sql = (
            f"INSERT INTO {self._schema_name}.{self._table_name} ({', '.join(self.COLUMNS)}) "
            f"VALUES %s ON CONFLICT DO NOTHING"
        )

        try:
            with self._connection.cursor() as cur:
                psycopg2.extras.execute_values(cur, insert_sql, records)
        except psycopg2.OperationalError as exc:
            # Backpressure for 30 seconds if DB is temporarily unavailable
            raise SinkBackpressureError(
                retry_after=30.0, topic=batch.topic, partition=batch.partition
            ) from exc

    # ---------------------------------------------------------------------#
    #  Helper functions                                                    #
    # ---------------------------------------------------------------------#
    def _ensure_table(self):
        """Create table and hypertable if they don't exist."""
        with self._connection.cursor() as cur:
            cur.execute(
                self.CREATE_TABLE_SQL.format(
                    schema=self._schema_name, table=self._table_name
                )
            )
            # Convert to hypertable (noop if already converted)
            cur.execute(
                "SELECT create_hypertable(%s, %s, if_not_exists => TRUE)",
                (f"{self._schema_name}.{self._table_name}", "date_time"),
            )

    @staticmethod
    def _transform_row(message_value: dict) -> Tuple:
        """
        Parse the outer Quix message value and extract the solar payload fields.
        Returns a tuple ordered according to COLUMNS.
        """
        # Parse outer envelope ------------------------------------------------
        envelope = message_value
        date_time_iso = envelope.get("dateTime")
        payload_raw = envelope.get("value")

        if not date_time_iso or not payload_raw:
            raise ValueError("Missing dateTime or value in message")

        # Parse the inner solar payload --------------------------------------
        payload = json.loads(payload_raw)

        # Compose record tuple ------------------------------------------------
        date_time = datetime.fromisoformat(date_time_iso.replace("Z", "+00:00"))

        return (
            date_time,
            payload.get("panel_id"),
            payload.get("location_id"),
            payload.get("location_name"),
            payload.get("latitude"),
            payload.get("longitude"),
            payload.get("timezone"),
            payload.get("power_output"),
            payload.get("unit_power"),
            payload.get("temperature"),
            payload.get("unit_temp"),
            payload.get("irradiance"),
            payload.get("unit_irradiance"),
            payload.get("voltage"),
            payload.get("unit_voltage"),
            payload.get("current"),
            payload.get("unit_current"),
            payload.get("inverter_status"),
            payload.get("timestamp"),
        )


# -----------------------------------------------------------------------------#
#  Application setup                                                           #
# -----------------------------------------------------------------------------#
# Initialize the TimescaleDB sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get("TIMESCALEDB_HOST"),
    port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
    dbname=os.environ.get("TIMESCALEDB_DBNAME"),
    user=os.environ.get("TIMESCALEDB_USER"),
    password=os.environ.get("TIMESCALEDB_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_panel_metrics"),
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public"),
)

# Initialize the Quix application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "solar-timescale-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000")),
)

# Define input topic
input_topic_name = os.environ.get("INPUT_TOPIC", "solar-data")
input_topic = app.topic(
    input_topic_name, key_deserializer="string", value_deserializer="json"
)

# Build the streaming dataframe
sdf = app.dataframe(input_topic)

# Add the sink (terminal operation)
sdf.sink(timescale_sink)

# -----------------------------------------------------------------------------#
#  Run the application                                                         #
# -----------------------------------------------------------------------------#
if __name__ == "__main__":
    # Process exactly 10 messages or stop after 20 seconds, whichever comes first
    app.run(sdf, count=10, timeout=20)