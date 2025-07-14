import json
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# ---------------------------------------------------------------------#
#  Load environment variables (handy for local testing)                #
# ---------------------------------------------------------------------#
load_dotenv()

# ---------------------------------------------------------------------#
#  TimescaleDB Sink Implementation                                     #
# ---------------------------------------------------------------------#
class TimescaleDBSink(BatchingSink):
    """
    A simple TimescaleDB sink that writes solar-panel data into a hypertable.
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
        self._conn_params = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }
        self._table = table_name
        self._schema = schema_name
        self._connection = None

    # -----------------------------------------------------------------#
    #  Quix Streams required overrides                                  #
    # -----------------------------------------------------------------#
    def setup(self):
        """
        Establish DB connection and ensure table / hypertable exist.
        NOTE: BatchingSink.start() will trigger the connection callbacks.
        """
        self._connection = psycopg2.connect(**self._conn_params)
        self._connection.autocommit = True
        self._ensure_table()

    def write(self, batch: SinkBatch):
        """
        Insert a batch of records into TimescaleDB.
        """
        rows = []
        for record in batch:
            try:
                payload = json.loads(record.value)
            except (json.JSONDecodeError, TypeError):
                # Skip malformed records
                continue

            ts_ns = payload.get("timestamp")
            if ts_ns is not None:
                # Convert nanoseconds epoch to datetime; fall back if bad value
                try:
                    ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
                except Exception:  # pragma: no cover
                    ts = datetime.utcnow().replace(tzinfo=timezone.utc)
            else:
                ts = datetime.utcnow().replace(tzinfo=timezone.utc)

            rows.append(
                (
                    ts,  # time
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
                    record.timestamp,  # Kafka record timestamp (ms since epoch)
                )
            )

        if not rows:
            return  # Nothing to write

        insert_sql = f"""
            INSERT INTO "{self._schema}"."{self._table}" (
                time,
                panel_id,
                location_id,
                location_name,
                latitude,
                longitude,
                timezone,
                power_output,
                unit_power,
                temperature,
                unit_temp,
                irradiance,
                unit_irradiance,
                voltage,
                unit_voltage,
                current,
                unit_current,
                inverter_status,
                source_timestamp
            ) VALUES %s
        """

        with self._connection.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                insert_sql,
                rows,
                template=None,
                page_size=1000,
            )

    # -----------------------------------------------------------------#
    #  Internal helpers                                                 #
    # -----------------------------------------------------------------#
    def _ensure_table(self):
        """
        Create the table if it doesn't exist and convert it into a hypertable.
        """
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS "{self._schema}"."{self._table}" (
                time              TIMESTAMPTZ       NOT NULL,
                panel_id          TEXT,
                location_id       TEXT,
                location_name     TEXT,
                latitude          DOUBLE PRECISION,
                longitude         DOUBLE PRECISION,
                timezone          INTEGER,
                power_output      INTEGER,
                unit_power        TEXT,
                temperature       DOUBLE PRECISION,
                unit_temp         TEXT,
                irradiance        INTEGER,
                unit_irradiance   TEXT,
                voltage           DOUBLE PRECISION,
                unit_voltage      TEXT,
                current           INTEGER,
                unit_current      TEXT,
                inverter_status   TEXT,
                source_timestamp  BIGINT
            );
        """
        create_hypertable_sql = f"""
            SELECT create_hypertable(
                '"{self._schema}"."{self._table}"',
                'time',
                if_not_exists => TRUE
            );
        """

        with self._connection.cursor() as cur:
            cur.execute(create_table_sql)
            cur.execute(create_hypertable_sql)


# ---------------------------------------------------------------------#
#  Sink instantiation                                                  #
# ---------------------------------------------------------------------#
timescaledb_sink = TimescaleDBSink(
    host=os.environ.get("TIMESCALEDB_HOST", "localhost"),
    port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
    dbname=os.environ.get("TIMESCALEDB_DBNAME", "postgres"),
    user=os.environ.get("TIMESCALEDB_USER", "postgres"),
    password=os.environ.get("TIMESCALEDB_PASSWORD", ""),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_panel_data"),
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public"),
)

# ---------------------------------------------------------------------#
#  Quix Streams Application setup                                      #
# ---------------------------------------------------------------------#
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "solar-timescale-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000")),
)

input_topic_name = os.environ.get("INPUT_TOPIC", "solar-data")
input_topic = app.topic(
    input_topic_name,
    key_deserializer="string",
    value_deserializer="string",
)

sdf = app.dataframe(input_topic)
sdf.sink(timescaledb_sink)

# ---------------------------------------------------------------------#
#  Entrypoint                                                          #
# ---------------------------------------------------------------------#
if __name__ == "__main__":
    # Stop after 10 messages or 20 seconds, whichever comes first.
    app.run(count=10, timeout=20)