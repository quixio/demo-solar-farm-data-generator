import os
import json
import psycopg2
from datetime import datetime, timezone
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()


class TimescaleDBSink(BatchingSink):
    """Custom TimescaleDB sink with table-creation in __init__ and
    automatic timestamp conversion."""

    def __init__(
        self,
        host,
        port,
        database,
        username,
        password,
        table_name,
        schema_name="public",
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.table_name = table_name
        self.schema_name = schema_name

        # Connect and make sure the table exists **before** the sink runs
        self._connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password,
        )
        # DDL must be committed immediately so writer threads can see it
        self._connection.autocommit = True
        self._ensure_table()

    # --------------------------------------------------------------------- #
    # Helper utilities
    # --------------------------------------------------------------------- #

    def _ensure_table(self):
        """Create the table (and hypertable) if missing."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
            panel_id        VARCHAR(255),
            location_id     VARCHAR(255),
            location_name   VARCHAR(255),
            latitude        DOUBLE PRECISION,
            longitude       DOUBLE PRECISION,
            timezone        INTEGER,
            power_output    DOUBLE PRECISION,
            unit_power      VARCHAR(50),
            temperature     DOUBLE PRECISION,
            unit_temp       VARCHAR(50),
            irradiance      DOUBLE PRECISION,
            unit_irradiance VARCHAR(50),
            voltage         DOUBLE PRECISION,
            unit_voltage    VARCHAR(50),
            current         DOUBLE PRECISION,
            unit_current    VARCHAR(50),
            inverter_status VARCHAR(50),
            timestamp       BIGINT,
            message_datetime TIMESTAMPTZ,
            PRIMARY KEY (panel_id, timestamp)
        );
        """
        hypertable_sql = f"""
        SELECT create_hypertable('{self.schema_name}.{self.table_name}',
                                 'message_datetime',
                                 if_not_exists => TRUE);
        """

        with self._connection.cursor() as cur:
            cur.execute(create_table_sql)
            try:
                cur.execute(hypertable_sql)
            except Exception as e:
                # Happens if Timescale extension is absent or already converted
                print(f"Note: could not create hypertable (safe to ignore): {e}")

        print(
            f"Connected to TimescaleDB and ensured table "
            f"{self.schema_name}.{self.table_name} exists"
        )

    @staticmethod
    def _as_datetime(value):
        """
        Convert raw timestamps (ms or s since epoch) or datetime objects
        to timezone-aware UTC datetimes for TimescaleDB.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        if isinstance(value, (int, float)):
            # Heuristic: if > 1e12 treat as milliseconds
            if value > 1_000_000_000_000:
                value /= 1_000.0
            return datetime.fromtimestamp(value, tz=timezone.utc)
        raise TypeError(f"Unsupported timestamp type: {type(value)}")

    # --------------------------------------------------------------------- #
    # BatchingSink interface
    # --------------------------------------------------------------------- #

    # setup() is no longer needed – everything happens in __init__
    def setup(self):
        pass

    def write(self, batch: SinkBatch):
        if not self._connection:
            raise RuntimeError("Database connection not established")

        insert_sql = f"""
        INSERT INTO {self.schema_name}.{self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status,
            timestamp, message_datetime
        ) VALUES (
            %(panel_id)s, %(location_id)s, %(location_name)s, %(latitude)s, %(longitude)s, %(timezone)s,
            %(power_output)s, %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s, %(unit_irradiance)s,
            %(voltage)s, %(unit_voltage)s, %(current)s, %(unit_current)s, %(inverter_status)s,
            %(timestamp)s, %(message_datetime)s
        )
        """

        try:
            with self._connection.cursor() as cur:
                cnt = 0
                for item in batch:
                    raw = item.value
                    data = raw if isinstance(raw, dict) else json.loads(raw)

                    record = {
                        "panel_id": data.get("panel_id"),
                        "location_id": data.get("location_id"),
                        "location_name": data.get("location_name"),
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                        "timezone": data.get("timezone"),
                        "power_output": data.get("power_output"),
                        "unit_power": data.get("unit_power"),
                        "temperature": data.get("temperature"),
                        "unit_temp": data.get("unit_temp"),
                        "irradiance": data.get("irradiance"),
                        "unit_irradiance": data.get("unit_irradiance"),
                        "voltage": data.get("voltage"),
                        "unit_voltage": data.get("unit_voltage"),
                        "current": data.get("current"),
                        "unit_current": data.get("unit_current"),
                        "inverter_status": data.get("inverter_status"),
                        "timestamp": data.get("timestamp"),
                        "message_datetime": self._as_datetime(item.timestamp),
                    }

                    cur.execute(insert_sql, record)
                    cnt += 1

                self._connection.commit()
                print(f"Successfully wrote {cnt} records to TimescaleDB")

        except Exception as e:
            self._connection.rollback()
            print(f"Error writing to TimescaleDB: {e}")
            raise

    def close(self):
        if self._connection:
            self._connection.close()
            print("Closed TimescaleDB connection")


# ------------------------------------------------------------------------- #
# Boiler-plate to wire the sink into a Quix Streams application
# ------------------------------------------------------------------------- #

# Port parsing with fallback
try:
    port = int(os.environ.get("TIMESCALEDB_PORT", "5432"))
except ValueError:
    port = 5432

timescale_sink = TimescaleDBSink(
    host=os.environ.get("TIMESCALEDB_HOST", "timescaledb"),
    port=port,
    database=os.environ.get("TIMESCALEDB_DATABASE", "metrics"),
    username=os.environ.get("TIMESCALEDB_USER", "tsadmin"),
    password=os.environ.get("TIMESCALE_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_data_v3a"),
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public"),
)

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000")),
)

input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")
sdf = app.dataframe(input_topic)
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)
