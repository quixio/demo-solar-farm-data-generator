import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch


def _get_timescale_password() -> str:
    """
    Retrieve the password from the most common Quix env-var names.
    Raise a clear error if none of them are present.
    """
    pw = (
        os.getenv("TIMESCALEDB_PASSWORD")                       # your own variable
        or os.getenv("TIMESCALEDB_PASSWORD__SECRET")            # Quix secret injection
        or os.getenv("TIMESCALE_PASSWORD_SECRET_KEY")           # legacy name (used in the code before)
    )
    if not pw:
        raise RuntimeError(
            "TimescaleDB password not found – please set "
            "`TIMESCALEDB_PASSWORD` or add a secret called "
            "`TIMESCALEDB_PASSWORD` in the Quix UI."
        )
    return pw


class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name):
        super().__init__()
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._table_name = table_name
        self._connection = None
        self._cursor = None

    def setup(self):
        """Initialize the connection to TimescaleDB"""
        try:
            self._connection = psycopg2.connect(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password
            )
            self._cursor = self._connection.cursor()
            self._create_table_if_not_exists()
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            panel_id VARCHAR(255),
            location_id VARCHAR(255),
            location_name VARCHAR(255),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone INTEGER,
            power_output DOUBLE PRECISION,
            unit_power VARCHAR(10),
            temperature DOUBLE PRECISION,
            unit_temp VARCHAR(10),
            irradiance DOUBLE PRECISION,
            unit_irradiance VARCHAR(10),
            voltage DOUBLE PRECISION,
            unit_voltage VARCHAR(10),
            current DOUBLE PRECISION,
            unit_current VARCHAR(10),
            inverter_status VARCHAR(50),
            timestamp BIGINT,
            datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (panel_id, timestamp)
        );
        """

        # Create hypertable if it doesn't exist (TimescaleDB specific)
        hypertable_query = f"""
        SELECT create_hypertable('{self._table_name}', 'datetime', if_not_exists => TRUE);
        """

        try:
            self._cursor.execute(create_table_query)
            self._cursor.execute(hypertable_query)
            self._connection.commit()
            print(f"Table {self._table_name} created or already exists")
        except Exception as e:
            print(f"Error creating table: {e}")
            self._connection.rollback()
            raise

    def write(self, batch: SinkBatch):
        """Write a batch of data to TimescaleDB"""
        if not self._connection or not self._cursor:
            raise RuntimeError("TimescaleDB connection not initialized")

        insert_query = f"""
        INSERT INTO {self._table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        records = []
        for item in batch:
            # Parse the JSON string from the value field
            data = json.loads(item.value)

            record = (
                data.get('panel_id'),
                data.get('location_id'),
                data.get('location_name'),
                data.get('latitude'),
                data.get('longitude'),
                data.get('timezone'),
                data.get('power_output'),
                data.get('unit_power'),
                data.get('temperature'),
                data.get('unit_temp'),
                data.get('irradiance'),
                data.get('unit_irradiance'),
                data.get('voltage'),
                data.get('unit_voltage'),
                data.get('current'),
                data.get('unit_current'),
                data.get('inverter_status'),
                data.get('timestamp')
            )
            records.append(record)

        try:
            self._cursor.executemany(insert_query, records)
            self._connection.commit()
            print(f"Successfully inserted {len(records)} records into {self._table_name}")
        except Exception as e:
            print(f"Error inserting data: {e}")
            self._connection.rollback()
            raise

    def close(self):
        """Close the database connection"""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()


def main():
    # Get port with safe integer conversion
    try:
        port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
    except ValueError:
        port = 5432

    # Initialize TimescaleDB Sink
    timescale_sink = TimescaleDBSink(
        host=os.getenv("TIMESCALEDB_HOST", "timescaledb"),
        port=port,
        database=os.getenv("TIMESCALEDB_DATABASE", "metrics"),
        user=os.getenv("TIMESCALEDB_USER", "tsadmn"),
        password=_get_timescale_password(),
        table_name=os.getenv("TIMESCALEDB_TABLE", "solar_data_v1")
    )

    # Initialize the application
    app = Application(
        consumer_group="timescale-sink-consumer",
        auto_offset_reset="earliest",
        commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
        commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
    )

    # Define the input topic
    input_topic = app.topic("input", value_deserializer="string")

    # Create streaming dataframe
    sdf = app.dataframe(input_topic)

    # Add debug print to show raw message structure
    sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)

    # Sink data to TimescaleDB
    sdf.sink(timescale_sink)

    # Run the application for 10 messages
    app.run(count=10, timeout=20)


if __name__ == "__main__":
    main()