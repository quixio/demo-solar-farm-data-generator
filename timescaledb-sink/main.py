import os
import logging
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TimescaleDBSink(BatchingSink):
    """
    Custom sink for writing data to TimescaleDB
    """
    
    def __init__(self, 
                 host: str,
                 port: int,
                 database: str,
                 username: str,
                 password: str,
                 table_name: str,
                 on_client_connect_success=None,
                 on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.table_name = table_name
        self.connection: Optional[psycopg2.connection] = None

    def setup(self):
        """Establish connection to TimescaleDB and create table if needed"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            self.connection.autocommit = True
            logger.info("Successfully connected to TimescaleDB")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            time TIMESTAMPTZ NOT NULL,
            panel_id TEXT,
            location_id TEXT,
            location_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone INTEGER,
            power_output DOUBLE PRECISION,
            unit_power TEXT,
            temperature DOUBLE PRECISION,
            unit_temp TEXT,
            irradiance DOUBLE PRECISION,
            unit_irradiance TEXT,
            voltage DOUBLE PRECISION,
            unit_voltage TEXT,
            current DOUBLE PRECISION,
            unit_current TEXT,
            inverter_status TEXT,
            original_timestamp BIGINT
        );
        """
        
        # Create hypertable for TimescaleDB
        hypertable_query = f"""
        SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
            cursor.execute(hypertable_query)
            logger.info(f"Table {self.table_name} created/verified successfully")

    def write(self, batch: SinkBatch):
        """Write batch of messages to TimescaleDB"""
        if not self.connection:
            raise RuntimeError("TimescaleDB connection not established")

        insert_query = f"""
        INSERT INTO {self.table_name} (
            time, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, original_timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        try:
            with self.connection.cursor() as cursor:
                for item in batch:
                    # ------------------------------------------------------------
                    # 1. Get an event-time value that TimescaleDB will accept
                    # ------------------------------------------------------------
                    if hasattr(item, "dateTime") and item.dateTime:
                        # Quix metadata — already ISO-8601
                        event_time = datetime.fromisoformat(
                            item.dateTime.replace("Z", "+00:00")
                        )
                    else:
                        # Fallback to nanoseconds in the payload (value already a dict)
                        ns = item.value.get("timestamp")
                        event_time = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)

                    # ------------------------------------------------------------
                    # 2. The payload is already a dict – no json.loads needed
                    # ------------------------------------------------------------
                    value_data = item.value  # <-- already deserialised

                    row_data = (
                        event_time,
                        value_data.get("panel_id"),
                        value_data.get("location_id"),
                        value_data.get("location_name"),
                        value_data.get("latitude"),
                        value_data.get("longitude"),
                        value_data.get("timezone"),
                        value_data.get("power_output"),
                        value_data.get("unit_power"),
                        value_data.get("temperature"),
                        value_data.get("unit_temp"),
                        value_data.get("irradiance"),
                        value_data.get("unit_irradiance"),
                        value_data.get("voltage"),
                        value_data.get("unit_voltage"),
                        value_data.get("current"),
                        value_data.get("unit_current"),
                        value_data.get("inverter_status"),
                        value_data.get("timestamp"),  # original ns value
                    )

                    cursor.execute(insert_query, row_data)

                logger.info("Successfully wrote %s records to TimescaleDB", len(batch))

        except Exception as e:
            logger.exception("Error writing to TimescaleDB")
            raise

    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            logger.info("TimescaleDB connection closed")


def debug_message(item):
    """Debug function to print raw message structure"""
    print(f'Raw message: {item}')
    return item


def main():
    # Initialize the Application
    app = Application(
        broker_address=os.environ.get("KAFKA_BROKER_ADDRESS", "localhost:9092"),
        consumer_group="timescaledb-sink-group",
        auto_offset_reset="earliest",
    )

    # Define input topic with JSON deserializer
    input_topic = app.topic(
        name=os.environ.get("KAFKA_INPUT_TOPIC", "solar-data"),
        value_deserializer="json"
    )

    # Create streaming dataframe
    sdf = app.dataframe(input_topic)
    
    # Add debug logging to see raw message structure
    sdf = sdf.apply(debug_message)

    # Initialize TimescaleDB sink
    timescale_sink = TimescaleDBSink(
        host=os.environ.get("TIMESCALEDB_HOST", "localhost"),
        port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
        database=os.environ.get("TIMESCALEDB_DATABASE", "solar_data"),
        username=os.environ.get("TIMESCALEDB_USER", "postgres"),
        password=os.environ.get("TIMESCALEDB_PASSWORD", "password"),
        table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_panel_data"),
        on_client_connect_success=lambda: logger.info("TimescaleDB client connected successfully"),
        on_client_connect_failure=lambda e: logger.error(f"TimescaleDB client connection failed: {e}")
    )

    # Sink data to TimescaleDB
    sdf.sink(timescale_sink)

    logger.info("Starting TimescaleDB sink application...")
    try:
        # Run the application for 10 messages with 20 second timeout
        app.run(count=10, timeout=20)
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        timescale_sink.close()
        logger.info("TimescaleDB sink application stopped")


if __name__ == "__main__":
    main()