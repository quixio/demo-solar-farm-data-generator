# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import logging
from datetime import datetime
from clickhouse_driver import Client

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    
    This sink processes solar panel sensor data and writes it to a ClickHouse database.
    The data comes from Kafka messages where the actual payload is JSON-encoded 
    in the 'value' field.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_created = False
    
    def setup(self):
        """Initialize ClickHouse connection and create table if needed."""
        try:
            # Parse port safely
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
            except ValueError:
                port = 9000
                
            self._client = Client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=port,
                user=os.environ.get('CLICKHOUSE_USER', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                database=os.environ.get('CLICKHOUSE_DATABASE', 'default')
            )
            
            # Test connection
            self._client.execute('SELECT 1')
            logger.info("Successfully connected to ClickHouse")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar_data table if it doesn't exist."""
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_data')
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            panel_id String,
            location_id String,
            location_name String,
            latitude Float64,
            longitude Float64,
            timezone Int32,
            power_output Float64,
            unit_power String,
            temperature Float64,
            unit_temp String,
            irradiance Float64,
            unit_irradiance String,
            voltage Float64,
            unit_voltage String,
            current Float64,
            unit_current String,
            inverter_status String,
            timestamp UInt64,
            message_datetime DateTime,
            kafka_offset UInt64,
            kafka_partition UInt32
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, message_datetime)
        """
        
        self._client.execute(create_table_sql)
        logger.info(f"Table {table_name} is ready")
        self._table_created = True
    
    def _process_message(self, message):
        """
        Process a single message and extract solar data.
        
        The message structure has the actual solar data JSON-encoded in the 'value' field.
        """
        try:
            # The 'value' field contains JSON-encoded solar data
            if isinstance(message.get('value'), str):
                solar_data = json.loads(message['value'])
            else:
                # If value is already parsed, use it directly
                solar_data = message.get('value', {})
            
            # Convert message datetime to proper datetime
            message_datetime = datetime.fromisoformat(
                message.get('dateTime', '').replace('Z', '+00:00')
            )
            
            # Prepare row for ClickHouse insertion
            row = (
                solar_data.get('panel_id', ''),
                solar_data.get('location_id', ''),
                solar_data.get('location_name', ''),
                float(solar_data.get('latitude', 0.0)),
                float(solar_data.get('longitude', 0.0)),
                int(solar_data.get('timezone', 0)),
                float(solar_data.get('power_output', 0.0)),
                solar_data.get('unit_power', ''),
                float(solar_data.get('temperature', 0.0)),
                solar_data.get('unit_temp', ''),
                float(solar_data.get('irradiance', 0.0)),
                solar_data.get('unit_irradiance', ''),
                float(solar_data.get('voltage', 0.0)),
                solar_data.get('unit_voltage', ''),
                float(solar_data.get('current', 0.0)),
                solar_data.get('unit_current', ''),
                solar_data.get('inverter_status', ''),
                int(solar_data.get('timestamp', 0)),
                message_datetime,
                int(message.get('offset', 0)),
                int(message.get('partition', 0))
            )
            
            return row
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(f"Message: {message}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar data to ClickHouse.
        
        This method processes each message in the batch and inserts the data
        into the ClickHouse table.
        """
        if not self._table_created:
            self._create_table_if_not_exists()
        
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_data')
        attempts_remaining = 3
        
        # Process messages and prepare data for insertion
        rows = []
        for item in batch:
            try:
                row = self._process_message(item.value)
                rows.append(row)
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
                continue
        
        if not rows:
            logger.warning("No valid rows to insert")
            return
        
        # Retry logic for database insertion
        while attempts_remaining > 0:
            try:
                insert_sql = f"""
                INSERT INTO {table_name} (
                    panel_id, location_id, location_name, latitude, longitude, timezone,
                    power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                    voltage, unit_voltage, current, unit_current, inverter_status, timestamp,
                    message_datetime, kafka_offset, kafka_partition
                ) VALUES
                """
                
                self._client.execute(insert_sql, rows)
                logger.info(f"Successfully inserted {len(rows)} rows into {table_name}")
                return
                
            except Exception as e:
                attempts_remaining -= 1
                logger.error(f"Error inserting data into ClickHouse: {e}")
                
                if "Connection" in str(e) or "Network" in str(e):
                    # Connection error - retry after short wait
                    if attempts_remaining > 0:
                        time.sleep(3)
                        continue
                elif "timeout" in str(e).lower():
                    # Timeout error - use backpressure
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # Other errors - don't retry
                    break
        
        raise Exception(f"Failed to write to ClickHouse after retries")


def main():
    """Set up our ClickHouse sink application for solar panel data."""

    # Setup necessary objects
    app = Application(
        consumer_group="solar_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create ClickHouse sink
    clickhouse_sink = ClickHouseSink()
    
    # Get input topic from environment variable
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Add debugging to show raw message structure
    def debug_message(row):
        print(f"Raw message: {row}")
        return row

    # Process and transform data before sinking
    sdf = sdf.apply(debug_message)
    
    # Sink data to ClickHouse
    sdf.sink(clickhouse_sink)

    # Run the application - limit to 10 messages for testing
    logger.info("Starting ClickHouse sink application...")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()