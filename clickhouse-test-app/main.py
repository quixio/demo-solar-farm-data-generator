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
from clickhouse_connect import get_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class ClickHouseSink(BatchingSink):
    """
    ClickHouse sink for storing solar panel sensor data.
    
    This sink receives solar panel data from Kafka and writes it to a ClickHouse database.
    It handles JSON parsing, data transformation, and batch writing with proper error handling.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_name = "solar_panel_data"
        
    def setup(self):
        """Initialize ClickHouse client and create table if it doesn't exist."""
        try:
            # Get connection parameters from environment variables
            host = os.environ.get("CLICKHOUSE_HOST", "localhost")
            try:
                port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
            except ValueError:
                port = 8123
                
            database = os.environ.get("CLICKHOUSE_DATABASE", "default")
            username = os.environ.get("CLICKHOUSE_USERNAME", "default")
            password = os.environ.get("CLICKHOUSE_PASSWORD", "")
            
            # Create ClickHouse client
            self._client = get_client(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password
            )
            
            logger.info(f"Connected to ClickHouse at {host}:{port}")
            
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
        """Create the solar panel data table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            message_datetime DateTime64(3),
            topic_id String,
            topic_name String,
            stream_id String,
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
            kafka_partition Int32,
            kafka_offset Int64
        ) ENGINE = MergeTree()
        ORDER BY (message_datetime, panel_id)
        """
        
        self._client.command(create_table_sql)
        logger.info(f"Table {self._table_name} is ready")

    def _parse_message(self, message):
        """Parse the Kafka message and extract solar panel data."""
        try:
            # Debug: Print raw message structure
            print(f"Raw message: {message}")
            
            # Extract message metadata
            topic_id = message.get("topicId", "")
            topic_name = message.get("topicName", "")
            stream_id = message.get("streamId", "")
            message_datetime = message.get("dateTime", "")
            partition = message.get("partition", 0)
            offset = message.get("offset", 0)
            
            # Parse the value field which contains the actual solar panel data as JSON string
            value_str = message.get("value", "{}")
            if isinstance(value_str, str):
                solar_data = json.loads(value_str)
            else:
                solar_data = value_str
            
            # Convert ISO datetime to ClickHouse format
            if message_datetime:
                dt = datetime.fromisoformat(message_datetime.replace('Z', '+00:00'))
            else:
                dt = datetime.utcnow()
            
            # Extract solar panel data with safe access
            parsed_data = {
                'message_datetime': dt,
                'topic_id': topic_id,
                'topic_name': topic_name,
                'stream_id': stream_id,
                'panel_id': solar_data.get('panel_id', ''),
                'location_id': solar_data.get('location_id', ''),
                'location_name': solar_data.get('location_name', ''),
                'latitude': float(solar_data.get('latitude', 0.0)),
                'longitude': float(solar_data.get('longitude', 0.0)),
                'timezone': int(solar_data.get('timezone', 0)),
                'power_output': float(solar_data.get('power_output', 0.0)),
                'unit_power': solar_data.get('unit_power', ''),
                'temperature': float(solar_data.get('temperature', 0.0)),
                'unit_temp': solar_data.get('unit_temp', ''),
                'irradiance': float(solar_data.get('irradiance', 0.0)),
                'unit_irradiance': solar_data.get('unit_irradiance', ''),
                'voltage': float(solar_data.get('voltage', 0.0)),
                'unit_voltage': solar_data.get('unit_voltage', ''),
                'current': float(solar_data.get('current', 0.0)),
                'unit_current': solar_data.get('unit_current', ''),
                'inverter_status': solar_data.get('inverter_status', ''),
                'timestamp': int(solar_data.get('timestamp', 0)),
                'kafka_partition': partition,
                'kafka_offset': offset
            }
            
            return parsed_data
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Error parsing message: {e}, message: {message}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing message: {e}, message: {message}")
            return None

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to ClickHouse.
        
        This method processes multiple Kafka messages, parses the solar panel data,
        and performs a batch insert to ClickHouse for optimal performance.
        """
        attempts_remaining = 3
        
        # Parse all messages in the batch
        parsed_data = []
        for item in batch:
            parsed_message = self._parse_message(item.value)
            if parsed_message:
                parsed_data.append(parsed_message)
        
        if not parsed_data:
            logger.warning("No valid data to write in this batch")
            return
        
        logger.info(f"Writing {len(parsed_data)} records to ClickHouse")
        
        while attempts_remaining:
            try:
                # Insert data into ClickHouse
                self._client.insert(self._table_name, parsed_data)
                logger.info(f"Successfully wrote {len(parsed_data)} records to {self._table_name}")
                return
                
            except Exception as e:
                error_msg = str(e).lower()
                
                if "connection" in error_msg or "network" in error_msg:
                    # Connection error - retry with backoff
                    attempts_remaining -= 1
                    if attempts_remaining:
                        logger.warning(f"Connection error, retrying in 3 seconds: {e}")
                        time.sleep(3)
                    else:
                        logger.error(f"Connection failed after all retries: {e}")
                        raise ConnectionError(f"Failed to connect to ClickHouse: {e}")
                        
                elif "timeout" in error_msg or "busy" in error_msg:
                    # Timeout or server busy - use backpressure
                    logger.warning(f"Server busy or timeout, applying backpressure: {e}")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # Other errors - don't retry
                    logger.error(f"Error writing to ClickHouse: {e}")
                    raise
        
        # If we get here, all retries failed
        raise Exception("Error while writing to ClickHouse database after all retries")


def main():
    """ Here we will set up our Application. """
    
    logger.info("Starting ClickHouse Solar Data Sink Application")

    # Setup necessary objects
    app = Application(
        consumer_group="clickhouse_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize ClickHouse sink
    clickhouse_sink = ClickHouseSink()
    
    # Set up input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Debug: Print incoming messages for troubleshooting
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Sink data to ClickHouse
    sdf.sink(clickhouse_sink)

    # Run the application with limited processing for testing
    logger.info("Starting processing (will stop after 10 messages or 20 seconds)")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()