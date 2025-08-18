# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import logging
import clickhouse_connect
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSolarSink(BatchingSink):
    """
    Custom ClickHouse sink for storing solar panel sensor data.
    
    This sink processes solar data messages and stores them in a ClickHouse database.
    It handles the message structure where actual data is nested within the 'value' field as a JSON string.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_name = "solar_panel_data"
        
    def setup(self):
        """Initialize ClickHouse connection and create table if it doesn't exist."""
        try:
            # Get connection parameters from environment variables
            host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
            
            # Handle port conversion safely
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
            except ValueError:
                port = 8123
                
            username = os.environ.get('CLICKHOUSE_USERNAME', 'default')
            password = os.environ.get('CLICKHOUSE_PASSWORD', '')
            database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
            
            logger.info(f"Connecting to ClickHouse at {host}:{port}")
            
            # Create ClickHouse client
            self._client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database
            )
            
            # Test connection
            self._client.ping()
            logger.info("ClickHouse connection established successfully")
            
            # Create table if it doesn't exist
            self._create_table()
            
            # Call success callback
            if hasattr(self, '_on_client_connect_success') and self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            if hasattr(self, '_on_client_connect_failure') and self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def _create_table(self):
        """Create the solar panel data table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
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
            sensor_timestamp DateTime64(3),
            message_timestamp DateTime64(3),
            kafka_partition Int32,
            kafka_offset Int64
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, sensor_timestamp)
        """
        
        try:
            self._client.command(create_table_sql)
            logger.info(f"Table '{self._table_name}' created or verified successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def _parse_solar_data(self, message):
        """
        Parse solar data from the message structure.
        
        Based on the schema analysis, the actual solar data is in the 'value' field
        as a JSON string that needs to be parsed.
        """
        try:
            # Debug: Print the raw message structure
            logger.info(f"Raw message: {message}")
            
            # The message should already be parsed as a dictionary by QuixStreams
            # The actual solar data is in the 'value' field as a JSON string
            if isinstance(message, dict) and 'value' in message:
                # Parse the nested JSON in the value field
                solar_data_str = message['value']
                if isinstance(solar_data_str, str):
                    solar_data = json.loads(solar_data_str)
                else:
                    solar_data = solar_data_str
                
                # Convert timestamp from nanoseconds to datetime
                sensor_timestamp = datetime.fromtimestamp(solar_data['timestamp'] / 1e9)
                
                # Parse message timestamp
                message_timestamp = datetime.fromisoformat(message['dateTime'].replace('Z', '+00:00'))
                
                # Prepare data for ClickHouse insertion
                parsed_data = {
                    'panel_id': solar_data['panel_id'],
                    'location_id': solar_data['location_id'],
                    'location_name': solar_data['location_name'],
                    'latitude': solar_data['latitude'],
                    'longitude': solar_data['longitude'],
                    'timezone': solar_data['timezone'],
                    'power_output': solar_data['power_output'],
                    'unit_power': solar_data['unit_power'],
                    'temperature': solar_data['temperature'],
                    'unit_temp': solar_data['unit_temp'],
                    'irradiance': solar_data['irradiance'],
                    'unit_irradiance': solar_data['unit_irradiance'],
                    'voltage': solar_data['voltage'],
                    'unit_voltage': solar_data['unit_voltage'],
                    'current': solar_data['current'],
                    'unit_current': solar_data['unit_current'],
                    'inverter_status': solar_data['inverter_status'],
                    'sensor_timestamp': sensor_timestamp,
                    'message_timestamp': message_timestamp,
                    'kafka_partition': message.get('partition', 0),
                    'kafka_offset': message.get('offset', 0)
                }
                
                return parsed_data
                
            else:
                logger.error(f"Unexpected message structure: {message}")
                return None
                
        except Exception as e:
            logger.error(f"Error parsing solar data: {e}")
            logger.error(f"Message that failed: {message}")
            return None

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar data to ClickHouse.
        
        Implements retry logic with exponential backoff for connection failures
        and uses backpressure for timeouts.
        """
        attempts_remaining = 3
        
        # Parse all messages in the batch
        parsed_data = []
        for item in batch:
            parsed_item = self._parse_solar_data(item.value)
            if parsed_item:
                parsed_data.append(parsed_item)
            else:
                logger.warning("Skipping invalid message")
        
        if not parsed_data:
            logger.warning("No valid data to write in this batch")
            return
        
        logger.info(f"Writing {len(parsed_data)} solar data records to ClickHouse")
        
        while attempts_remaining:
            try:
                # Insert data into ClickHouse
                self._client.insert(
                    self._table_name,
                    parsed_data,
                    column_names=[
                        'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
                        'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
                        'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage',
                        'current', 'unit_current', 'inverter_status', 'sensor_timestamp',
                        'message_timestamp', 'kafka_partition', 'kafka_offset'
                    ]
                )
                logger.info(f"Successfully wrote {len(parsed_data)} records to ClickHouse")
                return
                
            except ConnectionError as e:
                # Connection failed, try to reconnect
                logger.warning(f"Connection error: {e}. Retrying... ({attempts_remaining} attempts left)")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)  # Wait before retry
                    try:
                        self.setup()  # Re-establish connection
                    except Exception as setup_e:
                        logger.error(f"Failed to re-establish connection: {setup_e}")
                        
            except Exception as e:
                if "timeout" in str(e).lower():
                    # Server timeout, use backpressure
                    logger.warning(f"Timeout error: {e}. Using backpressure...")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # Other error, retry
                    logger.warning(f"Error writing to ClickHouse: {e}. Retrying... ({attempts_remaining} attempts left)")
                    attempts_remaining -= 1
                    if attempts_remaining:
                        time.sleep(3)
        
        # All attempts failed
        raise Exception(f"Failed to write to ClickHouse after all retry attempts")


def main():
    """Set up the Quix Streams Application for processing solar panel data."""
    
    logger.info("Starting Solar Panel ClickHouse Sink Application")
    
    # Setup necessary objects
    app = Application(
        consumer_group="solar_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create ClickHouse sink instance
    clickhouse_sink = ClickHouseSolarSink()
    
    # Get input topic from environment variable
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Process the data - just log and pass through for now
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Sink data to ClickHouse
    sdf.sink(clickhouse_sink)

    logger.info("Pipeline configured, starting to consume messages...")
    
    # Run for testing - process 10 messages then stop
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()