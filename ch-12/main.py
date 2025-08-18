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
from clickhouse_connect.driver.exceptions import ClickHouseError

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSolarDataSink(BatchingSink):
    """
    A custom sink for writing solar panel sensor data to ClickHouse database.
    
    This sink processes messages containing solar panel data from a Kafka topic
    and writes them to a ClickHouse table with proper schema mapping and
    timestamp conversion.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__()
        self._client = None
        self._table_name = "solar_panel_data"
        self._database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
        self._on_client_connect_success = on_client_connect_success
        self._on_client_connect_failure = on_client_connect_failure
        
    def setup(self):
        """Initialize ClickHouse client and create table if it doesn't exist"""
        try:
            # Parse port safely
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
            except ValueError:
                logger.warning("Invalid CLICKHOUSE_PORT, using default 8123")
                port = 8123
            
            # Initialize ClickHouse client
            self._client = get_client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=port,
                username=os.environ.get('CLICKHOUSE_USERNAME', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                database=self._database,
                secure=os.environ.get('CLICKHOUSE_SECURE', 'false').lower() == 'true'
            )
            
            # Test connection
            self._client.ping()
            logger.info(f"Successfully connected to ClickHouse at {os.environ.get('CLICKHOUSE_HOST', 'localhost')}:{port}")
            
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
        """Create the solar panel data table with appropriate schema"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._database}.{self._table_name} (
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
            kafka_offset UInt64,
            kafka_partition UInt32
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, sensor_timestamp)
        """
        
        try:
            self._client.command(create_table_sql)
            logger.info(f"Table {self._database}.{self._table_name} created or already exists")
        except ClickHouseError as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_message(self, item):
        """Parse the Kafka message and extract solar panel data"""
        try:
            # Print raw message for debugging
            logger.info(f"Raw message keys: {list(item.value.keys()) if isinstance(item.value, dict) else type(item.value)}")
            
            # The message structure has the actual data in the 'value' field as a JSON string
            message_data = item.value
            
            # Ensure message_data is a dictionary
            if not isinstance(message_data, dict):
                logger.error(f"Expected dict, got {type(message_data)}: {message_data}")
                return None
            
            # Parse the nested JSON in the value field
            value_field = message_data.get('value')
            if isinstance(value_field, str):
                try:
                    solar_data = json.loads(value_field)
                except json.JSONDecodeError as je:
                    logger.error(f"Failed to parse JSON in value field: {je}")
                    logger.error(f"Value field content: {value_field}")
                    return None
            elif isinstance(value_field, dict):
                # If value is already parsed, use it directly
                solar_data = value_field
            else:
                logger.error(f"Unexpected value field type: {type(value_field)}")
                return None
            
            # Convert epoch timestamp to datetime (handle nanosecond precision)
            sensor_timestamp_ns = solar_data.get('timestamp', 0)
            if sensor_timestamp_ns and sensor_timestamp_ns > 0:
                try:
                    # Convert from nanoseconds to seconds
                    sensor_timestamp = datetime.fromtimestamp(sensor_timestamp_ns / 1e9)
                except (ValueError, OverflowError) as e:
                    logger.warning(f"Invalid timestamp {sensor_timestamp_ns}, using current time: {e}")
                    sensor_timestamp = datetime.now()
            else:
                sensor_timestamp = datetime.now()
            
            # Parse message timestamp
            message_timestamp_str = message_data.get('dateTime', '')
            if message_timestamp_str:
                try:
                    message_timestamp = datetime.fromisoformat(message_timestamp_str.replace('Z', '+00:00'))
                except ValueError as e:
                    logger.warning(f"Invalid dateTime format {message_timestamp_str}, using current time: {e}")
                    message_timestamp = datetime.now()
            else:
                message_timestamp = datetime.now()
            
            # Helper function to safely convert to float
            def safe_float(value, default=0.0):
                try:
                    return float(value) if value is not None else default
                except (ValueError, TypeError):
                    return default
            
            # Helper function to safely convert to int
            def safe_int(value, default=0):
                try:
                    return int(value) if value is not None else default
                except (ValueError, TypeError):
                    return default
            
            # Map the data to table schema with safe type conversions
            record = {
                'panel_id': str(solar_data.get('panel_id', '')),
                'location_id': str(solar_data.get('location_id', '')),
                'location_name': str(solar_data.get('location_name', '')),
                'latitude': safe_float(solar_data.get('latitude')),
                'longitude': safe_float(solar_data.get('longitude')),
                'timezone': safe_int(solar_data.get('timezone')),
                'power_output': safe_float(solar_data.get('power_output')),
                'unit_power': str(solar_data.get('unit_power', 'W')),
                'temperature': safe_float(solar_data.get('temperature')),
                'unit_temp': str(solar_data.get('unit_temp', 'C')),
                'irradiance': safe_float(solar_data.get('irradiance')),
                'unit_irradiance': str(solar_data.get('unit_irradiance', 'W/m²')),
                'voltage': safe_float(solar_data.get('voltage')),
                'unit_voltage': str(solar_data.get('unit_voltage', 'V')),
                'current': safe_float(solar_data.get('current')),
                'unit_current': str(solar_data.get('unit_current', 'A')),
                'inverter_status': str(solar_data.get('inverter_status', '')),
                'sensor_timestamp': sensor_timestamp,
                'message_timestamp': message_timestamp,
                'kafka_offset': item.offset,
                'kafka_partition': safe_int(message_data.get('partition'))
            }
            
            logger.info(f"Parsed solar data for panel {record['panel_id']}: power={record['power_output']}W, temp={record['temperature']}C")
            return record
            
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            logger.error(f"Message content: {item.value}")
            return None

    def write(self, batch: SinkBatch):
        """Write batch of solar panel data to ClickHouse"""
        attempts_remaining = 3
        
        # Parse all messages in the batch
        records = []
        for item in batch:
            record = self._parse_message(item)
            if record:
                records.append(record)
        
        if not records:
            logger.warning("No valid records to write in batch")
            return
            
        logger.info(f"Writing {len(records)} solar panel records to ClickHouse")
        
        while attempts_remaining:
            try:
                # Insert data into ClickHouse
                self._client.insert(
                    f"{self._database}.{self._table_name}",
                    records,
                    column_names=[
                        'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
                        'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
                        'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current',
                        'unit_current', 'inverter_status', 'sensor_timestamp', 'message_timestamp',
                        'kafka_offset', 'kafka_partition'
                    ]
                )
                logger.info(f"Successfully wrote {len(records)} records to ClickHouse")
                return
                
            except ClickHouseError as e:
                logger.error(f"ClickHouse error while writing batch: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                else:
                    # If it's a temporary issue, use backpressure
                    if "timeout" in str(e).lower() or "too many" in str(e).lower():
                        raise SinkBackpressureError(
                            retry_after=30.0,
                            topic=batch.topic,
                            partition=batch.partition,
                        )
            except Exception as e:
                logger.error(f"Unexpected error while writing to ClickHouse: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
        
        raise Exception(f"Failed to write batch to ClickHouse after 3 attempts")


def main():
    """Set up our Application for sinking solar panel data to ClickHouse."""
    
    try:
        # Get input topic name from environment variable with fallback
        input_topic_name = os.environ.get("input", "solar-data")
        logger.info(f"Using input topic: {input_topic_name}")
        
        # Setup necessary objects
        app = Application(
            consumer_group="solar_data_clickhouse_sink",
            auto_create_topics=True,
            auto_offset_reset="earliest",
            # Configure batching for optimal performance
            commit_interval=5.0,
            commit_every=100,
        )
        
        # Initialize the ClickHouse sink
        clickhouse_sink = ClickHouseSolarDataSink()
        
        # Set up the input topic (using environment variable)
        input_topic = app.topic(
            name=input_topic_name,
            value_deserializer='json'  # Ensure JSON deserialization
        )
        
        sdf = app.dataframe(topic=input_topic)

        # Add debug logging to show raw messages
        sdf = sdf.apply(lambda row: row).print(metadata=True)

        # Sink the data to ClickHouse
        sdf.sink(clickhouse_sink)

        # Run the application with limited message count for testing
        logger.info("Starting solar data to ClickHouse sink application...")
        app.run(count=10, timeout=20)
        
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()