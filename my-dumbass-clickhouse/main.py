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

# Try to import ClickHouse specific exceptions, fall back if not available
try:
    from clickhouse_connect.driver.exceptions import ClickHouseError
except ImportError:
    # Fallback for different versions of clickhouse-connect
    ClickHouseError = Exception

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
            
            # Log connection details (mask password)
            logger.info(f"ClickHouse connection details: username={username}, database={database}, password={'***' if password else '(empty)'}")
            secure = os.environ.get('CLICKHOUSE_SECURE', 'false').lower() == 'true'
            verify = os.environ.get('CLICKHOUSE_VERIFY', 'false').lower() == 'true'
            
            logger.info(f"Connecting to ClickHouse at {host}:{port}")
            
            # Create ClickHouse client with better error handling
            logger.info(f"ClickHouse connection settings: host={host}, port={port}, database={database}, secure={secure}, verify={verify}")
            self._client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure,  # Use environment variable
                verify=verify,  # Use environment variable  
                connect_timeout=30,  # 30 second connection timeout
                send_receive_timeout=60  # 60 second send/receive timeout
            )
            
            # Test connection and authentication
            self._client.ping()
            logger.info("ClickHouse connection established successfully")
            
            # Test basic query to verify authentication and permissions
            try:
                version_result = self._client.query("SELECT version()")
                logger.info(f"ClickHouse version: {version_result.result_rows[0][0] if version_result.result_rows else 'Unknown'}")
            except Exception as auth_test_error:
                logger.error(f"Authentication or permission test failed: {auth_test_error}")
                raise
            
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
        
        The message can be in various formats:
        1. Direct solar panel data (dict with panel_id, location_id, etc.)
        2. QuixStreams message with 'value' containing solar data directly
        3. Kafka envelope with 'value' field containing JSON string
        """
        try:
            # Debug: Print the raw message structure
            logger.info(f"Raw message: {message}")
            
            solar_data = None
            message_timestamp = datetime.now()
            kafka_partition = 0
            kafka_offset = 0
            
            # Check if this is already direct solar panel data
            if isinstance(message, dict) and 'panel_id' in message:
                # This is direct solar panel data
                solar_data = message
                
            # Check if this is a QuixStreams message structure with 'value'
            elif isinstance(message, dict) and 'value' in message:
                value_data = message['value']
                
                # If value contains direct solar panel data
                if isinstance(value_data, dict) and 'panel_id' in value_data:
                    solar_data = value_data
                    # Extract metadata if available
                    if 'timestamp' in message and message['timestamp']:
                        message_timestamp = datetime.fromtimestamp(message['timestamp'] / 1000)
                    kafka_partition = message.get('partition', 0)
                    kafka_offset = message.get('offset', 0)
                    
                # If value is a JSON string that needs parsing
                elif isinstance(value_data, str):
                    try:
                        solar_data = json.loads(value_data)
                        # Parse message timestamp if available
                        if 'dateTime' in message:
                            message_timestamp = datetime.fromisoformat(message['dateTime'].replace('Z', '+00:00'))
                        kafka_partition = message.get('partition', 0)
                        kafka_offset = message.get('offset', 0)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse JSON from value field: {value_data}")
                        return None
                else:
                    logger.error(f"Unexpected value data type: {type(value_data)}")
                    return None
            else:
                logger.error(f"Unexpected message structure: {message}")
                return None
            
            # If we successfully extracted solar data, process it
            if solar_data and isinstance(solar_data, dict) and 'panel_id' in solar_data:
                # Convert timestamp from nanoseconds to datetime
                sensor_timestamp = datetime.fromtimestamp(solar_data['timestamp'] / 1e9)
                
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
                    'kafka_partition': kafka_partition,
                    'kafka_offset': kafka_offset
                }
                
                return parsed_data
            else:
                logger.error(f"No valid solar data found in message: {message}")
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
            # QuixStreams provides the message value in item.value
            # Additional metadata is available in item.key, item.timestamp, etc.
            message_data = item.value
            
            # Add metadata from the QuixStreams message item if available
            if hasattr(item, 'key') or hasattr(item, 'timestamp'):
                metadata_enhanced_message = {
                    'value': message_data,
                    'key': getattr(item, 'key', None),
                    'timestamp': getattr(item, 'timestamp', None),
                    'headers': getattr(item, 'headers', None),
                    'partition': getattr(item, 'partition', 0),
                    'offset': getattr(item, 'offset', 0)
                }
                parsed_item = self._parse_solar_data(metadata_enhanced_message)
            else:
                parsed_item = self._parse_solar_data(message_data)
            
            if parsed_item:
                parsed_data.append(parsed_item)
            else:
                logger.warning("Skipping invalid message")
        
        if not parsed_data:
            logger.warning("No valid data to write in this batch")
            return
        
        logger.info(f"Writing {len(parsed_data)} solar data records to ClickHouse")
        
        # Debug: Log a sample of the data being inserted
        if parsed_data:
            logger.info(f"Sample data record: {parsed_data[0]}")
        
        while attempts_remaining:
            try:
                # Test the connection before attempting to insert
                self._client.ping()
                logger.info("ClickHouse connection verified")
                
                # Test table access
                try:
                    result = self._client.query(f"SELECT COUNT(*) FROM {self._table_name}")
                    logger.info(f"Table {self._table_name} is accessible, current row count: {result.result_rows[0][0] if result.result_rows else 0}")
                except Exception as table_error:
                    logger.error(f"Table access test failed: {table_error}")
                    # Try to recreate the table
                    self._create_table()
                
                # Prepare the data as a list of tuples for insertion with type conversion
                data_tuples = []
                for item in parsed_data:
                    try:
                        data_tuple = (
                            str(item['panel_id']), str(item['location_id']), str(item['location_name']), 
                            float(item['latitude']), float(item['longitude']), int(item['timezone']),
                            float(item['power_output']), str(item['unit_power']), 
                            float(item['temperature']), str(item['unit_temp']),
                            float(item['irradiance']), str(item['unit_irradiance']), 
                            float(item['voltage']), str(item['unit_voltage']),
                            float(item['current']), str(item['unit_current']), 
                            str(item['inverter_status']), 
                            item['sensor_timestamp'], item['message_timestamp'], 
                            int(item['kafka_partition']), int(item['kafka_offset'])
                        )
                        data_tuples.append(data_tuple)
                    except (ValueError, KeyError) as conv_error:
                        logger.error(f"Data conversion error for item {item}: {conv_error}")
                        continue
                
                if not data_tuples:
                    logger.error("No valid data tuples after conversion")
                    return
                
                logger.info(f"Prepared {len(data_tuples)} data tuples for insertion")
                
                # Insert data into ClickHouse using the standard method
                self._client.insert(
                    self._table_name,
                    data_tuples,
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
                
            except ClickHouseError as e:
                # ClickHouse specific error
                logger.error(f"ClickHouse error: {e}")
                logger.error(f"ClickHouse error code: {getattr(e, 'code', 'N/A')}")
                logger.error(f"ClickHouse error message: {getattr(e, 'message', str(e))}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                    
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
                # Log the full error details for debugging
                logger.error(f"ClickHouse write error: {type(e).__name__}: {e}")
                logger.error(f"Error details: {str(e)}")
                
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