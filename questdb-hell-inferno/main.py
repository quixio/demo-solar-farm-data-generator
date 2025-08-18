# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import sys
import time
import json
import logging
from datetime import datetime
from questdb.ingress import Sender, TimestampNanos

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Log startup information
logger.info(f"Starting with log level: {log_level}")
logger.info(f"Python version: {sys.version}")
logger.info(f"Working directory: {os.getcwd()}")


class QuestDBSink(BatchingSink):
    """
    Custom sink for writing solar panel sensor data to QuestDB.
    
    QuestDB is a high-performance time-series database that's well-suited
    for sensor data ingestion and analysis.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__()
        self._sender = None
        self._on_client_connect_success = on_client_connect_success
        self._on_client_connect_failure = on_client_connect_failure
        self._host = os.environ.get('QUESTDB_HOST', 'localhost')
        try:
            self._port = int(os.environ.get('QUESTDB_PORT', '8812'))
        except ValueError:
            self._port = 8812
        self._username = os.environ.get('QUESTDB_USERNAME', '').strip()
        # Handle both direct password and secret reference, with multiple fallback patterns
        password_keys = ['QUESTDB_PASSWORD', 'QUESTDB_PW', 'questdb_password', 'questdb_pw']
        self._password = ''
        for key in password_keys:
            if os.environ.get(key):
                self._password = os.environ.get(key).strip()
                break
        
        self._database = os.environ.get('QUESTDB_DATABASE', 'qdb').strip()
        self._table = os.environ.get('QUESTDB_TABLE', 'solar_panel_data').strip()
        
        logger.info(f"QuestDB Sink initialized - Host: {self._host}, Port: {self._port}, Table: {self._table}")
        logger.info(f"QuestDB configuration - Database: {self._database}, Username: {'***' if self._username else 'None'}, Password: {'***' if self._password else 'None'}")
        
        # Debug environment variables
        env_vars = ['QUESTDB_HOST', 'QUESTDB_PORT', 'QUESTDB_USERNAME', 'QUESTDB_PASSWORD', 'QUESTDB_PW', 'QUESTDB_DATABASE', 'QUESTDB_TABLE']
        logger.debug("Environment variables:")
        for var in env_vars:
            value = os.environ.get(var)
            if value is not None:
                if 'PASSWORD' in var or 'PW' in var:
                    logger.debug(f"  {var}=***")
                else:
                    logger.debug(f"  {var}={value}")
            else:
                logger.debug(f"  {var}=<not set>")
    
    def setup(self):
        """Set up QuestDB connection and create table if needed"""
        max_retries = 5
        retry_delay = 5  # seconds
        
        # Try different connection methods
        connection_methods = []
        
        if self._username and self._password:
            # Try authenticated HTTP connection
            connection_methods.append(
                (f"http::addr={self._host}:{self._port};username={self._username};password={self._password};", "HTTP with auth")
            )
            # Try authenticated TCP connection (port 9009 is default for ILP)
            connection_methods.append(
                (f"tcp::addr={self._host}:9009;username={self._username};password={self._password};", "TCP with auth")
            )
        else:
            # Try simple HTTP connection
            connection_methods.append(
                (f"http::addr={self._host}:{self._port};", "HTTP without auth")
            )
            # Try simple TCP connection
            connection_methods.append(
                (f"tcp::addr={self._host}:9009;", "TCP without auth")
            )
        
        for method_attempt, (config_string, method_name) in enumerate(connection_methods):
            for attempt in range(max_retries):
                try:
                    logger.info(f"Connecting to QuestDB using {method_name} (method {method_attempt + 1}/{len(connection_methods)}, attempt {attempt + 1}/{max_retries})")
                    
                    # Create sender with timeout settings
                    self._sender = Sender.from_conf(config_string)
                    
                    # Test the connection with a simple write and flush
                    with self._sender:
                        # Test with a simple heartbeat write
                        self._sender.row(
                            'health_check',
                            symbols={'source': 'quix_sink'},
                            columns={'status': 'connecting'},
                            at=TimestampNanos.now()
                        )
                        self._sender.flush()
                    
                    logger.info(f"QuestDB connection established successfully using {method_name}")
                    
                    if self._on_client_connect_success:
                        self._on_client_connect_success()
                    
                    return  # Success, exit all loops
                    
                except Exception as e:
                    logger.error(f"Failed to connect using {method_name} (attempt {attempt + 1}/{max_retries}): {e}")
                    
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.5, 30)  # Cap at 30 seconds
        
        # All methods and attempts failed
        if self._on_client_connect_failure:
            self._on_client_connect_failure(Exception("All connection methods failed"))
        raise ConnectionError(f"Could not connect to QuestDB after trying {len(connection_methods)} methods with {max_retries} attempts each")
    
    def _parse_message_data(self, item):
        """Parse and validate the solar panel sensor data from message"""
        try:
            # Print raw message for debugging (truncated)
            logger.debug(f'Raw message type: {type(item)}, content: {str(item)[:200]}...')
            
            # Handle the message structure based on the Kafka topic schema
            # The message structure contains a 'value' field that contains the actual solar panel data
            data = None
            
            # Check if it's a dictionary with value field (full Kafka message structure)
            if isinstance(item, dict) and 'value' in item:
                data = item['value']
                logger.debug(f'Message value from dict: {str(data)[:200]}...')
                
                # If value is still a string, parse it as JSON
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse value as JSON: {e}")
                        logger.error(f"Raw value: {data}")
                        raise
            # Check if item has a value attribute (Quix Streams message object)
            elif hasattr(item, 'value'):
                data = item.value
                logger.debug(f'Message value from attr: {str(data)[:200]}...')
                
                # If value is still a string, parse it as JSON
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse value as JSON: {e}")
                        logger.error(f"Raw value: {data}")
                        raise
            # If it's already the parsed data
            elif isinstance(item, dict):
                data = item
                logger.debug(f'Using message as data directly: {str(data)[:200]}...')
            else:
                logger.error(f"Cannot extract data from message format. Item type: {type(item)}, attributes: {dir(item)}")
                raise ValueError("Cannot extract data from message format")
            
            # Additional validation for data structure
            if not isinstance(data, dict):
                raise ValueError(f"Parsed data is not a dictionary: {type(data)}")
                    
            # Validate required fields with better error messages
            required_fields = ['panel_id', 'location_id', 'timestamp', 'power_output']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                logger.error(f"Missing required fields: {missing_fields}")
                logger.error(f"Available fields: {list(data.keys())}")
                raise ValueError(f"Missing required fields: {missing_fields}")
                        
            return data
                
        except (json.JSONDecodeError, KeyError, AttributeError, TypeError) as e:
            logger.error(f"Failed to parse message data: {e}")
            logger.error(f"Item type: {type(item)}")
            logger.error(f"Item content: {str(item)[:500] if len(str(item)) > 500 else item}")
            raise ValueError(f"Invalid message format: {e}")
    
    def _convert_to_questdb_record(self, data):
        """Convert parsed solar data to QuestDB record format"""
        try:
            # Convert timestamp - the schema shows it's a large integer (possibly nanoseconds)
            timestamp_ns = int(data.get('timestamp', 0))
            
            # If timestamp is in microseconds or milliseconds, convert to nanoseconds
            # Check the magnitude to determine the unit
            if timestamp_ns < 1e12:  # Less than 1 trillion, likely seconds
                timestamp_ns *= 1_000_000_000
            elif timestamp_ns < 1e15:  # Less than 1 quadrillion, likely milliseconds
                timestamp_ns *= 1_000_000
            elif timestamp_ns < 1e18:  # Less than 1 quintillion, likely microseconds
                timestamp_ns *= 1_000
            # Otherwise assume it's already in nanoseconds
            
            record = {
                'panel_id': str(data.get('panel_id', '')),
                'location_id': str(data.get('location_id', '')),
                'location_name': str(data.get('location_name', '')),
                'latitude': float(data.get('latitude', 0.0)),
                'longitude': float(data.get('longitude', 0.0)),
                'timezone_offset': int(data.get('timezone', 0)),
                'power_output': float(data.get('power_output', 0.0)),
                'unit_power': str(data.get('unit_power', 'W')),
                'temperature': float(data.get('temperature', 0.0)),
                'unit_temp': str(data.get('unit_temp', 'C')),
                'irradiance': float(data.get('irradiance', 0.0)),
                'unit_irradiance': str(data.get('unit_irradiance', 'W/m²')),
                'voltage': float(data.get('voltage', 0.0)),
                'unit_voltage': str(data.get('unit_voltage', 'V')),
                'current': float(data.get('current', 0.0)),
                'unit_current': str(data.get('unit_current', 'A')),
                'inverter_status': str(data.get('inverter_status', 'UNKNOWN')),
                'timestamp_ns': timestamp_ns
            }
            
            return record
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to convert data to QuestDB record: {e}")
            raise
    
    def _write_to_questdb(self, records):
        """Write records to QuestDB using the sender"""
        try:
            # Re-establish connection if needed
            if self._sender is None:
                logger.warning("Sender is None, attempting to reconnect...")
                self.setup()
            
            with self._sender:
                for record in records:
                    # Build the row with measurements and tags
                    self._sender.row(
                        self._table,
                        symbols={
                            'panel_id': record['panel_id'],
                            'location_id': record['location_id'],
                            'location_name': record['location_name'],
                            'inverter_status': record['inverter_status']
                        },
                        columns={
                            'latitude': record['latitude'],
                            'longitude': record['longitude'],
                            'timezone_offset': record['timezone_offset'],
                            'power_output': record['power_output'],
                            'unit_power': record['unit_power'],
                            'temperature': record['temperature'],
                            'unit_temp': record['unit_temp'],
                            'irradiance': record['irradiance'],
                            'unit_irradiance': record['unit_irradiance'],
                            'voltage': record['voltage'],
                            'unit_voltage': record['unit_voltage'],
                            'current': record['current'],
                            'unit_current': record['unit_current']
                        },
                        at=TimestampNanos(record['timestamp_ns'])
                    )
                    
                # Flush all rows
                self._sender.flush()
                
            logger.info(f"Successfully wrote {len(records)} records to QuestDB table: {self._table}")
            
        except Exception as e:
            logger.error(f"Failed to write to QuestDB: {e}")
            # Log the records that failed to be written for debugging
            logger.error(f"Failed records sample: {records[0] if records else 'No records'}")
            raise
    
    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel sensor data to QuestDB.
        
        This method processes messages from the Kafka topic, parses the solar
        panel data, and writes it to QuestDB with proper error handling.
        """
        attempts_remaining = 3
        
        # Parse and convert all messages in the batch
        try:
            records = []
            for item in batch:
                try:
                    parsed_data = self._parse_message_data(item)
                    record = self._convert_to_questdb_record(parsed_data)
                    records.append(record)
                except Exception as e:
                    logger.error(f"Failed to process message: {e}, skipping...")
                    # Continue processing other messages in the batch
                    continue
                    
            if not records:
                logger.warning("No valid records to write in this batch")
                return
                
            logger.info(f"Processing batch with {len(records)} records")
            
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            raise
        
        # Write to QuestDB with retry logic
        while attempts_remaining:
            try:
                return self._write_to_questdb(records)
            except ConnectionError as e:
                # Connection failed, retry with backoff
                logger.warning(f"Connection error, retrying... ({attempts_remaining} attempts left): {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except TimeoutError as e:
                # Server timeout, use backpressure
                logger.warning(f"Timeout error, applying backpressure: {e}")
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            except Exception as e:
                logger.error(f"Unexpected error writing to QuestDB: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                else:
                    raise
                    
        raise Exception("Failed to write to QuestDB after all retry attempts")


def main():
    """ Set up the Quix Streams Application for QuestDB solar data sink. """

    logger.info("Starting QuestDB Solar Panel Data Sink Application")
    
    # Setup Application with proper configuration for solar data processing
    app = Application(
        consumer_group="questdb_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize QuestDB sink with connection callbacks
    def on_connect_success():
        logger.info("Successfully connected to QuestDB")
    
    def on_connect_failure(error):
        logger.error(f"Failed to connect to QuestDB: {error}")
    
    questdb_sink = QuestDBSink(
        on_client_connect_success=on_connect_success,
        on_client_connect_failure=on_connect_failure
    )
    
    # Try to set up the QuestDB connection with retry mechanism
    connection_successful = False
    try:
        questdb_sink.setup()
        connection_successful = True
    except Exception as e:
        logger.error(f"Failed to establish QuestDB connection: {e}")
        logger.info("Application will continue without QuestDB sink (data will be logged only)")
    
    # Set up input topic with JSON deserializer for the solar data
    input_topic_name = os.environ.get("input", "solar-data")
    logger.info(f"Reading from topic: {input_topic_name}")
    
    input_topic = app.topic(
        name=input_topic_name,
        value_deserializer='json'  # This will parse the outer JSON structure automatically
    )
    sdf = app.dataframe(topic=input_topic)

    # Add debugging and basic transformations
    sdf = sdf.apply(lambda row: row)  # Pass through for debugging
    
    # Print messages for debugging (shows structure)
    sdf.print(metadata=True)

    # Only sink to QuestDB if connection was successful
    if connection_successful:
        logger.info("QuestDB connection successful - data will be written to database")
        sdf.sink(questdb_sink)
    else:
        logger.warning("QuestDB connection failed - data will only be logged to console")
        # Add a function to log data even without DB connection
        def log_data(row):
            logger.info(f"Solar panel data: {row}")
            return row
        sdf = sdf.apply(log_data)

    # Run the application
    logger.info("Starting application")
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed with error: {e}")
        raise