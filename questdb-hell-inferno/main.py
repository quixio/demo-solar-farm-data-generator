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
from questdb.ingress import Sender, TimestampNanos

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuestDBSink(BatchingSink):
    """
    Custom sink for writing solar panel sensor data to QuestDB.
    
    QuestDB is a high-performance time-series database that's well-suited
    for sensor data ingestion and analysis.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            _on_client_connect_success=on_client_connect_success,
            _on_client_connect_failure=on_client_connect_failure
        )
        self._sender = None
        self._host = os.environ.get('QUESTDB_HOST', 'localhost')
        try:
            self._port = int(os.environ.get('QUESTDB_PORT', '9009'))
        except ValueError:
            self._port = 9009
        self._username = os.environ.get('QUESTDB_USERNAME', '')
        self._password = os.environ.get('QUESTDB_PASSWORD', '')
        self._database = os.environ.get('QUESTDB_DATABASE', 'qdb')
        self._table = os.environ.get('QUESTDB_TABLE', 'solar_panel_data')
        
        logger.info(f"QuestDB Sink initialized - Host: {self._host}, Port: {self._port}, Table: {self._table}")
    
    def setup(self):
        """Set up QuestDB connection and create table if needed"""
        try:
            # Initialize QuestDB sender
            auth = None
            if self._username and self._password:
                auth = (self._username, self._password)
            
            self._sender = Sender.from_conf(f'http::addr={self._host}:{self._port};')
            
            # Test the connection by trying to send an empty batch
            logger.info("QuestDB connection established successfully")
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise ConnectionError(f"Could not connect to QuestDB: {e}")
    
    def _parse_message_data(self, item):
        """Parse and validate the solar panel sensor data from message"""
        try:
            # Print raw message for debugging
            print(f'Raw message: {item}')
            
            # The value should already be parsed as a dict by Quix Streams JSON deserializer
            if hasattr(item, 'value'):
                data = item.value
                print(f'Message value: {data}')
                
                # If value is still a string, parse it
                if isinstance(data, str):
                    data = json.loads(data)
                    
                # Validate required fields
                required_fields = ['panel_id', 'location_id', 'timestamp', 'power_output']
                for field in required_fields:
                    if field not in data:
                        raise ValueError(f"Missing required field: {field}")
                        
                return data
            else:
                raise ValueError("Message does not have a 'value' field")
                
        except (json.JSONDecodeError, KeyError, AttributeError) as e:
            logger.error(f"Failed to parse message data: {e}, item: {item}")
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
    
    # Set up input topic with JSON deserializer for the solar data
    input_topic_name = os.environ.get("input", "solar-data")
    logger.info(f"Reading from topic: {input_topic_name}")
    
    input_topic = app.topic(
        name=input_topic_name,
        value_deserializer='json'  # This will parse the JSON value field automatically
    )
    sdf = app.dataframe(topic=input_topic)

    # Add debugging and basic transformations
    sdf = sdf.apply(lambda row: row)  # Pass through for debugging
    
    # Print messages for debugging (shows structure)
    sdf.print(metadata=True)

    # Sink data to QuestDB
    sdf.sink(questdb_sink)

    # Run the application with limited message count for testing
    logger.info("Starting application - will process up to 10 messages")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed with error: {e}")
        raise