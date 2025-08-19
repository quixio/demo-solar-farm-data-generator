# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import logging
from questdb.ingress import Sender, TimestampNanos
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuestDBSolarSink(BatchingSink):
    """
    A QuestDB sink that writes solar panel sensor data to QuestDB.
    
    This sink processes solar panel data messages and inserts them into QuestDB
    using the official QuestDB Python client with ILP (InfluxDB Line Protocol) over HTTP.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._questdb_config = None
        self._table_name = "solar_panel_data"
        self._setup_config()
        
    def _setup_config(self):
        """Setup QuestDB connection configuration"""
        host = os.environ.get('QDB_HOST', 'localhost')
        try:
            port = int(os.environ.get('QDB_PORT', '9000'))
        except ValueError:
            port = 9000
            
        token = os.environ.get('QDB_TOKEN')
        
        # Build configuration string for QuestDB client
        self._questdb_config = f"http::addr={host}:{port};"
        if token:
            self._questdb_config += f"token={token};"
            
        logger.info(f"QuestDB configuration: host={host}, port={port}, token={'***' if token else 'None'}")
        
    def setup(self):
        """Setup method to test QuestDB connection and create table if needed"""
        try:
            with Sender.from_conf(self._questdb_config) as sender:
                logger.info("Successfully connected to QuestDB")
                
                # Create table by inserting a dummy row and then removing it
                # QuestDB creates tables automatically with the first insert
                current_time = TimestampNanos.now()
                sender.row(
                    self._table_name,
                    symbols={
                        'panel_id': 'SETUP_TEST', 
                        'location_id': 'TEST',
                        'location_name': 'Test Location',
                        'inverter_status': 'OK'
                    },
                    columns={
                        'power_output': 0.0,
                        'temperature': 0.0, 
                        'irradiance': 0.0,
                        'voltage': 0.0,
                        'current': 0.0,
                        'latitude': 0.0,
                        'longitude': 0.0,
                        'timezone': 0
                    },
                    at=current_time
                )
                sender.flush()
                logger.info(f"QuestDB table '{self._table_name}' created/verified successfully")
                
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            raise ConnectionError(f"Cannot connect to QuestDB: {e}")

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data messages to QuestDB.
        
        Each message contains solar panel sensor data that needs to be parsed
        and inserted into QuestDB using ILP over HTTP.
        """
        attempts_remaining = 3
        batch_size = 0
        
        # Count batch size for logging (avoiding len() on SinkBatch)
        for _ in batch:
            batch_size += 1
            
        logger.info(f"Processing batch with {batch_size} messages")
        
        while attempts_remaining:
            try:
                with Sender.from_conf(self._questdb_config) as sender:
                    rows_written = 0
                    
                    for item in batch:
                        try:
                            # Debug: Show raw message structure
                            logger.debug(f"Raw message: {item.value}")
                            
                            # Parse the message - the actual solar data is in the 'value' field as a JSON string
                            if isinstance(item.value, dict) and 'value' in item.value:
                                # The solar data is in the 'value' field as a JSON string
                                solar_data_json = item.value['value']
                                if isinstance(solar_data_json, str):
                                    solar_data = json.loads(solar_data_json)
                                else:
                                    solar_data = solar_data_json
                            else:
                                # Assume the entire message is the solar data
                                solar_data = item.value
                            
                            # Extract data fields with safe access
                            panel_id = solar_data.get('panel_id', 'UNKNOWN')
                            location_id = solar_data.get('location_id', 'UNKNOWN')
                            location_name = solar_data.get('location_name', 'Unknown Location')
                            
                            # Convert timestamp to proper format
                            timestamp = solar_data.get('timestamp')
                            if timestamp:
                                # Convert from nanoseconds to TimestampNanos
                                timestamp_ns = TimestampNanos(timestamp)
                            else:
                                timestamp_ns = TimestampNanos.now()
                            
                            # Insert row into QuestDB
                            sender.row(
                                self._table_name,
                                symbols={
                                    'panel_id': str(panel_id),
                                    'location_id': str(location_id), 
                                    'location_name': str(location_name),
                                    'inverter_status': str(solar_data.get('inverter_status', 'UNKNOWN')),
                                    'unit_power': str(solar_data.get('unit_power', 'W')),
                                    'unit_temp': str(solar_data.get('unit_temp', 'C')),
                                    'unit_irradiance': str(solar_data.get('unit_irradiance', 'W/m²')),
                                    'unit_voltage': str(solar_data.get('unit_voltage', 'V')),
                                    'unit_current': str(solar_data.get('unit_current', 'A'))
                                },
                                columns={
                                    'power_output': float(solar_data.get('power_output', 0.0)),
                                    'temperature': float(solar_data.get('temperature', 0.0)),
                                    'irradiance': float(solar_data.get('irradiance', 0.0)),
                                    'voltage': float(solar_data.get('voltage', 0.0)),
                                    'current': float(solar_data.get('current', 0.0)),
                                    'latitude': float(solar_data.get('latitude', 0.0)),
                                    'longitude': float(solar_data.get('longitude', 0.0)),
                                    'timezone': int(solar_data.get('timezone', 0))
                                },
                                at=timestamp_ns
                            )
                            rows_written += 1
                            
                        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                            logger.error(f"Error processing message: {e}, raw message: {item.value}")
                            continue
                    
                    # Flush all rows to QuestDB
                    sender.flush()
                    logger.info(f"Successfully wrote {rows_written} rows to QuestDB table '{self._table_name}'")
                    return
                    
            except ConnectionError as e:
                logger.warning(f"Connection error writing to QuestDB: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except TimeoutError as e:
                logger.warning(f"Timeout error writing to QuestDB: {e}")
                # Use QuestDB-specific backpressure handling
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
                    
        raise Exception("Failed to write batch to QuestDB after multiple attempts")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="questdb_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create QuestDB sink
    questdb_sink = QuestDBSolarSink()
    
    # Setup the input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Add debug logging for incoming messages
    def log_message(row):
        print(f'Raw message received: {row}')
        return row
    
    sdf = sdf.apply(log_message)

    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(questdb_sink)

    # With our pipeline defined, now run the Application for testing
    logger.info("Starting QuestDB Solar Sink application...")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()