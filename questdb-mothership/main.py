# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
from datetime import datetime
from questdb.ingress import Sender, IngressError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class QuestDBSink(BatchingSink):
    """
    Custom sink for writing solar panel sensor data to QuestDB.
    
    This sink processes solar panel sensor messages and writes them to QuestDB
    using the ILP (InfluxDB Line Protocol) format for high performance ingestion.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = os.environ.get('QUESTDB_HOST', 'localhost')
        
        try:
            self.port = int(os.environ.get('QUESTDB_PORT', '9009'))
        except ValueError:
            self.port = 9009
            
        self.username = os.environ.get('QUESTDB_USERNAME')
        self.password = os.environ.get('QUESTDB_PASSWORD')
        self._sender = None
        
        logger.info(f"QuestDB sink configured for {self.host}:{self.port}")
    
    def setup(self):
        """Setup QuestDB connection and create table if needed."""
        try:
            # Create the ILP sender for high-performance ingestion
            conf = f'http::addr={self.host}:{self.port};'
            if self.username and self.password:
                conf += f'username={self.username};password={self.password};'
            
            self._sender = Sender.from_conf(conf)
            logger.info("QuestDB ILP sender initialized successfully")
            
            # Call success callback if it exists
            if hasattr(self, '_on_client_connect_success') and self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to setup QuestDB connection: {e}")
            # Call failure callback if it exists
            if hasattr(self, '_on_client_connect_failure') and self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise e

    def _write_to_questdb(self, data):
        """Write batched data to QuestDB using ILP format."""
        try:
            for record in data:
                # Parse the JSON value from the message
                if isinstance(record, dict) and 'value' in record:
                    # If value is a string, parse it as JSON
                    if isinstance(record['value'], str):
                        sensor_data = json.loads(record['value'])
                    else:
                        sensor_data = record['value']
                else:
                    # If record is the data itself
                    sensor_data = record
                
                # Convert timestamp from nanoseconds to microseconds for QuestDB
                timestamp_ns = sensor_data.get('timestamp')
                if timestamp_ns:
                    # Convert from nanoseconds to microseconds (QuestDB expects microseconds)
                    timestamp_us = int(timestamp_ns / 1000)
                else:
                    # Use current time if no timestamp
                    timestamp_us = int(time.time() * 1_000_000)
                
                # Write to QuestDB using ILP format
                # Table name: solar_panel_data
                self._sender.row(
                    'solar_panel_data',
                    symbols={
                        'panel_id': sensor_data.get('panel_id', 'unknown'),
                        'location_id': sensor_data.get('location_id', 'unknown'),
                        'location_name': sensor_data.get('location_name', 'unknown'),
                        'inverter_status': sensor_data.get('inverter_status', 'unknown')
                    },
                    columns={
                        'latitude': float(sensor_data.get('latitude', 0.0)),
                        'longitude': float(sensor_data.get('longitude', 0.0)),
                        'timezone': int(sensor_data.get('timezone', 0)),
                        'power_output': float(sensor_data.get('power_output', 0.0)),
                        'temperature': float(sensor_data.get('temperature', 0.0)),
                        'irradiance': float(sensor_data.get('irradiance', 0.0)),
                        'voltage': float(sensor_data.get('voltage', 0.0)),
                        'current': float(sensor_data.get('current', 0.0)),
                        'unit_power': sensor_data.get('unit_power', 'W'),
                        'unit_temp': sensor_data.get('unit_temp', 'C'),
                        'unit_irradiance': sensor_data.get('unit_irradiance', 'W/m²'),
                        'unit_voltage': sensor_data.get('unit_voltage', 'V'),
                        'unit_current': sensor_data.get('unit_current', 'A')
                    },
                    at=timestamp_us
                )
            
            # Flush all pending rows
            self._sender.flush()
            logger.info(f"Successfully wrote {len(data)} records to QuestDB")
            
        except Exception as e:
            logger.error(f"Error writing to QuestDB: {e}")
            raise e

    def write(self, batch: SinkBatch):
        """
        Write batch of solar panel sensor data to QuestDB.
        
        Handles retries and backpressure according to Quix Streams patterns.
        """
        attempts_remaining = 3
        # Extract the actual message data from the batch
        data = []
        for item in batch:
            print(f'Raw message: {item.value}')  # Debug print to show message structure
            data.append(item.value)
        
        while attempts_remaining:
            try:
                return self._write_to_questdb(data)
            except (ConnectionError, IngressError) as e:
                # Handle connection failures with retry logic
                logger.warning(f"Connection error writing to QuestDB: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                else:
                    logger.error("Max connection retry attempts reached")
                    raise e
            except TimeoutError:
                # Handle timeout with backpressure
                logger.warning("Timeout writing to QuestDB, applying backpressure")
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            except Exception as e:
                logger.error(f"Unexpected error writing to QuestDB: {e}")
                raise e
        
        logger.error("Failed to write to QuestDB after all retry attempts")
        raise Exception("Error while writing to QuestDB")

    def close(self):
        """Clean up resources."""
        if self._sender:
            try:
                self._sender.close()
                logger.info("QuestDB sender closed successfully")
            except Exception as e:
                logger.error(f"Error closing QuestDB sender: {e}")


def main():
    """ Setup and run the solar panel data sink application. """
    
    logger.info("Starting QuestDB solar panel data sink application")

    # Setup necessary objects
    app = Application(
        consumer_group="questdb_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create QuestDB sink
    questdb_sink = QuestDBSink()
    
    # Setup input topic with JSON deserialization
    input_topic = app.topic(name=os.environ["input"], value_deserializer='json')
    sdf = app.dataframe(topic=input_topic)

    # Process and validate incoming data
    def process_solar_data(row):
        """Process and validate solar panel sensor data."""
        try:
            logger.info(f"Processing solar data: {row}")
            return row
        except Exception as e:
            logger.error(f"Error processing solar data: {e}")
            raise e

    # Apply transformation and sink to QuestDB
    sdf = sdf.apply(process_solar_data)
    sdf.sink(questdb_sink)

    # Run the application with testing parameters
    logger.info("Starting data processing (will process 10 messages)")
    app.run(count=10, timeout=20)
    logger.info("Application completed")


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()