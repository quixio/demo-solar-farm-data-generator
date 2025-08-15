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
from typing import List, Dict, Any
import clickhouse_connect

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSolarDataSink(BatchingSink):
    """
    A ClickHouse sink for solar panel sensor data.
    
    This sink handles writing solar panel data to a ClickHouse database,
    with proper schema mapping and error handling.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_name = os.environ.get('CLICKHOUSE_TABLE_NAME', 'solar_data')
        self._database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
        
    def setup(self):
        """Set up the ClickHouse client and create table if needed."""
        try:
            self._client = clickhouse_connect.get_client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=int(os.environ.get('CLICKHOUSE_PORT', '8123')),
                username=os.environ.get('CLICKHOUSE_USERNAME', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                database=self._database,
                secure=os.environ.get('CLICKHOUSE_SECURE', 'false').lower() == 'true'
            )
            
            # Test connection
            self._client.ping()
            logger.info("Successfully connected to ClickHouse")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            if callable(self.on_client_connect_failure):
                self.on_client_connect_failure(e)
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar_data table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._database}.{self._table_name} (
            kafka_timestamp DateTime64(3, 'UTC'),
            kafka_key String,
            kafka_topic String,
            kafka_partition Int32,
            kafka_offset Int64,
            
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
            sensor_timestamp DateTime64(3, 'UTC'),
            
            ingestion_time DateTime64(3, 'UTC') DEFAULT now()
        ) 
        ENGINE = MergeTree() 
        ORDER BY (location_id, panel_id, sensor_timestamp)
        PARTITION BY toYYYYMM(sensor_timestamp);
        """
        
        try:
            self._client.command(create_table_sql)
            logger.info(f"Table {self._database}.{self._table_name} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_message(self, item) -> Dict[str, Any]:
        """Parse a Kafka message and extract solar data."""
        try:
            # Debug: Print raw message structure
            logger.debug(f"Raw message structure: {type(item.value)} - {str(item.value)[:200]}...")
            
            # Based on the schema analysis, the actual data is in a nested JSON string within the 'value' field
            message_data = item.value
            
            # If the message value is a string (JSON), parse it
            if isinstance(message_data, str):
                message_data = json.loads(message_data)
            
            # Extract the nested JSON from the 'value' field
            if 'value' in message_data and isinstance(message_data['value'], str):
                solar_data = json.loads(message_data['value'])
            else:
                # If the structure is different, try to use the data directly
                solar_data = message_data
            
            # Convert Unix timestamp (nanoseconds) to datetime
            sensor_timestamp = datetime.fromtimestamp(solar_data['timestamp'] / 1_000_000_000)
            kafka_timestamp = datetime.fromtimestamp(item.timestamp / 1000)  # Kafka timestamp is in milliseconds
            
            parsed_data = {
                # Kafka metadata
                'kafka_timestamp': kafka_timestamp,
                'kafka_key': str(item.key) if item.key else '',
                'kafka_topic': item.topic,
                'kafka_partition': item.partition,
                'kafka_offset': item.offset,
                
                # Solar panel data
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
                'sensor_timestamp': sensor_timestamp
            }
            
            logger.debug(f"Parsed solar data for panel {parsed_data['panel_id']}")
            return parsed_data
            
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            logger.error(f"Message content: {item.value}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar data to ClickHouse.
        
        Implements retry logic with exponential backoff for handling connection issues.
        """
        if not self._client:
            self.setup()
        
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Parse all messages in the batch
                parsed_data = []
                for item in batch:
                    try:
                        data = self._parse_message(item)
                        parsed_data.append(data)
                    except Exception as e:
                        logger.error(f"Failed to parse message, skipping: {e}")
                        continue
                
                if not parsed_data:
                    logger.warning("No valid data to write in this batch")
                    return
                
                # Insert data into ClickHouse
                self._client.insert(
                    table=f'{self._database}.{self._table_name}',
                    data=parsed_data
                )
                
                logger.info(f"Successfully wrote {len(parsed_data)} records to ClickHouse")
                return
                
            except clickhouse_connect.driver.exceptions.ClickHouseError as e:
                error_code = getattr(e, 'code', None)
                
                # Handle specific ClickHouse errors
                if error_code in [81, 159, 164]:  # Connection errors
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"ClickHouse connection error (attempt {attempt + 1}/{max_retries}): {e}")
                    
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                        # Reconnect
                        try:
                            self.setup()
                        except Exception as setup_error:
                            logger.error(f"Failed to reconnect: {setup_error}")
                    continue
                else:
                    logger.error(f"ClickHouse error: {e}")
                    raise
                    
            except ConnectionError as e:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    raise
                    
            except Exception as e:
                # For server busy or timeout errors, use backpressure
                if "timeout" in str(e).lower() or "busy" in str(e).lower():
                    logger.warning(f"Server busy/timeout, applying backpressure: {e}")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                
                logger.error(f"Unexpected error writing to ClickHouse: {e}")
                raise
        
        # If we've exhausted all retries
        logger.error(f"Failed to write batch after {max_retries} attempts")
        raise Exception("Failed to write to ClickHouse after multiple retries")


def main():
    """Set up and run the ClickHouse solar data sink application."""
    
    logger.info("Starting ClickHouse Solar Data Sink")
    
    # Setup necessary objects
    app = Application(
        consumer_group="clickhouse_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create the ClickHouse sink
    clickhouse_sink = ClickHouseSolarDataSink()
    
    # Get input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)
    
    # Add debugging to show raw message structure
    def debug_message(row):
        logger.info(f"Processing message from topic: {input_topic.name}")
        logger.info(f"Message type: {type(row)}")
        logger.info(f"Message sample: {str(row)[:500]}...")
        return row
    
    # Process messages and sink to ClickHouse
    sdf = sdf.apply(debug_message)
    sdf.sink(clickhouse_sink)
    
    # Run the application
    # For initial testing, you can limit the number of messages
    # app.run(count=10, timeout=20)
    
    logger.info("Application setup complete, starting to process messages...")
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()