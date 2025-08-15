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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    
    This sink reads solar panel sensor data from Kafka and writes it to ClickHouse.
    It handles the nested JSON structure where actual data is in the 'value' field.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self.table_name = "solar_panel_data"
        self.database = os.environ.get("CLICKHOUSE_DATABASE", "default")
        
    def setup(self):
        """Initialize ClickHouse connection and create table if needed."""
        try:
            # Initialize ClickHouse client
            self.client = clickhouse_connect.get_client(
                host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
                port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
                username=os.environ.get("CLICKHOUSE_USERNAME", "default"),
                password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
                database=self.database,
                secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true"
            )
            
            # Test connection
            self.client.ping()
            logger.info("Successfully connected to ClickHouse")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar panel data table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            kafka_timestamp DateTime64(3),
            kafka_topic String,
            kafka_partition UInt32,
            kafka_offset UInt64,
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
            sensor_timestamp DateTime64(3)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(kafka_timestamp)
        ORDER BY (kafka_timestamp, panel_id)
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Table {self.table_name} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_message(self, item) -> Dict[str, Any]:
        """
        Parse the Kafka message and extract solar panel data.
        
        Based on the schema analysis, the message structure is:
        - Top level: topicId, topicName, streamId, type, value, dateTime, partition, offset, headers
        - value field contains JSON string with actual solar panel data
        """
        try:
            # Debug: Print raw message structure
            logger.debug(f"Raw message: {item.value}")
            
            message_data = item.value
            
            # Parse the nested JSON in the 'value' field
            if isinstance(message_data, dict) and 'value' in message_data:
                solar_data_str = message_data['value']
                if isinstance(solar_data_str, str):
                    solar_data = json.loads(solar_data_str)
                else:
                    solar_data = solar_data_str
                
                # Convert Unix timestamp to datetime
                sensor_timestamp = datetime.fromtimestamp(solar_data['timestamp'] / 1_000_000_000)
                kafka_timestamp = datetime.fromtimestamp(item.timestamp / 1000)
                
                return {
                    'kafka_timestamp': kafka_timestamp,
                    'kafka_topic': item.topic,
                    'kafka_partition': item.partition,
                    'kafka_offset': item.offset,
                    'stream_id': message_data.get('streamId', ''),
                    'panel_id': solar_data.get('panel_id', ''),
                    'location_id': solar_data.get('location_id', ''),
                    'location_name': solar_data.get('location_name', ''),
                    'latitude': float(solar_data.get('latitude', 0.0)),
                    'longitude': float(solar_data.get('longitude', 0.0)),
                    'timezone': int(solar_data.get('timezone', 0)),
                    'power_output': float(solar_data.get('power_output', 0.0)),
                    'unit_power': solar_data.get('unit_power', 'W'),
                    'temperature': float(solar_data.get('temperature', 0.0)),
                    'unit_temp': solar_data.get('unit_temp', 'C'),
                    'irradiance': float(solar_data.get('irradiance', 0.0)),
                    'unit_irradiance': solar_data.get('unit_irradiance', 'W/m²'),
                    'voltage': float(solar_data.get('voltage', 0.0)),
                    'unit_voltage': solar_data.get('unit_voltage', 'V'),
                    'current': float(solar_data.get('current', 0.0)),
                    'unit_current': solar_data.get('unit_current', 'A'),
                    'inverter_status': solar_data.get('inverter_status', 'UNKNOWN'),
                    'sensor_timestamp': sensor_timestamp
                }
            else:
                # Fallback: assume direct structure without nested JSON
                logger.warning("Message doesn't have expected nested structure, trying direct parsing")
                solar_data = message_data
                
                sensor_timestamp = datetime.fromtimestamp(solar_data.get('timestamp', item.timestamp) / 1000)
                kafka_timestamp = datetime.fromtimestamp(item.timestamp / 1000)
                
                return {
                    'kafka_timestamp': kafka_timestamp,
                    'kafka_topic': item.topic,
                    'kafka_partition': item.partition,
                    'kafka_offset': item.offset,
                    'stream_id': solar_data.get('streamId', ''),
                    'panel_id': solar_data.get('panel_id', ''),
                    'location_id': solar_data.get('location_id', ''),
                    'location_name': solar_data.get('location_name', ''),
                    'latitude': float(solar_data.get('latitude', 0.0)),
                    'longitude': float(solar_data.get('longitude', 0.0)),
                    'timezone': int(solar_data.get('timezone', 0)),
                    'power_output': float(solar_data.get('power_output', 0.0)),
                    'unit_power': solar_data.get('unit_power', 'W'),
                    'temperature': float(solar_data.get('temperature', 0.0)),
                    'unit_temp': solar_data.get('unit_temp', 'C'),
                    'irradiance': float(solar_data.get('irradiance', 0.0)),
                    'unit_irradiance': solar_data.get('unit_irradiance', 'W/m²'),
                    'voltage': float(solar_data.get('voltage', 0.0)),
                    'unit_voltage': solar_data.get('unit_voltage', 'V'),
                    'current': float(solar_data.get('current', 0.0)),
                    'unit_current': solar_data.get('unit_current', 'A'),
                    'inverter_status': solar_data.get('inverter_status', 'UNKNOWN'),
                    'sensor_timestamp': sensor_timestamp
                }
                
        except Exception as e:
            logger.error(f"Error parsing message: {e}, raw data: {item.value}")
            raise

    def _write_to_clickhouse(self, data: List[Dict[str, Any]]):
        """Write parsed data to ClickHouse."""
        if not data:
            logger.warning("No data to write")
            return
            
        try:
            # Insert data using clickhouse-connect
            self.client.insert(
                table=self.table_name,
                data=data,
                column_names=list(data[0].keys())
            )
            logger.info(f"Successfully wrote {len(data)} records to ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to write to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to ClickHouse.
        
        Implements retry logic with exponential backoff for robust operation.
        """
        max_attempts = 3
        base_delay = 2
        
        # Parse all messages in the batch
        try:
            parsed_data = []
            for item in batch:
                try:
                    parsed_item = self._parse_message(item)
                    parsed_data.append(parsed_item)
                except Exception as e:
                    logger.error(f"Error parsing single message: {e}")
                    # Continue processing other messages
                    continue
                    
            if not parsed_data:
                logger.warning("No valid messages in batch to write")
                return
                
        except Exception as e:
            logger.error(f"Error parsing batch: {e}")
            raise
        
        # Attempt to write with retries
        for attempt in range(max_attempts):
            try:
                self._write_to_clickhouse(parsed_data)
                return  # Success!
                
            except Exception as e:
                if attempt < max_attempts - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Write attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"All {max_attempts} write attempts failed: {e}")
                    # Check if it's a temporary issue
                    if "timeout" in str(e).lower() or "connection" in str(e).lower():
                        raise SinkBackpressureError(
                            retry_after=30.0,
                            topic=batch.topic,
                            partition=batch.partition,
                        )
                    else:
                        raise


def main():
    """Initialize and run the solar panel data ClickHouse sink application."""
    
    # Setup necessary objects
    app = Application(
        consumer_group="clickhouse_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize ClickHouse sink
    clickhouse_sink = ClickHouseSink()
    clickhouse_sink.setup()
    
    # Configure input topic
    input_topic = app.topic(name=os.environ.get("input", "solar-data"))
    sdf = app.dataframe(topic=input_topic)
    
    # Debug: Print raw messages to understand structure
    sdf = sdf.apply(lambda row: print(f"DEBUG - Raw message structure: {type(row)}, keys: {row.keys() if isinstance(row, dict) else 'N/A'}, sample: {str(row)[:200]}...") or row)
    
    # Apply transformations (if needed in the future)
    sdf = sdf.apply(lambda row: row)
    
    # Sink to ClickHouse
    sdf.sink(clickhouse_sink)
    
    # Run the application
    logger.info("Starting ClickHouse solar panel sink application...")
    
    # For testing: run with limited messages and timeout
    if os.environ.get("TESTING", "false").lower() == "true":
        app.run(count=10, timeout=20)
    else:
        app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()