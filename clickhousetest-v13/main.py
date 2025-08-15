# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Any
import clickhouse_connect

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSolarPanelSink(BatchingSink):
    """
    A ClickHouse sink for solar panel sensor data.
    
    This sink reads solar panel data from Kafka messages, parses the JSON payload,
    and writes it to a ClickHouse database with proper schema mapping.
    """
    
    def __init__(self):
        super().__init__()
        
        # ClickHouse connection parameters
        self.host = os.environ.get("CLICKHOUSE_HOST", "localhost")
        self.port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
        self.username = os.environ.get("CLICKHOUSE_USERNAME", "default")
        self.password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.database = os.environ.get("CLICKHOUSE_DATABASE", "solar_data")
        self.table = os.environ.get("CLICKHOUSE_TABLE", "panel_readings")
        
        # Connection settings
        self.connect_timeout = int(os.environ.get("CLICKHOUSE_CONNECT_TIMEOUT", "10"))
        self.send_timeout = int(os.environ.get("CLICKHOUSE_SEND_TIMEOUT", "30"))
        
        self.client = None
        
        logger.info(f"ClickHouse Sink initialized - Host: {self.host}:{self.port}, Database: {self.database}, Table: {self.table}")
    
    def setup(self):
        """Initialize ClickHouse connection and create table if needed."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                connect_timeout=self.connect_timeout,
                send_receive_timeout=self.send_timeout
            )
            
            # Test connection
            result = self.client.command('SELECT 1')
            logger.info("ClickHouse connection established successfully")
            
            # Create database if it doesn't exist
            self.client.command(f'CREATE DATABASE IF NOT EXISTS {self.database}')
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Failed to setup ClickHouse connection: {e}")
            raise ConnectionError(f"ClickHouse setup failed: {e}")
    
    def _create_table_if_not_exists(self):
        """Create the solar panel data table with appropriate schema."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
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
            kafka_timestamp DateTime64(3),
            kafka_topic String,
            kafka_partition Int32,
            kafka_offset Int64,
            ingestion_time DateTime64(3) DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, sensor_timestamp)
        PARTITION BY toYYYYMM(sensor_timestamp)
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Table {self.database}.{self.table} created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def _parse_solar_data(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the solar panel message and extract relevant data.
        Based on schema analysis, the actual sensor data is in the 'value' field as JSON string.
        """
        try:
            # Debug: Print raw message structure
            logger.debug(f"Raw message structure: {message_data}")
            
            # The 'value' field contains the JSON string with actual sensor data
            if 'value' in message_data and isinstance(message_data['value'], str):
                sensor_data = json.loads(message_data['value'])
            elif 'value' in message_data and isinstance(message_data['value'], dict):
                sensor_data = message_data['value']
            else:
                # If no 'value' field, assume the message is directly the sensor data
                sensor_data = message_data
            
            # Convert Unix timestamp to datetime
            sensor_timestamp = datetime.fromtimestamp(sensor_data.get('timestamp', 0) / 1000000000)
            
            # Extract kafka metadata
            kafka_timestamp = datetime.fromisoformat(message_data.get('dateTime', '').replace('Z', '+00:00')) if 'dateTime' in message_data else datetime.now()
            
            parsed_data = {
                'panel_id': sensor_data.get('panel_id', ''),
                'location_id': sensor_data.get('location_id', ''),
                'location_name': sensor_data.get('location_name', ''),
                'latitude': float(sensor_data.get('latitude', 0.0)),
                'longitude': float(sensor_data.get('longitude', 0.0)),
                'timezone': int(sensor_data.get('timezone', 0)),
                'power_output': float(sensor_data.get('power_output', 0.0)),
                'unit_power': sensor_data.get('unit_power', 'W'),
                'temperature': float(sensor_data.get('temperature', 0.0)),
                'unit_temp': sensor_data.get('unit_temp', 'C'),
                'irradiance': float(sensor_data.get('irradiance', 0.0)),
                'unit_irradiance': sensor_data.get('unit_irradiance', 'W/m²'),
                'voltage': float(sensor_data.get('voltage', 0.0)),
                'unit_voltage': sensor_data.get('unit_voltage', 'V'),
                'current': float(sensor_data.get('current', 0.0)),
                'unit_current': sensor_data.get('unit_current', 'A'),
                'inverter_status': sensor_data.get('inverter_status', 'UNKNOWN'),
                'sensor_timestamp': sensor_timestamp,
                'kafka_timestamp': kafka_timestamp,
                'kafka_topic': message_data.get('topicName', ''),
                'kafka_partition': int(message_data.get('partition', 0)),
                'kafka_offset': int(message_data.get('offset', 0))
            }
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing solar data: {e}")
            logger.error(f"Message data: {message_data}")
            raise ValueError(f"Failed to parse solar panel data: {e}")
    
    def _write_to_clickhouse(self, data: List[Dict[str, Any]]):
        """Write batch of solar panel data to ClickHouse."""
        if not data:
            logger.warning("No data to write")
            return
        
        try:
            # Prepare data for insertion
            rows = []
            for record in data:
                parsed_record = self._parse_solar_data(record)
                rows.append([
                    parsed_record['panel_id'],
                    parsed_record['location_id'],
                    parsed_record['location_name'],
                    parsed_record['latitude'],
                    parsed_record['longitude'],
                    parsed_record['timezone'],
                    parsed_record['power_output'],
                    parsed_record['unit_power'],
                    parsed_record['temperature'],
                    parsed_record['unit_temp'],
                    parsed_record['irradiance'],
                    parsed_record['unit_irradiance'],
                    parsed_record['voltage'],
                    parsed_record['unit_voltage'],
                    parsed_record['current'],
                    parsed_record['unit_current'],
                    parsed_record['inverter_status'],
                    parsed_record['sensor_timestamp'],
                    parsed_record['kafka_timestamp'],
                    parsed_record['kafka_topic'],
                    parsed_record['kafka_partition'],
                    parsed_record['kafka_offset']
                ])
            
            # Column names for insertion
            columns = [
                'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
                'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
                'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current',
                'unit_current', 'inverter_status', 'sensor_timestamp', 'kafka_timestamp',
                'kafka_topic', 'kafka_partition', 'kafka_offset'
            ]
            
            # Insert data
            self.client.insert(
                table=f"{self.database}.{self.table}",
                data=rows,
                column_names=columns
            )
            
            logger.info(f"Successfully wrote {len(rows)} records to ClickHouse")
            
        except Exception as e:
            logger.error(f"Error writing to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to ClickHouse.
        
        Implements retry logic and backpressure handling as per Quix Streams best practices.
        """
        attempts_remaining = 3
        data = [item.value for item in batch]
        
        # Debug: Print first message structure for troubleshooting
        if data:
            logger.info(f"Processing batch of {len(data)} messages")
            logger.debug(f"First message sample: {data[0]}")
        
        while attempts_remaining:
            try:
                return self._write_to_clickhouse(data)
                
            except ConnectionError as e:
                logger.warning(f"ClickHouse connection error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                    # Try to reconnect
                    try:
                        self.setup()
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect: {reconnect_error}")
                        
            except TimeoutError as e:
                logger.warning(f"ClickHouse timeout: {e}")
                # For timeout errors, use backpressure mechanism
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            except Exception as e:
                logger.error(f"Unexpected error writing to ClickHouse: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(5)
        
        raise Exception(f"Failed to write to ClickHouse after 3 attempts")


def main():
    """Set up the ClickHouse solar panel data sink application."""
    
    # Setup necessary objects
    app = Application(
        consumer_group="clickhouse_solar_panel_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize ClickHouse sink
    clickhouse_sink = ClickHouseSolarPanelSink()
    
    # Get input topic from environment variable
    input_topic = app.topic(
        name=os.environ.get("input", "solar-data"),
        value_deserializer="json"  # Deserialize JSON messages
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=input_topic)
    
    # Add message processing and debugging
    def debug_message(message):
        """Debug function to print raw message structure."""
        logger.info(f"Processing message from topic: {message.get('topic', 'unknown')}")
        logger.debug(f"Raw message keys: {list(message.keys())}")
        return message
    
    def validate_message(message):
        """Validate that the message contains required solar panel data."""
        try:
            # Check if message has 'value' field or direct sensor data
            if 'value' in message:
                if isinstance(message['value'], str):
                    sensor_data = json.loads(message['value'])
                else:
                    sensor_data = message['value']
            else:
                sensor_data = message
            
            # Validate required fields
            required_fields = ['panel_id', 'location_id', 'power_output', 'temperature']
            for field in required_fields:
                if field not in sensor_data:
                    logger.warning(f"Missing required field: {field}")
            
            return message
            
        except Exception as e:
            logger.error(f"Message validation error: {e}")
            logger.error(f"Invalid message: {message}")
            return message  # Still return the message to avoid blocking the pipeline
    
    # Process messages with debugging and validation
    sdf = sdf.apply(debug_message)
    sdf = sdf.apply(validate_message)
    sdf = sdf.print(metadata=True)  # Print for debugging
    
    # Sink to ClickHouse
    sdf.sink(clickhouse_sink)
    
    logger.info("Starting ClickHouse solar panel sink application...")
    
    # For testing: run with a limited count and timeout
    # Remove these parameters for production use
    try:
        if os.environ.get("TESTING_MODE", "false").lower() == "true":
            logger.info("Running in testing mode with limited message count")
            app.run(count=10, timeout=20)
        else:
            app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()