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
from typing import Dict, Any, List
import clickhouse_connect

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class ClickHouseSink(BatchingSink):
    """
    A custom sink that writes solar panel sensor data to ClickHouse database.
    
    This sink handles data from solar panel sensors including power output,
    temperature, irradiance, voltage, current, and inverter status.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Initialize ClickHouse connection"""
        try:
            # Get connection parameters from environment
            host = os.environ["CLICKHOUSE_HOST"]
            port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
            username = os.environ.get("CLICKHOUSE_USERNAME", "default")
            password = os.environ.get("CLICKHOUSE_PASSWORD", "")
            database = os.environ.get("CLICKHOUSE_DATABASE", "default")
            secure = os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true"
            
            logger.info(f"Connecting to ClickHouse at {host}:{port} (secure={secure})")
            
            self.client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure
            )
            
            # Test the connection
            result = self.client.command("SELECT 1")
            logger.info(f"ClickHouse connection test successful: {result}")
            
            self._create_table_if_not_exists()
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar_panel_data table if it doesn't exist"""
        table_name = os.environ.get("CLICKHOUSE_TABLE", "solar_panel_data")
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            kafka_timestamp DateTime64(3),
            kafka_partition UInt32,
            kafka_offset UInt64,
            kafka_key String,
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
            sensor_timestamp UInt64,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, kafka_timestamp)
        PARTITION BY toYYYYMM(kafka_timestamp)
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Table {table_name} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def _parse_message_data(self, item) -> Dict[str, Any]:
        """Parse and extract data from a Kafka message"""
        try:
            # Debug: Print raw message structure
            logger.debug(f"Raw message value type: {type(item.value)}")
            logger.debug(f"Raw message value: {item.value}")
            
            # Handle different message formats
            if isinstance(item.value, dict):
                # If value is already a dict, check if it has a nested 'value' field
                if 'value' in item.value and isinstance(item.value['value'], str):
                    # Parse the nested JSON string
                    sensor_data = json.loads(item.value['value'])
                else:
                    # The dict IS the sensor data
                    sensor_data = item.value
            elif isinstance(item.value, str):
                # If value is a JSON string, parse it
                try:
                    parsed_value = json.loads(item.value)
                    if isinstance(parsed_value, dict) and 'value' in parsed_value:
                        sensor_data = json.loads(parsed_value['value'])
                    else:
                        sensor_data = parsed_value
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse JSON string: {item.value}")
                    raise
            else:
                logger.error(f"Unexpected message value type: {type(item.value)}")
                raise ValueError(f"Cannot parse message value of type {type(item.value)}")
            
            # Convert Kafka timestamp to datetime (handle both milliseconds and nanoseconds)
            if item.timestamp > 1e12:  # Likely nanoseconds
                kafka_timestamp = datetime.fromtimestamp(item.timestamp / 1e9)
            else:  # Likely milliseconds
                kafka_timestamp = datetime.fromtimestamp(item.timestamp / 1000.0)
            
            # Extract data with safe defaults and type conversion
            def safe_float(value, default=0.0):
                try:
                    return float(value) if value is not None else default
                except (ValueError, TypeError):
                    return default
                    
            def safe_int(value, default=0):
                try:
                    return int(value) if value is not None else default
                except (ValueError, TypeError):
                    return default
            
            parsed_data = {
                'kafka_timestamp': kafka_timestamp,
                'kafka_partition': item.partition if item.partition is not None else 0,
                'kafka_offset': item.offset if item.offset is not None else 0,
                'kafka_key': str(item.key) if item.key is not None else '',
                'panel_id': str(sensor_data.get('panel_id', '')),
                'location_id': str(sensor_data.get('location_id', '')),
                'location_name': str(sensor_data.get('location_name', '')),
                'latitude': safe_float(sensor_data.get('latitude'), 0.0),
                'longitude': safe_float(sensor_data.get('longitude'), 0.0),
                'timezone': safe_int(sensor_data.get('timezone'), 0),
                'power_output': safe_float(sensor_data.get('power_output'), 0.0),
                'unit_power': str(sensor_data.get('unit_power', 'W')),
                'temperature': safe_float(sensor_data.get('temperature'), 0.0),
                'unit_temp': str(sensor_data.get('unit_temp', 'C')),
                'irradiance': safe_float(sensor_data.get('irradiance'), 0.0),
                'unit_irradiance': str(sensor_data.get('unit_irradiance', 'W/m²')),
                'voltage': safe_float(sensor_data.get('voltage'), 0.0),
                'unit_voltage': str(sensor_data.get('unit_voltage', 'V')),
                'current': safe_float(sensor_data.get('current'), 0.0),
                'unit_current': str(sensor_data.get('unit_current', 'A')),
                'inverter_status': str(sensor_data.get('inverter_status', 'UNKNOWN')),
                'sensor_timestamp': safe_int(sensor_data.get('timestamp'), 0)
            }
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Failed to parse message data: {e}")
            logger.error(f"Message content: {item.value}")
            raise
    
    def _write_to_clickhouse(self, data: List[Dict[str, Any]]):
        """Write batch of data to ClickHouse"""
        if not data:
            logger.warning("No data to write to ClickHouse")
            return
        
        table_name = os.environ.get("CLICKHOUSE_TABLE", "solar_panel_data")
        
        try:
            # Prepare column names and data
            columns = list(data[0].keys())
            rows = [[row[col] for col in columns] for row in data]
            
            logger.info(f"Writing {len(rows)} records to ClickHouse table {table_name}")
            
            # Insert data
            self.client.insert(table_name, rows, column_names=columns)
            
            logger.info(f"Successfully wrote {len(rows)} records to ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to write to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batches of solar panel data to ClickHouse.
        
        This method processes each message in the batch, extracts the solar panel
        sensor data, and writes it to ClickHouse with retry logic and proper
        error handling.
        """
        attempts_remaining = 3
        
        try:
            # Parse all messages in the batch
            parsed_data = []
            for item in batch:
                try:
                    data = self._parse_message_data(item)
                    parsed_data.append(data)
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
                    # Continue processing other messages in the batch
                    continue
            
            if not parsed_data:
                logger.warning("No valid data found in batch")
                return
            
            # Attempt to write to ClickHouse with retry logic
            while attempts_remaining:
                try:
                    return self._write_to_clickhouse(parsed_data)
                except Exception as e:
                    logger.error(f"Write attempt failed: {e}")
                    attempts_remaining -= 1
                    
                    if "Connection" in str(e) or "timeout" in str(e).lower():
                        if attempts_remaining:
                            logger.info(f"Retrying in 3 seconds... ({attempts_remaining} attempts left)")
                            time.sleep(3)
                            # Re-establish connection
                            try:
                                logger.info("Re-establishing ClickHouse connection...")
                                self._setup_connection()
                                logger.info("Connection re-established successfully")
                            except Exception as setup_error:
                                logger.error(f"Failed to re-establish connection: {setup_error}")
                                if attempts_remaining == 1:
                                    raise setup_error
                        continue
                    elif "too many requests" in str(e).lower() or "rate limit" in str(e).lower():
                        # Handle backpressure
                        logger.warning("ClickHouse is rate limiting, triggering backpressure")
                        raise SinkBackpressureError(
                            retry_after=30.0,
                            topic=batch.topic,
                            partition=batch.partition,
                        )
                    else:
                        # For other errors, don't retry
                        break
            
            # All attempts failed
            raise Exception(f"Failed to write to ClickHouse after 3 attempts")
            
        except SinkBackpressureError:
            # Re-raise backpressure errors
            raise
        except Exception as e:
            logger.error(f"Batch write failed: {e}")
            raise


def parse_solar_data(row):
    """
    Parse and transform solar panel sensor data.
    
    This function handles the nested JSON structure where actual sensor data
    is contained within a 'value' field as a JSON string.
    """
    try:
        # Print raw row for debugging
        print(f"DEBUG - Raw row type: {type(row)}")
        print(f"DEBUG - Raw row content: {row}")
        
        # Handle different input formats
        if isinstance(row, dict):
            if 'value' in row and isinstance(row['value'], str):
                # Parse nested JSON string
                sensor_data = json.loads(row['value'])
                # Add metadata from outer message
                sensor_data['_kafka_topic'] = row.get('topicName', 'unknown')
                sensor_data['_kafka_partition'] = row.get('partition', 0)
                sensor_data['_message_datetime'] = row.get('dateTime', '')
                return sensor_data
            else:
                # Row is already the sensor data
                return row
        else:
            logger.warning(f"Unexpected row type: {type(row)}")
            return row
            
    except Exception as e:
        logger.error(f"Failed to parse solar data: {e}")
        logger.error(f"Row content: {row}")
        # Return original row to avoid losing data
        return row


def main():
    """ Set up the ClickHouse sink application for solar panel data. """
    
    logger.info("Starting ClickHouse Sink Application for Solar Panel Data")
    
    # Validate required environment variables
    required_env_vars = ["input", "CLICKHOUSE_HOST"]
    missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        raise EnvironmentError(f"Missing required environment variables: {missing_vars}")
    
    # Log environment configuration (without sensitive data)
    logger.info(f"Input topic: {os.environ['input']}")
    logger.info(f"ClickHouse host: {os.environ['CLICKHOUSE_HOST']}")
    logger.info(f"ClickHouse port: {os.environ.get('CLICKHOUSE_PORT', '8123')}")
    logger.info(f"ClickHouse database: {os.environ.get('CLICKHOUSE_DATABASE', 'default')}")
    logger.info(f"ClickHouse table: {os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')}")
    logger.info(f"ClickHouse secure: {os.environ.get('CLICKHOUSE_SECURE', 'false')}")
    
    # Setup application
    app = Application(
        consumer_group="solar_panel_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest",
        commit_interval=5.0,  # Commit every 5 seconds
        commit_every=100      # Or every 100 messages
    )
    
    # Create ClickHouse sink
    clickhouse_sink = ClickHouseSink()
    
    # Setup input topic - handle JSON values properly
    input_topic = app.topic(
        name=os.environ["input"],
        value_deserializer="json"  # Automatically deserialize JSON
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=input_topic)
    
    # Apply transformations and debugging
    sdf = sdf.apply(parse_solar_data)
    
    # Print processed data for debugging (remove in production)
    sdf = sdf.print(metadata=True)
    
    # Sink to ClickHouse
    sdf.sink(clickhouse_sink)
    
    logger.info("Application configured successfully. Starting to process messages...")
    
    # Run the application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()