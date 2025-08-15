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
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError, NetworkError

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClickHouseSink(BatchingSink):
    """
    A ClickHouse sink that writes batches of solar panel sensor data to ClickHouse database.
    Handles the complex nested JSON structure from Kafka messages and transforms them
    into a proper ClickHouse table format.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__()
        self._client = None
        self._table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')
        self._database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
        self._on_client_connect_success = on_client_connect_success
        self._on_client_connect_failure = on_client_connect_failure
        
    def setup(self):
        """Initialize ClickHouse client and create table if necessary"""
        try:
            # Get connection parameters from environment variables
            host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
            except ValueError:
                logger.warning("Invalid CLICKHOUSE_PORT, using default 9000")
                port = 9000
                
            user = os.environ.get('CLICKHOUSE_USER', 'default')
            password = os.environ.get('CLICKHOUSE_PASSWORD', '')
            
            logger.info(f"Connecting to ClickHouse at {host}:{port} as user {user}")
            
            self._client = Client(
                host=host,
                port=port,
                user=user,
                password=password,
                database=self._database,
                connect_timeout=10,
                send_receive_timeout=30
            )
            
            # Test connection
            self._client.execute('SELECT 1')
            logger.info("ClickHouse connection established successfully")
            
            # Create table if it doesn't exist
            self._create_table()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def _create_table(self):
        """Create the solar panel data table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            kafka_timestamp DateTime64(3),
            kafka_topic String,
            kafka_partition UInt32,
            kafka_offset UInt64,
            kafka_key String,
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
            sensor_timestamp DateTime64(3),
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, kafka_timestamp)
        PARTITION BY toYYYYMM(kafka_timestamp)
        """
        
        try:
            self._client.execute(create_table_sql)
            logger.info(f"Table {self._table_name} created or already exists")
        except ClickHouseError as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_message_data(self, batch_item):
        """
        Parse the complex nested message structure from Kafka.
        
        Based on the schema analysis, messages have this structure:
        - Top level fields: topicId, topicName, streamId, type, value, dateTime, partition, offset, headers
        - The actual sensor data is in the 'value' field as a JSON string
        """
        try:
            # Get the raw message value
            message_data = batch_item.value
            
            # Debug: Print raw message structure for the first few messages
            logger.debug(f"Raw message structure: {type(message_data)} - {message_data}")
            
            # Handle case where message_data is already parsed JSON (dict)
            if isinstance(message_data, dict):
                # Extract the nested JSON from the 'value' field
                if 'value' in message_data and isinstance(message_data['value'], str):
                    # Parse the JSON string in the 'value' field
                    sensor_data = json.loads(message_data['value'])
                elif 'value' in message_data and isinstance(message_data['value'], dict):
                    # Value is already parsed
                    sensor_data = message_data['value']
                else:
                    # Assume the message_data is the sensor data directly
                    sensor_data = message_data
                
                # Extract metadata from top level if available
                stream_id = message_data.get('streamId', '')
                kafka_datetime = message_data.get('dateTime', '')
            else:
                # If message_data is a string, parse it as JSON
                parsed_message = json.loads(message_data)
                sensor_data = json.loads(parsed_message.get('value', '{}'))
                stream_id = parsed_message.get('streamId', '')
                kafka_datetime = parsed_message.get('dateTime', '')
            
            # Convert timestamp from Unix nanoseconds to datetime
            sensor_timestamp_ns = sensor_data.get('timestamp', 0)
            if sensor_timestamp_ns > 0:
                # Convert from nanoseconds to seconds for datetime
                sensor_timestamp = datetime.fromtimestamp(sensor_timestamp_ns / 1_000_000_000)
            else:
                sensor_timestamp = datetime.now()
            
            # Convert Kafka datetime string to datetime object
            if kafka_datetime:
                try:
                    kafka_timestamp = datetime.fromisoformat(kafka_datetime.replace('Z', '+00:00'))
                except:
                    kafka_timestamp = datetime.now()
            else:
                kafka_timestamp = datetime.fromtimestamp(batch_item.timestamp / 1000)
            
            # Prepare the row data for ClickHouse
            row_data = {
                'kafka_timestamp': kafka_timestamp,
                'kafka_topic': batch_item.topic,
                'kafka_partition': batch_item.partition,
                'kafka_offset': batch_item.offset,
                'kafka_key': str(batch_item.key) if batch_item.key else '',
                'stream_id': stream_id,
                'panel_id': sensor_data.get('panel_id', ''),
                'location_id': sensor_data.get('location_id', ''),
                'location_name': sensor_data.get('location_name', ''),
                'latitude': float(sensor_data.get('latitude', 0.0)),
                'longitude': float(sensor_data.get('longitude', 0.0)),
                'timezone': int(sensor_data.get('timezone', 0)),
                'power_output': float(sensor_data.get('power_output', 0.0)),
                'unit_power': sensor_data.get('unit_power', ''),
                'temperature': float(sensor_data.get('temperature', 0.0)),
                'unit_temp': sensor_data.get('unit_temp', ''),
                'irradiance': float(sensor_data.get('irradiance', 0.0)),
                'unit_irradiance': sensor_data.get('unit_irradiance', ''),
                'voltage': float(sensor_data.get('voltage', 0.0)),
                'unit_voltage': sensor_data.get('unit_voltage', ''),
                'current': float(sensor_data.get('current', 0.0)),
                'unit_current': sensor_data.get('unit_current', ''),
                'inverter_status': sensor_data.get('inverter_status', ''),
                'sensor_timestamp': sensor_timestamp
            }
            
            return row_data
            
        except Exception as e:
            logger.error(f"Error parsing message data: {e}")
            logger.error(f"Message data: {batch_item.value}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of messages to ClickHouse with retry logic and backpressure handling.
        """
        if not self._client:
            logger.error("ClickHouse client not initialized. Calling setup()...")
            self.setup()
            if not self._client:
                raise Exception("ClickHouse client not initialized. Setup failed.")
        
        attempts_remaining = 3
        backoff_delay = 1
        
        while attempts_remaining > 0:
            try:
                # Parse all messages in the batch
                rows_data = []
                for item in batch:
                    try:
                        row_data = self._parse_message_data(item)
                        rows_data.append(row_data)
                    except Exception as e:
                        logger.error(f"Failed to parse message, skipping: {e}")
                        continue
                
                if not rows_data:
                    logger.warning("No valid data to write in this batch")
                    return
                
                # Prepare the insert statement
                columns = list(rows_data[0].keys())
                values = [list(row.values()) for row in rows_data]
                
                insert_sql = f"INSERT INTO {self._table_name} ({', '.join(columns)}) VALUES"
                
                # Execute the batch insert
                self._client.execute(insert_sql, values)
                
                logger.info(f"Successfully wrote {len(rows_data)} rows to ClickHouse")
                return
                
            except NetworkError as e:
                logger.error(f"ClickHouse network error: {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    time.sleep(backoff_delay)
                    backoff_delay *= 2
                else:
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                    
            except ClickHouseError as e:
                logger.error(f"ClickHouse error: {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    time.sleep(backoff_delay)
                    backoff_delay *= 2
                else:
                    # For ClickHouse-specific errors, raise backpressure
                    raise SinkBackpressureError(
                        retry_after=60.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                    
            except Exception as e:
                logger.error(f"Unexpected error writing to ClickHouse: {e}")
                raise
        
        raise Exception("Failed to write to ClickHouse after all retry attempts")

    def cleanup(self):
        """Clean up resources"""
        if self._client:
            try:
                self._client.disconnect()
                logger.info("ClickHouse connection closed")
            except Exception as e:
                logger.warning(f"Error closing ClickHouse connection: {e}")
            finally:
                self._client = None


def debug_message_structure(row):
    """Debug function to print raw message structure"""
    print(f"DEBUG - Message type: {type(row)}")
    print(f"DEBUG - Message content (first 500 chars): {str(row)[:500]}")
    if isinstance(row, dict):
        print(f"DEBUG - Message keys: {list(row.keys())}")
        if 'value' in row:
            print(f"DEBUG - Value type: {type(row['value'])}")
            if isinstance(row['value'], str):
                try:
                    parsed_value = json.loads(row['value'])
                    print(f"DEBUG - Parsed value keys: {list(parsed_value.keys()) if isinstance(parsed_value, dict) else 'Not a dict'}")
                except:
                    print("DEBUG - Value is not valid JSON")
    return row


def main():
    """ Here we will set up our Application. """
    
    logger.info("Starting ClickHouse solar panel data sink application...")
    
    # Log environment variables for debugging (excluding passwords)
    logger.info(f"CLICKHOUSE_HOST: {os.environ.get('CLICKHOUSE_HOST', 'localhost')}")
    logger.info(f"CLICKHOUSE_PORT: {os.environ.get('CLICKHOUSE_PORT', '9000')}")
    logger.info(f"CLICKHOUSE_USER: {os.environ.get('CLICKHOUSE_USER', 'default')}")
    logger.info(f"CLICKHOUSE_DATABASE: {os.environ.get('CLICKHOUSE_DATABASE', 'default')}")
    logger.info(f"CLICKHOUSE_TABLE: {os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')}")
    logger.info(f"Input topic: {os.environ.get('input', 'solar-data')}")

    # Setup necessary objects
    app = Application(
        consumer_group="clickhouse_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest",
        commit_interval=5.0,  # Commit every 5 seconds
        commit_every=100      # Or every 100 messages
    )
    
    # Initialize ClickHouse sink with connection callbacks
    def on_connect_success():
        logger.info("ClickHouse sink connected successfully")
    
    def on_connect_failure(error):
        logger.error(f"ClickHouse sink connection failed: {error}")
        raise error
    
    clickhouse_sink = ClickHouseSink(
        on_client_connect_success=on_connect_success,
        on_client_connect_failure=on_connect_failure
    )
    
    # Get input topic from environment
    input_topic = app.topic(
        name=os.environ.get("input", "solar-data"),
        value_deserializer="json"  # Ensure JSON deserialization
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=input_topic)

    # Add debugging to understand message structure
    sdf = sdf.apply(debug_message_structure)
    
    # Print messages with metadata for debugging
    sdf = sdf.print(metadata=True)

    # Sink to ClickHouse
    sdf.sink(clickhouse_sink)

    # For initial testing, run with limits
    try:
        # For testing: limit to 10 messages with 20 second timeout
        # Remove or modify these parameters for production use
        logger.info("Running application with limits: count=10, timeout=20 seconds")
        app.run(count=10, timeout=20)
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        # Clean up ClickHouse connection
        try:
            clickhouse_sink.cleanup()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()