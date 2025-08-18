# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import clickhouse_connect
import logging
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickHouseSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    Writes batches of solar panel data to a ClickHouse database.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self._setup_database()
    
    def _get_client(self):
        """Get or create ClickHouse client with error handling for port conversion"""
        if self.client is None:
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
            except ValueError:
                logger.warning("Invalid CLICKHOUSE_PORT value, using default 8123")
                port = 8123
            
            # Get connection parameters with defaults
            host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
            username = os.environ.get('CLICKHOUSE_USER', 'default')
            password = os.environ.get('CLICKHOUSE_PASSWORD', '')
            database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
            
            logger.info(f"Connecting to ClickHouse at {host}:{port}, database: {database}, user: {username}")
            
            try:
                self.client = clickhouse_connect.get_client(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    database=database
                )
                # Test the connection
                self.client.ping()
                logger.info("ClickHouse connection established successfully")
            except Exception as conn_error:
                logger.error(f"Failed to connect to ClickHouse: {conn_error}")
                raise
        return self.client
    
    def _setup_database(self):
        """Create the solar_panel_data table if it doesn't exist"""
        try:
            client = self._get_client()
            
            # Create table DDL
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS solar_panel_data (
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
                kafka_topic String,
                kafka_partition Int32,
                kafka_offset Int64
            ) ENGINE = MergeTree()
            ORDER BY (panel_id, sensor_timestamp)
            """
            
            client.command(create_table_sql)
            logger.info("Solar panel data table created/verified in ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            raise
    
    def _parse_message(self, item):
        """Parse the Kafka message and extract solar panel data"""
        try:
            # SinkItem has attributes: value, key, timestamp, headers, topic, partition, offset
            # Get the message value from the SinkItem
            message_data = item.value
            
            # Debug only the actual data content, not the object references
            logger.debug(f"Processing message data type: {type(message_data)}")
            if isinstance(message_data, dict):
                logger.debug(f"Message keys: {list(message_data.keys())}")
            
            # Based on the logs, the data appears to be already parsed by Quix Streams
            # Check if message_data is directly the solar panel data dict
            if isinstance(message_data, dict) and 'panel_id' in message_data:
                # Direct solar data - this is what we're getting based on the logs
                solar_data = message_data
                logger.debug(f"Found direct solar data with panel_id: {solar_data.get('panel_id')}")
            elif isinstance(message_data, dict) and 'value' in message_data:
                # Check if the message has the outer structure with 'value' field containing JSON string
                if isinstance(message_data['value'], str):
                    solar_data = json.loads(message_data['value'])
                else:
                    solar_data = message_data['value']
            elif isinstance(message_data, str):
                # If the entire message is a JSON string, parse it
                parsed_message = json.loads(message_data)
                if 'value' in parsed_message and isinstance(parsed_message['value'], str):
                    solar_data = json.loads(parsed_message['value'])
                else:
                    solar_data = parsed_message
            else:
                # Direct solar data
                solar_data = message_data
            
            # Validate that we have solar panel data
            if not isinstance(solar_data, dict):
                logger.error(f"Expected solar data to be dict, got: {type(solar_data)}")
                return None
            
            # Convert epoch timestamp to datetime for sensor_timestamp
            # Handle large nanosecond timestamps by dividing appropriately
            timestamp_value = solar_data.get('timestamp', 0)
            try:
                if timestamp_value > 1e15:  # If timestamp looks like nanoseconds (very large)
                    sensor_timestamp = datetime.fromtimestamp(timestamp_value / 1_000_000_000)
                elif timestamp_value > 1e12:  # If timestamp looks like microseconds
                    sensor_timestamp = datetime.fromtimestamp(timestamp_value / 1_000_000)
                elif timestamp_value > 1e9:  # If timestamp looks like milliseconds
                    sensor_timestamp = datetime.fromtimestamp(timestamp_value / 1000)
                else:  # Assume seconds
                    sensor_timestamp = datetime.fromtimestamp(timestamp_value)
            except (ValueError, OSError) as ts_error:
                logger.warning(f"Invalid timestamp {timestamp_value}, using current time: {ts_error}")
                sensor_timestamp = datetime.now()
            
            # Get message timestamp
            message_timestamp = datetime.now()
            try:
                if hasattr(item, 'timestamp') and item.timestamp:
                    # item.timestamp is usually in milliseconds
                    message_timestamp = datetime.fromtimestamp(item.timestamp / 1000.0)
            except (ValueError, OSError, AttributeError) as msg_ts_error:
                logger.warning(f"Could not parse message timestamp, using current time: {msg_ts_error}")
                message_timestamp = datetime.now()
            
            # Map the data to our table schema with safe access and type conversion
            parsed_data = {
                'panel_id': str(solar_data.get('panel_id', '')),
                'location_id': str(solar_data.get('location_id', '')),
                'location_name': str(solar_data.get('location_name', '')),
                'latitude': float(solar_data.get('latitude', 0.0)),
                'longitude': float(solar_data.get('longitude', 0.0)),
                'timezone': int(solar_data.get('timezone', 0)),
                'power_output': float(solar_data.get('power_output', 0.0)),
                'unit_power': str(solar_data.get('unit_power', '')),
                'temperature': float(solar_data.get('temperature', 0.0)),
                'unit_temp': str(solar_data.get('unit_temp', '')),
                'irradiance': float(solar_data.get('irradiance', 0.0)),
                'unit_irradiance': str(solar_data.get('unit_irradiance', '')),
                'voltage': float(solar_data.get('voltage', 0.0)),
                'unit_voltage': str(solar_data.get('unit_voltage', '')),
                'current': float(solar_data.get('current', 0.0)),
                'unit_current': str(solar_data.get('unit_current', '')),
                'inverter_status': str(solar_data.get('inverter_status', '')),
                'sensor_timestamp': sensor_timestamp,
                'message_timestamp': message_timestamp,
                'kafka_topic': str(item.topic if hasattr(item, 'topic') else ''),
                'kafka_partition': int(item.partition if hasattr(item, 'partition') else 0),
                'kafka_offset': int(item.offset if hasattr(item, 'offset') else 0)
            }
            
            logger.debug(f"Successfully parsed data for panel: {parsed_data['panel_id']}")
            return parsed_data
            
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            logger.error(f"Message type: {type(item)}")
            logger.error(f"Available attributes: {dir(item)}")
            return None
    
    def _write_to_clickhouse(self, data):
        """Write parsed data to ClickHouse"""
        if not data:
            return
            
        client = self._get_client()
        
        # Define column names to ensure proper ordering
        column_names = [
            'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
            'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
            'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current',
            'unit_current', 'inverter_status', 'sensor_timestamp', 'message_timestamp',
            'kafka_topic', 'kafka_partition', 'kafka_offset'
        ]
        
        # Prepare data for insertion using dictionary format for clarity
        rows = []
        for parsed_item in data:
            if parsed_item:  # Skip None values from failed parsing
                # Create row as dictionary to ensure proper column mapping
                row = {
                    'panel_id': parsed_item['panel_id'],
                    'location_id': parsed_item['location_id'], 
                    'location_name': parsed_item['location_name'],
                    'latitude': parsed_item['latitude'],
                    'longitude': parsed_item['longitude'],
                    'timezone': parsed_item['timezone'],
                    'power_output': parsed_item['power_output'],
                    'unit_power': parsed_item['unit_power'],
                    'temperature': parsed_item['temperature'],
                    'unit_temp': parsed_item['unit_temp'],
                    'irradiance': parsed_item['irradiance'],
                    'unit_irradiance': parsed_item['unit_irradiance'],
                    'voltage': parsed_item['voltage'],
                    'unit_voltage': parsed_item['unit_voltage'],
                    'current': parsed_item['current'],
                    'unit_current': parsed_item['unit_current'],
                    'inverter_status': parsed_item['inverter_status'],
                    'sensor_timestamp': parsed_item['sensor_timestamp'],
                    'message_timestamp': parsed_item['message_timestamp'],
                    'kafka_topic': parsed_item['kafka_topic'],
                    'kafka_partition': parsed_item['kafka_partition'],
                    'kafka_offset': parsed_item['kafka_offset']
                }
                rows.append(row)
        
        if rows:
            try:
                # Log the data structure for debugging
                logger.info(f"Attempting to insert {len(rows)} rows")
                logger.info(f"Sample row: {rows[0] if rows else 'No rows'}")
                logger.info(f"Column names: {column_names}")
                logger.info(f"Number of columns: {len(column_names)}")
                
                # Convert rows to list of lists in the correct column order
                rows_as_lists = []
                for row_dict in rows:
                    row_list = [row_dict[col_name] for col_name in column_names]
                    rows_as_lists.append(row_list)
                
                # Insert using list format - more reliable
                client.insert('solar_panel_data', rows_as_lists, column_names=column_names)
                logger.info(f"Successfully inserted {len(rows_as_lists)} rows into ClickHouse")
            except Exception as insert_error:
                logger.error(f"Insert failed: {insert_error}")
                logger.error(f"Row data types: {[(k, type(v)) for k, v in rows[0].items()] if rows else 'No rows'}")
                # Try alternative insert method without column_names parameter
                try:
                    logger.info("Trying alternative insert method without column_names")
                    rows_as_lists = []
                    for row_dict in rows:
                        row_list = [row_dict[col_name] for col_name in column_names]
                        rows_as_lists.append(row_list)
                    client.insert('solar_panel_data', rows_as_lists)
                    logger.info(f"Successfully inserted {len(rows_as_lists)} rows using alternative method")
                except Exception as alt_error:
                    logger.error(f"Alternative insert also failed: {alt_error}")
                    raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to ClickHouse.
        Implements retry logic with exponential backoff.
        """
        attempts_remaining = 3
        backoff_delay = 1
        
        # Convert batch to list once to avoid re-iteration issues
        batch_items = list(batch)
        batch_count = len(batch_items)
        logger.info(f"Processing batch of {batch_count} messages")
        
        # Parse all messages in the batch
        parsed_data = []
        for item in batch_items:
            parsed_item = self._parse_message(item)
            parsed_data.append(parsed_item)
        
        # Count successful parses
        valid_parsed_data = [item for item in parsed_data if item is not None]
        logger.info(f"Successfully parsed {len(valid_parsed_data)} out of {len(parsed_data)} messages")
        
        while attempts_remaining:
            try:
                return self._write_to_clickhouse(parsed_data)
            except (ConnectionError, Exception) as e:
                logger.warning(f"Write attempt failed: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(backoff_delay)
                    backoff_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"All write attempts failed for batch")
                    raise
            except TimeoutError:
                # Server is busy, signal for backpressure
                # Get topic and partition from the first item in the batch if available
                topic_name = None
                partition_num = None
                if batch_items:
                    first_item = batch_items[0]
                    topic_name = getattr(first_item, 'topic', None)
                    partition_num = getattr(first_item, 'partition', None)
                    
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=topic_name,
                    partition=partition_num,
                )
        
        raise Exception("Failed to write batch to ClickHouse after all retries")


def main():
    """ Here we will set up our Application for solar panel data. """
    
    # Validate required environment variables
    required_env_vars = ['input']
    for var in required_env_vars:
        if var not in os.environ:
            logger.error(f"Required environment variable '{var}' is not set")
            raise ValueError(f"Missing required environment variable: {var}")
    
    logger.info(f"Using input topic: {os.environ['input']}")
    logger.info(f"ClickHouse host: {os.environ.get('CLICKHOUSE_HOST', 'localhost')}")

    # Setup necessary objects
    # Configure for Quix platform deployment
    app_config = {
        "consumer_group": "solar_data_clickhouse_sink",
        "auto_create_topics": True,
        "auto_offset_reset": "earliest"
    }
    
    # Set state directory for Quix platform if running in cloud environment
    if os.environ.get("QUIX_PORTAL_API") or "/app" in os.getcwd():
        app_config["state_dir"] = "/app/state"
    
    app = Application(**app_config)
    
    clickhouse_sink = ClickHouseSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Process the data stream (remove .print() for cleaner output)
    sdf = sdf.apply(lambda row: row)

    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(clickhouse_sink)

    # With our pipeline defined, now run the Application
    # Process 10 messages for initial testing
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()