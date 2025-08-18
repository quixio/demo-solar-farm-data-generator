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
logging.basicConfig(level=logging.INFO)
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
            
            self.client = clickhouse_connect.get_client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=port,
                username=os.environ.get('CLICKHOUSE_USER', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                database=os.environ.get('CLICKHOUSE_DATABASE', 'default')
            )
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
            # Debug: print raw message structure
            print(f"Raw message: {item}")
            
            # Based on the schema analysis, the message structure has a 'value' field
            # that contains a JSON string with the actual solar panel data
            message_data = item.value
            
            # Check if the message has the outer structure with 'value' field containing JSON string
            if isinstance(message_data, dict) and 'value' in message_data:
                # Parse the JSON string in the 'value' field
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
            if timestamp_value > 1e12:  # If timestamp looks like nanoseconds
                sensor_timestamp = datetime.fromtimestamp(timestamp_value / 1_000_000_000)
            elif timestamp_value > 1e9:  # If timestamp looks like milliseconds
                sensor_timestamp = datetime.fromtimestamp(timestamp_value / 1000)
            else:  # Assume seconds
                sensor_timestamp = datetime.fromtimestamp(timestamp_value)
            
            # Get message timestamp
            message_timestamp = datetime.now()
            if hasattr(item, 'timestamp') and item.timestamp:
                message_timestamp = datetime.fromtimestamp(item.timestamp / 1000.0)
            
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
                'kafka_topic': str(getattr(item, 'topic', '')),
                'kafka_partition': int(getattr(item, 'partition', 0)),
                'kafka_offset': int(getattr(item, 'offset', 0))
            }
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            logger.error(f"Message content: {item}")
            return None
    
    def _write_to_clickhouse(self, data):
        """Write parsed data to ClickHouse"""
        if not data:
            return
            
        client = self._get_client()
        
        # Prepare data for insertion
        rows = []
        for parsed_item in data:
            if parsed_item:  # Skip None values from failed parsing
                rows.append([
                    parsed_item['panel_id'],
                    parsed_item['location_id'], 
                    parsed_item['location_name'],
                    parsed_item['latitude'],
                    parsed_item['longitude'],
                    parsed_item['timezone'],
                    parsed_item['power_output'],
                    parsed_item['unit_power'],
                    parsed_item['temperature'],
                    parsed_item['unit_temp'],
                    parsed_item['irradiance'],
                    parsed_item['unit_irradiance'],
                    parsed_item['voltage'],
                    parsed_item['unit_voltage'],
                    parsed_item['current'],
                    parsed_item['unit_current'],
                    parsed_item['inverter_status'],
                    parsed_item['sensor_timestamp'],
                    parsed_item['message_timestamp'],
                    parsed_item['kafka_topic'],
                    parsed_item['kafka_partition'],
                    parsed_item['kafka_offset']
                ])
        
        if rows:
            client.insert('solar_panel_data', rows)
            logger.info(f"Successfully inserted {len(rows)} rows into ClickHouse")

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to ClickHouse.
        Implements retry logic with exponential backoff.
        """
        attempts_remaining = 3
        backoff_delay = 1
        
        # Parse all messages in the batch
        parsed_data = []
        for item in batch:
            parsed_item = self._parse_message(item)
            parsed_data.append(parsed_item)
        
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
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
        
        raise Exception("Failed to write batch to ClickHouse after all retries")


def main():
    """ Here we will set up our Application for solar panel data. """

    # Setup necessary objects
    app = Application(
        consumer_group="solar_data_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    clickhouse_sink = ClickHouseSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Debug: print received messages
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(clickhouse_sink)

    # With our pipeline defined, now run the Application
    # Process 10 messages for initial testing
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()