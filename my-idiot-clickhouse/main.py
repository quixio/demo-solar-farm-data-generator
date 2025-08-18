# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import logging
from clickhouse_driver import Client
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSolarDataSink(BatchingSink):
    """
    Custom ClickHouse sink for solar panel sensor data.
    
    This sink receives solar panel data from a Kafka topic and writes it to ClickHouse.
    The incoming messages have a nested structure where the actual sensor data is 
    contained in a JSON string within the 'value' field.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self.table_name = "solar_sensor_data"
        
    def setup(self):
        """Initialize ClickHouse connection and create table if not exists."""
        try:
            # Get connection parameters from environment variables
            host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
            except ValueError:
                port = 9000
                
            database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
            user = os.environ.get('CLICKHOUSE_USER', 'default')
            password = os.environ.get('CLICKHOUSE_PASSWORD', '')
            
            logger.info(f"Connecting to ClickHouse at {host}:{port}")
            
            # Create ClickHouse client
            self.client = Client(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            # Test connection
            self.client.execute('SELECT 1')
            logger.info("ClickHouse connection established successfully")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Failed to setup ClickHouse connection: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar sensor data table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            message_timestamp DateTime64(3),
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
            kafka_partition Int32,
            kafka_offset Int64
        ) ENGINE = MergeTree()
        ORDER BY (message_timestamp, panel_id)
        """
        
        try:
            self.client.execute(create_table_sql)
            logger.info(f"Table {self.table_name} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_message(self, item):
        """Parse the nested message structure to extract solar sensor data."""
        try:
            # The message structure has the actual data in the 'value' field as a JSON string
            if hasattr(item, 'value') and isinstance(item.value, dict):
                # First check if this is already the parsed solar data
                if 'panel_id' in item.value:
                    return item.value
                
                # Otherwise, look for the nested 'value' field containing JSON string
                if 'value' in item.value:
                    sensor_data = json.loads(item.value['value'])
                    
                    # Add metadata from the outer message
                    sensor_data['message_datetime'] = item.value.get('dateTime')
                    sensor_data['kafka_partition'] = item.value.get('partition', 0)
                    sensor_data['kafka_offset'] = item.value.get('offset', 0)
                    
                    return sensor_data
            
            logger.warning(f"Unexpected message structure: {item.value}")
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from message value: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return None

    def _write_to_clickhouse(self, data_rows):
        """Write solar sensor data to ClickHouse."""
        if not data_rows:
            logger.warning("No data to write to ClickHouse")
            return
            
        insert_sql = f"""
        INSERT INTO {self.table_name} 
        (message_timestamp, panel_id, location_id, location_name, latitude, longitude, 
         timezone, power_output, unit_power, temperature, unit_temp, irradiance, 
         unit_irradiance, voltage, unit_voltage, current, unit_current, 
         inverter_status, sensor_timestamp, kafka_partition, kafka_offset)
        VALUES
        """
        
        try:
            self.client.execute(insert_sql, data_rows)
            logger.info(f"Successfully wrote {len(data_rows)} rows to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to write to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batches of solar sensor data to ClickHouse.
        
        This method processes each message in the batch, parses the nested JSON structure,
        and writes the sensor data to ClickHouse with proper error handling and retries.
        """
        attempts_remaining = 3
        
        # Parse and prepare data for ClickHouse
        data_rows = []
        
        for item in batch:
            # Debug: Print raw message structure
            print(f"Raw message: {item.value}")
            
            sensor_data = self._parse_message(item)
            if sensor_data:
                try:
                    # Convert message timestamp to DateTime
                    message_dt = datetime.fromisoformat(
                        sensor_data.get('message_datetime', '').replace('Z', '+00:00')
                    ) if sensor_data.get('message_datetime') else datetime.utcnow()
                    
                    data_row = (
                        message_dt,
                        sensor_data.get('panel_id', ''),
                        sensor_data.get('location_id', ''),
                        sensor_data.get('location_name', ''),
                        sensor_data.get('latitude', 0.0),
                        sensor_data.get('longitude', 0.0),
                        sensor_data.get('timezone', 0),
                        sensor_data.get('power_output', 0.0),
                        sensor_data.get('unit_power', ''),
                        sensor_data.get('temperature', 0.0),
                        sensor_data.get('unit_temp', ''),
                        sensor_data.get('irradiance', 0.0),
                        sensor_data.get('unit_irradiance', ''),
                        sensor_data.get('voltage', 0.0),
                        sensor_data.get('unit_voltage', ''),
                        sensor_data.get('current', 0.0),
                        sensor_data.get('unit_current', ''),
                        sensor_data.get('inverter_status', ''),
                        sensor_data.get('timestamp', 0),
                        sensor_data.get('kafka_partition', 0),
                        sensor_data.get('kafka_offset', 0)
                    )
                    data_rows.append(data_row)
                    
                except Exception as e:
                    logger.error(f"Error preparing data row: {e}")
                    continue
        
        # Attempt to write data with retry logic
        while attempts_remaining and data_rows:
            try:
                return self._write_to_clickhouse(data_rows)
            except ConnectionError as e:
                logger.warning(f"ClickHouse connection error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except Exception as e:
                # For other errors that might be temporary (like server busy)
                if "timeout" in str(e).lower() or "busy" in str(e).lower():
                    logger.warning(f"ClickHouse server busy or timeout: {e}")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # For other errors, reduce retry count and continue
                    logger.error(f"Error writing to ClickHouse: {e}")
                    attempts_remaining -= 1
                    if attempts_remaining:
                        time.sleep(3)
                    
        if data_rows and not attempts_remaining:
            raise Exception("Failed to write to ClickHouse after multiple attempts")


def main():
    """Set up and run the solar data to ClickHouse sink application."""

    # Setup necessary objects
    app = Application(
        consumer_group="solar_data_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize the ClickHouse sink and setup connection
    clickhouse_sink = ClickHouseSolarDataSink()
    clickhouse_sink.setup()
    
    # Configure the input topic - expecting JSON messages
    input_topic = app.topic(
        name=os.environ["input"],
        value_deserializer="json"
    )
    sdf = app.dataframe(topic=input_topic)

    # Add some basic logging/debugging - print messages with metadata
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Sink data to ClickHouse
    sdf.sink(clickhouse_sink)

    # Run the application for testing (process 10 messages with 20s timeout)
    logger.info("Starting solar data sink application...")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()