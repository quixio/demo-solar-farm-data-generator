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
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import ClickHouseError

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSolarDataSink(BatchingSink):
    """
    A custom sink for writing solar panel sensor data to ClickHouse database.
    
    This sink processes messages containing solar panel data from a Kafka topic
    and writes them to a ClickHouse table with proper schema mapping and
    timestamp conversion.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_name = "solar_panel_data"
        self._database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
        
    def setup(self):
        """Initialize ClickHouse client and create table if it doesn't exist"""
        try:
            # Parse port safely
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
            except ValueError:
                logger.warning("Invalid CLICKHOUSE_PORT, using default 8123")
                port = 8123
            
            # Initialize ClickHouse client
            self._client = get_client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=port,
                username=os.environ.get('CLICKHOUSE_USERNAME', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                database=self._database,
                secure=os.environ.get('CLICKHOUSE_SECURE', 'false').lower() == 'true'
            )
            
            # Test connection
            self._client.ping()
            logger.info(f"Successfully connected to ClickHouse at {os.environ.get('CLICKHOUSE_HOST', 'localhost')}:{port}")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self.on_client_connect_success:
                self.on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            if self.on_client_connect_failure:
                self.on_client_connect_failure(e)
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar panel data table with appropriate schema"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._database}.{self._table_name} (
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
            kafka_offset UInt64,
            kafka_partition UInt32
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, sensor_timestamp)
        """
        
        try:
            self._client.command(create_table_sql)
            logger.info(f"Table {self._database}.{self._table_name} created or already exists")
        except ClickHouseError as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_message(self, item):
        """Parse the Kafka message and extract solar panel data"""
        try:
            # Print raw message for debugging
            logger.info(f"Raw message: {item.value}")
            
            # The message structure has the actual data in the 'value' field as a JSON string
            message_data = item.value
            
            # Parse the nested JSON in the value field
            if isinstance(message_data.get('value'), str):
                solar_data = json.loads(message_data['value'])
            else:
                # If value is already parsed, use it directly
                solar_data = message_data.get('value', {})
            
            # Convert epoch timestamp to datetime (handle nanosecond precision)
            sensor_timestamp_ns = solar_data.get('timestamp', 0)
            if sensor_timestamp_ns > 0:
                # Convert from nanoseconds to seconds
                sensor_timestamp = datetime.fromtimestamp(sensor_timestamp_ns / 1e9)
            else:
                sensor_timestamp = datetime.now()
            
            # Parse message timestamp
            message_timestamp_str = message_data.get('dateTime', '')
            if message_timestamp_str:
                message_timestamp = datetime.fromisoformat(message_timestamp_str.replace('Z', '+00:00'))
            else:
                message_timestamp = datetime.now()
            
            # Map the data to table schema
            record = {
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
                'inverter_status': solar_data.get('inverter_status', ''),
                'sensor_timestamp': sensor_timestamp,
                'message_timestamp': message_timestamp,
                'kafka_offset': item.offset,
                'kafka_partition': message_data.get('partition', 0)
            }
            
            logger.info(f"Parsed solar data for panel {record['panel_id']}: power={record['power_output']}W, temp={record['temperature']}C")
            return record
            
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            logger.error(f"Message content: {item.value}")
            return None

    def write(self, batch: SinkBatch):
        """Write batch of solar panel data to ClickHouse"""
        attempts_remaining = 3
        
        # Parse all messages in the batch
        records = []
        for item in batch:
            record = self._parse_message(item)
            if record:
                records.append(record)
        
        if not records:
            logger.warning("No valid records to write in batch")
            return
            
        logger.info(f"Writing {len(records)} solar panel records to ClickHouse")
        
        while attempts_remaining:
            try:
                # Insert data into ClickHouse
                self._client.insert(
                    f"{self._database}.{self._table_name}",
                    records,
                    column_names=[
                        'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
                        'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
                        'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current',
                        'unit_current', 'inverter_status', 'sensor_timestamp', 'message_timestamp',
                        'kafka_offset', 'kafka_partition'
                    ]
                )
                logger.info(f"Successfully wrote {len(records)} records to ClickHouse")
                return
                
            except ClickHouseError as e:
                logger.error(f"ClickHouse error while writing batch: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                else:
                    # If it's a temporary issue, use backpressure
                    if "timeout" in str(e).lower() or "too many" in str(e).lower():
                        raise SinkBackpressureError(
                            retry_after=30.0,
                            topic=batch.topic,
                            partition=batch.partition,
                        )
            except Exception as e:
                logger.error(f"Unexpected error while writing to ClickHouse: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
        
        raise Exception(f"Failed to write batch to ClickHouse after 3 attempts")


def main():
    """Set up our Application for sinking solar panel data to ClickHouse."""

    # Setup necessary objects
    app = Application(
        consumer_group="solar_data_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest",
        # Configure batching for optimal performance
        commit_interval=5.0,
        commit_every=100,
    )
    
    # Initialize the ClickHouse sink
    clickhouse_sink = ClickHouseSolarDataSink()
    
    # Set up the input topic (using environment variable)
    input_topic = app.topic(
        name=os.environ["input"],
        value_deserializer='json'  # Ensure JSON deserialization
    )
    
    sdf = app.dataframe(topic=input_topic)

    # Add debug logging to show raw messages
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Sink the data to ClickHouse
    sdf.sink(clickhouse_sink)

    # Run the application with limited message count for testing
    logger.info("Starting solar data to ClickHouse sink application...")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()