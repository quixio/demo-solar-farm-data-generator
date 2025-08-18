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
import clickhouse_connect

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseSolarSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    Processes messages containing solar panel telemetry and inserts them into ClickHouse.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self.host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
        
        # Handle port conversion safely
        try:
            self.port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
        except ValueError:
            logger.warning("Invalid CLICKHOUSE_PORT value, using default 8123")
            self.port = 8123
            
        self.username = os.environ.get('CLICKHOUSE_USERNAME', 'default')
        self.password = os.environ.get('CLICKHOUSE_PASSWORD', '')
        self.database = os.environ.get('CLICKHOUSE_DATABASE', 'solar_farm')
        self.table = os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')
        
        logger.info(f"ClickHouse configuration: {self.host}:{self.port}, database: {self.database}, table: {self.table}")
        
        # Initialize connection and create table
        self._initialize_connection()
        self._create_table_if_not_exists()

    def _initialize_connection(self):
        """Initialize ClickHouse connection"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database
            )
            logger.info("ClickHouse connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create the solar panel data table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            message_datetime DateTime64(3),
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
        ORDER BY (message_datetime, panel_id)
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Table {self.table} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_solar_message(self, message):
        """Parse solar panel message and extract data"""
        try:
            # Print raw message for debugging
            print(f'Raw message: {message}')
            
            # The message structure has a 'value' field that contains JSON string
            if 'value' in message:
                # Parse the JSON string in the value field
                solar_data = json.loads(message['value'])
                
                # Convert message datetime to proper DateTime
                message_dt = datetime.fromisoformat(message['dateTime'].replace('Z', '+00:00'))
                
                # Extract solar panel data
                parsed_data = {
                    'message_datetime': message_dt,
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
                    'sensor_timestamp': int(solar_data.get('timestamp', 0)),
                    'kafka_partition': int(message.get('partition', 0)),
                    'kafka_offset': int(message.get('offset', 0))
                }
                
                logger.info(f"Parsed solar data for panel: {parsed_data['panel_id']}")
                return parsed_data
                
            else:
                logger.error("Message does not contain 'value' field")
                return None
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON in message value: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing solar message: {e}")
            return None

    def _write_to_clickhouse(self, data):
        """Write parsed data to ClickHouse"""
        try:
            if not data:
                logger.warning("No data to write")
                return
                
            # Prepare data for insertion
            rows = []
            for item in data:
                if item:  # Only add valid parsed items
                    rows.append([
                        item['message_datetime'],
                        item['panel_id'],
                        item['location_id'],
                        item['location_name'],
                        item['latitude'],
                        item['longitude'],
                        item['timezone'],
                        item['power_output'],
                        item['unit_power'],
                        item['temperature'],
                        item['unit_temp'],
                        item['irradiance'],
                        item['unit_irradiance'],
                        item['voltage'],
                        item['unit_voltage'],
                        item['current'],
                        item['unit_current'],
                        item['inverter_status'],
                        item['sensor_timestamp'],
                        item['kafka_partition'],
                        item['kafka_offset']
                    ])
            
            if rows:
                column_names = [
                    'message_datetime', 'panel_id', 'location_id', 'location_name',
                    'latitude', 'longitude', 'timezone', 'power_output', 'unit_power',
                    'temperature', 'unit_temp', 'irradiance', 'unit_irradiance',
                    'voltage', 'unit_voltage', 'current', 'unit_current',
                    'inverter_status', 'sensor_timestamp', 'kafka_partition', 'kafka_offset'
                ]
                
                self.client.insert(self.table, rows, column_names=column_names)
                logger.info(f"Successfully inserted {len(rows)} rows into {self.table}")
            else:
                logger.warning("No valid rows to insert")
                
        except Exception as e:
            logger.error(f"Failed to write to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of solar panel data to ClickHouse.
        Implements retry logic with exponential backoff.
        """
        attempts_remaining = 3
        # Extract and parse message data
        parsed_data = []
        for item in batch:
            parsed_item = self._parse_solar_message(item.value)
            if parsed_item:
                parsed_data.append(parsed_item)
        
        while attempts_remaining:
            try:
                return self._write_to_clickhouse(parsed_data)
            except ConnectionError as e:
                logger.warning(f"Connection error, attempts remaining: {attempts_remaining - 1}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                else:
                    logger.error("Failed to connect to ClickHouse after all retries")
                    raise
            except Exception as e:
                if "timeout" in str(e).lower():
                    logger.warning("Database timeout, triggering backpressure")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    logger.error(f"Unexpected error: {e}")
                    raise
        
        raise Exception("Failed to write to ClickHouse after all retry attempts")


def main():
    """ Setup and run the solar data ClickHouse sink application """
    
    logger.info("Starting Solar Data ClickHouse Sink Application")
    
    # Setup application
    app = Application(
        consumer_group="solar_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create ClickHouse sink
    clickhouse_sink = ClickHouseSolarSink()
    
    # Setup input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)
    
    # Apply transformations and debugging
    sdf = sdf.apply(lambda row: row).print(metadata=True)
    
    # Sink to ClickHouse
    sdf.sink(clickhouse_sink)
    
    logger.info("Starting message processing...")
    
    # Run application with count limit for testing
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()