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
from clickhouse_driver.errors import Error as ClickHouseError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class ClickHouseSink(BatchingSink):
    """
    Custom ClickHouse sink for writing solar panel sensor data to ClickHouse database.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__()
        # Use the private attributes to avoid AttributeError
        self._on_client_connect_success = on_client_connect_success
        self._on_client_connect_failure = on_client_connect_failure
        self._client = None
        self._host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
        try:
            self._port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
        except ValueError:
            self._port = 9000
        self._database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
        self._username = os.environ.get('CLICKHOUSE_USERNAME', 'default')
        self._password = os.environ.get('CLICKHOUSE_PASSWORD', '')
        self._table = os.environ.get('CLICKHOUSE_TABLE', 'solar_data')
        
        logger.info(f"ClickHouse sink initialized with host: {self._host}:{self._port}, database: {self._database}, table: {self._table}")

    def setup(self):
        """Setup ClickHouse connection and create table if it doesn't exist."""
        try:
            self._client = Client(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._username,
                password=self._password
            )
            
            # Test connection
            self._client.execute('SELECT 1')
            logger.info("ClickHouse connection established successfully")
            
            # Create table if it doesn't exist
            self._create_table()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except ClickHouseError as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
            
    def _create_table(self):
        """Create the solar_data table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table} (
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
            timestamp UInt64,
            datetime DateTime,
            received_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, datetime)
        """
        
        try:
            self._client.execute(create_table_sql)
            logger.info(f"Table {self._table} created or verified successfully")
        except ClickHouseError as e:
            logger.error(f"Failed to create table: {e}")
            raise
            
    def _prepare_data(self, data):
        """Prepare data for insertion into ClickHouse."""
        prepared_rows = []
        
        for record in data:
            try:
                # Parse the value field if it's a string
                if isinstance(record, str):
                    record_data = json.loads(record)
                elif isinstance(record, dict):
                    # If it's already a dict, check if we need to parse the 'value' field
                    if 'value' in record and isinstance(record['value'], str):
                        record_data = json.loads(record['value'])
                    elif 'value' in record and isinstance(record['value'], dict):
                        record_data = record['value']
                    else:
                        record_data = record
                else:
                    logger.warning(f"Unexpected record type: {type(record)}")
                    continue
                    
                # Convert timestamp to datetime
                timestamp = record_data.get('timestamp', 0)
                # Convert from nanoseconds to seconds if timestamp is very large
                if timestamp > 1e12:  # Assume nanoseconds if larger than this
                    timestamp_seconds = timestamp / 1e9
                else:
                    timestamp_seconds = timestamp
                    
                dt = datetime.fromtimestamp(timestamp_seconds)
                
                # Prepare row for insertion
                row = (
                    record_data.get('panel_id', ''),
                    record_data.get('location_id', ''),
                    record_data.get('location_name', ''),
                    float(record_data.get('latitude', 0.0)),
                    float(record_data.get('longitude', 0.0)),
                    int(record_data.get('timezone', 0)),
                    float(record_data.get('power_output', 0.0)),
                    record_data.get('unit_power', ''),
                    float(record_data.get('temperature', 0.0)),
                    record_data.get('unit_temp', ''),
                    float(record_data.get('irradiance', 0.0)),
                    record_data.get('unit_irradiance', ''),
                    float(record_data.get('voltage', 0.0)),
                    record_data.get('unit_voltage', ''),
                    float(record_data.get('current', 0.0)),
                    record_data.get('unit_current', ''),
                    record_data.get('inverter_status', ''),
                    int(record_data.get('timestamp', 0)),
                    dt
                )
                prepared_rows.append(row)
                
            except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
                logger.error(f"Error processing record: {e}, record: {record}")
                continue
                
        return prepared_rows
        
    def _write_to_db(self, data):
        """Write data to ClickHouse database."""
        if not data:
            logger.info("No data to write")
            return
            
        prepared_data = self._prepare_data(data)
        
        if not prepared_data:
            logger.warning("No valid data to write after preparation")
            return
            
        insert_sql = f"""
        INSERT INTO {self._table} 
        (panel_id, location_id, location_name, latitude, longitude, timezone, 
         power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
         voltage, unit_voltage, current, unit_current, inverter_status, timestamp, datetime)
        VALUES
        """
        
        try:
            self._client.execute(insert_sql, prepared_data)
            logger.info(f"Successfully inserted {len(prepared_data)} records into ClickHouse")
        except ClickHouseError as e:
            logger.error(f"Failed to insert data into ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of solar panel data to ClickHouse.
        Implements retry logic with exponential backoff.
        """
        attempts_remaining = 3
        data = [item.value for item in batch]
        
        logger.info(f"Writing batch of {len(data)} records to ClickHouse")
        
        while attempts_remaining:
            try:
                return self._write_to_db(data)
            except (ConnectionError, ClickHouseError) as e:
                logger.warning(f"Connection/ClickHouse error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    wait_time = 2 ** (3 - attempts_remaining)  # Exponential backoff: 2, 4, 8 seconds
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retry attempts reached")
            except TimeoutError as e:
                logger.warning(f"Timeout error: {e}, using backpressure")
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
        
        raise Exception("Failed to write to ClickHouse after all retry attempts")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="solar_data_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize ClickHouse sink
    clickhouse_sink = ClickHouseSink(
        on_client_connect_success=lambda: logger.info("ClickHouse client connected successfully"),
        on_client_connect_failure=lambda error: logger.error(f"ClickHouse client connection failed: {error}")
    )
    
    # Setup input topic with JSON deserialization
    input_topic = app.topic(
        name=os.environ["input"],
        value_deserializer="json"
    )
    
    sdf = app.dataframe(topic=input_topic)

    # Add debugging to show raw message structure
    def debug_message(row):
        print(f"Raw message: {row}")
        return row

    # Process the data
    sdf = sdf.apply(debug_message)
    
    # Print metadata for debugging
    sdf = sdf.print(metadata=True)

    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(clickhouse_sink)

    # With our pipeline defined, now run the Application
    # Process only 10 messages for testing
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()