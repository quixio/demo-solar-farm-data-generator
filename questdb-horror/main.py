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
import requests

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuestDBSink(BatchingSink):
    """
    A sink that writes solar panel sensor data to QuestDB using HTTP REST API.
    
    QuestDB is a time-series database optimized for high-performance ingestion
    and real-time analytics of time-series data.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._connection = None
        self._host = None
        self._port = None
        self._username = None
        self._password = None
        self._table_name = "solar_panel_data"
        
    def setup(self):
        """Setup the connection to QuestDB and create the table if it doesn't exist."""
        # Get configuration from environment variables
        self._host = os.environ.get('QUESTDB_HOST', 'localhost')
        try:
            self._port = int(os.environ.get('QUESTDB_PORT', '9000'))
        except ValueError:
            self._port = 9000
            
        self._username = os.environ.get('QUESTDB_USERNAME', 'admin')
        self._password = os.environ.get('QUESTDB_PASSWORD', 'quest')
        
        logger.info(f"Connecting to QuestDB at {self._host}:{self._port}")
        
        # Test connection and create table
        self._create_table_if_not_exists()
        
        # Call success callback if provided
        if hasattr(self, '_on_client_connect_success') and self._on_client_connect_success:
            self._on_client_connect_success()
    
    def _create_table_if_not_exists(self):
        """Create the solar panel data table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            timestamp TIMESTAMP,
            panel_id SYMBOL,
            location_id SYMBOL,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output DOUBLE,
            temperature DOUBLE,
            irradiance DOUBLE,
            voltage DOUBLE,
            current DOUBLE,
            inverter_status SYMBOL,
            raw_timestamp LONG
        ) timestamp(timestamp) PARTITION BY DAY WAL;
        """
        
        try:
            response = requests.post(
                f"http://{self._host}:{self._port}/exec",
                params={'query': create_table_sql},
                auth=(self._username, self._password) if self._username != 'admin' or self._password != 'quest' else None,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully created/verified table '{self._table_name}'")
            else:
                logger.error(f"Failed to create table: {response.status_code} - {response.text}")
                raise ConnectionError(f"Failed to create table: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error while creating table: {e}")
            # Call failure callback if provided
            if hasattr(self, '_on_client_connect_failure') and self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise ConnectionError(f"Failed to connect to QuestDB: {e}")
    
    def _write_to_questdb(self, data):
        """Write batch data to QuestDB using INSERT statements."""
        if not data:
            return
        
        # Prepare INSERT statements for batch insert
        insert_values = []
        
        for record in data:
            try:
                # Parse the solar panel data from the value field
                solar_data = json.loads(record['value']) if isinstance(record['value'], str) else record['value']
                
                # Convert the raw timestamp to a proper datetime
                # The timestamp appears to be in nanoseconds since epoch
                raw_ts = solar_data.get('timestamp', 0)
                if raw_ts > 0:
                    # Convert nanoseconds to seconds and create datetime
                    ts_seconds = raw_ts / 1_000_000_000
                    timestamp = datetime.fromtimestamp(ts_seconds).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                else:
                    # Fallback to current time if timestamp is missing
                    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                
                # Prepare values for insertion - handle quote escaping properly for all string fields
                panel_id = solar_data.get('panel_id', '').replace("'", "''")
                location_id = solar_data.get('location_id', '').replace("'", "''")
                location_name = solar_data.get('location_name', '').replace("'", "''")
                inverter_status = solar_data.get('inverter_status', '').replace("'", "''")
                
                values = (
                    f"'{timestamp}'",  # timestamp
                    f"'{panel_id}'",  # panel_id
                    f"'{location_id}'",  # location_id
                    f"'{location_name}'",  # location_name (escape quotes)
                    str(solar_data.get('latitude', 0)),  # latitude
                    str(solar_data.get('longitude', 0)),  # longitude
                    str(solar_data.get('timezone', 0)),  # timezone
                    str(solar_data.get('power_output', 0)),  # power_output
                    str(solar_data.get('temperature', 0)),  # temperature
                    str(solar_data.get('irradiance', 0)),  # irradiance
                    str(solar_data.get('voltage', 0)),  # voltage
                    str(solar_data.get('current', 0)),  # current
                    f"'{inverter_status}'",  # inverter_status
                    str(raw_ts)  # raw_timestamp
                )
                
                insert_values.append(f"({','.join(values)})")
                
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing solar data: {e} - Record: {record}")
                continue
        
        if not insert_values:
            logger.warning("No valid records to insert")
            return
        
        # Create the INSERT statement
        insert_sql = f"""
        INSERT INTO {self._table_name} 
        (timestamp, panel_id, location_id, location_name, latitude, longitude, 
         timezone, power_output, temperature, irradiance, voltage, current, 
         inverter_status, raw_timestamp)
        VALUES {','.join(insert_values)};
        """
        
        # Execute the insert
        try:
            response = requests.post(
                f"http://{self._host}:{self._port}/exec",
                params={'query': insert_sql},
                auth=(self._username, self._password) if self._username != 'admin' or self._password != 'quest' else None,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully inserted {len(insert_values)} records into QuestDB")
            else:
                logger.error(f"Failed to insert data: {response.status_code} - {response.text}")
                raise ConnectionError(f"Failed to insert data: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error while inserting data: {e}")
            raise ConnectionError(f"Failed to write to QuestDB: {e}")

    def write(self, batch: SinkBatch):
        """
        Write a batch of messages to QuestDB.
        Implements retry logic with exponential backoff for connection issues.
        """
        attempts_remaining = 3
        data = [item.value for item in batch]
        
        # Debug: Print raw message structure for the first message
        if data:
            logger.info(f"Raw message structure: {data[0]}")
        
        while attempts_remaining:
            try:
                return self._write_to_questdb(data)
            except ConnectionError as e:
                logger.warning(f"Connection error: {e}")
                # Maybe we just failed to connect, do a short wait and try again
                attempts_remaining -= 1
                if attempts_remaining:
                    wait_time = 3 * (4 - attempts_remaining)  # Exponential backoff: 3, 6 seconds
                    logger.info(f"Retrying in {wait_time} seconds... ({attempts_remaining} attempts remaining)")
                    time.sleep(wait_time)
            except requests.exceptions.Timeout:
                logger.warning("Request timeout to QuestDB")
                # Maybe the server is busy, do a sanctioned extended pause
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
        
        error_msg = f"Failed to write to QuestDB after all retry attempts"
        logger.error(error_msg)
        raise Exception(error_msg)


def main():
    """ Here we will set up our Application for QuestDB solar data sink. """

    # Setup necessary objects
    app = Application(
        consumer_group="questdb_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize QuestDB sink
    questdb_sink = QuestDBSink()
    
    # Setup input topic - expects JSON serialized messages
    input_topic = app.topic(
        name=os.environ["input"],
        value_deserializer="json"
    )
    
    sdf = app.dataframe(topic=input_topic)

    # Debug: Print raw message for troubleshooting
    sdf = sdf.apply(lambda row: logger.info(f"Processing message: {row}") or row)

    # Process the message and extract solar data
    def process_solar_data(message):
        """Process the incoming message and prepare it for QuestDB insertion."""
        try:
            # The message structure from schema shows data is in the 'value' field
            if isinstance(message, dict) and 'value' in message:
                # The 'value' field contains JSON-encoded solar panel data
                if isinstance(message['value'], str):
                    solar_data = json.loads(message['value'])
                else:
                    solar_data = message['value']
                
                # Return the original message format for the sink to process
                return message
            else:
                logger.warning(f"Unexpected message format: {message}")
                return message
                
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error processing solar data: {e} - Message: {message}")
            return message

    # Apply processing and sink to QuestDB
    sdf = sdf.apply(process_solar_data)
    sdf.sink(questdb_sink)

    # With our pipeline defined, now run the Application
    logger.info("Starting solar data sink to QuestDB...")
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()