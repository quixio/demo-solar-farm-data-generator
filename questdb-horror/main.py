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
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

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
        self._session = None
        self._table_created = False
        
    def setup(self):
        """Setup the connection to QuestDB. Table creation is deferred to first write."""
        # Get configuration from environment variables
        self._host = os.environ.get('QUESTDB_HOST', 'localhost')
        try:
            self._port = int(os.environ.get('QUESTDB_PORT', '9000'))
        except ValueError:
            self._port = 9000
            
        self._username = os.environ.get('QUESTDB_USERNAME', 'tsadmin')
        # Handle secret environment variable - it might be empty or contain the secret name
        password_env = os.environ.get('QUESTDB_PASSWORD', '')
        if password_env and password_env != 'QUESTDB_PW':  # If it's not the placeholder
            self._password = password_env
        else:
            self._password = None  # No authentication
        
        logger.info(f"Configuring QuestDB connection to {self._host}:{self._port} with user '{self._username}'")
        
        # Setup requests session with retry strategy
        self._setup_session()
        
        # Don't test connection during setup to avoid startup failures
        # Table creation will be attempted on first write
        
        # Call success callback if provided
        if hasattr(self, '_on_client_connect_success') and self._on_client_connect_success:
            self._on_client_connect_success()
    
    def _setup_session(self):
        """Setup HTTP session with retry strategy for better reliability."""
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST"],  # Updated for newer urllib3
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        
        self._session = requests.Session()
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
    
    def _test_connection(self):
        """Test if QuestDB is reachable with a simple query."""
        try:
            auth = None
            if self._username and self._password:
                auth = (self._username, self._password)
                
            response = self._session.get(
                f"http://{self._host}:{self._port}/exec",
                params={'query': 'SELECT 1;'},
                auth=auth,
                timeout=10
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def _create_table_if_not_exists(self):
        """Create the solar panel data table if it doesn't exist."""
        if self._table_created:
            return True
        
        # First test if QuestDB is reachable
        if not self._test_connection():
            logger.warning(f"QuestDB at {self._host}:{self._port} is not reachable. Will retry on next write attempt.")
            return False
            
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
            auth = None
            if self._username and self._password:
                auth = (self._username, self._password)
                
            response = self._session.post(
                f"http://{self._host}:{self._port}/exec",
                params={'query': create_table_sql},
                auth=auth,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully created/verified table '{self._table_name}'")
                self._table_created = True
                return True
            else:
                logger.error(f"Failed to create table: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Connection error while creating table: {e}")
            return False
    
    def _write_to_questdb(self, data):
        """Write batch data to QuestDB using INSERT statements."""
        if not data:
            return
        
        # Ensure table exists before attempting to insert
        if not self._create_table_if_not_exists():
            logger.warning("Could not create/verify table. Attempting to insert anyway...")
        
        # Prepare INSERT statements for batch insert
        insert_values = []
        
        for record in data:
            try:
                # Parse the solar panel data from the value field
                if isinstance(record, dict) and 'value' in record:
                    value = record['value']
                    if isinstance(value, str):
                        solar_data = json.loads(value)
                    elif isinstance(value, dict):
                        solar_data = value
                    else:
                        logger.warning(f"Unexpected value type in record: {type(value)}")
                        continue
                else:
                    logger.warning(f"Record missing 'value' field or not a dict: {record}")
                    continue
                
                # Validate that we have essential fields
                if not isinstance(solar_data, dict):
                    logger.warning(f"Solar data is not a dictionary: {solar_data}")
                    continue
                
                if not solar_data.get('panel_id'):
                    logger.warning(f"Record missing panel_id: {solar_data}")
                    continue
                
                # Convert the raw timestamp to a proper datetime
                # The timestamp appears to be in nanoseconds since epoch
                raw_ts = solar_data.get('timestamp', 0)
                try:
                    if raw_ts and raw_ts > 0:
                        # Convert nanoseconds to seconds and create datetime
                        ts_seconds = raw_ts / 1_000_000_000
                        timestamp = datetime.fromtimestamp(ts_seconds).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    else:
                        # Fallback to current time if timestamp is missing
                        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except (ValueError, OSError) as e:
                    logger.warning(f"Invalid timestamp {raw_ts}: {e}. Using current time.")
                    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                
                # Prepare values for insertion - handle quote escaping properly for all string fields
                def safe_str(value):
                    """Safely convert value to string and escape quotes."""
                    if value is None:
                        return ''
                    return str(value).replace("'", "''")
                
                panel_id = safe_str(solar_data.get('panel_id', ''))
                location_id = safe_str(solar_data.get('location_id', ''))
                location_name = safe_str(solar_data.get('location_name', ''))
                inverter_status = safe_str(solar_data.get('inverter_status', ''))
                
                def safe_float(value, default=0.0):
                    """Safely convert value to float."""
                    try:
                        return float(value if value is not None else default)
                    except (ValueError, TypeError):
                        return default
                
                def safe_int(value, default=0):
                    """Safely convert value to int."""
                    try:
                        return int(value if value is not None else default)
                    except (ValueError, TypeError):
                        return default
                
                values = (
                    f"'{timestamp}'",  # timestamp
                    f"'{panel_id}'",  # panel_id
                    f"'{location_id}'",  # location_id
                    f"'{location_name}'",  # location_name
                    str(safe_float(solar_data.get('latitude', 0))),  # latitude
                    str(safe_float(solar_data.get('longitude', 0))),  # longitude
                    str(safe_int(solar_data.get('timezone', 0))),  # timezone
                    str(safe_float(solar_data.get('power_output', 0))),  # power_output
                    str(safe_float(solar_data.get('temperature', 0))),  # temperature
                    str(safe_float(solar_data.get('irradiance', 0))),  # irradiance
                    str(safe_float(solar_data.get('voltage', 0))),  # voltage
                    str(safe_float(solar_data.get('current', 0))),  # current
                    f"'{inverter_status}'",  # inverter_status
                    str(safe_int(raw_ts))  # raw_timestamp
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
            auth = None
            if self._username and self._password:
                auth = (self._username, self._password)
                
            response = self._session.post(
                f"http://{self._host}:{self._port}/exec",
                params={'query': insert_sql},
                auth=auth,
                timeout=60
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
        attempts_remaining = 5  # Increased retry attempts
        data = [item.value for item in batch]
        
        # Debug: Print raw message structure for the first message (only once)
        if data and not hasattr(self, '_debug_logged'):
            logger.info(f"Raw message structure: {data[0]}")
            self._debug_logged = True
        
        while attempts_remaining:
            try:
                return self._write_to_questdb(data)
            except ConnectionError as e:
                logger.warning(f"Connection error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    # Exponential backoff with jitter: 2, 4, 8, 16 seconds
                    wait_time = (2 ** (5 - attempts_remaining)) + (time.time() % 2)
                    logger.info(f"Retrying in {wait_time:.1f} seconds... ({attempts_remaining} attempts remaining)")
                    time.sleep(wait_time)
                else:
                    # If all retries failed, use backpressure to retry later
                    logger.warning("All connection attempts failed, applying backpressure")
                    raise SinkBackpressureError(
                        retry_after=60.0,  # Wait longer before retrying
                        topic=batch.topic,
                        partition=batch.partition,
                    )
            except requests.exceptions.Timeout:
                logger.warning("Request timeout to QuestDB")
                # Server busy, apply backpressure
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )


def main():
    """ Here we will set up our Application for QuestDB solar data sink. """
    
    logger.info("Starting QuestDB Solar Data Sink application...")
    
    # Log environment configuration
    logger.info(f"Input topic: {os.environ.get('input', 'NOT SET')}")
    logger.info(f"QuestDB host: {os.environ.get('QUESTDB_HOST', 'questdb (default)')}")
    logger.info(f"QuestDB port: {os.environ.get('QUESTDB_PORT', '9000 (default)')}")
    logger.info(f"QuestDB username: {os.environ.get('QUESTDB_USERNAME', 'tsadmin (default)')}")
    
    # Give QuestDB time to start if services are starting together
    logger.info("Waiting a moment for services to initialize...")
    time.sleep(5)
    
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
    logger.info("Application will begin processing messages. QuestDB connection will be established on first write.")
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()