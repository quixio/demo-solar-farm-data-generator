# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import logging
import psycopg2
from datetime import datetime, timezone

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TimescaleSink(BatchingSink):
    """
    A custom sink for writing solar panel sensor data to TimescaleDB.
    
    This sink processes solar panel data from Kafka topics and writes it to 
    a TimescaleDB hypertable for time-series data storage and analysis.
    """

    def __init__(self):
        super().__init__()
        self._connection = None
        self._connection_params = self._get_connection_params()

    def _get_connection_params(self):
        """Get database connection parameters from environment variables"""
        try:
            port = int(os.environ.get('DB_PORT', '5432'))
        except ValueError:
            port = 5432
        
        params = {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': port,
            'database': os.environ.get('DB_NAME', 'solar_data'),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', '')
        }
        
        # Log connection parameters (without password)
        logger.info(f"Database connection parameters:")
        logger.info(f"  host: {params['host']}")
        logger.info(f"  port: {params['port']}")
        logger.info(f"  database: {params['database']}")
        logger.info(f"  user: {params['user']}")
        logger.info(f"  password: {'SET' if params['password'] else 'NOT_SET'}")
            
        return params

    def add(self, value, key, timestamp, headers):
        """Override add method to establish connection on first call"""
        # Lazy initialization of database connection
        if self._connection is None:
            try:
                logger.info("Establishing database connection...")
                self._connection = psycopg2.connect(**self._connection_params)
                self._connection.autocommit = False
                logger.info("Successfully connected to TimescaleDB")
                
                # Create the table and hypertable if they don't exist
                self._create_table()
                
            except Exception as e:
                logger.error(f"Failed to connect to TimescaleDB: {e}")
                raise
        
        # Call the parent add method
        return super().add(value, key, timestamp, headers)
    
    def _on_client_connect_success(self):
        """Callback for successful database connection - no-op implementation"""
        pass
    
    def _on_client_connect_failure(self, error):
        """Callback for failed database connection - no-op implementation"""
        pass

    def _create_table(self):
        """Create the solar_data table and hypertable if they don't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS solar_data (
            timestamp TIMESTAMPTZ NOT NULL,
            panel_id TEXT NOT NULL,
            location_id TEXT,
            location_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone INTEGER,
            power_output DOUBLE PRECISION,
            unit_power TEXT,
            temperature DOUBLE PRECISION,
            unit_temp TEXT,
            irradiance DOUBLE PRECISION,
            unit_irradiance TEXT,
            voltage DOUBLE PRECISION,
            unit_voltage TEXT,
            current DOUBLE PRECISION,
            unit_current TEXT,
            inverter_status TEXT,
            original_timestamp BIGINT,
            message_datetime TIMESTAMPTZ
        );
        """
        
        # Check if hypertable exists
        check_hypertable_sql = """
        SELECT EXISTS (
            SELECT 1 FROM timescaledb_information.hypertables 
            WHERE hypertable_name = 'solar_data'
        );
        """
        
        create_hypertable_sql = """
        SELECT create_hypertable('solar_data', 'timestamp', if_not_exists => TRUE);
        """
        
        try:
            with self._connection.cursor() as cursor:
                # Create table
                cursor.execute(create_table_sql)
                
                # Check if hypertable exists, create if not
                cursor.execute(check_hypertable_sql)
                hypertable_exists = cursor.fetchone()[0]
                
                if not hypertable_exists:
                    cursor.execute(create_hypertable_sql)
                    logger.info("Created TimescaleDB hypertable for solar_data")
                
                self._connection.commit()
                logger.info("Database table setup completed")
                
        except Exception as e:
            self._connection.rollback()
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_solar_data(self, message):
        """Parse the solar data from the Kafka message"""
        try:
            # Handle two possible message formats:
            # 1. Direct solar data (what we're actually receiving)
            # 2. Wrapped format with 'value' field containing JSON string (from schema docs)
            
            if isinstance(message, dict):
                # Check if this is already direct solar panel data
                if 'panel_id' in message:
                    solar_data = message
                    message_datetime = None
                    logger.info("Processing direct solar panel data format")
                elif 'value' in message:
                    # Parse the JSON string in the value field
                    try:
                        solar_data = json.loads(message['value']) if isinstance(message['value'], str) else message['value']
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON from value field: {e}")
                        logger.error(f"Value content: {message['value']}")
                        return None
                    
                    # Convert message datetime from ISO format if present
                    message_datetime = None
                    if 'dateTime' in message:
                        try:
                            message_datetime = datetime.fromisoformat(message['dateTime'].replace('Z', '+00:00'))
                        except ValueError as e:
                            logger.warning(f"Failed to parse dateTime: {e}")
                    logger.info("Processing wrapped solar data format with 'value' field")
                else:
                    logger.warning(f"Unexpected message format - no 'panel_id' or 'value' field found: {list(message.keys())}")
                    return None
                
                # Convert the original timestamp (looks like nanoseconds) to a proper datetime
                # The timestamp appears to be in nanoseconds, convert to seconds
                original_timestamp = solar_data.get('timestamp', 0)
                
                # Use message datetime as the primary timestamp, fallback to converting original timestamp or current time
                if message_datetime:
                    timestamp = message_datetime
                elif original_timestamp and original_timestamp > 0:
                    # Convert from nanoseconds to seconds and create datetime with UTC timezone
                    timestamp_seconds = original_timestamp / 1_000_000_000
                    timestamp = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
                else:
                    timestamp = datetime.now(timezone.utc)
                
                # Validate essential fields
                if not solar_data.get('panel_id'):
                    logger.warning("Missing panel_id in solar data - skipping record")
                    return None
                
                return {
                    'timestamp': timestamp,
                    'panel_id': solar_data.get('panel_id'),
                    'location_id': solar_data.get('location_id'),
                    'location_name': solar_data.get('location_name'),
                    'latitude': solar_data.get('latitude'),
                    'longitude': solar_data.get('longitude'),
                    'timezone': solar_data.get('timezone'),
                    'power_output': solar_data.get('power_output'),
                    'unit_power': solar_data.get('unit_power'),
                    'temperature': solar_data.get('temperature'),
                    'unit_temp': solar_data.get('unit_temp'),
                    'irradiance': solar_data.get('irradiance'),
                    'unit_irradiance': solar_data.get('unit_irradiance'),
                    'voltage': solar_data.get('voltage'),
                    'unit_voltage': solar_data.get('unit_voltage'),
                    'current': solar_data.get('current'),
                    'unit_current': solar_data.get('unit_current'),
                    'inverter_status': solar_data.get('inverter_status'),
                    'original_timestamp': original_timestamp,
                    'message_datetime': message_datetime
                }
            else:
                logger.warning(f"Unexpected message format: {message}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to parse solar data: {e}")
            logger.error(f"Raw message: {message}")
            return None

    def _write_to_db(self, data):
        """Write batch of parsed solar data to TimescaleDB"""
        if not data:
            return
            
        insert_sql = """
        INSERT INTO solar_data (
            timestamp, panel_id, location_id, location_name, latitude, longitude,
            timezone, power_output, unit_power, temperature, unit_temp, irradiance,
            unit_irradiance, voltage, unit_voltage, current, unit_current,
            inverter_status, original_timestamp, message_datetime
        ) VALUES (
            %(timestamp)s, %(panel_id)s, %(location_id)s, %(location_name)s,
            %(latitude)s, %(longitude)s, %(timezone)s, %(power_output)s,
            %(unit_power)s, %(temperature)s, %(unit_temp)s, %(irradiance)s,
            %(unit_irradiance)s, %(voltage)s, %(unit_voltage)s, %(current)s,
            %(unit_current)s, %(inverter_status)s, %(original_timestamp)s,
            %(message_datetime)s
        )
        """
        
        try:
            with self._connection.cursor() as cursor:
                cursor.executemany(insert_sql, data)
                self._connection.commit()
                logger.info(f"Successfully inserted {len(data)} solar data records")
                if data:
                    sample_panel = data[0].get('panel_id', 'unknown')
                    logger.info(f"Sample panel inserted: {sample_panel}")
                
        except Exception as e:
            self._connection.rollback()
            logger.error(f"Failed to insert data: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of messages to TimescaleDB.
        
        This method processes incoming Kafka messages, parses the solar data,
        and writes it to the TimescaleDB hypertable with retry logic and
        proper error handling.
        """
        logger.info(f"write() called with batch containing {len(batch)} messages")
        
        # Ensure connection is established (should be done in add(), but double-check)
        if self._connection is None:
            logger.info("Connection not established yet, establishing now...")
            self._connection = psycopg2.connect(**self._connection_params)
            self._connection.autocommit = False
            logger.info("Successfully connected to TimescaleDB in write()")
            self._create_table()
        
        attempts_remaining = 3
        
        # Parse the solar data from each message
        parsed_data = []
        logger.info(f"Parsing {len(batch)} messages from batch...")
        
        for i, item in enumerate(batch):
            logger.info(f"Processing message {i+1}/{len(batch)}")
            logger.info(f"Message value type: {type(item.value)}")
            logger.info(f"Message value preview: {str(item.value)[:200]}...")
            
            parsed = self._parse_solar_data(item.value)
            if parsed:
                parsed_data.append(parsed)
                logger.info(f"Successfully parsed message {i+1}: panel_id={parsed.get('panel_id')}")
            else:
                logger.warning(f"Skipping invalid message {i+1}: {str(item.value)[:100]}...")
        
        if not parsed_data:
            logger.warning("No valid data to write in this batch")
            return
        else:
            logger.info(f"Successfully parsed {len(parsed_data)} messages, writing to database...")
            
        while attempts_remaining:
            try:
                return self._write_to_db(parsed_data)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                # Connection issues - try to reconnect
                logger.warning(f"Connection error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    try:
                        if self._connection:
                            self._connection.close()
                        self._connection = psycopg2.connect(**self._connection_params)
                        self._connection.autocommit = False
                        time.sleep(3)
                    except Exception as conn_e:
                        logger.error(f"Reconnection failed: {conn_e}")
                        if not attempts_remaining:
                            raise
            except psycopg2.errors.AdminShutdown:
                # Database is being shut down - use backpressure
                logger.warning("Database is shutting down, applying backpressure")
                raise SinkBackpressureError(
                    retry_after=60.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            except Exception as e:
                logger.error(f"Unexpected error writing to database: {e}")
                raise
                
        raise Exception("Failed to write to TimescaleDB after 3 attempts")

    def close(self):
        """Close the database connection"""
        if self._connection:
            try:
                self._connection.close()
                logger.info("TimescaleDB connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
            finally:
                self._connection = None


def main():
    """Set up the Quix Streams Application for solar data processing."""
    
    logger.info("Starting TimescaleDB solar data sink application...")
    logger.info(f"Environment variables:")
    logger.info(f"- input topic: {os.environ.get('input', 'NOT_SET')}")
    logger.info(f"- DB_HOST: {os.environ.get('DB_HOST', 'NOT_SET')}")
    logger.info(f"- DB_PORT: {os.environ.get('DB_PORT', 'NOT_SET')}")
    logger.info(f"- DB_NAME: {os.environ.get('DB_NAME', 'NOT_SET')}")
    logger.info(f"- DB_USER: {os.environ.get('DB_USER', 'NOT_SET')}")
    logger.info(f"- DB_PASSWORD: {'SET' if os.environ.get('DB_PASSWORD') else 'NOT_SET'}")

    # Setup necessary objects
    app = Application(
        consumer_group="timescale_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create TimescaleDB sink
    logger.info("Creating TimescaleDB sink...")
    timescale_sink = TimescaleSink()
    
    # Get input topic from environment variable
    logger.info(f"Setting up input topic: {os.environ['input']}")
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Apply minimal transformations - just pass through and add debug printing
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Sink the data to TimescaleDB
    logger.info("Connecting sink to dataframe...")
    sdf.sink(timescale_sink)

    # Run the application
    logger.info("Starting Quix Streams application...")
    try:
        # For testing, we can limit the count and add timeout
        # For production, remove count parameter to run indefinitely
        app.run(count=10, timeout=20)
        logger.info("Application completed successfully")
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()