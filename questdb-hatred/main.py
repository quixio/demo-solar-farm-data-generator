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
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuestDBSink(BatchingSink):
    """
    Custom QuestDB Sink that writes solar panel sensor data to QuestDB.
    
    QuestDB uses PostgreSQL wire protocol, so we can use psycopg2 for connectivity.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._connection = None
        self._table_created = False
    
    def setup(self):
        """Setup QuestDB connection and create table if needed"""
        try:
            # Get connection parameters from environment variables
            host = os.environ.get('questdb_host', 'localhost')
            
            # Safe port conversion
            try:
                port = int(os.environ.get('questdb_port', '8812'))
            except ValueError:
                port = 8812
                logger.warning("Invalid questdb_port value, using default 8812")
            
            database = os.environ.get('questdb_database', 'qdb')
            username = os.environ.get('questdb_username', 'admin')
            password = os.environ.get('questdb_password', 'quest')
            
            logger.info(f"Connecting to QuestDB at {host}:{port}")
            
            # Connect to QuestDB
            self._connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            
            logger.info("Successfully connected to QuestDB")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            # Call success callback
            if hasattr(self, '_on_client_connect_success') and self._on_client_connect_success:
                self._on_client_connect_success(self)
                
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            # Call failure callback  
            if hasattr(self, '_on_client_connect_failure') and self._on_client_connect_failure:
                self._on_client_connect_failure(self, e)
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar_panel_data table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS solar_panel_data (
            timestamp TIMESTAMP,
            panel_id STRING,
            location_id STRING,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output DOUBLE,
            unit_power STRING,
            temperature DOUBLE,
            unit_temp STRING,
            irradiance DOUBLE,
            unit_irradiance STRING,
            voltage DOUBLE,
            unit_voltage STRING,
            current DOUBLE,
            unit_current STRING,
            inverter_status STRING,
            original_timestamp LONG
        ) timestamp(timestamp) PARTITION BY DAY;
        """
        
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self._connection.commit()
                self._table_created = True
                logger.info("Solar panel data table created successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of solar panel data to QuestDB
        """
        if not self._connection:
            self.setup()
        
        attempts_remaining = 3
        
        while attempts_remaining:
            try:
                with self._connection.cursor() as cursor:
                    for item in batch:
                        # Debug: print raw message structure
                        print(f"Raw message: {item}")
                        
                        # Parse the message value - it contains JSON string
                        try:
                            # The value field contains a JSON string that needs to be parsed
                            if isinstance(item.value, str):
                                solar_data = json.loads(item.value)
                            elif isinstance(item.value, dict):
                                # If already parsed, use directly
                                solar_data = item.value
                            else:
                                logger.warning(f"Unexpected value type: {type(item.value)}")
                                continue
                            
                            # Convert epoch timestamp to datetime
                            # The timestamp appears to be in nanoseconds, convert to seconds
                            epoch_timestamp = solar_data.get('timestamp', 0)
                            if epoch_timestamp:
                                # Convert nanoseconds to seconds
                                timestamp_seconds = epoch_timestamp / 1_000_000_000
                                timestamp = datetime.fromtimestamp(timestamp_seconds)
                            else:
                                timestamp = datetime.now()
                            
                            # Insert data into QuestDB
                            insert_sql = """
                                INSERT INTO solar_panel_data (
                                    timestamp, panel_id, location_id, location_name, 
                                    latitude, longitude, timezone, power_output, unit_power,
                                    temperature, unit_temp, irradiance, unit_irradiance,
                                    voltage, unit_voltage, current, unit_current,
                                    inverter_status, original_timestamp
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                                );
                            """
                            
                            cursor.execute(insert_sql, (
                                timestamp,
                                solar_data.get('panel_id'),
                                solar_data.get('location_id'),
                                solar_data.get('location_name'),
                                solar_data.get('latitude'),
                                solar_data.get('longitude'),
                                solar_data.get('timezone'),
                                solar_data.get('power_output'),
                                solar_data.get('unit_power'),
                                solar_data.get('temperature'),
                                solar_data.get('unit_temp'),
                                solar_data.get('irradiance'),
                                solar_data.get('unit_irradiance'),
                                solar_data.get('voltage'),
                                solar_data.get('unit_voltage'),
                                solar_data.get('current'),
                                solar_data.get('unit_current'),
                                solar_data.get('inverter_status'),
                                solar_data.get('timestamp')
                            ))
                            
                            logger.info(f"Inserted solar panel data for panel: {solar_data.get('panel_id')}")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse JSON from message value: {e}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            continue
                    
                    # Commit the transaction
                    self._connection.commit()
                    logger.info(f"Successfully wrote batch of {len(batch)} messages to QuestDB")
                    return
                    
            except psycopg2.OperationalError as e:
                logger.error(f"Database connection error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                    # Try to reconnect
                    try:
                        self.setup()
                    except Exception:
                        pass
            except psycopg2.errors.QueryCanceled:
                # Query timeout - use backpressure
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            except Exception as e:
                logger.error(f"Unexpected error writing to QuestDB: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
        
        raise Exception("Failed to write to QuestDB after multiple attempts")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="questdb_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create QuestDB sink
    questdb_sink = QuestDBSink()
    
    # Setup input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Debug raw messages
    sdf = sdf.apply(lambda row: row, metadata=True).print(metadata=True)

    # Sink data to QuestDB
    sdf.sink(questdb_sink)

    # Run the application for testing with limited messages
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()