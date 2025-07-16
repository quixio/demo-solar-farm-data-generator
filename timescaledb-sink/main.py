import os
import json
import logging
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink for solar panel data
    """
    
    def __init__(self, connection_params, table_name="solar_data", on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.connection_params = connection_params
        self.table_name = table_name
        self.connection = None
        
    def setup(self):
        """Initialize the connection to TimescaleDB"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info("Connected to TimescaleDB successfully")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar_data table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            timestamp TIMESTAMPTZ NOT NULL,
            panel_id VARCHAR(50) NOT NULL,
            location_id VARCHAR(50) NOT NULL,
            location_name VARCHAR(100) NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            timezone INTEGER NOT NULL,
            power_output DOUBLE PRECISION NOT NULL,
            unit_power VARCHAR(10) NOT NULL,
            temperature DOUBLE PRECISION NOT NULL,
            unit_temp VARCHAR(10) NOT NULL,
            irradiance DOUBLE PRECISION NOT NULL,
            unit_irradiance VARCHAR(10) NOT NULL,
            voltage DOUBLE PRECISION NOT NULL,
            unit_voltage VARCHAR(10) NOT NULL,
            current DOUBLE PRECISION NOT NULL,
            unit_current VARCHAR(10) NOT NULL,
            inverter_status VARCHAR(20) NOT NULL,
            message_timestamp BIGINT NOT NULL
        );
        """
        
        # Create hypertable if it doesn't exist
        create_hypertable_query = f"""
        SELECT create_hypertable('{self.table_name}', 'timestamp', if_not_exists => TRUE);
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
            try:
                cursor.execute(create_hypertable_query)
                logger.info(f"Hypertable {self.table_name} created or already exists")
            except psycopg2.Error as e:
                logger.warning(f"Could not create hypertable (may already exist): {e}")
            
            self.connection.commit()
            logger.info(f"Table {self.table_name} is ready")
    
    def write(self, batch: SinkBatch):
        """Write a batch of data to TimescaleDB"""
        if not batch:
            return
            
        insert_query = f"""
        INSERT INTO {self.table_name} (
            timestamp, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, message_timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        data_to_insert = []
        for item in batch:
            # Parse the JSON-encoded value field
            try:
                solar_data = json.loads(item.value)
                
                # Convert timestamp from nanoseconds to datetime
                timestamp_ns = solar_data.get('timestamp', 0)
                timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                
                # Map the data to table columns
                row_data = (
                    timestamp,
                    solar_data.get('panel_id', ''),
                    solar_data.get('location_id', ''),
                    solar_data.get('location_name', ''),
                    solar_data.get('latitude', 0.0),
                    solar_data.get('longitude', 0.0),
                    solar_data.get('timezone', 0),
                    solar_data.get('power_output', 0.0),
                    solar_data.get('unit_power', ''),
                    solar_data.get('temperature', 0.0),
                    solar_data.get('unit_temp', ''),
                    solar_data.get('irradiance', 0.0),
                    solar_data.get('unit_irradiance', ''),
                    solar_data.get('voltage', 0.0),
                    solar_data.get('unit_voltage', ''),
                    solar_data.get('current', 0.0),
                    solar_data.get('unit_current', ''),
                    solar_data.get('inverter_status', ''),
                    timestamp_ns
                )
                
                data_to_insert.append(row_data)
                
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing message data: {e}")
                continue
        
        if data_to_insert:
            try:
                with self.connection.cursor() as cursor:
                    cursor.executemany(insert_query, data_to_insert)
                    self.connection.commit()
                    logger.info(f"Successfully inserted {len(data_to_insert)} records into TimescaleDB")
            except psycopg2.Error as e:
                logger.error(f"Error writing to TimescaleDB: {e}")
                self.connection.rollback()
                raise
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            logger.info("TimescaleDB connection closed")

def main():
    # Configure the application
    app = Application(
        broker_address="localhost:9092",
        consumer_group="timescaledb-sink-group",
        commit_every=10,
        commit_interval=5.0
    )
    
    # Define the input topic
    input_topic = app.topic("solar-data", value_deserializer="json")
    
    # TimescaleDB connection parameters
    connection_params = {
        'host': os.environ.get('TIMESCALEDB_HOST', 'localhost'),
        'port': os.environ.get('TIMESCALEDB_PORT', '5432'),
        'database': os.environ.get('TIMESCALEDB_DATABASE', 'solar_data'),
        'user': os.environ.get('TIMESCALEDB_USER'),
        'password': os.environ.get('TIMESCALEDB_PASSWORD')
    }
    
    # Create the TimescaleDB sink
    timescale_sink = TimescaleDBSink(
        connection_params=connection_params,
        table_name="solar_data"
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(input_topic)
    
    # Add debug logging to see raw message structure
    sdf = sdf.apply(lambda message: print(f'Raw message: {message}') or message)
    
    # Sink the data to TimescaleDB
    sdf.sink(timescale_sink)
    
    # Run the application for 10 messages
    app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()