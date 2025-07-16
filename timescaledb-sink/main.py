import os
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimescaleDBSink(BatchingSink):
    """Custom sink for TimescaleDB"""
    
    def __init__(self, connection_params: Dict[str, Any], table_name: str = "solar_data"):
        super().__init__()
        self.connection_params = connection_params
        self.table_name = table_name
        self.connection = None
        
    def setup(self):
        """Set up the TimescaleDB connection and create table if needed"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = True
            logger.info("Connected to TimescaleDB successfully")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar_data table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            time TIMESTAMPTZ NOT NULL,
            panel_id TEXT,
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
            original_timestamp BIGINT
        );
        """
        
        # Create hypertable if it doesn't exist
        hypertable_query = f"""
        SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
                cursor.execute(hypertable_query)
                logger.info(f"Table {self.table_name} created/verified successfully")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    def write(self, batch: SinkBatch):
        """Write batch of messages to TimescaleDB"""
        if not self.connection:
            raise RuntimeError("TimescaleDB connection not established")
        
        insert_query = f"""
        INSERT INTO {self.table_name} (
            time, panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, original_timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            with self.connection.cursor() as cursor:
                record_count = 0
                for item in batch:
                    record_count += 1
                    
                    print(f'Raw message: {item}')
                    
                    # Get timestamp - prefer dateTime if available, otherwise use timestamp from value
                    if hasattr(item, 'dateTime') and item.dateTime:
                        event_time = datetime.fromisoformat(
                            item.dateTime.replace('Z', '+00:00')
                        )
                    else:
                        # Fallback to timestamp from value
                        ns = item.value.get('timestamp')
                        if ns:
                            event_time = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
                        else:
                            event_time = datetime.now(timezone.utc)
                    
                    # The value field contains the JSON data
                    value_data = item.value
                    
                    row_data = (
                        event_time,
                        value_data.get('panel_id'),
                        value_data.get('location_id'),
                        value_data.get('location_name'),
                        value_data.get('latitude'),
                        value_data.get('longitude'),
                        value_data.get('timezone'),
                        value_data.get('power_output'),
                        value_data.get('unit_power'),
                        value_data.get('temperature'),
                        value_data.get('unit_temp'),
                        value_data.get('irradiance'),
                        value_data.get('unit_irradiance'),
                        value_data.get('voltage'),
                        value_data.get('unit_voltage'),
                        value_data.get('current'),
                        value_data.get('unit_current'),
                        value_data.get('inverter_status'),
                        value_data.get('timestamp')
                    )
                    
                    cursor.execute(insert_query, row_data)
                
                logger.info(f"Successfully wrote {record_count} records to TimescaleDB")
                
        except Exception as e:
            logger.exception("Error writing to TimescaleDB")
            raise
    
    def close(self):
        """Close the TimescaleDB connection"""
        if self.connection:
            self.connection.close()
            logger.info("TimescaleDB connection closed")

def main():
    # Initialize the Quix Streams application
    app = Application(
        broker_address=os.environ.get('BROKER_ADDRESS', 'localhost:9092'),
        consumer_group='timescaledb-sink'
    )
    
    # Configure the input topic
    input_topic = app.topic('solar-data', value_deserializer='json')
    
    # TimescaleDB connection parameters
    connection_params = {
        'host': os.environ.get('TIMESCALEDB_HOST', 'localhost'),
        'port': int(os.environ.get('TIMESCALEDB_PORT', '5432')),
        'database': os.environ.get('TIMESCALEDB_DATABASE', 'solar_data'),
        'user': os.environ.get('TIMESCALEDB_USER', 'postgres'),
        'password': os.environ.get('TIMESCALEDB_PASSWORD', 'password')
    }
    
    # Create TimescaleDB sink
    timescale_sink = TimescaleDBSink(
        connection_params=connection_params,
        table_name='solar_data'
    )
    
    # Create streaming dataframe
    sdf = app.dataframe(input_topic)
    
    # Apply the sink
    sdf.sink(timescale_sink)
    
    logger.info("Starting TimescaleDB sink application...")
    
    try:
        # Run the application with a limit of 10 messages and 20 second timeout
        app.run(count=10, timeout=20)
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        # Clean up
        timescale_sink.close()
        logger.info("TimescaleDB sink application stopped")

if __name__ == "__main__":
    main()