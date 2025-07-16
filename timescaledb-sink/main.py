import json
import os
import logging
from typing import Dict, Any
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink implementation
    """
    
    def __init__(self, connection_config: Dict[str, Any], table_name: str):
        super().__init__()
        self.connection_config = connection_config
        self.table_name = table_name
        self.connection = None
        
    def setup(self):
        """Setup connection to TimescaleDB"""
        try:
            self.connection = psycopg2.connect(**self.connection_config)
            self.connection.autocommit = True
            self._create_table_if_not_exists()
            logger.info("Connected to TimescaleDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar_data table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            panel_id VARCHAR(50),
            location_id VARCHAR(50),
            location_name VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone INTEGER,
            power_output DOUBLE PRECISION,
            unit_power VARCHAR(10),
            temperature DOUBLE PRECISION,
            unit_temp VARCHAR(10),
            irradiance DOUBLE PRECISION,
            unit_irradiance VARCHAR(10),
            voltage DOUBLE PRECISION,
            unit_voltage VARCHAR(10),
            current DOUBLE PRECISION,
            unit_current VARCHAR(10),
            inverter_status VARCHAR(20),
            timestamp BIGINT,
            message_datetime TIMESTAMPTZ
        );
        
        -- Create hypertable if it doesn't exist (TimescaleDB specific)
        SELECT create_hypertable('{self.table_name}', 'message_datetime', if_not_exists => TRUE);
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
            logger.info(f"Table {self.table_name} created or already exists")
    
    def write(self, batch: SinkBatch):
        """Write batch data to TimescaleDB"""
        if not batch:
            return
            
        try:
            with self.connection.cursor() as cursor:
                for item in batch:
                    # Parse the JSON value from the message
                    raw_message = item.value
                    print(f'Raw message: {raw_message}')
                    
                    # Parse the nested JSON in the value field
                    if isinstance(raw_message, dict) and 'value' in raw_message:
                        value_json = raw_message['value']
                        if isinstance(value_json, str):
                            solar_data = json.loads(value_json)
                        else:
                            solar_data = value_json
                    else:
                        solar_data = raw_message
                    
                    # Map message data to table columns
                    insert_query = f"""
                    INSERT INTO {self.table_name} (
                        panel_id, location_id, location_name, latitude, longitude, timezone,
                        power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                        voltage, unit_voltage, current, unit_current, inverter_status, timestamp,
                        message_datetime
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
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
                        solar_data.get('timestamp'),
                        raw_message.get('dateTime') if isinstance(raw_message, dict) else None
                    )
                    
                    cursor.execute(insert_query, values)
                    logger.info(f"Successfully wrote message to TimescaleDB: {solar_data.get('panel_id')}")
                    
        except Exception as e:
            logger.error(f"Failed to write to TimescaleDB: {e}")
            raise
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            logger.info("TimescaleDB connection closed")

def main():
    # TimescaleDB connection configuration
    timescale_config = {
        'host': os.environ.get('TIMESCALEDB_HOST', 'localhost'),
        'port': int(os.environ.get('TIMESCALEDB_PORT', '5432')),
        'database': os.environ.get('TIMESCALEDB_DATABASE', 'solar_data'),
        'user': os.environ.get('TIMESCALEDB_USER'),
        'password': os.environ.get('TIMESCALEDB_PASSWORD')
    }
    
    # Create Quix application
    app = Application(
        consumer_group="timescaledb-sink-consumer-group",
        auto_offset_reset="earliest",
    )
    
    # Configure topic
    topic = app.topic("solar-data", value_deserializer="json")
    
    # Create streaming dataframe
    sdf = app.dataframe(topic)
    
    # Create TimescaleDB sink
    timescale_sink = TimescaleDBSink(
        connection_config=timescale_config,
        table_name="solar_data"
    )
    
    # Sink data to TimescaleDB
    sdf.sink(timescale_sink)
    
    # Run the application with limit
    logger.info("Starting TimescaleDB sink application...")
    app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()