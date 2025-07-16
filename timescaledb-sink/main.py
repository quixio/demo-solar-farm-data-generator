import os
import json
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimescaleDBSink(BatchingSink):
    def __init__(self, host, port, database, user, password, table_name="solar_data"):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        
    def setup(self):
        """Set up the TimescaleDB connection and create table if needed."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.connection.autocommit = True
            logger.info("Connected to TimescaleDB")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the table and hypertable if they don't exist."""
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
        
        # Create hypertable query
        create_hypertable_query = f"""
        SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
                cursor.execute(create_hypertable_query)
                logger.info(f"Table {self.table_name} and hypertable created successfully")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    def write(self, batch: SinkBatch):
        """Write batch of records to TimescaleDB."""
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
                for item in batch:
                    # Parse the datetime from message metadata
                    timestamp = datetime.fromisoformat(item.timestamp.replace('Z', '+00:00'))
                    
                    # Parse the value field which contains JSON data
                    value_data = json.loads(item.value)
                    
                    # Map the data to table columns
                    row_data = (
                        timestamp,
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
                
                logger.info(f"Successfully wrote {len(batch)} records to TimescaleDB")
                
        except Exception as e:
            logger.error(f"Error writing to TimescaleDB: {e}")
            raise
    
    def close(self):
        """Close the TimescaleDB connection."""
        if self.connection:
            self.connection.close()
            logger.info("TimescaleDB connection closed")

def main():
    # Initialize the Application
    app = Application()
    
    # Define the input topic
    input_topic = app.topic("solar-data", value_deserializer="json")
    
    # Create StreamingDataFrame
    sdf = app.dataframe(input_topic)
    
    # Add debug print to see raw message structure
    def debug_message(item):
        print(f"Raw message: {item}")
        return item
    
    sdf = sdf.apply(debug_message)
    
    # Initialize TimescaleDB sink
    timescale_sink = TimescaleDBSink(
        host=os.environ.get('TIMESCALEDB_HOST', 'localhost'),
        port=int(os.environ.get('TIMESCALEDB_PORT', 5432)),
        database=os.environ.get('TIMESCALEDB_DATABASE', 'solar_data'),
        user=os.environ.get('TIMESCALEDB_USER', 'postgres'),
        password=os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
        table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data')
    )
    
    # Sink the data to TimescaleDB
    sdf.sink(timescale_sink)
    
    # Run the application with count=10 and timeout=20
    logger.info("Starting TimescaleDB sink application")
    app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()