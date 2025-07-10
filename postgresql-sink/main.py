import json
import logging
import os
from datetime import datetime
from quixstreams import Application
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Configuration from environment variables
    app = Application(
        broker_address=os.environ["Quix__Broker__Address"],
        consumer_group="timescaledb-sink-consumer",
        auto_offset_reset="latest"
    )
    
    # TimescaleDB connection parameters from environment variables
    db_config = {
        'host': os.environ['TIMESCALE_HOST'],
        'port': int(os.environ.get('TIMESCALE_PORT', '5432')),
        'database': os.environ['TIMESCALE_DATABASE'],
        'user': os.environ['TIMESCALE_USER'],
        'password': os.environ['TIMESCALE_PASSWORD']
    }
    
    input_topic = app.topic("solar-data")
    
    def connect_to_timescale():
        """Establish connection to TimescaleDB"""
        try:
            conn = psycopg2.connect(**db_config)
            logger.info("Successfully connected to TimescaleDB")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Error connecting to TimescaleDB: {e}")
            raise
    
    def setup_hypertable(conn):
        """Create table and hypertable if they don't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS solar_panel_data (
            time TIMESTAMPTZ NOT NULL,
            panel_id TEXT NOT NULL,
            location_id TEXT NOT NULL,
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
        
        create_hypertable_sql = """
        SELECT create_hypertable('solar_panel_data', 'time', 
                                if_not_exists => TRUE,
                                chunk_time_interval => INTERVAL '1 day');
        """
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                cursor.execute(create_hypertable_sql)
                conn.commit()
                logger.info("Table and hypertable setup completed")
        except psycopg2.Error as e:
            logger.error(f"Error setting up table/hypertable: {e}")
            raise
    
    def insert_solar_data(conn, data):
        """Insert solar panel data into TimescaleDB"""
        insert_sql = """
        INSERT INTO solar_panel_data (
            time, panel_id, location_id, location_name, latitude, longitude,
            timezone, power_output, unit_power, temperature, unit_temp,
            irradiance, unit_irradiance, voltage, unit_voltage, current,
            unit_current, inverter_status, original_timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """
        
        # Convert nanosecond timestamp to datetime
        timestamp_ns = data['timestamp']
        timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
        
        values = (
            timestamp_dt,
            data['panel_id'],
            data['location_id'],
            data['location_name'],
            data['latitude'],
            data['longitude'],
            data['timezone'],
            data['power_output'],
            data['unit_power'],
            data['temperature'],
            data['unit_temp'],
            data['irradiance'],
            data['unit_irradiance'],
            data['voltage'],
            data['unit_voltage'],
            data['current'],
            data['unit_current'],
            data['inverter_status'],
            timestamp_ns
        )
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(insert_sql, values)
                conn.commit()
                logger.info(f"Inserted data for panel {data['panel_id']}")
        except psycopg2.Error as e:
            logger.error(f"Error inserting data: {e}")
            conn.rollback()
            raise
    
    # Initialize database connection
    connection = connect_to_timescale()
    setup_hypertable(connection)
    
    # Create streaming dataframe
    sdf = app.dataframe(input_topic)
    
    def process_message(row):
        """Process each message from the topic"""
        try:
            # Parse the JSON data from the message value
            solar_data = json.loads(row)
            
            # Insert into TimescaleDB
            insert_solar_data(connection, solar_data)
            
            logger.info(f"Successfully processed data for panel: {solar_data.get('panel_id', 'unknown')}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    # Apply processing function to each message
    sdf = sdf.apply(process_message)
    
    logger.info("Starting TimescaleDB sink application...")
    
    try:
        app.run(sdf)
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()