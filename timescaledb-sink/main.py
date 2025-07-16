import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TimescaleDBSink(BatchingSink):
    """Custom TimescaleDB sink for solar panel data"""
    
    def __init__(self, connection_string: str, table_name: str = "solar_data"):
        super().__init__()
        self.connection_string = connection_string
        self.table_name = table_name
        self.connection = None
        
    def setup(self):
        """Initialize connection and create table if needed"""
        self.connection = psycopg2.connect(self.connection_string)
        self.connection.autocommit = False
        self._create_table_if_not_exists()
        logger.info(f"TimescaleDB sink initialized for table {self.table_name}")
        
    def _create_table_if_not_exists(self):
        """Create or extend the solar_data table so it always has the right schema."""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            "time" TIMESTAMPTZ NOT NULL,
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

        # For already-existing tables: add any missing columns
        alter_table_statements = [
            ('"time"',             'TIMESTAMPTZ NOT NULL'),
            ('panel_id',           'TEXT'),
            ('location_id',        'TEXT'),
            ('location_name',      'TEXT'),
            ('latitude',           'DOUBLE PRECISION'),
            ('longitude',          'DOUBLE PRECISION'),
            ('timezone',           'INTEGER'),
            ('power_output',       'DOUBLE PRECISION'),
            ('unit_power',         'TEXT'),
            ('temperature',        'DOUBLE PRECISION'),
            ('unit_temp',          'TEXT'),
            ('irradiance',         'DOUBLE PRECISION'),
            ('unit_irradiance',    'TEXT'),
            ('voltage',            'DOUBLE PRECISION'),
            ('unit_voltage',       'TEXT'),
            ('current',            'DOUBLE PRECISION'),
            ('unit_current',       'TEXT'),
            ('inverter_status',    'TEXT'),
            ('original_timestamp', 'BIGINT')
        ]
        alter_table_query = "\n".join(
            f'ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS {col} {dtype};'
            for col, dtype in alter_table_statements
        )

        hypertable_query = (
            f"SELECT create_hypertable('{self.table_name}', 'time', if_not_exists => TRUE);"
        )

        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
            cursor.execute(alter_table_query)
            cursor.execute(hypertable_query)
        logger.info(f"Table {self.table_name} ready (created or amended).")
        
    def write(self, batch: SinkBatch):
        """Write batch of messages to TimescaleDB"""
        if not batch:
            return
            
        records = []
        for item in batch:
            try:
                # Parse the JSON value field
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert timestamp to datetime
                timestamp = data.get('timestamp', 0)
                if timestamp:
                    # Convert nanoseconds to seconds
                    timestamp_seconds = timestamp / 1e9
                    dt = datetime.fromtimestamp(timestamp_seconds)
                else:
                    dt = datetime.now()
                
                record = (
                    dt,
                    data.get('panel_id'),
                    data.get('location_id'),
                    data.get('location_name'),
                    data.get('latitude'),
                    data.get('longitude'),
                    data.get('timezone'),
                    data.get('power_output'),
                    data.get('unit_power'),
                    data.get('temperature'),
                    data.get('unit_temp'),
                    data.get('irradiance'),
                    data.get('unit_irradiance'),
                    data.get('voltage'),
                    data.get('unit_voltage'),
                    data.get('current'),
                    data.get('unit_current'),
                    data.get('inverter_status'),
                    data.get('timestamp')
                )
                records.append(record)
                
            except Exception as e:
                logger.error(f"Error processing record: {e}")
                continue
        
        if records:
            self._insert_records(records)
            logger.info(f"Successfully wrote {len(records)} records to TimescaleDB")
    
    def _insert_records(self, records: List[tuple]):
        """Insert records into TimescaleDB"""
        insert_query = f"""
        INSERT INTO {self.table_name} (
            "time", panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, original_timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(insert_query, records)
            self.connection.commit()
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error inserting records: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("TimescaleDB connection closed")


def main():
    # Build connection string from environment variables
    connection_string = (
        f"host={os.environ.get('TIMESCALEDB_HOST', 'localhost')} "
        f"port={os.environ.get('TIMESCALEDB_PORT', '5432')} "
        f"dbname={os.environ.get('TIMESCALEDB_DATABASE', 'solar_data')} "
        f"user={os.environ.get('TIMESCALEDB_USER', 'postgres')} "
        f"password={os.environ.get('TIMESCALEDB_PASSWORD', 'password')}"
    )
    
    # Initialize Quix application
    app = Application(
        broker_address=os.environ.get('KAFKA_BROKER', 'localhost:9092'),
        consumer_group=os.environ.get('CONSUMER_GROUP', 'timescaledb-sink-group'),
        commit_every=100,
        commit_interval=5.0
    )
    
    # Define topic
    topic = app.topic(
        name=os.environ.get('KAFKA_TOPIC', 'solar-data'),
        value_deserializer='json'
    )
    
    # Create dataframe
    sdf = app.dataframe(topic)
    
    # Debug: Print raw message structure
    def debug_message(message):
        print(f"Raw message: {message}")
        return message
    
    sdf = sdf.apply(debug_message)
    
    # Initialize TimescaleDB sink
    timescale_sink = TimescaleDBSink(
        connection_string=connection_string,
        table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data')
    )
    
    # Sink data to TimescaleDB
    sdf.sink(timescale_sink)
    
    # Run the application for exactly 10 messages
    logger.info("Starting TimescaleDB sink application...")
    app.run(count=10, timeout=20)


if __name__ == '__main__':
    main()