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
from typing import Dict, Any

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuestDBSink(BatchingSink):
    """
    A custom sink that writes solar panel data from Kafka to QuestDB.
    
    QuestDB is compatible with PostgreSQL wire protocol, so we use psycopg2 for connectivity.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.connection = None
        self.table_name = os.environ.get("QUESTDB_TABLE", "solar_panel_data")
        self._ensure_table_exists()
    
    def _get_connection(self):
        """Establish connection to QuestDB"""
        if self.connection is None or self.connection.closed:
            try:
                port = int(os.environ.get('QUESTDB_PORT', '8812'))
            except ValueError:
                port = 8812
            
            self.connection = psycopg2.connect(
                host=os.environ.get("QUESTDB_HOST", "localhost"),
                port=port,
                database=os.environ.get("QUESTDB_DATABASE", "qdb"),
                user=os.environ.get("QUESTDB_USER", "admin"),
                password=os.environ.get("QUESTDB_PASSWORD", "quest")
            )
            self.connection.autocommit = True
            logger.info("Successfully connected to QuestDB")
        return self.connection
    
    def _ensure_table_exists(self):
        """Create the solar_panel_data table if it doesn't exist"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create table with appropriate schema for solar panel data
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
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
                inverter_status STRING
            ) timestamp(timestamp) PARTITION BY DAY;
            """
            
            cursor.execute(create_table_sql)
            cursor.close()
            logger.info(f"Table {self.table_name} is ready in QuestDB")
            
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    def _parse_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the Kafka message to extract solar panel data.
        The message structure has the actual solar data directly in the message.
        """
        try:
            # In the actual runtime, the message IS the solar data
            # No need to extract from a nested 'value' field
            solar_data = message
            
            # Validate that we have the expected solar data
            if solar_data is None or not isinstance(solar_data, dict):
                raise ValueError(f"Expected dictionary with solar data, got: {type(solar_data)}")
            
            # Convert the internal timestamp to a proper datetime
            internal_timestamp = solar_data.get('timestamp')
            if internal_timestamp:
                # Convert nanosecond timestamp to datetime
                timestamp_dt = datetime.fromtimestamp(internal_timestamp / 1_000_000_000)
            else:
                timestamp_dt = datetime.now()
            
            # Return structured data for database insertion
            return {
                'timestamp': timestamp_dt,
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
                'inverter_status': solar_data.get('inverter_status')
            }
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            logger.error(f"Raw message: {message}")
            raise
    
    def _write_to_questdb(self, data_batch):
        """Write batch of solar panel data to QuestDB"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Prepare batch insert
            insert_sql = f"""
            INSERT INTO {self.table_name} (
                timestamp, panel_id, location_id, location_name, latitude, longitude, timezone,
                power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
                voltage, unit_voltage, current, unit_current, inverter_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Prepare values for batch insert
            values = []
            for record in data_batch:
                values.append((
                    record['timestamp'], record['panel_id'], record['location_id'],
                    record['location_name'], record['latitude'], record['longitude'],
                    record['timezone'], record['power_output'], record['unit_power'],
                    record['temperature'], record['unit_temp'], record['irradiance'],
                    record['unit_irradiance'], record['voltage'], record['unit_voltage'],
                    record['current'], record['unit_current'], record['inverter_status']
                ))
            
            # Execute batch insert
            cursor.executemany(insert_sql, values)
            cursor.close()
            
            logger.info(f"✅ BATCH WRITTEN: Successfully wrote {len(data_batch)} records to QuestDB table {self.table_name}")
            
            # Verify the write by checking table count and showing sample data
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            total_count = cursor.fetchone()[0]
            logger.info(f"✅ TABLE VERIFICATION: Total records in {self.table_name}: {total_count}")
            
            # Show sample of the most recent records to prove data was written
            cursor.execute(f"SELECT panel_id, power_output, timestamp FROM {self.table_name} ORDER BY timestamp DESC LIMIT 3")
            recent_records = cursor.fetchall()
            logger.info(f"✅ RECENT RECORDS: {recent_records}")
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error writing to QuestDB: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of messages to QuestDB with retry logic and error handling.
        """
        attempts_remaining = 3
        
        # Parse all messages in the batch
        try:
            parsed_data = []
            for item in batch:
                logger.info(f"📥 Raw message: {item.value}")
                parsed_record = self._parse_message(item.value)
                parsed_data.append(parsed_record)
                logger.info(f"✅ Parsed record for panel: {parsed_record.get('panel_id')}, power: {parsed_record.get('power_output')}W")
            
            logger.info(f"🔄 Processing batch of {len(parsed_data)} messages")
        except Exception as e:
            logger.error(f"❌ Error parsing batch: {e}")
            raise
        
        # Attempt to write to database with retries
        while attempts_remaining:
            try:
                self._write_to_questdb(parsed_data)
                return
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                # Connection issues - retry with backoff
                logger.warning(f"Connection error, retrying... ({e})")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                self.connection = None  # Force reconnection
            except psycopg2.Error as e:
                # Database error that might be temporary
                logger.warning(f"Database error, applying backpressure... ({e})")
                raise SinkBackpressureError(
                    retry_after=10.0
                )
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
        
        raise Exception(f"Failed to write batch to QuestDB after 3 attempts")


def main():
    """Set up the Quix Streams application with QuestDB sink"""
    
    # Setup application
    app = Application(
        consumer_group="questdb_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize QuestDB sink
    questdb_sink = QuestDBSink()
    
    # Configure input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)
    
    # Optional: Add transformations here if needed
    sdf = sdf.print(metadata=True)
    
    # Sink to QuestDB
    sdf.sink(questdb_sink)
    
    # Run application for testing (process 10 messages then stop)
    logger.info("Starting QuestDB sink application...")
    app.run(count=10, timeout=20)


# Execute under conditional main
if __name__ == "__main__":
    main()