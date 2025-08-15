# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError, NetworkError

import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Any

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickHouseSolarPanelSink(BatchingSink):
    """
    ClickHouse sink for solar panel data.
    
    This sink writes solar panel telemetry data to a ClickHouse database
    with proper error handling, retry logic, and batching for optimal performance.
    """
    
    def __init__(self):
        self.client = None
        self._connect()
        self._ensure_table_exists()
    
    def _connect(self):
        """Initialize ClickHouse client connection"""
        try:
            self.client = Client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=int(os.environ.get('CLICKHOUSE_PORT', 9000)),
                user=os.environ.get('CLICKHOUSE_USER', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                database=os.environ.get('CLICKHOUSE_DATABASE', 'default'),
                settings={'use_numpy': True}
            )
            # Test the connection
            self.client.execute('SELECT 1')
            logger.info("Successfully connected to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise ConnectionError(f"ClickHouse connection failed: {e}")
    
    def _ensure_table_exists(self):
        """Create the solar panel data table if it doesn't exist"""
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp DateTime64(3),
            panel_id String,
            power_output Float64,
            voltage Float64,
            current Float64,
            temperature Float64,
            irradiance Float64,
            efficiency Float64,
            location_lat Float64,
            location_lon Float64,
            weather_conditions String,
            raw_data String
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, timestamp)
        PARTITION BY toYYYYMM(timestamp)
        """
        
        try:
            self.client.execute(create_table_sql)
            logger.info(f"Table {table_name} is ready")
        except ClickHouseError as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise
    
    def _transform_data(self, raw_data: List[Dict[str, Any]]) -> List[tuple]:
        """Transform raw solar panel data into ClickHouse-compatible format"""
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')
        transformed_data = []
        
        for record in raw_data:
            try:
                # Handle timestamp - convert to datetime if it's a string or number
                timestamp = record.get('timestamp')
                if isinstance(timestamp, (int, float)):
                    timestamp = datetime.fromtimestamp(timestamp / 1000 if timestamp > 1e10 else timestamp)
                elif isinstance(timestamp, str):
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except ValueError:
                        timestamp = datetime.now()
                else:
                    timestamp = datetime.now()
                
                # Extract solar panel specific fields with defaults
                panel_id = record.get('panel_id', record.get('device_id', 'unknown'))
                power_output = float(record.get('power_output', record.get('power', 0.0)))
                voltage = float(record.get('voltage', 0.0))
                current = float(record.get('current', 0.0))
                temperature = float(record.get('temperature', record.get('temp', 0.0)))
                irradiance = float(record.get('irradiance', record.get('solar_irradiance', 0.0)))
                efficiency = float(record.get('efficiency', 0.0))
                location_lat = float(record.get('location_lat', record.get('latitude', 0.0)))
                location_lon = float(record.get('location_lon', record.get('longitude', 0.0)))
                weather_conditions = str(record.get('weather_conditions', record.get('weather', 'unknown')))
                raw_data_str = str(record)
                
                transformed_data.append((
                    timestamp,
                    panel_id,
                    power_output,
                    voltage,
                    current,
                    temperature,
                    irradiance,
                    efficiency,
                    location_lat,
                    location_lon,
                    weather_conditions,
                    raw_data_str
                ))
                
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(f"Failed to transform record {record}: {e}")
                continue
        
        return transformed_data
    
    def _write_to_clickhouse(self, data: List[Dict[str, Any]]):
        """Write batch of solar panel data to ClickHouse"""
        if not data:
            logger.warning("No data to write to ClickHouse")
            return
        
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')
        transformed_data = self._transform_data(data)
        
        if not transformed_data:
            logger.warning("No valid data after transformation")
            return
        
        insert_sql = f"""
        INSERT INTO {table_name} 
        (timestamp, panel_id, power_output, voltage, current, temperature, 
         irradiance, efficiency, location_lat, location_lon, weather_conditions, raw_data)
        VALUES
        """
        
        try:
            self.client.execute(insert_sql, transformed_data)
            logger.info(f"Successfully inserted {len(transformed_data)} records into {table_name}")
        except ClickHouseError as e:
            logger.error(f"Failed to insert data into ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to ClickHouse.
        
        Implements retry logic and proper error handling for production use.
        """
        max_attempts = int(os.environ.get('CLICKHOUSE_MAX_RETRIES', '3'))
        attempts_remaining = max_attempts
        data = [item.value for item in batch]
        
        logger.info(f"Processing batch of {len(data)} records for ClickHouse")
        
        while attempts_remaining > 0:
            try:
                return self._write_to_clickhouse(data)
            except (NetworkError, ConnectionError) as e:
                # Network or connection issues - retry with backoff
                attempts_remaining -= 1
                logger.warning(f"Connection error writing to ClickHouse: {e}. Attempts remaining: {attempts_remaining}")
                
                if attempts_remaining > 0:
                    backoff_time = (max_attempts - attempts_remaining) * 2 + 1
                    logger.info(f"Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                    # Try to reconnect
                    try:
                        self._connect()
                    except Exception as reconnect_error:
                        logger.error(f"Reconnection failed: {reconnect_error}")
                else:
                    logger.error("Max connection attempts exceeded")
                    
            except ClickHouseError as e:
                # ClickHouse specific errors that might be temporary
                if "Too many parts" in str(e) or "Memory limit" in str(e):
                    logger.warning(f"ClickHouse server busy: {e}. Implementing backpressure.")
                    raise SinkBackpressureError(
                        retry_after=60.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # Other ClickHouse errors might be permanent (schema issues, etc.)
                    logger.error(f"ClickHouse error: {e}")
                    raise
            except Exception as e:
                # Unexpected errors
                logger.error(f"Unexpected error writing to ClickHouse: {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    time.sleep(3)
                else:
                    raise
        
        raise Exception(f"Failed to write to ClickHouse after {max_attempts} attempts")


def main():
    """
    Main application entry point.
    Sets up the Quix Streams application to consume solar panel data and sink to ClickHouse.
    """
    logger.info("Starting ClickHouse Solar Panel Data Sink Application")
    
    try:
        # Setup Quix Streams Application
        app = Application(
            consumer_group=os.environ.get("CONSUMER_GROUP", "clickhouse_solar_sink"),
            auto_create_topics=True,
            auto_offset_reset="earliest",
            consumer_extra_config={
                "max_poll_interval_ms": 300000,  # 5 minutes
                "session_timeout_ms": 30000,     # 30 seconds
            }
        )
        
        # Initialize ClickHouse sink
        logger.info("Initializing ClickHouse sink...")
        clickhouse_sink = ClickHouseSolarPanelSink()
        
        # Setup input topic
        input_topic_name = os.environ["input"]
        logger.info(f"Listening to input topic: {input_topic_name}")
        input_topic = app.topic(name=input_topic_name)
        sdf = app.dataframe(topic=input_topic)
        
        # Optional: Add data transformations or filtering
        # For example, filter out invalid readings or enrich data
        sdf = sdf.filter(lambda row: _validate_solar_data(row))
        
        # Add logging for monitoring
        sdf = sdf.apply(lambda row: _log_processing_stats(row))
        
        # Sink to ClickHouse
        sdf.sink(clickhouse_sink)
        
        logger.info("Application setup complete. Starting stream processing...")
        
        # Start the application
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


def _validate_solar_data(data: Dict[str, Any]) -> bool:
    """
    Validate incoming solar panel data.
    Returns True if data is valid, False otherwise.
    """
    try:
        # Basic validation - ensure we have some form of identifier and timestamp
        has_id = any(key in data for key in ['panel_id', 'device_id', 'id'])
        has_timestamp = 'timestamp' in data
        
        # Optional: validate power readings are reasonable (not negative, not impossibly high)
        power = data.get('power_output', data.get('power', 0))
        if isinstance(power, (int, float)) and power < 0:
            logger.warning(f"Negative power reading detected: {power}")
            return False
        
        return has_id and has_timestamp
    except Exception as e:
        logger.warning(f"Data validation error: {e}")
        return False


def _log_processing_stats(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Log processing statistics for monitoring.
    Returns the data unchanged (passthrough function).
    """
    try:
        panel_id = data.get('panel_id', data.get('device_id', 'unknown'))
        power = data.get('power_output', data.get('power', 0))
        logger.debug(f"Processing data for panel {panel_id}, power: {power}W")
    except Exception as e:
        logger.warning(f"Failed to log processing stats: {e}")
    
    return data


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()