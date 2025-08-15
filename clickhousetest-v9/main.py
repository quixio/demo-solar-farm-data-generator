# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Any
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError, NetworkError

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickHouseSolarSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    
    This sink reads solar panel sensor data from Kafka and writes it to ClickHouse.
    It handles batching, retries, and proper error handling for production use.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self._initialize_client()
        self._ensure_table_exists()
    
    def _initialize_client(self):
        """Initialize ClickHouse client connection."""
        try:
            self.client = clickhouse_connect.get_client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=int(os.environ.get('CLICKHOUSE_PORT', '8123')),
                username=os.environ.get('CLICKHOUSE_USERNAME', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                database=os.environ.get('CLICKHOUSE_DATABASE', 'default'),
                secure=os.environ.get('CLICKHOUSE_SECURE', 'false').lower() == 'true'
            )
            logger.info("ClickHouse client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse client: {e}")
            raise
    
    def _ensure_table_exists(self):
        """Create the solar_data table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS solar_data (
            timestamp DateTime64(3),
            panel_id String,
            voltage_v Float64,
            current_a Float64,
            power_w Float64,
            temperature_c Float64,
            irradiance_w_m2 Float64,
            efficiency_percent Float64,
            status String,
            location_lat Float64,
            location_lon Float64,
            created_at DateTime64(3) DEFAULT now64()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, panel_id)
        PARTITION BY toYYYYMM(timestamp)
        """
        
        try:
            self.client.command(create_table_query)
            logger.info("Solar data table created or already exists")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def _transform_data(self, raw_data: List[Dict[str, Any]]) -> List[List[Any]]:
        """Transform raw sensor data into format suitable for ClickHouse insertion."""
        transformed = []
        
        for record in raw_data:
            try:
                # Handle timestamp - convert to datetime if it's a string or number
                timestamp = record.get('timestamp')
                if isinstance(timestamp, str):
                    # Try parsing ISO format
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except ValueError:
                        # If that fails, use current time
                        timestamp = datetime.utcnow()
                elif isinstance(timestamp, (int, float)):
                    # Assume Unix timestamp
                    timestamp = datetime.fromtimestamp(timestamp)
                else:
                    timestamp = datetime.utcnow()
                
                # Extract and validate data fields with defaults
                row = [
                    timestamp,
                    str(record.get('panel_id', 'unknown')),
                    float(record.get('voltage_v', 0.0)),
                    float(record.get('current_a', 0.0)),
                    float(record.get('power_w', 0.0)),
                    float(record.get('temperature_c', 0.0)),
                    float(record.get('irradiance_w_m2', 0.0)),
                    float(record.get('efficiency_percent', 0.0)),
                    str(record.get('status', 'unknown')),
                    float(record.get('location_lat', 0.0)),
                    float(record.get('location_lon', 0.0)),
                    datetime.utcnow()
                ]
                
                transformed.append(row)
                
            except Exception as e:
                logger.warning(f"Failed to transform record {record}: {e}")
                continue
        
        return transformed
    
    def _write_to_clickhouse(self, data: List[Dict[str, Any]]) -> None:
        """Write transformed data to ClickHouse."""
        if not data:
            logger.info("No data to write")
            return
        
        transformed_data = self._transform_data(data)
        if not transformed_data:
            logger.warning("No valid data after transformation")
            return
        
        try:
            self.client.insert(
                'solar_data',
                transformed_data,
                column_names=[
                    'timestamp', 'panel_id', 'voltage_v', 'current_a', 'power_w',
                    'temperature_c', 'irradiance_w_m2', 'efficiency_percent', 
                    'status', 'location_lat', 'location_lon', 'created_at'
                ]
            )
            logger.info(f"Successfully inserted {len(transformed_data)} records into ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to insert data into ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar sensor data to ClickHouse.
        
        Implements retry logic with exponential backoff and proper error handling
        for production reliability.
        """
        max_attempts = 3
        base_delay = 2
        data = [item.value for item in batch]
        
        for attempt in range(max_attempts):
            try:
                # Reinitialize client if needed
                if self.client is None:
                    self._initialize_client()
                
                self._write_to_clickhouse(data)
                return
                
            except (NetworkError, ConnectionError) as e:
                logger.warning(f"Network/Connection error (attempt {attempt + 1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    delay = base_delay * (2 ** attempt)
                    time.sleep(delay)
                    # Try to reconnect
                    try:
                        self._initialize_client()
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect: {reconnect_error}")
                else:
                    raise ConnectionError(f"Failed to write to ClickHouse after {max_attempts} attempts")
            
            except DatabaseError as e:
                if "timeout" in str(e).lower():
                    logger.warning(f"ClickHouse timeout (attempt {attempt + 1}/{max_attempts}): {e}")
                    # Use backpressure for timeouts
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    logger.error(f"ClickHouse database error: {e}")
                    raise
            
            except Exception as e:
                logger.error(f"Unexpected error writing to ClickHouse: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(base_delay * (attempt + 1))
                else:
                    raise Exception(f"Failed to write to ClickHouse after {max_attempts} attempts: {e}")
        
    def close(self):
        """Clean up resources."""
        if self.client:
            try:
                self.client.close()
                logger.info("ClickHouse client connection closed")
            except Exception as e:
                logger.warning(f"Error closing ClickHouse client: {e}")
            finally:
                self.client = None


def validate_solar_data(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and enrich solar panel sensor data.
    
    This function performs data validation, calculates derived metrics,
    and enriches the data before sending to ClickHouse.
    """
    try:
        # Ensure required fields exist
        if 'panel_id' not in row:
            row['panel_id'] = f"panel_{hash(str(row)) % 10000}"
        
        # Validate and clean numeric fields
        voltage = float(row.get('voltage_v', 0.0))
        current = float(row.get('current_a', 0.0))
        temperature = float(row.get('temperature_c', 0.0))
        irradiance = float(row.get('irradiance_w_m2', 0.0))
        
        # Calculate power if not provided or validate if provided
        calculated_power = voltage * current
        provided_power = float(row.get('power_w', calculated_power))
        
        # Use calculated power if provided power seems incorrect (>10% difference)
        if abs(provided_power - calculated_power) > (calculated_power * 0.1):
            logger.warning(f"Power mismatch for panel {row['panel_id']}: "
                         f"provided={provided_power}, calculated={calculated_power}")
            row['power_w'] = calculated_power
        else:
            row['power_w'] = provided_power
        
        # Calculate efficiency if irradiance and panel area data is available
        panel_area = float(row.get('panel_area_m2', 1.0))  # Default to 1m² if not provided
        if irradiance > 0 and panel_area > 0:
            theoretical_max_power = irradiance * panel_area
            efficiency = (row['power_w'] / theoretical_max_power) * 100 if theoretical_max_power > 0 else 0.0
            row['efficiency_percent'] = min(efficiency, 100.0)  # Cap at 100%
        else:
            row['efficiency_percent'] = float(row.get('efficiency_percent', 0.0))
        
        # Determine status based on power output
        if row['power_w'] <= 0:
            row['status'] = 'offline'
        elif row['power_w'] < (voltage * current * 0.5):  # Less than 50% expected
            row['status'] = 'degraded'
        else:
            row['status'] = row.get('status', 'operational')
        
        # Add timestamp if missing
        if 'timestamp' not in row:
            row['timestamp'] = datetime.utcnow().isoformat()
        
        # Validate coordinates
        row['location_lat'] = max(-90, min(90, float(row.get('location_lat', 0.0))))
        row['location_lon'] = max(-180, min(180, float(row.get('location_lon', 0.0))))
        
        # Clean up any extra fields that aren't part of our schema
        schema_fields = {
            'timestamp', 'panel_id', 'voltage_v', 'current_a', 'power_w',
            'temperature_c', 'irradiance_w_m2', 'efficiency_percent', 
            'status', 'location_lat', 'location_lon'
        }
        
        # Keep only schema fields
        cleaned_row = {k: v for k, v in row.items() if k in schema_fields}
        
        logger.debug(f"Validated solar data for panel {cleaned_row['panel_id']}: "
                    f"Power={cleaned_row['power_w']:.2f}W, Efficiency={cleaned_row['efficiency_percent']:.1f}%")
        
        return cleaned_row
        
    except Exception as e:
        logger.error(f"Error validating solar data: {e}, row: {row}")
        # Return minimal valid record on error
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'panel_id': str(row.get('panel_id', 'unknown')),
            'voltage_v': 0.0,
            'current_a': 0.0,
            'power_w': 0.0,
            'temperature_c': 0.0,
            'irradiance_w_m2': 0.0,
            'efficiency_percent': 0.0,
            'status': 'error',
            'location_lat': 0.0,
            'location_lon': 0.0
        }


def main():
    """
    Main application function that sets up the Kafka consumer and ClickHouse sink
    for processing solar panel sensor data.
    """
    logger.info("Starting Solar Panel Data Sink Application")
    
    # Setup Quix Streams Application
    app = Application(
        consumer_group="solar_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest",
        # Configure for production reliability
        consumer_extra_config={
            "max.poll.records": 100,  # Process in smaller batches for better performance
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }
    )
    
    # Initialize ClickHouse sink
    try:
        clickhouse_sink = ClickHouseSolarSink()
        logger.info("ClickHouse sink initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize ClickHouse sink: {e}")
        raise
    
    # Setup input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)
    
    # Data processing pipeline
    sdf = (
        sdf
        # Log incoming messages for monitoring
        .apply(lambda row: logger.debug(f"Processing message: {row}") or row)
        # Validate and transform solar sensor data
        .apply(validate_solar_data)
        # Log processed data
        .apply(lambda row: logger.info(f"Processed solar data for panel {row['panel_id']}: "
                                      f"{row['power_w']:.2f}W") or row)
        # Print for debugging (can be removed in production)
        .print(metadata=True)
    )
    
    # Connect to ClickHouse sink
    sdf.sink(clickhouse_sink)
    
    logger.info("Pipeline configured, starting application...")
    
    # Run the application with graceful shutdown
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        # Clean up resources
        try:
            clickhouse_sink.close()
        except Exception as e:
            logger.warning(f"Error closing sink: {e}")
        logger.info("Application shutdown complete")


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()