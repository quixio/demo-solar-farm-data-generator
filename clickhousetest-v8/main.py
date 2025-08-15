# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import logging
import json
from datetime import datetime
from typing import Dict, List, Any
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import ClickHouseError, DatabaseError

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SolarPanelClickHouseSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    
    This sink reads solar panel sensor data from Kafka and writes it to ClickHouse
    in batches for optimal performance. It handles various data formats and provides
    robust error handling with retry logic.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self.host = os.environ["CLICKHOUSE_HOST"]
        self.port = int(os.environ.get("CLICKHOUSE_PORT", 8123))
        self.database = os.environ["CLICKHOUSE_DATABASE"]
        self.table = os.environ["CLICKHOUSE_TABLE"]
        self.username = os.environ.get("CLICKHOUSE_USERNAME", "default")
        self.password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.secure = os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true"
        
        # Batch configuration
        self.max_batch_size = int(os.environ.get("CLICKHOUSE_MAX_BATCH_SIZE", "1000"))
        self.batch_timeout = int(os.environ.get("CLICKHOUSE_BATCH_TIMEOUT", "30"))
        
        logger.info(f"Initialized ClickHouse sink for {self.host}:{self.port}/{self.database}.{self.table}")
    
    def _get_client(self):
        """Get or create ClickHouse client with connection pooling"""
        if not self.client:
            try:
                self.client = get_client(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    username=self.username,
                    password=self.password,
                    secure=self.secure,
                    connect_timeout=30,
                    send_receive_timeout=300
                )
                logger.info("Successfully connected to ClickHouse")
            except Exception as e:
                logger.error(f"Failed to connect to ClickHouse: {e}")
                raise ConnectionError(f"ClickHouse connection failed: {e}")
        return self.client
    
    def _ensure_table_exists(self):
        """Create the solar panel data table if it doesn't exist"""
        try:
            client = self._get_client()
            
            # Create table with appropriate schema for solar panel data
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                timestamp DateTime64(3),
                panel_id String,
                voltage Float64,
                current Float64,
                power Float64,
                temperature Float64,
                irradiance Float64,
                efficiency Float64,
                location_lat Float64,
                location_lon Float64,
                weather_condition String,
                metadata String,
                ingestion_time DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (panel_id, timestamp)
            """
            
            client.command(create_table_query)
            logger.info(f"Ensured table {self.table} exists")
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def _normalize_solar_data(self, data: List[Dict[str, Any]]) -> List[tuple]:
        """
        Normalize solar panel sensor data to match the ClickHouse table schema.
        Handles various input formats and provides sensible defaults.
        """
        normalized_rows = []
        
        for record in data:
            try:
                # Extract timestamp - support multiple formats
                timestamp = self._extract_timestamp(record)
                
                # Extract core solar panel metrics
                panel_id = str(record.get('panel_id', record.get('id', record.get('device_id', 'unknown'))))
                voltage = float(record.get('voltage', record.get('V', 0.0)))
                current = float(record.get('current', record.get('I', record.get('amperage', 0.0))))
                power = float(record.get('power', record.get('P', record.get('wattage', voltage * current))))
                temperature = float(record.get('temperature', record.get('temp', record.get('T', 0.0))))
                irradiance = float(record.get('irradiance', record.get('solar_irradiance', record.get('irr', 0.0))))
                
                # Calculate efficiency if not provided (power / (irradiance * panel_area))
                # Using a default panel area of 2m² if not specified
                panel_area = float(record.get('panel_area', 2.0))
                if irradiance > 0:
                    efficiency = float(record.get('efficiency', (power / (irradiance * panel_area)) * 100))
                else:
                    efficiency = float(record.get('efficiency', 0.0))
                
                # Extract location data
                location_lat = float(record.get('latitude', record.get('lat', record.get('location', {}).get('lat', 0.0))))
                location_lon = float(record.get('longitude', record.get('lon', record.get('location', {}).get('lon', 0.0))))
                
                # Extract weather condition
                weather_condition = str(record.get('weather', record.get('weather_condition', record.get('condition', 'unknown'))))
                
                # Store additional metadata as JSON
                metadata_dict = {k: v for k, v in record.items() 
                               if k not in ['timestamp', 'panel_id', 'id', 'device_id', 'voltage', 'V', 
                                           'current', 'I', 'amperage', 'power', 'P', 'wattage', 
                                           'temperature', 'temp', 'T', 'irradiance', 'solar_irradiance', 
                                           'irr', 'efficiency', 'latitude', 'lat', 'longitude', 'lon',
                                           'location', 'weather', 'weather_condition', 'condition']}
                metadata = json.dumps(metadata_dict)
                
                normalized_row = (
                    timestamp, panel_id, voltage, current, power, temperature,
                    irradiance, efficiency, location_lat, location_lon,
                    weather_condition, metadata
                )
                
                normalized_rows.append(normalized_row)
                
            except Exception as e:
                logger.warning(f"Failed to normalize record {record}: {e}")
                continue
        
        return normalized_rows
    
    def _extract_timestamp(self, record: Dict[str, Any]) -> datetime:
        """Extract and normalize timestamp from various formats"""
        timestamp_field = record.get('timestamp', record.get('time', record.get('ts', record.get('datetime'))))
        
        if not timestamp_field:
            # Use current time if no timestamp provided
            return datetime.utcnow()
        
        # Handle different timestamp formats
        if isinstance(timestamp_field, (int, float)):
            # Assume Unix timestamp (seconds or milliseconds)
            if timestamp_field > 1e10:  # Milliseconds
                return datetime.fromtimestamp(timestamp_field / 1000)
            else:  # Seconds
                return datetime.fromtimestamp(timestamp_field)
        
        elif isinstance(timestamp_field, str):
            # Try parsing ISO format
            try:
                return datetime.fromisoformat(timestamp_field.replace('Z', '+00:00'))
            except:
                try:
                    return datetime.strptime(timestamp_field, '%Y-%m-%d %H:%M:%S')
                except:
                    logger.warning(f"Could not parse timestamp: {timestamp_field}")
                    return datetime.utcnow()
        
        return datetime.utcnow()
    
    def _write_to_clickhouse(self, data: List[Dict[str, Any]]):
        """Write normalized data to ClickHouse"""
        if not data:
            logger.info("No data to write")
            return
        
        try:
            client = self._get_client()
            self._ensure_table_exists()
            
            # Normalize the data
            normalized_data = self._normalize_solar_data(data)
            
            if not normalized_data:
                logger.warning("No valid records to insert after normalization")
                return
            
            # Insert data
            column_names = [
                'timestamp', 'panel_id', 'voltage', 'current', 'power', 'temperature',
                'irradiance', 'efficiency', 'location_lat', 'location_lon',
                'weather_condition', 'metadata'
            ]
            
            client.insert(
                table=self.table,
                data=normalized_data,
                column_names=column_names
            )
            
            logger.info(f"Successfully inserted {len(normalized_data)} records into ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to write to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of solar panel data to ClickHouse.
        
        Implements retry logic with exponential backoff and proper error handling
        for connection issues and database errors.
        """
        max_attempts = int(os.environ.get("CLICKHOUSE_MAX_RETRIES", "3"))
        attempts_remaining = max_attempts
        data = [item.value for item in batch]
        
        logger.info(f"Processing batch of {len(data)} records")
        
        while attempts_remaining > 0:
            try:
                self._write_to_clickhouse(data)
                return  # Success, exit retry loop
                
            except ConnectionError as e:
                logger.warning(f"Connection error (attempt {max_attempts - attempts_remaining + 1}/{max_attempts}): {e}")
                attempts_remaining -= 1
                
                if attempts_remaining > 0:
                    # Exponential backoff
                    sleep_time = (max_attempts - attempts_remaining) * 2
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    # Reset client to force reconnection
                    self.client = None
                else:
                    logger.error("Max connection retry attempts reached")
                    raise
                    
            except (DatabaseError, ClickHouseError) as e:
                logger.error(f"Database error: {e}")
                # For database errors, trigger backpressure instead of retrying
                raise SinkBackpressureError(
                    retry_after=60.0,  # Wait longer for database issues
                    topic=batch.topic,
                    partition=batch.partition,
                )
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                attempts_remaining -= 1
                
                if attempts_remaining > 0:
                    sleep_time = 5
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error("Max retry attempts reached for unexpected error")
                    raise
        
        raise Exception(f"Failed to write batch after {max_attempts} attempts")


def main():
    """
    Set up and run the solar panel data ClickHouse sink application.
    
    This application reads solar panel sensor data from a Kafka topic
    and writes it to ClickHouse in batches for optimal performance.
    """
    
    logger.info("Starting Solar Panel ClickHouse Sink Application")
    
    try:
        # Setup Quix Streams Application
        consumer_group = os.environ.get("CONSUMER_GROUP", "solar_panel_clickhouse_sink")
        app = Application(
            consumer_group=consumer_group,
            auto_create_topics=True,
            auto_offset_reset="earliest",
            commit_interval=5.0  # Commit offsets every 5 seconds
        )
        
        # Initialize ClickHouse sink
        clickhouse_sink = SolarPanelClickHouseSink()
        
        # Setup input topic
        input_topic = app.topic(name=os.environ["input"])
        sdf = app.dataframe(topic=input_topic)
        
        # Add data validation and transformation
        def validate_solar_data(row):
            """Validate and enrich solar panel data"""
            try:
                # Basic validation - ensure we have essential fields
                if not any(key in row for key in ['panel_id', 'id', 'device_id']):
                    logger.warning(f"Record missing panel identifier: {row}")
                    row['panel_id'] = 'unknown'
                
                # Ensure numeric fields are properly typed
                numeric_fields = ['voltage', 'V', 'current', 'I', 'amperage', 'power', 'P', 'wattage', 
                                'temperature', 'temp', 'T', 'irradiance', 'solar_irradiance', 'irr',
                                'efficiency', 'latitude', 'lat', 'longitude', 'lon']
                
                for field in numeric_fields:
                    if field in row and row[field] is not None:
                        try:
                            row[field] = float(row[field])
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid numeric value for {field}: {row[field]}")
                            row[field] = 0.0
                
                # Add processing timestamp
                row['processing_timestamp'] = datetime.utcnow().isoformat()
                
                return row
                
            except Exception as e:
                logger.error(f"Error validating solar data: {e}")
                return row
        
        # Apply validation and transformations
        sdf = sdf.apply(validate_solar_data)
        
        # Optional: Add debugging output (remove in production for performance)
        if os.environ.get("DEBUG_MODE", "false").lower() == "true":
            sdf = sdf.print(metadata=True)
        
        # Configure batching for optimal ClickHouse performance
        max_batch_size = int(os.environ.get("CLICKHOUSE_MAX_BATCH_SIZE", "1000"))
        batch_timeout = int(os.environ.get("CLICKHOUSE_BATCH_TIMEOUT", "30"))
        
        # Sink to ClickHouse
        sdf.sink(clickhouse_sink, batch_size=max_batch_size, batch_timeout=batch_timeout)
        
        logger.info(f"Pipeline configured - Consumer Group: {consumer_group}, Batch Size: {max_batch_size}, Batch Timeout: {batch_timeout}s")
        
        # Run the application
        logger.info("Starting data processing...")
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()