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
from typing import List, Dict, Any

import clickhouse_connect
from clickhouse_connect.driver import Client

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO if os.getenv("DEBUG_MODE", "false").lower() != "true" else logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ClickHouseSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    
    This sink reads solar panel sensor data from a Kafka topic and writes it
    to a ClickHouse database with proper error handling and retry logic.
    """
    
    def __init__(self):
        super().__init__()
        self.client: Client = None
        self.host = os.getenv("CLICKHOUSE_HOST", "localhost")
        self.port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.database = os.getenv("CLICKHOUSE_DATABASE", "solar_data")
        self.table = os.getenv("CLICKHOUSE_TABLE", "solar_panel_data")
        self.username = os.getenv("CLICKHOUSE_USERNAME", "default")
        self.password = os.getenv("CLICKHOUSE_PASSWORD", "")
        self.secure = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
        self.max_retries = int(os.getenv("CLICKHOUSE_MAX_RETRIES", "3"))
        
        logger.info(f"Initializing ClickHouse sink: {self.host}:{self.port}/{self.database}.{self.table}")
        
    def _get_client(self) -> Client:
        """Get or create ClickHouse client connection"""
        if self.client is None:
            try:
                self.client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    username=self.username,
                    password=self.password,
                    secure=self.secure
                )
                logger.info("Successfully connected to ClickHouse")
                self._ensure_table_exists()
            except Exception as e:
                logger.error(f"Failed to connect to ClickHouse: {e}")
                raise ConnectionError(f"Failed to connect to ClickHouse: {e}")
        return self.client
    
    def _ensure_table_exists(self):
        """Create the solar panel data table if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            timestamp DateTime64(3),
            panel_id String,
            voltage_v Float64,
            current_a Float64,
            power_w Float64,
            temperature_c Float64,
            irradiance_w_m2 Float64,
            efficiency_percent Float64,
            status String,
            error_code Nullable(String),
            metadata String DEFAULT '{{}}',
            ingestion_time DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, timestamp)
        PARTITION BY toYYYYMM(timestamp)
        """
        
        try:
            self.client.command(create_table_query)
            logger.info(f"Table {self.table} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create table {self.table}: {e}")
            raise
    
    def _transform_data(self, records: List[Dict[str, Any]]) -> List[tuple]:
        """Transform Kafka records to ClickHouse-compatible format"""
        transformed_records = []
        
        for record in records:
            try:
                # Handle different timestamp formats
                timestamp = record.get("timestamp")
                if timestamp is None:
                    timestamp = datetime.now()
                elif isinstance(timestamp, (int, float)):
                    # Assume Unix timestamp in milliseconds if > 1e12, otherwise seconds
                    if timestamp > 1e12:
                        timestamp = datetime.fromtimestamp(timestamp / 1000)
                    else:
                        timestamp = datetime.fromtimestamp(timestamp)
                elif isinstance(timestamp, str):
                    # Try to parse ISO format timestamp
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except ValueError:
                        timestamp = datetime.now()
                
                # Extract solar panel data fields with defaults
                panel_id = str(record.get("panel_id", "unknown"))
                voltage_v = float(record.get("voltage_v", record.get("voltage", 0.0)))
                current_a = float(record.get("current_a", record.get("current", 0.0)))
                power_w = float(record.get("power_w", record.get("power", voltage_v * current_a)))
                temperature_c = float(record.get("temperature_c", record.get("temperature", 0.0)))
                irradiance_w_m2 = float(record.get("irradiance_w_m2", record.get("irradiance", 0.0)))
                efficiency_percent = float(record.get("efficiency_percent", record.get("efficiency", 0.0)))
                status = str(record.get("status", "unknown"))
                error_code = record.get("error_code")
                
                # Store additional metadata as JSON
                metadata_fields = {k: v for k, v in record.items() 
                                 if k not in ["timestamp", "panel_id", "voltage_v", "voltage", 
                                            "current_a", "current", "power_w", "power", 
                                            "temperature_c", "temperature", "irradiance_w_m2", 
                                            "irradiance", "efficiency_percent", "efficiency", 
                                            "status", "error_code"]}
                metadata_json = json.dumps(metadata_fields) if metadata_fields else "{}"
                
                transformed_record = (
                    timestamp,
                    panel_id,
                    voltage_v,
                    current_a,
                    power_w,
                    temperature_c,
                    irradiance_w_m2,
                    efficiency_percent,
                    status,
                    error_code,
                    metadata_json,
                    datetime.now()  # ingestion_time
                )
                
                transformed_records.append(transformed_record)
                
            except Exception as e:
                logger.error(f"Failed to transform record {record}: {e}")
                # Skip malformed records rather than failing the entire batch
                continue
                
        return transformed_records
    
    def _write_to_clickhouse(self, data: List[Dict[str, Any]]) -> None:
        """Write batch of data to ClickHouse"""
        if not data:
            logger.warning("Received empty batch, skipping write")
            return
            
        client = self._get_client()
        transformed_data = self._transform_data(data)
        
        if not transformed_data:
            logger.warning("No valid records to write after transformation")
            return
            
        columns = [
            "timestamp", "panel_id", "voltage_v", "current_a", "power_w",
            "temperature_c", "irradiance_w_m2", "efficiency_percent", 
            "status", "error_code", "metadata", "ingestion_time"
        ]
        
        try:
            client.insert(
                table=self.table,
                data=transformed_data,
                column_names=columns
            )
            logger.info(f"Successfully wrote {len(transformed_data)} records to ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to write to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of messages to ClickHouse with retry logic.
        
        Implements proper error handling patterns for database sinks,
        including connection retries and backpressure handling.
        """
        attempts_remaining = self.max_retries
        data = [item.value for item in batch]
        
        logger.debug(f"Processing batch of {len(data)} records for topic {batch.topic}")
        
        while attempts_remaining > 0:
            try:
                self._write_to_clickhouse(data)
                return  # Success
                
            except ConnectionError as e:
                logger.warning(f"Connection error, {attempts_remaining} attempts remaining: {e}")
                attempts_remaining -= 1
                
                # Reset client connection on connection errors
                self.client = None
                
                if attempts_remaining > 0:
                    time.sleep(3)  # Brief retry delay
                    
            except TimeoutError as e:
                logger.warning(f"Timeout error, raising backpressure: {e}")
                # Server is busy, use sanctioned extended pause
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
                
            except Exception as e:
                logger.error(f"Unexpected error writing to ClickHouse: {e}")
                attempts_remaining -= 1
                
                if attempts_remaining > 0:
                    time.sleep(3)
        
        # All retries exhausted
        raise Exception(f"Failed to write batch to ClickHouse after {self.max_retries} attempts")


def main():
    """Set up and run the solar panel data ClickHouse sink application."""
    
    logger.info("Starting Solar Panel Data ClickHouse Sink")
    
    # Get configuration from environment
    input_topic_name = os.getenv("input", "solar_panel_data")
    consumer_group = os.getenv("CONSUMER_GROUP", "solar_panel_clickhouse_sink")
    max_batch_size = int(os.getenv("CLICKHOUSE_MAX_BATCH_SIZE", "1000"))
    batch_timeout = float(os.getenv("CLICKHOUSE_BATCH_TIMEOUT", "30"))
    
    logger.info(f"Configuration: topic={input_topic_name}, consumer_group={consumer_group}, "
                f"batch_size={max_batch_size}, batch_timeout={batch_timeout}")
    
    # Setup Quix Streams Application
    app = Application(
        consumer_group=consumer_group,
        auto_create_topics=True,
        auto_offset_reset="earliest",
        # Configure batching for optimal ClickHouse performance
        consumer_extra_config={
            "max_poll_records": max_batch_size,
            "fetch_max_wait_ms": int(batch_timeout * 1000),
        }
    )
    
    # Initialize ClickHouse sink
    clickhouse_sink = ClickHouseSink()
    
    # Create input topic
    input_topic = app.topic(name=input_topic_name)
    
    # Create streaming dataframe
    sdf = app.dataframe(topic=input_topic)
    
    # Data validation and transformation pipeline
    def validate_solar_data(row: dict) -> dict:
        """Validate and enrich solar panel data"""
        try:
            # Ensure required fields exist
            if "panel_id" not in row:
                row["panel_id"] = "unknown"
                
            # Calculate power if not provided
            if "power_w" not in row and "power" not in row:
                voltage = row.get("voltage_v", row.get("voltage", 0))
                current = row.get("current_a", row.get("current", 0))
                row["power_w"] = float(voltage) * float(current)
                
            # Add data quality flags
            voltage = float(row.get("voltage_v", row.get("voltage", 0)))
            current = float(row.get("current_a", row.get("current", 0)))
            
            # Flag potential data quality issues
            quality_flags = []
            if voltage < 0:
                quality_flags.append("negative_voltage")
            if current < 0:
                quality_flags.append("negative_current")
            if voltage > 1000:  # Unusually high voltage
                quality_flags.append("high_voltage")
            if current > 100:   # Unusually high current
                quality_flags.append("high_current")
                
            if quality_flags:
                row["data_quality_flags"] = quality_flags
                logger.warning(f"Data quality issues detected for panel {row.get('panel_id')}: {quality_flags}")
                
            return row
            
        except Exception as e:
            logger.error(f"Failed to validate solar data: {e}")
            return row  # Return original row if validation fails
    
    # Apply data validation and transformation
    sdf = sdf.apply(validate_solar_data)
    
    # Optional: Print processed data in debug mode
    if os.getenv("DEBUG_MODE", "false").lower() == "true":
        sdf = sdf.print(metadata=True)
    
    # Sink to ClickHouse with batching configuration
    sdf.sink(clickhouse_sink, batch_size=max_batch_size, batch_timeout=batch_timeout)
    
    logger.info("Pipeline configured, starting to process messages...")
    
    try:
        # Run the application
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()