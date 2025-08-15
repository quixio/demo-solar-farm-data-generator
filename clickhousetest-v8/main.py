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

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG_MODE", "false").lower() == "true" else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickHouseSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    
    This sink batches incoming messages and writes them to ClickHouse database.
    It includes proper error handling, retry logic, and connection management.
    """
    
    def __init__(self):
        """Initialize the ClickHouse sink with configuration from environment variables."""
        self.host = os.environ.get("CLICKHOUSE_HOST", "localhost")
        self.port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
        self.database = os.environ.get("CLICKHOUSE_DATABASE", "solar_data")
        self.table = os.environ.get("CLICKHOUSE_TABLE", "solar_panel_data")
        self.username = os.environ.get("CLICKHOUSE_USERNAME", "default")
        self.password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.secure = os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true"
        self.max_retries = int(os.environ.get("CLICKHOUSE_MAX_RETRIES", "3"))
        
        self.client = None
        self._initialize_client()
        self._ensure_database_and_table()
        
        logger.info(f"ClickHouse sink initialized for {self.host}:{self.port}/{self.database}.{self.table}")
    
    def _initialize_client(self):
        """Initialize the ClickHouse client connection."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                secure=self.secure,
                connect_timeout=30,
                send_receive_timeout=300
            )
            # Test the connection
            self.client.ping()
            logger.info("ClickHouse client initialized and connection verified")
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse client: {e}")
            raise
    
    def _ensure_database_and_table(self):
        """Create database and table if they don't exist."""
        try:
            # Create database if it doesn't exist
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info(f"Database {self.database} ensured")
            
            # Create table for solar panel data
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
                timestamp DateTime64(3) DEFAULT now64(3),
                panel_id String,
                voltage Float64,
                current Float64,
                power Float64,
                temperature Float64,
                irradiance Float64,
                efficiency Float64,
                status String DEFAULT 'active',
                location String DEFAULT '',
                metadata String DEFAULT '{}',
                ingestion_time DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (panel_id, timestamp)
            PARTITION BY toYYYYMM(timestamp)
            """
            
            self.client.command(create_table_sql)
            logger.info(f"Table {self.database}.{self.table} ensured")
            
        except Exception as e:
            logger.error(f"Failed to ensure database and table: {e}")
            raise
    
    def _normalize_solar_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize solar panel data to match the expected schema.
        
        This method handles various input formats and ensures all required fields are present.
        """
        normalized = {}
        
        # Handle timestamp - try multiple possible field names
        timestamp_fields = ['timestamp', 'time', 'datetime', 'ts', 'created_at']
        for field in timestamp_fields:
            if field in raw_data and raw_data[field] is not None:
                try:
                    # Handle various timestamp formats
                    if isinstance(raw_data[field], str):
                        # Try parsing ISO format first
                        normalized['timestamp'] = datetime.fromisoformat(raw_data[field].replace('Z', '+00:00'))
                    elif isinstance(raw_data[field], (int, float)):
                        # Assume Unix timestamp
                        normalized['timestamp'] = datetime.fromtimestamp(raw_data[field])
                    else:
                        normalized['timestamp'] = raw_data[field]
                    break
                except (ValueError, TypeError):
                    continue
        
        if 'timestamp' not in normalized:
            normalized['timestamp'] = datetime.now()
        
        # Handle panel ID - try multiple possible field names
        panel_id_fields = ['panel_id', 'id', 'device_id', 'sensor_id', 'panel']
        for field in panel_id_fields:
            if field in raw_data and raw_data[field] is not None:
                normalized['panel_id'] = str(raw_data[field])
                break
        else:
            normalized['panel_id'] = 'unknown'
        
        # Handle numeric measurements with fallbacks
        numeric_fields = {
            'voltage': ['voltage', 'v', 'volt'],
            'current': ['current', 'amp', 'amperage', 'i'],
            'power': ['power', 'watt', 'watts', 'p'],
            'temperature': ['temperature', 'temp', 't'],
            'irradiance': ['irradiance', 'solar_irradiance', 'sunlight'],
            'efficiency': ['efficiency', 'eff', 'performance']
        }
        
        for target_field, possible_fields in numeric_fields.items():
            for field in possible_fields:
                if field in raw_data and raw_data[field] is not None:
                    try:
                        normalized[target_field] = float(raw_data[field])
                        break
                    except (ValueError, TypeError):
                        continue
            else:
                normalized[target_field] = 0.0
        
        # Handle status
        status_fields = ['status', 'state', 'condition']
        for field in status_fields:
            if field in raw_data and raw_data[field] is not None:
                normalized['status'] = str(raw_data[field])
                break
        else:
            normalized['status'] = 'active'
        
        # Handle location
        location_fields = ['location', 'site', 'position', 'place']
        for field in location_fields:
            if field in raw_data and raw_data[field] is not None:
                normalized['location'] = str(raw_data[field])
                break
        else:
            normalized['location'] = ''
        
        # Store any additional metadata as JSON string
        metadata = {}
        excluded_fields = set(normalized.keys()) | {'metadata'}
        for key, value in raw_data.items():
            if key not in excluded_fields:
                metadata[key] = value
        
        normalized['metadata'] = str(metadata) if metadata else '{}'
        normalized['ingestion_time'] = datetime.now()
        
        return normalized
    
    def _write_to_clickhouse(self, data: List[Dict[str, Any]]) -> None:
        """Write normalized data to ClickHouse."""
        if not data:
            logger.warning("No data to write to ClickHouse")
            return
        
        try:
            # Normalize all records
            normalized_data = [self._normalize_solar_data(record) for record in data]
            
            # Insert data
            self.client.insert(
                table=f"{self.database}.{self.table}",
                data=normalized_data,
                column_names=list(normalized_data[0].keys())
            )
            
            logger.info(f"Successfully wrote {len(normalized_data)} records to ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to write data to ClickHouse: {e}")
            raise
    
    def write(self, batch: SinkBatch):
        """
        Write a batch of messages to ClickHouse with retry logic.
        
        This method implements the required write interface for Quix Streams sinks.
        """
        attempts_remaining = self.max_retries
        data = [item.value for item in batch]
        
        logger.debug(f"Writing batch of {len(data)} records to ClickHouse")
        
        while attempts_remaining:
            try:
                return self._write_to_clickhouse(data)
                
            except clickhouse_connect.driver.exceptions.DatabaseError as e:
                logger.error(f"ClickHouse database error: {e}")
                # For database errors, we might want to recreate the connection
                attempts_remaining -= 1
                if attempts_remaining:
                    logger.info(f"Retrying write operation ({attempts_remaining} attempts remaining)")
                    time.sleep(2)
                    try:
                        # Reinitialize client on database errors
                        self._initialize_client()
                    except Exception as reinit_error:
                        logger.error(f"Failed to reinitialize client: {reinit_error}")
                
            except (ConnectionError, OSError) as e:
                logger.error(f"Connection error: {e}")
                # Network/connection issues - retry with backoff
                attempts_remaining -= 1
                if attempts_remaining:
                    logger.info(f"Retrying write operation ({attempts_remaining} attempts remaining)")
                    time.sleep(3)
                    try:
                        # Reinitialize client on connection errors
                        self._initialize_client()
                    except Exception as reinit_error:
                        logger.error(f"Failed to reinitialize client: {reinit_error}")
                        
            except TimeoutError as e:
                logger.error(f"Timeout error: {e}")
                # Server timeout - use sanctioned extended pause
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
                
            except Exception as e:
                logger.error(f"Unexpected error writing to ClickHouse: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    logger.info(f"Retrying write operation ({attempts_remaining} attempts remaining)")
                    time.sleep(1)
        
        # If we've exhausted all retries, raise an exception
        error_msg = f"Failed to write batch to ClickHouse after {self.max_retries} attempts"
        logger.error(error_msg)
        raise Exception(error_msg)


def main():
    """Set up and run the solar panel data sink application."""
    
    logger.info("Starting Solar Panel ClickHouse Sink Application")
    
    try:
        # Setup Quix Streams Application
        app = Application(
            consumer_group=os.environ.get("CONSUMER_GROUP", "solar_panel_clickhouse_sink"),
            auto_create_topics=True,
            auto_offset_reset="earliest"
        )
        
        # Initialize ClickHouse sink
        clickhouse_sink = ClickHouseSink()
        
        # Setup input topic and streaming dataframe
        input_topic = app.topic(name=os.environ["input"])
        sdf = app.dataframe(topic=input_topic)
        
        # Log incoming messages if in debug mode
        if os.getenv("DEBUG_MODE", "false").lower() == "true":
            sdf = sdf.apply(lambda row: row).print(metadata=True)
        else:
            # Log periodically in production
            sdf = sdf.apply(lambda row, ctx: _log_periodic_stats(row, ctx))
        
        # Sink to ClickHouse
        sdf.sink(clickhouse_sink)
        
        logger.info("Application configured successfully, starting to consume messages...")
        
        # Run the application
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


def _log_periodic_stats(row, ctx):
    """Log periodic statistics about processed messages."""
    # Log every 1000 messages
    if ctx.get("message_count", 0) % 1000 == 0:
        logger.info(f"Processed {ctx.get('message_count', 0)} messages")
    return row


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()