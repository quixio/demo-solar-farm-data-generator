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
from typing import Dict, List, Any, Optional
import clickhouse_connect
from clickhouse_connect.driver import Client

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO if os.getenv('DEBUG_MODE', 'false').lower() != 'true' else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ClickHouseSolarPanelSink(BatchingSink):
    """
    A ClickHouse sink specifically designed for solar panel sensor data.
    
    This sink handles batching, error recovery, and schema management
    for solar panel telemetry data including power generation, voltage,
    current, temperature, and environmental conditions.
    """

    def __init__(self):
        super().__init__()
        self.client: Optional[Client] = None
        self.host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        self.database = os.getenv('CLICKHOUSE_DATABASE', 'solar_data')
        self.table = os.getenv('CLICKHOUSE_TABLE', 'solar_panel_data')
        self.username = os.getenv('CLICKHOUSE_USERNAME', 'default')
        self.password = os.getenv('CLICKHOUSE_PASSWORD', '')
        self.secure = os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true'
        self.max_batch_size = int(os.getenv('CLICKHOUSE_MAX_BATCH_SIZE', '1000'))
        self.max_retries = int(os.getenv('CLICKHOUSE_MAX_RETRIES', '3'))
        
        logger.info(f"Initializing ClickHouse sink for {self.host}:{self.port}/{self.database}.{self.table}")
        
        # Initialize connection and ensure table exists
        self._ensure_connection()
        self._ensure_table_exists()

    def _ensure_connection(self) -> None:
        """Establish connection to ClickHouse server."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                secure=self.secure,
                connect_timeout=10,
                send_receive_timeout=30
            )
            # Test the connection
            self.client.ping()
            logger.info("Successfully connected to ClickHouse")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise ConnectionError(f"ClickHouse connection failed: {e}")

    def _ensure_table_exists(self) -> None:
        """Create the solar panel data table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD),
            panel_id String CODEC(ZSTD),
            site_id String CODEC(ZSTD),
            power_output Float64 CODEC(Gorilla, ZSTD),
            voltage Float64 CODEC(Gorilla, ZSTD),
            current Float64 CODEC(Gorilla, ZSTD),
            panel_temperature Float64 CODEC(Gorilla, ZSTD),
            ambient_temperature Float64 CODEC(Gorilla, ZSTD),
            irradiance Float64 CODEC(Gorilla, ZSTD),
            humidity Float64 CODEC(Gorilla, ZSTD),
            wind_speed Float64 CODEC(Gorilla, ZSTD),
            efficiency Float64 CODEC(Gorilla, ZSTD),
            status String CODEC(ZSTD),
            error_codes Array(String) CODEC(ZSTD),
            metadata String CODEC(ZSTD)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (site_id, panel_id, timestamp)
        TTL timestamp + INTERVAL 2 YEAR
        SETTINGS index_granularity = 8192
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Table {self.table} is ready")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _normalize_solar_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize and validate solar panel data for ClickHouse insertion.
        
        This handles various data formats and ensures all required fields are present.
        """
        normalized_data = []
        
        for record in raw_data:
            try:
                # Handle different timestamp formats
                timestamp = record.get('timestamp', record.get('time', record.get('ts')))
                if timestamp is None:
                    timestamp = datetime.utcnow().isoformat()
                elif isinstance(timestamp, (int, float)):
                    timestamp = datetime.fromtimestamp(timestamp).isoformat()
                elif not isinstance(timestamp, str):
                    timestamp = str(timestamp)

                # Normalize the record
                normalized_record = {
                    'timestamp': timestamp,
                    'panel_id': str(record.get('panel_id', record.get('panelId', record.get('id', 'unknown')))),
                    'site_id': str(record.get('site_id', record.get('siteId', record.get('location', 'default')))),
                    'power_output': float(record.get('power_output', record.get('power', record.get('powerW', 0)))),
                    'voltage': float(record.get('voltage', record.get('voltageV', record.get('v', 0)))),
                    'current': float(record.get('current', record.get('currentA', record.get('i', 0)))),
                    'panel_temperature': float(record.get('panel_temperature', record.get('panelTemp', record.get('temp_panel', 25)))),
                    'ambient_temperature': float(record.get('ambient_temperature', record.get('ambientTemp', record.get('temp_ambient', 25)))),
                    'irradiance': float(record.get('irradiance', record.get('solar_irradiance', record.get('sunlight', 0)))),
                    'humidity': float(record.get('humidity', record.get('humidityPercent', record.get('rh', 50)))),
                    'wind_speed': float(record.get('wind_speed', record.get('windSpeed', record.get('wind', 0)))),
                    'efficiency': float(record.get('efficiency', record.get('efficiencyPercent', record.get('eff', 0)))),
                    'status': str(record.get('status', record.get('state', 'operational'))),
                    'error_codes': record.get('error_codes', record.get('errors', record.get('alerts', []))),
                    'metadata': json.dumps(record.get('metadata', record.get('extra', {})))
                }
                
                # Validate critical fields
                if normalized_record['power_output'] < 0:
                    normalized_record['power_output'] = 0
                if normalized_record['efficiency'] > 100:
                    normalized_record['efficiency'] = 100
                if normalized_record['efficiency'] < 0:
                    normalized_record['efficiency'] = 0
                
                normalized_data.append(normalized_record)
                
            except Exception as e:
                logger.warning(f"Failed to normalize record {record}: {e}")
                continue
        
        return normalized_data

    def _write_to_clickhouse(self, data: List[Dict[str, Any]]) -> None:
        """Write normalized data to ClickHouse."""
        if not data:
            logger.warning("No data to write to ClickHouse")
            return

        normalized_data = self._normalize_solar_data(data)
        if not normalized_data:
            logger.warning("No valid data after normalization")
            return

        try:
            # Insert data in batches
            batch_size = min(self.max_batch_size, len(normalized_data))
            for i in range(0, len(normalized_data), batch_size):
                batch = normalized_data[i:i + batch_size]
                self.client.insert(
                    table=self.table,
                    data=batch,
                    column_names=list(batch[0].keys())
                )
            
            logger.info(f"Successfully wrote {len(normalized_data)} records to ClickHouse")
            
        except Exception as e:
            logger.error(f"Failed to write data to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to ClickHouse.
        
        Implements retry logic and proper error handling for production use.
        """
        attempts_remaining = self.max_retries
        data = [item.value for item in batch]
        
        logger.debug(f"Processing batch of {len(data)} records from topic {batch.topic}")
        
        while attempts_remaining > 0:
            try:
                # Ensure we have a valid connection
                if not self.client:
                    self._ensure_connection()
                
                # Write the data
                self._write_to_clickhouse(data)
                return  # Success!
                
            except ConnectionError as e:
                logger.warning(f"Connection error (attempts remaining: {attempts_remaining - 1}): {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    time.sleep(3)
                    # Try to reconnect
                    try:
                        self._ensure_connection()
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect: {reconnect_error}")
                        
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check for timeout or server busy conditions
                if any(keyword in error_msg for keyword in ['timeout', 'busy', 'overloaded', 'too many']):
                    logger.warning(f"Server busy, implementing backpressure: {e}")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                
                # Check for retryable errors
                elif any(keyword in error_msg for keyword in ['network', 'connection', 'dns']):
                    logger.warning(f"Retryable error (attempts remaining: {attempts_remaining - 1}): {e}")
                    attempts_remaining -= 1
                    if attempts_remaining > 0:
                        time.sleep(5)
                else:
                    # Non-retryable error
                    logger.error(f"Non-retryable error writing to ClickHouse: {e}")
                    raise
        
        # If we get here, all retries have been exhausted
        raise Exception(f"Failed to write to ClickHouse after {self.max_retries} attempts")

    def __del__(self):
        """Clean up the ClickHouse connection."""
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass


def main():
    """
    Set up and run the solar panel data to ClickHouse sink application.
    """
    
    # Get configuration from environment
    consumer_group = os.getenv('CONSUMER_GROUP', 'solar_panel_clickhouse_sink')
    input_topic_name = os.getenv('input', 'solar_panel_data')
    
    logger.info(f"Starting solar panel ClickHouse sink application")
    logger.info(f"Consumer group: {consumer_group}")
    logger.info(f"Input topic: {input_topic_name}")
    
    try:
        # Setup Quix Streams application
        app = Application(
            consumer_group=consumer_group,
            auto_create_topics=True,
            auto_offset_reset="earliest",
            # Configure for better performance
            consumer_extra_config={
                "session.timeout.ms": 45000,
                "heartbeat.interval.ms": 15000,
                "max.poll.interval.ms": 300000,
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 5000,
                "fetch.min.bytes": 1,
                "fetch.max.wait.ms": 500
            }
        )
        
        # Initialize ClickHouse sink
        logger.info("Initializing ClickHouse sink...")
        clickhouse_sink = ClickHouseSolarPanelSink()
        
        # Setup input topic and streaming dataframe
        input_topic = app.topic(name=input_topic_name)
        sdf = app.dataframe(topic=input_topic)
        
        # Optional data transformations and enrichment
        def enrich_solar_data(row):
            """Add computed fields and validate data before sinking."""
            try:
                # Calculate power if voltage and current are available
                if 'voltage' in row and 'current' in row and 'power_output' not in row:
                    row['power_output'] = row['voltage'] * row['current']
                
                # Calculate efficiency if we have expected vs actual power
                if 'expected_power' in row and 'power_output' in row and row['expected_power'] > 0:
                    row['efficiency'] = (row['power_output'] / row['expected_power']) * 100
                
                # Add processing timestamp
                row['processed_at'] = datetime.utcnow().isoformat()
                
                return row
            except Exception as e:
                logger.warning(f"Failed to enrich data: {e}")
                return row
        
        # Apply transformations and log data
        sdf = sdf.apply(enrich_solar_data)
        
        # Add debug logging if enabled
        if os.getenv('DEBUG_MODE', 'false').lower() == 'true':
            sdf = sdf.apply(lambda row: logger.debug(f"Processing record: {json.dumps(row, default=str)}") or row)
        else:
            # Just log basic info about processed records
            sdf = sdf.apply(lambda row: logger.info(f"Processing solar data from panel {row.get('panel_id', 'unknown')} at site {row.get('site_id', 'unknown')}") or row)
        
        # Sink the data to ClickHouse
        sdf.sink(clickhouse_sink)
        
        logger.info("Starting application - processing will begin...")
        
        # Run the application
        app.run()
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()