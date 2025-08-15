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
from clickhouse_connect.driver.exceptions import DatabaseError

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
    A ClickHouse sink for solar panel sensor data.
    
    This sink writes solar panel sensor data to ClickHouse in batches for optimal performance.
    It handles connection failures, implements retry logic, and provides proper error handling.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self._connect()
        self._ensure_table_exists()
    
    def _connect(self):
        """Establish connection to ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=int(os.environ.get('CLICKHOUSE_PORT', '8123')),
                username=os.environ.get('CLICKHOUSE_USERNAME', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                database=os.environ.get('CLICKHOUSE_DATABASE', 'default'),
                secure=os.environ.get('CLICKHOUSE_SECURE', 'false').lower() == 'true'
            )
            # Test connection
            self.client.ping()
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
            voltage Float64,
            current Float64,
            power Float64,
            temperature Float64,
            irradiance Float64,
            efficiency Float64,
            status String,
            location String,
            inserted_at DateTime64(3) DEFAULT now64()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, panel_id)
        PARTITION BY toYYYYMM(timestamp)
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Table {table_name} is ready")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise
    
    def _transform_data(self, data: List[Dict[str, Any]]) -> List[List[Any]]:
        """Transform and validate solar panel data for ClickHouse insertion"""
        transformed_data = []
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')
        
        for record in data:
            try:
                # Handle different timestamp formats
                if 'timestamp' in record:
                    if isinstance(record['timestamp'], (int, float)):
                        # Unix timestamp
                        timestamp = datetime.fromtimestamp(record['timestamp'] / 1000 if record['timestamp'] > 1e10 else record['timestamp'])
                    elif isinstance(record['timestamp'], str):
                        # ISO format string
                        timestamp = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
                    else:
                        timestamp = record['timestamp']
                else:
                    timestamp = datetime.now()
                
                # Extract solar panel metrics with defaults
                row = [
                    timestamp,
                    str(record.get('panel_id', 'unknown')),
                    float(record.get('voltage', 0.0)),
                    float(record.get('current', 0.0)),
                    float(record.get('power', record.get('voltage', 0.0) * record.get('current', 0.0))),
                    float(record.get('temperature', 0.0)),
                    float(record.get('irradiance', 0.0)),
                    float(record.get('efficiency', 0.0)),
                    str(record.get('status', 'unknown')),
                    str(record.get('location', 'unknown'))
                ]
                
                transformed_data.append(row)
                
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(f"Skipping invalid record due to transformation error: {e}, record: {record}")
                continue
        
        logger.info(f"Transformed {len(transformed_data)} records for insertion")
        return transformed_data
    
    def _write_to_clickhouse(self, data: List[Dict[str, Any]]):
        """Write transformed data to ClickHouse"""
        if not data:
            logger.info("No data to write")
            return
        
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_panel_data')
        transformed_data = self._transform_data(data)
        
        if not transformed_data:
            logger.warning("No valid records to insert after transformation")
            return
        
        try:
            columns = [
                'timestamp', 'panel_id', 'voltage', 'current', 'power',
                'temperature', 'irradiance', 'efficiency', 'status', 'location'
            ]
            
            self.client.insert(
                table=table_name,
                data=transformed_data,
                column_names=columns
            )
            
            logger.info(f"Successfully inserted {len(transformed_data)} records into {table_name}")
            
        except DatabaseError as e:
            logger.error(f"ClickHouse database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during ClickHouse insertion: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel sensor data to ClickHouse.
        
        Implements retry logic and proper error handling as per Quix Streams patterns.
        """
        max_retries = int(os.environ.get('CLICKHOUSE_MAX_RETRIES', '3'))
        retry_delay = float(os.environ.get('CLICKHOUSE_RETRY_DELAY', '3.0'))
        backpressure_retry_after = float(os.environ.get('CLICKHOUSE_BACKPRESSURE_RETRY', '30.0'))
        
        attempts_remaining = max_retries
        data = [item.value for item in batch]
        
        logger.info(f"Processing batch with {len(data)} records")
        
        while attempts_remaining > 0:
            try:
                return self._write_to_clickhouse(data)
                
            except ConnectionError as e:
                logger.warning(f"Connection error on attempt {max_retries - attempts_remaining + 1}: {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    # Try to reconnect
                    try:
                        self._connect()
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect: {reconnect_error}")
                        
            except DatabaseError as e:
                if "too many parts" in str(e).lower() or "memory limit" in str(e).lower():
                    # ClickHouse is under pressure, use backpressure
                    logger.warning(f"ClickHouse under pressure: {e}")
                    raise SinkBackpressureError(
                        retry_after=backpressure_retry_after,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # Other database errors should not be retried
                    logger.error(f"Non-recoverable database error: {e}")
                    raise
                    
            except Exception as e:
                logger.error(f"Unexpected error on attempt {max_retries - attempts_remaining + 1}: {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        
        error_msg = f"Failed to write to ClickHouse after {max_retries} attempts"
        logger.error(error_msg)
        raise Exception(error_msg)


def enrich_solar_data(row):
    """
    Enrich solar panel data with calculated metrics and validation.
    """
    # Calculate power if not present
    if 'power' not in row and 'voltage' in row and 'current' in row:
        row['power'] = row['voltage'] * row['current']
    
    # Calculate efficiency if irradiance and power are available
    if 'efficiency' not in row and 'power' in row and 'irradiance' in row and row['irradiance'] > 0:
        # Assuming standard panel area of 2 square meters for calculation
        panel_area = float(os.environ.get('SOLAR_PANEL_AREA', '2.0'))
        max_possible_power = row['irradiance'] * panel_area
        if max_possible_power > 0:
            row['efficiency'] = (row['power'] / max_possible_power) * 100
        else:
            row['efficiency'] = 0.0
    
    # Add status based on conditions
    if 'status' not in row:
        if row.get('power', 0) > 0:
            if row.get('temperature', 25) > float(os.environ.get('HIGH_TEMP_THRESHOLD', '85')):
                row['status'] = 'high_temperature'
            elif row.get('efficiency', 0) < float(os.environ.get('LOW_EFFICIENCY_THRESHOLD', '15')):
                row['status'] = 'low_efficiency'
            else:
                row['status'] = 'operational'
        else:
            row['status'] = 'inactive'
    
    # Add timestamp if missing
    if 'timestamp' not in row:
        row['timestamp'] = time.time()
    
    return row


def main():
    """
    Set up the Quix Streams application for solar panel data processing.
    
    This application reads solar panel sensor data from a Kafka topic,
    processes and enriches the data, then sinks it to ClickHouse.
    """
    logger.info("Starting Solar Panel ClickHouse Sink Application")
    
    # Setup necessary objects
    consumer_group = os.environ.get('CONSUMER_GROUP', 'solar_panel_clickhouse_sink')
    
    app = Application(
        consumer_group=consumer_group,
        auto_create_topics=True,
        auto_offset_reset="earliest",
        # Add some performance tuning
        consumer_extra_config={
            "max.poll.interval.ms": 300000,  # 5 minutes
            "session.timeout.ms": 45000,     # 45 seconds
        }
    )
    
    # Initialize the ClickHouse sink
    try:
        clickhouse_sink = SolarPanelClickHouseSink()
        logger.info("ClickHouse sink initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize ClickHouse sink: {e}")
        raise
    
    # Set up input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)
    
    # Apply transformations and enrichments
    sdf = sdf.apply(enrich_solar_data, metadata=True)
    
    # Optional: Print data for debugging (can be controlled via environment variable)
    if os.environ.get('DEBUG_PRINT', 'false').lower() == 'true':
        sdf = sdf.print(metadata=True)
    
    # Apply additional filtering if needed
    # Filter out records with invalid data
    sdf = sdf.filter(lambda row: row.get('panel_id') is not None)
    
    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(clickhouse_sink)
    
    logger.info("Application pipeline configured, starting processing...")
    
    # With our pipeline defined, now run the Application
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()