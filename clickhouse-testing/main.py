import json
import logging
import os
import time
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError
import clickhouse_connect

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class ClickHouseSink(BatchingSink):
    """
    ClickHouse Sink for processing solar panel sensor data.
    
    This sink connects to ClickHouse database and inserts solar panel
    sensor data into a table with proper schema mapping and error handling.
    """
    
    def __init__(self):
        super().__init__()
        self.client = None
        self.table_name = "solar_panel_data"
        
    def setup(self):
        """Initialize ClickHouse connection and create table if needed"""
        try:
            # Get connection parameters with safe port handling
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
            except ValueError:
                port = 8123
                logger.warning("Invalid port value, using default 8123")
            
            # Create ClickHouse client
            self.client = clickhouse_connect.get_client(
                host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
                port=port,
                database=os.environ.get('CLICKHOUSE_DATABASE', 'default'),
                username=os.environ.get('CLICKHOUSE_USER', 'default'),
                password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
                secure=os.environ.get('CLICKHOUSE_SECURE', 'false').lower() == 'true'
            )
            
            # Test connection
            self.client.ping()
            logger.info("Successfully connected to ClickHouse")
            
            # Create table if it doesn't exist
            self._create_table()
            
        except Exception as e:
            logger.error(f"Failed to setup ClickHouse connection: {e}")
            raise
    
    def _create_table(self):
        """Create the solar panel data table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            panel_id String,
            location_id String,
            location_name String,
            latitude Float64,
            longitude Float64,
            timezone Int32,
            power_output Float64,
            unit_power String,
            temperature Float64,
            unit_temp String,
            irradiance Float64,
            unit_irradiance String,
            voltage Float64,
            unit_voltage String,
            current Float64,
            unit_current String,
            inverter_status String,
            timestamp UInt64,
            message_datetime DateTime64(3),
            inserted_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, timestamp)
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Table {self.table_name} created successfully or already exists")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def _parse_solar_data(self, message):
        """Parse solar panel data from message"""
        try:
            # Debug: Print raw message structure
            logger.info(f"Raw message: {message}")
            
            # Extract the value field and parse it as JSON
            if isinstance(message.get('value'), str):
                solar_data = json.loads(message['value'])
            elif isinstance(message.get('value'), dict):
                solar_data = message['value']
            else:
                logger.error(f"Unexpected value type: {type(message.get('value'))}")
                return None
            
            # Convert message datetime to proper format
            message_datetime = None
            if 'dateTime' in message:
                try:
                    message_datetime = datetime.fromisoformat(message['dateTime'].replace('Z', '+00:00'))
                except:
                    message_datetime = datetime.utcnow()
            else:
                message_datetime = datetime.utcnow()
            
            # Map solar data to table schema
            parsed_data = {
                'panel_id': solar_data.get('panel_id', ''),
                'location_id': solar_data.get('location_id', ''),
                'location_name': solar_data.get('location_name', ''),
                'latitude': float(solar_data.get('latitude', 0.0)),
                'longitude': float(solar_data.get('longitude', 0.0)),
                'timezone': int(solar_data.get('timezone', 0)),
                'power_output': float(solar_data.get('power_output', 0.0)),
                'unit_power': solar_data.get('unit_power', ''),
                'temperature': float(solar_data.get('temperature', 0.0)),
                'unit_temp': solar_data.get('unit_temp', ''),
                'irradiance': float(solar_data.get('irradiance', 0.0)),
                'unit_irradiance': solar_data.get('unit_irradiance', ''),
                'voltage': float(solar_data.get('voltage', 0.0)),
                'unit_voltage': solar_data.get('unit_voltage', ''),
                'current': float(solar_data.get('current', 0.0)),
                'unit_current': solar_data.get('unit_current', ''),
                'inverter_status': solar_data.get('inverter_status', ''),
                'timestamp': int(solar_data.get('timestamp', 0)),
                'message_datetime': message_datetime
            }
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Failed to parse solar data: {e}")
            return None

    def write(self, batch: SinkBatch):
        """Write batch of solar panel data to ClickHouse"""
        attempts_remaining = 3
        parsed_data = []
        
        # Parse all messages in the batch
        for item in batch:
            data = self._parse_solar_data(item.value)
            if data:
                parsed_data.append(data)
        
        if not parsed_data:
            logger.warning("No valid data to insert")
            return
        
        while attempts_remaining:
            try:
                # Insert data into ClickHouse
                self.client.insert(self.table_name, parsed_data)
                logger.info(f"Successfully inserted {len(parsed_data)} records into ClickHouse")
                return
                
            except Exception as e:
                logger.error(f"Failed to insert data: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                else:
                    # If it's a connection issue, raise backpressure
                    if "connection" in str(e).lower() or "timeout" in str(e).lower():
                        raise SinkBackpressureError(
                            retry_after=30.0,
                            topic=batch.topic,
                            partition=batch.partition,
                        )
                    else:
                        raise Exception(f"Failed to write to ClickHouse after retries: {e}")


def main():
    """Set up and run the ClickHouse sink application"""
    
    # Setup Quix Streams Application
    app = Application(
        consumer_group="solar_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize ClickHouse sink
    clickhouse_sink = ClickHouseSink()
    
    # Setup sink connection
    clickhouse_sink.setup()
    
    # Configure input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)
    
    # Add debugging to show raw message structure
    def debug_message(row):
        print(f"Raw message: {row}")
        return row
    
    # Process messages and sink to ClickHouse
    sdf = sdf.apply(debug_message)
    sdf.sink(clickhouse_sink)
    
    # Run the application with limited message count for testing
    logger.info("Starting solar panel data sink to ClickHouse...")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()