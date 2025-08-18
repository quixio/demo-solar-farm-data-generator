# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import clickhouse_connect
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class ClickHouseSolarDataSink(BatchingSink):
    """
    ClickHouse sink for solar panel sensor data.
    
    This sink writes solar panel metrics (power output, temperature, voltage, current, 
    irradiance, etc.) to a ClickHouse database table.
    """
    
    def __init__(self):
        super().__init__()
        self._client = None
        self._table_created = False
    
    def setup(self):
        """Initialize ClickHouse connection and create table if it doesn't exist."""
        try:
            # Get connection parameters from environment
            host = os.environ.get('CLICKHOUSE_HOST', 'localhost')
            
            # Handle port safely with try/except
            try:
                port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
            except ValueError:
                port = 8123
                
            username = os.environ.get('CLICKHOUSE_USERNAME', 'default')
            password = os.environ.get('CLICKHOUSE_PASSWORD', '')
            database = os.environ.get('CLICKHOUSE_DATABASE', 'default')
            
            print(f"Connecting to ClickHouse at {host}:{port}")
            
            # Create ClickHouse client
            self._client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database
            )
            
            # Test connection
            result = self._client.query('SELECT 1').result_rows
            print(f"ClickHouse connection successful: {result}")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            print(f"Error setting up ClickHouse connection: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the solar_data table if it doesn't exist."""
        if self._table_created:
            return
            
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_data')
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
            sensor_timestamp UInt64,
            message_datetime DateTime64(3),
            inserted_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree()
        ORDER BY (panel_id, message_datetime)
        """
        
        try:
            print(f"Creating table {table_name} if it doesn't exist...")
            self._client.command(create_table_sql)
            print(f"Table {table_name} created successfully or already exists")
            self._table_created = True
        except Exception as e:
            print(f"Error creating table: {e}")
            raise

    def _parse_solar_data(self, message):
        """Parse solar data from the message structure."""
        try:
            # The actual solar data is in the 'value' field as a JSON string
            if isinstance(message, dict) and 'value' in message:
                solar_data_str = message['value']
                if isinstance(solar_data_str, str):
                    solar_data = json.loads(solar_data_str)
                else:
                    solar_data = solar_data_str
                
                # Get message datetime - convert from ISO format to datetime
                message_datetime = message.get('dateTime', datetime.utcnow().isoformat())
                if isinstance(message_datetime, str):
                    try:
                        # Parse ISO 8601 timestamp
                        message_datetime = datetime.fromisoformat(message_datetime.replace('Z', '+00:00'))
                    except ValueError:
                        # Fallback to current time if parsing fails
                        message_datetime = datetime.utcnow()
                elif not isinstance(message_datetime, datetime):
                    message_datetime = datetime.utcnow()
                
                # Extract all solar panel fields
                parsed_data = {
                    'panel_id': solar_data.get('panel_id', ''),
                    'location_id': solar_data.get('location_id', ''),
                    'location_name': solar_data.get('location_name', ''),
                    'latitude': float(solar_data.get('latitude', 0.0)),
                    'longitude': float(solar_data.get('longitude', 0.0)),
                    'timezone': int(solar_data.get('timezone', 0)),
                    'power_output': float(solar_data.get('power_output', 0.0)),
                    'unit_power': solar_data.get('unit_power', 'W'),
                    'temperature': float(solar_data.get('temperature', 0.0)),
                    'unit_temp': solar_data.get('unit_temp', 'C'),
                    'irradiance': float(solar_data.get('irradiance', 0.0)),
                    'unit_irradiance': solar_data.get('unit_irradiance', 'W/m²'),
                    'voltage': float(solar_data.get('voltage', 0.0)),
                    'unit_voltage': solar_data.get('unit_voltage', 'V'),
                    'current': float(solar_data.get('current', 0.0)),
                    'unit_current': solar_data.get('unit_current', 'A'),
                    'inverter_status': solar_data.get('inverter_status', ''),
                    'sensor_timestamp': int(solar_data.get('timestamp', 0)),
                    'message_datetime': message_datetime
                }
                
                return parsed_data
            else:
                print(f"Warning: Unexpected message format: {message}")
                return None
                
        except Exception as e:
            print(f"Error parsing solar data: {e}")
            print(f"Raw message: {message}")
            return None

    def _write_to_clickhouse(self, data):
        """Write parsed solar data to ClickHouse."""
        if not data:
            return
        
        table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_data')
        
        try:
            # Prepare data for insertion
            rows_to_insert = []
            for item in data:
                if item is not None:
                    rows_to_insert.append([
                        item['panel_id'],
                        item['location_id'], 
                        item['location_name'],
                        item['latitude'],
                        item['longitude'],
                        item['timezone'],
                        item['power_output'],
                        item['unit_power'],
                        item['temperature'],
                        item['unit_temp'],
                        item['irradiance'],
                        item['unit_irradiance'],
                        item['voltage'],
                        item['unit_voltage'],
                        item['current'],
                        item['unit_current'],
                        item['inverter_status'],
                        item['sensor_timestamp'],
                        item['message_datetime']
                    ])
            
            if rows_to_insert:
                columns = [
                    'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
                    'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp',
                    'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current',
                    'unit_current', 'inverter_status', 'sensor_timestamp', 'message_datetime'
                ]
                
                self._client.insert(table_name, rows_to_insert, column_names=columns)
                print(f"Successfully inserted {len(rows_to_insert)} rows into {table_name}")
            
        except Exception as e:
            print(f"Error writing to ClickHouse: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write batch of solar data messages to ClickHouse.
        
        This method handles retries and backpressure according to Quix Streams patterns.
        """
        attempts_remaining = 3
        
        # Parse all messages in the batch
        parsed_data = []
        for item in batch:
            print(f'Raw message: {item.value}')
            parsed_item = self._parse_solar_data(item.value)
            if parsed_item:
                parsed_data.append(parsed_item)
        
        if not parsed_data:
            print("No valid data to write in this batch")
            return
            
        while attempts_remaining:
            try:
                return self._write_to_clickhouse(parsed_data)
            except clickhouse_connect.driver.exceptions.DatabaseError as e:
                print(f"ClickHouse database error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except ConnectionError as e:
                print(f"Connection error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except TimeoutError:
                print("ClickHouse timeout, using backpressure")
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            except Exception as e:
                print(f"Unexpected error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
        
        raise Exception("Error while writing to ClickHouse after multiple attempts")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="solar_data_clickhouse_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create and setup the ClickHouse sink
    clickhouse_sink = ClickHouseSolarDataSink()
    clickhouse_sink.setup()
    
    # Create input topic - note that the topic name comes from environment variable
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Do SDF operations/transformations - just print for debugging
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Sink data to ClickHouse
    sdf.sink(clickhouse_sink)

    # With our pipeline defined, now run the Application for testing (10 messages max)
    print("Starting solar data processing...")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()