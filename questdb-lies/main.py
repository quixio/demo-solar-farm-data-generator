# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import logging
from datetime import datetime
from questdb.ingress import Sender, TimestampNanos

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuestDBSolarSink(BatchingSink):
    """
    Custom QuestDB sink for solar panel sensor data.
    
    Processes solar panel sensor data and writes it to QuestDB using the 
    official QuestDB Python client with ILP (InfluxDB Line Protocol) over HTTP.
    """

    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            # Use internal callback names to avoid AttributeError
            _on_client_connect_success=on_client_connect_success,
            _on_client_connect_failure=on_client_connect_failure
        )
        self.host = os.environ.get("QUESTDB_HOST", "localhost")
        try:
            self.port = int(os.environ.get("QUESTDB_PORT", "9000"))
        except ValueError:
            self.port = 9000
            
        self.token = os.environ.get("QUESTDB_TOKEN")
        self.table_name = os.environ.get("QUESTDB_TABLE_NAME", "solar_panel_data")
        
        # Build configuration string
        self.config_string = f"http::addr={self.host}:{self.port};"
        if self.token:
            self.config_string += f"token={self.token};"
        
        logger.info(f"QuestDB Sink initialized - Host: {self.host}:{self.port}, Table: {self.table_name}")
        
    def setup(self):
        """Test connection to QuestDB and create table if needed"""
        logger.info("Setting up QuestDB connection...")
        try:
            with Sender.from_conf(self.config_string) as sender:
                # Test connection by attempting to send a test row
                logger.info("QuestDB connection test successful")
                
                # Create table schema if it doesn't exist - use REST API for DDL
                self._ensure_table_exists()
                
                if self._on_client_connect_success:
                    self._on_client_connect_success()
                    
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure()
            raise ConnectionError(f"Could not connect to QuestDB: {e}")

    def _ensure_table_exists(self):
        """Create the solar panel data table if it doesn't exist"""
        import requests
        
        # Create table using REST API
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            panel_id SYMBOL,
            location_id SYMBOL,
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
            inverter_status STRING,
            internal_timestamp LONG,
            message_datetime TIMESTAMP,
            ts TIMESTAMP
        ) TIMESTAMP(ts) PARTITION BY HOUR;
        """
        
        try:
            url = f"http://{self.host}:{self.port}/exec"
            headers = {}
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
                
            response = requests.post(
                url, 
                headers=headers,
                data={"query": create_table_sql}
            )
            
            if response.status_code == 200:
                logger.info(f"Table '{self.table_name}' created/verified successfully")
            else:
                logger.warning(f"Table creation response: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            # Continue anyway - table might already exist

    def _parse_solar_data(self, message):
        """Parse solar panel data from the nested message structure"""
        try:
            # Debug: Print raw message structure
            print(f"Raw message: {message}")
            
            # Extract the nested value field which contains JSON string
            if isinstance(message, dict) and 'value' in message:
                value_str = message.get('value', '{}')
                if isinstance(value_str, str):
                    # Parse the JSON string to get the actual solar data
                    solar_data = json.loads(value_str)
                else:
                    solar_data = value_str
                
                # Extract message metadata
                message_datetime = message.get('dateTime', datetime.utcnow().isoformat())
                
                # Parse ISO datetime to timestamp
                if isinstance(message_datetime, str):
                    try:
                        dt = datetime.fromisoformat(message_datetime.replace('Z', '+00:00'))
                        message_ts = int(dt.timestamp() * 1000000000)  # Convert to nanoseconds
                    except:
                        message_ts = TimestampNanos.now()
                else:
                    message_ts = TimestampNanos.now()
                
                # Add message timestamp for insertion
                solar_data['message_datetime'] = message_datetime
                solar_data['message_ts'] = message_ts
                
                return solar_data
            else:
                logger.warning(f"Unexpected message structure: {message}")
                return None
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from value field: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing solar data: {e}")
            return None

    def write(self, batch: SinkBatch):
        """Write batch of solar panel data to QuestDB"""
        attempts_remaining = 3
        
        # Parse all messages in the batch
        parsed_data = []
        for item in batch:
            solar_data = self._parse_solar_data(item.value)
            if solar_data:
                parsed_data.append(solar_data)
        
        if not parsed_data:
            logger.warning("No valid solar data found in batch")
            return
            
        logger.info(f"Writing batch of {len(parsed_data)} solar panel records to QuestDB")
        
        while attempts_remaining > 0:
            try:
                with Sender.from_conf(self.config_string) as sender:
                    for data in parsed_data:
                        # Convert timestamp to TimestampNanos
                        internal_ts = data.get('timestamp', TimestampNanos.now())
                        if isinstance(internal_ts, int):
                            # Convert to TimestampNanos if it's an integer
                            ts_nanos = TimestampNanos(internal_ts)
                        else:
                            ts_nanos = TimestampNanos.now()
                        
                        # Send row to QuestDB
                        sender.row(
                            self.table_name,
                            symbols={
                                'panel_id': str(data.get('panel_id', '')),
                                'location_id': str(data.get('location_id', '')),
                            },
                            columns={
                                'location_name': str(data.get('location_name', '')),
                                'latitude': float(data.get('latitude', 0.0)),
                                'longitude': float(data.get('longitude', 0.0)),
                                'timezone': int(data.get('timezone', 0)),
                                'power_output': float(data.get('power_output', 0.0)),
                                'unit_power': str(data.get('unit_power', '')),
                                'temperature': float(data.get('temperature', 0.0)),
                                'unit_temp': str(data.get('unit_temp', '')),
                                'irradiance': float(data.get('irradiance', 0.0)),
                                'unit_irradiance': str(data.get('unit_irradiance', '')),
                                'voltage': float(data.get('voltage', 0.0)),
                                'unit_voltage': str(data.get('unit_voltage', '')),
                                'current': float(data.get('current', 0.0)),
                                'unit_current': str(data.get('unit_current', '')),
                                'inverter_status': str(data.get('inverter_status', '')),
                                'internal_timestamp': int(data.get('timestamp', 0)),
                                'message_datetime': str(data.get('message_datetime', '')),
                            },
                            at=ts_nanos
                        )
                    
                    # Flush to ensure data is written
                    sender.flush()
                    
                logger.info(f"Successfully wrote {len(parsed_data)} records to QuestDB table '{self.table_name}'")
                return
                
            except ConnectionError as e:
                logger.warning(f"Connection error writing to QuestDB: {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    time.sleep(3)
            except TimeoutError:
                logger.warning("Timeout writing to QuestDB, using backpressure")
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            except Exception as e:
                logger.error(f"Unexpected error writing to QuestDB: {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    time.sleep(3)
                    
        raise Exception(f"Failed to write batch to QuestDB after 3 attempts")


def main():
    """Main application setup and execution"""
    
    # Setup necessary objects
    app = Application(
        consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb_solar_sink"),
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize QuestDB sink
    questdb_sink = QuestDBSolarSink()
    
    # Define input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Add debugging to show message structure
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Sink data to QuestDB
    sdf.sink(questdb_sink)

    # Run with limited count for testing
    logger.info("Starting QuestDB solar data sink application...")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()