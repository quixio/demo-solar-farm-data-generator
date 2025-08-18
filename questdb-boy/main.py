# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import requests
import psycopg2
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class QuestDBSink(BatchingSink):
    """
    A custom sink for writing solar panel data to QuestDB.
    QuestDB supports both HTTP REST API and PostgreSQL wire protocol.
    We'll use HTTP for data ingestion as it's more efficient for batch writes.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            _on_client_connect_success=on_client_connect_success,
            _on_client_connect_failure=on_client_connect_failure
        )
        self.host = os.environ.get('QUESTDB_HOST', 'localhost')
        try:
            self.http_port = int(os.environ.get('QUESTDB_HTTP_PORT', '9000'))
        except ValueError:
            self.http_port = 9000
        
        self.username = os.environ.get('QUESTDB_USERNAME', '')
        self.password = os.environ.get('QUESTDB_PASSWORD', '')
        self.database = os.environ.get('QUESTDB_DATABASE', 'qdb')
        self.table_name = os.environ.get('QUESTDB_TABLE', 'solar_panel_data')
        
        self.http_url = f"http://{self.host}:{self.http_port}"
        self._table_created = False
        
        print(f"QuestDB Sink initialized - Host: {self.host}, HTTP Port: {self.http_port}, Table: {self.table_name}")

    def setup(self):
        """Test connection and create table if needed"""
        try:
            # Test HTTP connection first
            response = requests.get(f"{self.http_url}/", timeout=5)
            response.raise_for_status()
            print("QuestDB HTTP connection successful")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            print(f"QuestDB connection failed: {e}")
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise

    def _create_table_if_not_exists(self):
        """Create the solar panel data table if it doesn't exist"""
        if self._table_created:
            return
            
        # SQL to create table with proper schema for solar panel data
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
            timestamp TIMESTAMP,
            ingested_at TIMESTAMP
        ) timestamp(timestamp) PARTITION BY DAY;
        """
        
        try:
            response = requests.get(
                f"{self.http_url}/exec",
                params={'query': create_table_sql},
                timeout=10
            )
            response.raise_for_status()
            print(f"Table {self.table_name} created/verified successfully")
            self._table_created = True
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to create table {self.table_name}: {e}")
            raise

    def _convert_timestamp(self, epoch_timestamp):
        """Convert epoch timestamp to QuestDB timestamp format"""
        try:
            # The timestamp appears to be in nanoseconds based on the schema analysis
            # Convert to seconds for datetime
            timestamp_seconds = epoch_timestamp / 1_000_000_000
            return datetime.fromtimestamp(timestamp_seconds)
        except (ValueError, TypeError):
            # Fallback to current time if conversion fails
            print(f"Warning: Could not convert timestamp {epoch_timestamp}, using current time")
            return datetime.now()

    def _prepare_insert_sql(self, data_batch):
        """Prepare INSERT SQL statement with multiple rows"""
        if not data_batch:
            return None
            
        values_list = []
        current_time = datetime.now()
        
        for item in data_batch:
            # Parse the JSON value if it's a string
            if isinstance(item, str):
                try:
                    item = json.loads(item)
                except json.JSONDecodeError:
                    print(f"Warning: Could not parse JSON: {item}")
                    continue
            
            # Convert timestamp
            timestamp_dt = self._convert_timestamp(item.get('timestamp', 0))
            
            # Escape single quotes in string values
            def escape_string(value):
                if isinstance(value, str):
                    return value.replace("'", "''")
                return value
            
            values = f"""(
                '{escape_string(item.get('panel_id', ''))}',
                '{escape_string(item.get('location_id', ''))}',
                '{escape_string(item.get('location_name', ''))}',
                {item.get('latitude', 0)},
                {item.get('longitude', 0)},
                {item.get('timezone', 0)},
                {item.get('power_output', 0)},
                '{escape_string(item.get('unit_power', ''))}',
                {item.get('temperature', 0)},
                '{escape_string(item.get('unit_temp', ''))}',
                {item.get('irradiance', 0)},
                '{escape_string(item.get('unit_irradiance', ''))}',
                {item.get('voltage', 0)},
                '{escape_string(item.get('unit_voltage', ''))}',
                {item.get('current', 0)},
                '{escape_string(item.get('unit_current', ''))}',
                '{escape_string(item.get('inverter_status', ''))}',
                '{timestamp_dt.isoformat()}',
                '{current_time.isoformat()}'
            )"""
            values_list.append(values)
        
        if not values_list:
            return None
            
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, ingested_at
        ) VALUES {', '.join(values_list)};
        """
        
        return insert_sql

    def write(self, batch: SinkBatch):
        """Write batch of solar panel data to QuestDB"""
        attempts_remaining = 3
        
        # Extract the actual data from the batch
        data = []
        for item in batch:
            # Parse JSON from the value field
            try:
                if isinstance(item.value, str):
                    parsed_data = json.loads(item.value)
                else:
                    parsed_data = item.value
                data.append(parsed_data)
                print(f"Raw message: {item.value}")
            except (json.JSONDecodeError, AttributeError) as e:
                print(f"Error parsing message: {e}, Raw message: {item.value}")
                continue
        
        if not data:
            print("No valid data to write")
            return
            
        print(f"Writing batch of {len(data)} records to QuestDB")
        
        while attempts_remaining:
            try:
                # Prepare the INSERT SQL
                insert_sql = self._prepare_insert_sql(data)
                if not insert_sql:
                    print("No valid SQL to execute")
                    return
                
                # Execute the INSERT via HTTP
                response = requests.get(
                    f"{self.http_url}/exec",
                    params={'query': insert_sql},
                    timeout=30
                )
                response.raise_for_status()
                
                # Check response for any errors
                result = response.json()
                if 'error' in result:
                    raise Exception(f"QuestDB error: {result['error']}")
                
                print(f"Successfully wrote {len(data)} records to {self.table_name}")
                return
                
            except requests.exceptions.ConnectionError as e:
                print(f"Connection error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                    
            except requests.exceptions.Timeout as e:
                print(f"Timeout error: {e}")
                # Use backpressure for timeout - tells the app to wait and retry
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
                
            except Exception as e:
                print(f"Unexpected error writing to QuestDB: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
        
        raise Exception("Failed to write to QuestDB after all retry attempts")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="questdb_solar_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize QuestDB sink
    questdb_sink = QuestDBSink(
        on_client_connect_success=lambda: print("QuestDB connection established successfully"),
        on_client_connect_failure=lambda e: print(f"QuestDB connection failed: {e}")
    )
    
    # Setup the input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Process the data - print raw message for debugging
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Sink the data to QuestDB
    sdf.sink(questdb_sink)

    # Run the application for testing (10 messages max, 20 second timeout)
    print("Starting QuestDB solar data sink application...")
    app.run(count=10, timeout=20)


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()