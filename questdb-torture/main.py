# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import datetime
from questdb.ingress import Sender, TimestampNanos

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class QuestDBSink(BatchingSink):
    """
    A sink for writing solar panel data to QuestDB time-series database.
    
    This sink processes messages containing solar panel sensor data and writes
    them to QuestDB using the ILP (InfluxDB Line Protocol) over HTTP.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__()
        self._sender = None
        self._questdb_host = os.environ.get('QUESTDB_HOST', 'localhost')
        
        # Handle port conversion safely
        try:
            self._questdb_port = int(os.environ.get('QUESTDB_PORT', '9000'))
        except ValueError:
            self._questdb_port = 9000
            
        self._questdb_token = os.environ.get('QUESTDB_TOKEN')
        self._questdb_username = os.environ.get('QUESTDB_USERNAME')
        self._questdb_password = os.environ.get('QUESTDB_PASSWORD')
        self._table_name = os.environ.get('QUESTDB_TABLE', 'solar_panel_data')
        
        # Store callbacks
        self._on_client_connect_success = on_client_connect_success
        self._on_client_connect_failure = on_client_connect_failure
        
        # Initialize connection flag
        self._is_setup = False
        self._is_shutting_down = False
        self._cleanup_called = False

    def setup(self):
        """
        Setup the QuestDB connection.
        Called once when the sink is initialized.
        """
        if self._is_setup:
            return  # Already set up
            
        try:
            print(f"Connecting to QuestDB at {self._questdb_host}:{self._questdb_port}")
            
            # Test connection by ensuring sender can be created
            self._ensure_sender()
            self._is_setup = True
            
            # Call success callback if connection succeeds
            if self._on_client_connect_success:
                self._on_client_connect_success()
                
        except Exception as e:
            print(f"Failed to connect to QuestDB: {e}")
            # Call failure callback if connection fails  
            if self._on_client_connect_failure:
                self._on_client_connect_failure(e)
            raise
    
    def cleanup(self):
        """
        Cleanup resources when the sink is shutting down.
        """
        if self._cleanup_called:
            print("Cleanup already called, skipping...")
            return
            
        print("QuestDB sink cleanup initiated...")
        self._cleanup_called = True
        self._is_shutting_down = True
        
        if self._sender:
            try:
                # Check if sender is still open before attempting operations
                if not hasattr(self._sender, '_closed') or not self._sender._closed:
                    # Ensure all pending data is flushed before closing
                    print("Flushing remaining data before shutdown...")
                    self._sender.flush()
                    self._sender.close()
                self._sender = None
                self._is_setup = False
                print("QuestDB connection closed successfully")
            except Exception as e:
                print(f"Error closing QuestDB connection: {e}")
                # Don't raise the error during cleanup
                self._sender = None
                self._is_setup = False
    
    def close(self):
        """
        Close method called by Quix Streams framework when shutting down.
        This ensures proper cleanup sequence.
        """
        print("QuestDB sink close() called")
        self.cleanup()
    
    def _parse_solar_data(self, item):
        """
        Parse the incoming message to extract solar panel data.
        
        Based on the logs, the data is already parsed and item.value 
        contains the solar panel data directly as a dictionary.
        """
        try:
            # The value field should already contain the parsed solar data
            if isinstance(item.value, dict):
                # If the value is already a dict, use it directly
                solar_data = item.value
            elif isinstance(item.value, str):
                # If it's still a JSON string, parse it
                solar_data = json.loads(item.value)
            elif isinstance(item.value, dict) and 'value' in item.value:
                # If the message has nested structure with 'value' field containing JSON string
                if isinstance(item.value['value'], str):
                    solar_data = json.loads(item.value['value'])
                else:
                    solar_data = item.value['value']
            else:
                raise ValueError(f"Unexpected data format: {type(item.value)}")
            
            return solar_data
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"Error parsing solar data: {e}")
            print(f"Raw data: {item.value}")
            raise

    def _ensure_sender(self):
        """
        Ensure the QuestDB sender is available, creating it if necessary.
        """
        if self._sender is None:
            print("Sender is None, creating new sender...")
            try:
                # Build configuration string based on available credentials
                if self._questdb_token:
                    conf = f"http::addr={self._questdb_host}:{self._questdb_port};token={self._questdb_token};"
                elif self._questdb_username and self._questdb_password:
                    conf = f"http::addr={self._questdb_host}:{self._questdb_port};username={self._questdb_username};password={self._questdb_password};"
                else:
                    conf = f"http::addr={self._questdb_host}:{self._questdb_port};"
                
                # Mask sensitive info for logging
                safe_conf = conf
                if self._questdb_token:
                    safe_conf = conf.replace(self._questdb_token, '***')
                if self._questdb_password:
                    safe_conf = safe_conf.replace(self._questdb_password, '***')
                print(f"Creating QuestDB sender with config: {safe_conf}")
                self._sender = Sender.from_conf(conf)
                print("QuestDB sender created successfully")
                
                # Print diagnostics
                print(f"Sender object: {self._sender}")
                print(f"Sender type: {type(self._sender)}")
                if hasattr(self._sender, '_closed'):
                    print(f"Sender closed state: {self._sender._closed}")
                    
            except Exception as e:
                print(f"Failed to create QuestDB sender: {e}")
                raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to QuestDB.
        
        This method processes each message in the batch, extracts the solar panel
        data, and writes it to QuestDB using the ILP protocol.
        """
        # Check if we're shutting down
        if self._is_shutting_down:
            print("Sink is shutting down, skipping write operation")
            return
            
        # Ensure sender is available
        self._ensure_sender()
        
        # Check if sender is closed before proceeding
        if hasattr(self._sender, '_closed') and self._sender._closed:
            if self._is_shutting_down:
                print("Sender is closed during shutdown, skipping batch")
                return
            print("Sender is closed, recreating...")
            self._sender = None
            self._ensure_sender()
        
        print(f"About to write batch, sender: {type(self._sender)}")
        
        attempts_remaining = 3
        
        while attempts_remaining:
            try:
                processed_count = 0
                # Process each item in the batch
                for item in batch:
                    try:
                        # Check if we're shutting down or sender is closed
                        if self._is_shutting_down:
                            print("Shutting down during batch processing, skipping remaining items")
                            break
                        if hasattr(self._sender, '_closed') and self._sender._closed:
                            print("Sender closed during batch processing, skipping remaining items")
                            break
                            
                        # Parse the solar panel data from the message
                        solar_data = self._parse_solar_data(item)
                        
                        # Extract timestamp - convert epoch nanoseconds to TimestampNanos
                        timestamp_ns = solar_data.get('timestamp', int(time.time() * 1_000_000_000))
                        if timestamp_ns > 1_000_000_000_000_000_000:  # Looks like nanoseconds
                            timestamp_obj = TimestampNanos(timestamp_ns)
                        else:  # Might be seconds or milliseconds
                            timestamp_obj = TimestampNanos.now()
                        
                        # Create symbols (indexed columns) for efficient querying
                        symbols = {
                            'panel_id': solar_data.get('panel_id', 'unknown'),
                            'location_id': solar_data.get('location_id', 'unknown'),
                            'location_name': solar_data.get('location_name', 'unknown'),
                            'inverter_status': solar_data.get('inverter_status', 'unknown')
                        }
                        
                        # Create columns for numerical and other data
                        columns = {
                            'latitude': float(solar_data.get('latitude', 0.0)),
                            'longitude': float(solar_data.get('longitude', 0.0)),
                            'timezone': int(solar_data.get('timezone', 0)),
                            'power_output': float(solar_data.get('power_output', 0.0)),
                            'temperature': float(solar_data.get('temperature', 0.0)),
                            'irradiance': float(solar_data.get('irradiance', 0.0)),
                            'voltage': float(solar_data.get('voltage', 0.0)),
                            'current': float(solar_data.get('current', 0.0)),
                            'unit_power': solar_data.get('unit_power', 'W'),
                            'unit_temp': solar_data.get('unit_temp', 'C'),
                            'unit_irradiance': solar_data.get('unit_irradiance', 'W/m²'),
                            'unit_voltage': solar_data.get('unit_voltage', 'V'),
                            'unit_current': solar_data.get('unit_current', 'A')
                        }
                        
                        # Write the row to QuestDB
                        self._sender.row(
                            self._table_name,
                            symbols=symbols,
                            columns=columns,
                            at=timestamp_obj
                        )
                        
                        processed_count += 1
                        
                    except Exception as e:
                        print(f"Error processing individual message: {e}")
                        print(f"Message data: {item.value}")
                        # Continue processing other messages in the batch
                        continue
                
                # Flush all rows in the batch only if sender is still open and we're not shutting down
                if processed_count > 0:
                    if self._is_shutting_down:
                        print(f"Batch had {processed_count} processed messages but sink is shutting down, skipping flush")
                    elif not self._sender:
                        print(f"Batch had {processed_count} processed messages but sender is None, skipping flush")
                    elif hasattr(self._sender, '_closed') and self._sender._closed:
                        print(f"Batch had {processed_count} processed messages but sender was closed, skipping flush")
                    else:
                        try:
                            self._sender.flush()
                            print(f"Successfully wrote batch of {processed_count} messages to QuestDB table '{self._table_name}'")
                        except Exception as flush_error:
                            print(f"Error flushing batch: {flush_error}")
                            # Don't re-raise flush errors during shutdown sequence
                            if not self._is_shutting_down:
                                raise
                    
                return  # Success, exit the retry loop
                
            except ConnectionError as e:
                print(f"Connection error writing to QuestDB: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                    # Try to recreate the sender
                    self._sender = None
                    self._ensure_sender()
            except TimeoutError as e:
                print(f"Timeout error writing to QuestDB: {e}")
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
            except Exception as e:
                error_message = str(e).lower()
                if "sender is closed" in error_message or "closed" in error_message:
                    if self._is_shutting_down:
                        print(f"Sender closed during shutdown: {e}, skipping retry")
                        return  # Exit gracefully during shutdown
                    print(f"Sender closed error: {e}, attempting to recreate sender...")
                    attempts_remaining -= 1
                    if attempts_remaining:
                        self._sender = None
                        try:
                            self._ensure_sender()
                            time.sleep(1)  # Brief pause before retry
                        except Exception as recreate_error:
                            print(f"Failed to recreate sender: {recreate_error}")
                            if attempts_remaining == 1:  # Last attempt
                                raise
                else:
                    print(f"Unexpected error writing to QuestDB: {e}")
                    raise
        
        raise Exception("Failed to write to QuestDB after 3 attempts")

    def flush(self):
        """
        Flush any pending data. Called by the framework during checkpoint commits.
        """
        if self._is_shutting_down or self._cleanup_called:
            print("Flush called during shutdown, skipping")
            return
            
        if self._sender:
            try:
                if not hasattr(self._sender, '_closed') or not self._sender._closed:
                    self._sender.flush()
                    print("QuestDB sink flushed pending data")
                else:
                    print("Sender is closed, skipping flush")
            except Exception as e:
                error_msg = str(e).lower()
                if "closed" in error_msg:
                    print(f"Sender closed during flush: {e}")
                    # Mark as shutting down to prevent further operations
                    self._is_shutting_down = True
                else:
                    print(f"Error flushing QuestDB sink: {e}")
                    # Don't re-raise to avoid disrupting checkpoint commits


def main():
    """Set up and run the solar panel data sink application."""
    
    print("Starting Solar Panel QuestDB Sink Application")
    
    # Setup necessary objects
    app = Application(
        consumer_group="solar_questdb_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize the QuestDB sink
    questdb_sink = QuestDBSink(
        on_client_connect_success=lambda: print("✅ Connected to QuestDB successfully!"),
        on_client_connect_failure=lambda e: print(f"❌ Failed to connect to QuestDB: {e}")
    )
    
    # Set up the input topic - using environment variable for flexibility
    input_topic_name = os.environ.get("input", "solar-data")
    input_topic = app.topic(name=input_topic_name, value_deserializer="json")
    sdf = app.dataframe(topic=input_topic)
    
    # Add some processing and debugging
    sdf = sdf.apply(lambda row: row).print(metadata=True)
    
    # Sink the data to QuestDB
    sdf.sink(questdb_sink)
    
    print(f"Configured to read from topic: {input_topic_name}")
    print(f"Target QuestDB table: {os.environ.get('QUESTDB_TABLE', 'solar_panel_data')}")
    print("Pipeline configured. Starting to process messages...")
    
    try:
        # Run the application for testing (process 10 messages then stop)
        app.run(count=10, timeout=20)
        print("Application completed successfully")
    except KeyboardInterrupt:
        print("Application interrupted by user")
    except Exception as e:
        print(f"Application error: {e}")
        raise


if __name__ == "__main__":
    main()