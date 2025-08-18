# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import csv
import json
import time
import logging
from datetime import datetime
from io import StringIO
from google.cloud import storage
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GCSCSVSink(BatchingSink):
    """
    A sink that writes solar sensor data to a CSV file in Google Cloud Storage bucket.
    
    The sink processes batches of Kafka messages containing solar panel data and writes 
    them to a CSV file in GCS with proper formatting and error handling.
    """
    
    def __init__(self):
        super().__init__()
        self.bucket_name = os.environ.get('GCS_BUCKET_NAME')
        self.csv_filename = os.environ.get('GCS_CSV_FILENAME', 'solar_data.csv')
        self.gcs_credentials_json = os.environ.get('GCS_CREDENTIALS_JSON')
        
        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable is required")
        if not self.gcs_credentials_json:
            raise ValueError("GCS_CREDENTIALS_JSON environment variable is required")
        
        self.client = None
        self.bucket = None
        
        logger.info(f"Initialized GCS CSV Sink - Bucket: {self.bucket_name}, File: {self.csv_filename}")

    def setup(self):
        """Initialize GCS client and validate bucket access"""
        try:
            # Parse credentials JSON from environment variable
            credentials_info = json.loads(self.gcs_credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            
            # Create GCS client
            self.client = storage.Client(credentials=credentials)
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test bucket access
            if not self.bucket.exists():
                raise Exception(f"GCS bucket '{self.bucket_name}' does not exist")
                
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse GCS credentials JSON: {e}")
        except Exception as e:
            logger.error(f"Failed to setup GCS client: {e}")
            raise

    def _convert_timestamp_to_datetime(self, timestamp):
        """Convert epoch timestamp to readable datetime string"""
        try:
            if not isinstance(timestamp, (int, float)):
                return str(timestamp)
            
            # Handle both seconds and nanoseconds timestamps
            if timestamp > 1e12:  # Likely nanoseconds
                dt = datetime.fromtimestamp(timestamp / 1e9)
            elif timestamp > 1e9:  # Likely milliseconds  
                dt = datetime.fromtimestamp(timestamp / 1e3)
            else:  # Likely seconds
                dt = datetime.fromtimestamp(timestamp)
            
            return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Include milliseconds
        except (ValueError, OSError, OverflowError) as e:
            logger.warning(f"Failed to convert timestamp {timestamp}: {e}")
            return str(timestamp)  # Return original if conversion fails

    def _extract_solar_data(self, message_data):
        """Extract and structure solar sensor data from message"""
        try:
            # Parse the nested JSON value if it's still a string
            if 'value' in message_data and isinstance(message_data['value'], str):
                solar_data = json.loads(message_data['value'])
            elif 'value' in message_data and isinstance(message_data['value'], dict):
                solar_data = message_data['value']
            else:
                # Data might be directly in the message
                solar_data = message_data
            
            # Extract fields with safe access
            extracted = {
                'timestamp': self._convert_timestamp_to_datetime(
                    solar_data.get('timestamp', message_data.get('timestamp', time.time()))
                ),
                'panel_id': solar_data.get('panel_id', ''),
                'location_id': solar_data.get('location_id', ''),
                'location_name': solar_data.get('location_name', ''),
                'latitude': solar_data.get('latitude', 0.0),
                'longitude': solar_data.get('longitude', 0.0),
                'timezone': solar_data.get('timezone', 0),
                'power_output': solar_data.get('power_output', 0.0),
                'unit_power': solar_data.get('unit_power', 'W'),
                'temperature': solar_data.get('temperature', 0.0),
                'unit_temp': solar_data.get('unit_temp', 'C'),
                'irradiance': solar_data.get('irradiance', 0.0),
                'unit_irradiance': solar_data.get('unit_irradiance', 'W/m²'),
                'voltage': solar_data.get('voltage', 0.0),
                'unit_voltage': solar_data.get('unit_voltage', 'V'),
                'current': solar_data.get('current', 0.0),
                'unit_current': solar_data.get('unit_current', 'A'),
                'inverter_status': solar_data.get('inverter_status', ''),
                'topic_id': message_data.get('topicId', ''),
                'topic_name': message_data.get('topicName', ''),
                'stream_id': message_data.get('streamId', ''),
                'kafka_timestamp': self._convert_timestamp_to_datetime(
                    message_data.get('dateTime', time.time())
                ) if isinstance(message_data.get('dateTime'), (int, float)) else message_data.get('dateTime', ''),
                'partition': message_data.get('partition', 0),
                'offset': message_data.get('offset', 0)
            }
            
            return extracted
            
        except Exception as e:
            logger.error(f"Failed to extract solar data: {e}")
            logger.error(f"Message data: {message_data}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar sensor data to CSV file in GCS.
        
        Handles retry logic for connection issues and backpressure for temporary failures.
        """
        attempts_remaining = 3
        logger.info("Writing batch to GCS")
        
        while attempts_remaining:
            try:
                # Create CSV content in memory
                csv_buffer = StringIO()
                
                # Define CSV headers
                headers = [
                    'timestamp', 'panel_id', 'location_id', 'location_name', 'latitude', 'longitude',
                    'timezone', 'power_output', 'unit_power', 'temperature', 'unit_temp', 
                    'irradiance', 'unit_irradiance', 'voltage', 'unit_voltage', 'current', 
                    'unit_current', 'inverter_status', 'topic_id', 'topic_name', 'stream_id',
                    'kafka_timestamp', 'partition', 'offset'
                ]
                
                writer = csv.DictWriter(csv_buffer, fieldnames=headers)
                
                # Check if file exists to determine whether to write header
                blob = self.bucket.blob(self.csv_filename)
                write_header = not blob.exists()
                
                if write_header:
                    writer.writeheader()
                
                # Process each message in the batch
                processed_count = 0
                for item in batch:
                    # Debug: Show raw message structure
                    logger.info(f"Raw message: {item.value}")
                    
                    try:
                        extracted_data = self._extract_solar_data(item.value)
                        writer.writerow(extracted_data)
                        processed_count += 1
                        logger.debug(f"Processed message: {extracted_data['panel_id']}")
                    except Exception as e:
                        logger.error(f"Failed to process message: {e}")
                        logger.error(f"Message content: {item.value}")
                        # Continue processing other messages in the batch
                        continue
                
                # Upload to GCS
                csv_content = csv_buffer.getvalue()
                
                if blob.exists() and not write_header:
                    # Append to existing file
                    existing_content = blob.download_as_text()
                    csv_content = existing_content + csv_content
                
                # Upload the complete content
                blob.upload_from_string(
                    csv_content,
                    content_type='text/csv'
                )
                
                logger.info(f"Successfully wrote {processed_count} records to {self.bucket_name}/{self.csv_filename}")
                return
                
            except Exception as e:
                logger.error(f"Attempt failed: {e}")
                attempts_remaining -= 1
                
                if "429" in str(e) or "quota" in str(e).lower():
                    # Rate limiting, use backpressure
                    logger.warning("GCS rate limit detected, triggering backpressure")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                elif "connection" in str(e).lower() or "network" in str(e).lower():
                    # Connection issue, retry with delay
                    if attempts_remaining:
                        logger.warning(f"Connection error, retrying in 3 seconds. Attempts left: {attempts_remaining}")
                        time.sleep(3)
                    else:
                        raise Exception(f"Failed to write to GCS after retries: {e}")
                else:
                    # Other errors, don't retry
                    raise Exception(f"GCS write error: {e}")
        
        raise Exception("Failed to write to GCS after all retry attempts")


def main():
    """Set up and run the solar data to GCS CSV sink application."""
    
    logger.info("Starting Solar Data to GCS CSV Sink Application")
    
    try:
        # Setup necessary objects
        app = Application(
            consumer_group="solar_gcs_csv_sink",
            auto_create_topics=True,
            auto_offset_reset="earliest"
        )
        
        # Initialize the GCS CSV sink
        gcs_sink = GCSCSVSink()
        gcs_sink.setup()
        
        # Configure input topic
        input_topic = app.topic(name=os.environ["input"], value_deserializer="json")
        sdf = app.dataframe(topic=input_topic)
        
        # Process and debug messages
        sdf = sdf.apply(lambda row: row).print(metadata=True)
        
        # Sink data to GCS
        sdf.sink(gcs_sink)
        
        # Run the application for testing (process 10 messages)
        logger.info("Starting application - will process 10 messages for testing")
        app.run(count=10, timeout=20)
        
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()