# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import csv
import logging
from datetime import datetime
from io import StringIO
from google.cloud import storage
from google.api_core.exceptions import GoogleCloudError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class GCSSolarDataSink(BatchingSink):
    """
    A custom sink that writes solar sensor data to a CSV file in Google Cloud Storage.
    
    The sink processes messages containing solar panel data with the following structure:
    - Messages have a nested JSON string in the 'value' field
    - The JSON contains solar panel metrics like power_output, temperature, irradiance, etc.
    - Data is batched and written to GCS as CSV files
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.gcs_client = None
        self.bucket_name = None
        self.folder_path = None
        self.csv_headers = [
            'message_timestamp', 'kafka_key', 'kafka_topic', 'kafka_partition', 'kafka_offset',
            'panel_id', 'location_id', 'location_name', 'latitude', 'longitude', 'timezone',
            'power_output', 'unit_power', 'temperature', 'unit_temp', 'irradiance', 'unit_irradiance',
            'voltage', 'unit_voltage', 'current', 'unit_current', 'inverter_status', 'sensor_timestamp'
        ]
        
    def setup(self):
        """Initialize Google Cloud Storage client and validate bucket access"""
        try:
            # Get configuration from environment variables
            self.bucket_name = os.environ.get('GCS_BUCKET_NAME')
            self.folder_path = os.environ.get('GCS_FOLDER_PATH', 'solar-data')
            service_account_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            
            if not self.bucket_name:
                raise ValueError("GCS_BUCKET_NAME environment variable is required")
            
            # Initialize GCS client
            if service_account_path:
                self.gcs_client = storage.Client.from_service_account_json(service_account_path)
                logger.info(f"Initialized GCS client with service account: {service_account_path}")
            else:
                # Use default credentials (e.g., from metadata service)
                self.gcs_client = storage.Client()
                logger.info("Initialized GCS client with default credentials")
            
            # Test bucket access
            bucket = self.gcs_client.bucket(self.bucket_name)
            if not bucket.exists():
                raise ValueError(f"GCS bucket '{self.bucket_name}' does not exist or is not accessible")
            
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to setup GCS client: {str(e)}")
            raise
    
    def _extract_solar_data(self, item):
        """Extract and parse solar sensor data from Kafka message"""
        try:
            # Debug: Print raw message structure
            logger.debug(f"Raw message structure: {type(item.value)} - {item.value}")
            
            # Initialize row with Kafka metadata
            row = {
                'message_timestamp': datetime.fromtimestamp(item.timestamp / 1000).isoformat() if item.timestamp else '',
                'kafka_key': item.key.decode('utf-8') if isinstance(item.key, bytes) else str(item.key) if item.key else '',
                'kafka_topic': item.topic if hasattr(item, 'topic') else '',
                'kafka_partition': item.partition if hasattr(item, 'partition') else '',
                'kafka_offset': item.offset if hasattr(item, 'offset') else ''
            }
            
            # Handle the nested JSON structure
            solar_data = {}
            
            # Check if the value is already a dict (deserialized JSON)
            if isinstance(item.value, dict):
                # Check if there's a 'value' field containing JSON string
                if 'value' in item.value and isinstance(item.value['value'], str):
                    try:
                        solar_data = json.loads(item.value['value'])
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse nested JSON in 'value' field: {item.value['value']}")
                        solar_data = item.value
                else:
                    solar_data = item.value
            elif isinstance(item.value, str):
                # Try to parse as JSON
                try:
                    parsed_value = json.loads(item.value)
                    if isinstance(parsed_value, dict) and 'value' in parsed_value:
                        solar_data = json.loads(parsed_value['value'])
                    else:
                        solar_data = parsed_value
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse JSON from string: {item.value}")
                    solar_data = {}
            else:
                logger.warning(f"Unexpected value type: {type(item.value)}")
                solar_data = {}
            
            # Extract solar sensor fields with defaults
            row.update({
                'panel_id': solar_data.get('panel_id', ''),
                'location_id': solar_data.get('location_id', ''),
                'location_name': solar_data.get('location_name', ''),
                'latitude': solar_data.get('latitude', ''),
                'longitude': solar_data.get('longitude', ''),
                'timezone': solar_data.get('timezone', ''),
                'power_output': solar_data.get('power_output', ''),
                'unit_power': solar_data.get('unit_power', ''),
                'temperature': solar_data.get('temperature', ''),
                'unit_temp': solar_data.get('unit_temp', ''),
                'irradiance': solar_data.get('irradiance', ''),
                'unit_irradiance': solar_data.get('unit_irradiance', ''),
                'voltage': solar_data.get('voltage', ''),
                'unit_voltage': solar_data.get('unit_voltage', ''),
                'current': solar_data.get('current', ''),
                'unit_current': solar_data.get('unit_current', ''),
                'inverter_status': solar_data.get('inverter_status', ''),
                'sensor_timestamp': datetime.fromtimestamp(solar_data['timestamp'] / 1000000000).isoformat() if solar_data.get('timestamp') else ''
            })
            
            return row
            
        except Exception as e:
            logger.error(f"Error extracting solar data: {str(e)}")
            logger.error(f"Item value: {item.value}")
            # Return a minimal row with error info
            return {header: '' for header in self.csv_headers}
    
    def _upload_csv_to_gcs(self, csv_content, filename):
        """Upload CSV content to Google Cloud Storage"""
        try:
            bucket = self.gcs_client.bucket(self.bucket_name)
            blob_path = f"{self.folder_path}/{filename}" if self.folder_path else filename
            blob = bucket.blob(blob_path)
            
            # Upload with proper content type
            blob.upload_from_string(csv_content, content_type='text/csv')
            logger.info(f"Successfully uploaded CSV to gs://{self.bucket_name}/{blob_path}")
            
        except GoogleCloudError as e:
            logger.error(f"Google Cloud Storage error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error uploading to GCS: {str(e)}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar sensor data to GCS as CSV.
        
        This method processes each message in the batch, extracts solar data,
        converts it to CSV format, and uploads to Google Cloud Storage.
        """
        attempts_remaining = 3
        
        while attempts_remaining > 0:
            try:
                logger.info(f"Processing batch with {len(batch)} messages from topic {batch.topic}")
                
                # Process each message and extract solar data
                rows = []
                for item in batch:
                    row_data = self._extract_solar_data(item)
                    rows.append(row_data)
                
                if not rows:
                    logger.warning("No data extracted from batch")
                    return
                
                # Create CSV content
                csv_buffer = StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=self.csv_headers)
                writer.writeheader()
                writer.writerows(rows)
                csv_content = csv_buffer.getvalue()
                
                # Generate filename with timestamp and batch info
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                filename = f"solar_data_{batch.topic}_{batch.partition}_{timestamp}.csv"
                
                # Upload to GCS
                self._upload_csv_to_gcs(csv_content, filename)
                
                logger.info(f"Successfully processed batch of {len(rows)} records")
                return
                
            except GoogleCloudError as e:
                if "429" in str(e) or "quota" in str(e).lower():
                    # Rate limiting - use backpressure
                    logger.warning(f"GCS rate limiting detected: {str(e)}")
                    raise SinkBackpressureError(
                        retry_after=60.0,  # Wait 1 minute for rate limiting
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # Other GCS errors - retry with exponential backoff
                    attempts_remaining -= 1
                    if attempts_remaining > 0:
                        wait_time = (4 - attempts_remaining) * 3  # 3, 6, 9 seconds
                        logger.warning(f"GCS error: {str(e)}. Retrying in {wait_time} seconds... ({attempts_remaining} attempts left)")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to upload to GCS after all retries: {str(e)}")
                        raise
                        
            except Exception as e:
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    wait_time = (4 - attempts_remaining) * 3
                    logger.warning(f"Error writing batch: {str(e)}. Retrying in {wait_time} seconds... ({attempts_remaining} attempts left)")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to write batch after all retries: {str(e)}")
                    raise
        
        raise Exception("Failed to write batch to GCS after all retry attempts")


def main():
    """ Here we will set up our Application. """
    
    logger.info("Starting Solar Data GCS Sink Application")
    
    # Setup necessary objects
    app = Application(
        consumer_group="solar_gcs_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest",
        # Optimize for batching - commit less frequently to get larger batches
        commit_interval=10.0,  # Commit every 10 seconds
        commit_every=100       # Or after processing 100 messages
    )
    
    # Initialize the GCS sink
    try:
        gcs_sink = GCSSolarDataSink(
            on_client_connect_success=lambda: logger.info("GCS connection successful"),
            on_client_connect_failure=lambda e: logger.error(f"GCS connection failed: {e}")
        )
        
        # Setup the sink (validate GCS access)
        gcs_sink.setup()
        
    except Exception as e:
        logger.error(f"Failed to initialize GCS sink: {str(e)}")
        raise
    
    # Setup input topic - use JSON deserializer to handle the message structure
    input_topic = app.topic(
        name=os.environ["input"], 
        value_deserializer="json"  # This will automatically parse JSON strings to dicts
    )
    sdf = app.dataframe(topic=input_topic)
    
    # Add some debugging and basic data validation
    def debug_message(row):
        """Debug function to inspect message structure"""
        logger.info(f"Message received - Type: {type(row)}, Keys: {list(row.keys()) if isinstance(row, dict) else 'Not a dict'}")
        return row
    
    def validate_message(row):
        """Basic validation of message structure"""
        if not isinstance(row, dict):
            logger.warning(f"Expected dict message, got {type(row)}")
            return row
            
        # Check for the nested value structure from schema analysis
        if 'value' in row and isinstance(row['value'], str):
            try:
                # This is the nested JSON structure from the schema
                parsed_data = json.loads(row['value'])
                if 'panel_id' in parsed_data:
                    logger.debug(f"Valid solar data found for panel: {parsed_data.get('panel_id')}")
                else:
                    logger.warning("No panel_id found in parsed solar data")
            except json.JSONDecodeError:
                logger.warning("Failed to parse nested JSON in value field")
        
        return row
    
    # Apply transformations and debugging
    sdf = sdf.apply(debug_message)
    sdf = sdf.apply(validate_message)
    sdf = sdf.print(metadata=True)  # Print for debugging
    
    # Sink to GCS
    sdf.sink(gcs_sink)
    
    logger.info("Pipeline configured, starting to process messages...")
    
    # For testing, limit the number of messages
    testing_mode = os.environ.get('TESTING_MODE', 'false').lower() == 'true'
    if testing_mode:
        logger.info("Running in testing mode - processing limited messages")
        app.run(count=10, timeout=20)
    else:
        # Production mode - run indefinitely
        app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()