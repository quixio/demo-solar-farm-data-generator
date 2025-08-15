# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
import json
import csv
import io
from datetime import datetime
from google.cloud import storage
from google.auth.exceptions import GoogleAuthError
import logging

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GoogleStorageCSVSink(BatchingSink):
    """
    A sink that writes solar sensor data to CSV files in Google Cloud Storage.
    
    This sink processes Kafka messages containing solar sensor data and writes them
    as CSV files to a Google Cloud Storage bucket. Each batch is written as a separate
    CSV file with a timestamp-based filename.
    """
    
    def __init__(self):
        super().__init__()
        
        # Get environment variables
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME")
        self.credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        self.file_prefix = os.environ.get("GCS_FILE_PREFIX", "solar_data")
        
        # Validate required environment variables
        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable is required")
        
        logger.info(f"Initializing Google Storage CSV Sink for bucket: {self.bucket_name}")
        
        # Initialize the storage client
        self.client = None
        self.bucket = None
        
        # CSV headers based on the schema analysis
        self.csv_headers = [
            'topicId', 'topicName', 'streamId', 'type', 'dateTime', 'partition', 'offset',
            'panel_id', 'location_id', 'location_name', 'latitude', 'longitude', 'timezone',
            'power_output', 'unit_power', 'temperature', 'unit_temp', 'irradiance', 
            'unit_irradiance', 'voltage', 'unit_voltage', 'current', 'unit_current', 
            'inverter_status', 'timestamp'
        ]
    
    def setup(self):
        """Initialize the Google Cloud Storage client and validate bucket access"""
        try:
            logger.info("Setting up Google Cloud Storage client...")
            
            # Initialize the storage client
            if self.credentials_path:
                self.client = storage.Client.from_service_account_json(self.credentials_path)
            else:
                # Use default credentials (ADC)
                self.client = storage.Client()
            
            # Get the bucket reference
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test bucket access
            if not self.bucket.exists():
                raise Exception(f"Bucket '{self.bucket_name}' does not exist or is not accessible")
            
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except GoogleAuthError as e:
            logger.error(f"Google Cloud authentication failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to setup Google Cloud Storage client: {e}")
            raise

    def _parse_message_value(self, message_value):
        """
        Parse the message value to extract solar sensor data.
        Based on the schema analysis, the actual data is in a nested JSON string within the 'value' field.
        """
        try:
            # Check if message_value is a string (JSON), if so parse it
            if isinstance(message_value, str):
                message_data = json.loads(message_value)
            else:
                message_data = message_value
            
            # Extract the nested JSON from the 'value' field if present
            if 'value' in message_data and isinstance(message_data['value'], str):
                # Parse the nested JSON string in the 'value' field
                solar_data = json.loads(message_data['value'])
                
                # Combine top-level fields with parsed solar data
                result = {
                    'topicId': message_data.get('topicId', ''),
                    'topicName': message_data.get('topicName', ''),
                    'streamId': message_data.get('streamId', ''),
                    'type': message_data.get('type', ''),
                    'dateTime': message_data.get('dateTime', ''),
                    'partition': message_data.get('partition', ''),
                    'offset': message_data.get('offset', ''),
                    **solar_data  # Add all fields from the parsed solar data
                }
            else:
                # If no nested 'value' field, assume the message is already parsed
                result = message_data
            
            return result
            
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse message value: {e}")
            logger.error(f"Raw message value: {message_value}")
            return {}

    def _create_csv_content(self, data_batch):
        """Create CSV content from a batch of solar sensor data"""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=self.csv_headers, extrasaction='ignore')
        
        # Write header
        writer.writeheader()
        
        # Write data rows
        for data in data_batch:
            parsed_data = self._parse_message_value(data)
            if parsed_data:  # Only write non-empty data
                writer.writerow(parsed_data)
        
        return output.getvalue()

    def _generate_filename(self, batch):
        """Generate a unique filename for the CSV file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        return f"{self.file_prefix}_topic_{batch.topic}_partition_{batch.partition}_{timestamp}.csv"

    def write(self, batch: SinkBatch):
        """
        Write the batch of solar sensor data to Google Cloud Storage as a CSV file.
        """
        attempts_remaining = 3
        data = [item.value for item in batch]
        
        # Debug: Print raw message structure for the first few messages
        logger.info(f"Processing batch with {len(data)} messages from topic: {batch.topic}, partition: {batch.partition}")
        for i, item in enumerate(data[:2]):  # Log first 2 messages for debugging
            logger.info(f"Raw message {i+1} structure: {type(item)} - {str(item)[:200]}...")
        
        while attempts_remaining > 0:
            try:
                return self._write_to_gcs(data, batch)
            except GoogleAuthError as e:
                logger.error(f"Google Cloud authentication error: {e}")
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    time.sleep(5)
            except Exception as e:
                if "rate limit" in str(e).lower() or "quota" in str(e).lower():
                    # Handle rate limiting with backpressure
                    logger.warning(f"Rate limit encountered, triggering backpressure: {e}")
                    raise SinkBackpressureError(
                        retry_after=60.0,  # Wait 60 seconds for rate limits
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                elif "timeout" in str(e).lower():
                    # Handle timeout with backpressure
                    logger.warning(f"Timeout encountered, triggering backpressure: {e}")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # Other errors - retry with exponential backoff
                    attempts_remaining -= 1
                    if attempts_remaining > 0:
                        wait_time = (4 - attempts_remaining) * 5  # 5, 10, 15 seconds
                        logger.warning(f"Error writing to GCS, retrying in {wait_time}s: {e}")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to write to GCS after all retries: {e}")
                        raise
        
        raise Exception("Failed to write to Google Cloud Storage after all retry attempts")

    def _write_to_gcs(self, data, batch):
        """Write the data to Google Cloud Storage"""
        try:
            # Create CSV content
            csv_content = self._create_csv_content(data)
            
            if not csv_content.strip():
                logger.warning("No valid data to write, skipping batch")
                return
            
            # Generate filename
            filename = self._generate_filename(batch)
            
            # Create blob and upload
            blob = self.bucket.blob(filename)
            blob.upload_from_string(csv_content, content_type='text/csv')
            
            logger.info(f"Successfully uploaded {len(data)} records to gs://{self.bucket_name}/{filename}")
            
        except Exception as e:
            logger.error(f"Error uploading to GCS: {e}")
            raise


def main():
    """Set up the Application to sink solar sensor data to Google Cloud Storage as CSV files."""
    
    try:
        logger.info("Starting Google Storage CSV Sink Application...")
        
        # Setup necessary objects
        app = Application(
            consumer_group="gcs_csv_sink",
            auto_create_topics=True,
            auto_offset_reset="earliest",
            commit_interval=5.0,  # Commit every 5 seconds
            commit_every=100,     # Or after 100 messages
        )
        
        # Initialize the GCS CSV sink
        gcs_sink = GoogleStorageCSVSink()
        
        # Setup the input topic - use JSON deserializer to handle the nested JSON structure
        input_topic = app.topic(
            name=os.environ.get("input", "solar-data"),
            value_deserializer="json"  # This will parse the JSON automatically
        )
        
        # Create streaming dataframe
        sdf = app.dataframe(topic=input_topic)
        
        # Debug: Print message structure
        sdf = sdf.apply(lambda row: logger.info(f"Processing message from stream: {row.get('streamId', 'unknown')} with keys: {list(row.keys()) if isinstance(row, dict) else 'not a dict'}") or row)
        
        # Optional: Add any transformations here if needed
        # For example, you could filter, enrich, or validate the data
        sdf = sdf.filter(lambda row: isinstance(row, dict) and row.get('value'))  # Only process messages with 'value' field
        
        # Print some metadata for debugging
        sdf = sdf.apply(lambda row: row).print(metadata=True)
        
        # Sink to Google Cloud Storage
        sdf.sink(gcs_sink)
        
        # Start the application
        logger.info("Application setup complete. Starting to process messages...")
        
        # For initial testing - process limited messages
        # Remove count and timeout for production use
        try:
            app.run(count=10, timeout=20)  # Test with 10 messages and 20 second timeout
            logger.info("Test run completed successfully!")
        except Exception as e:
            logger.warning(f"Test run encountered an issue (this might be expected if no messages are available): {e}")
            # For production, use: app.run()
            
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()