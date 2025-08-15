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
import logging
from datetime import datetime
from google.cloud import storage
from google.cloud.exceptions import NotFound, GoogleCloudError

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GCPStorageCSVSink(BatchingSink):
    """
    A sink that writes solar sensor data to CSV files in Google Cloud Storage.
    
    This sink processes batches of messages containing solar panel data,
    converts them to CSV format, and uploads them to a GCS bucket.
    """
    
    def __init__(self):
        super().__init__()
        # Get environment variables
        self.bucket_name = os.environ.get('GCP_BUCKET_NAME')
        self.csv_file_prefix = os.environ.get('GCP_CSV_FILE_PREFIX', 'solar_data')
        self.credentials_json = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
        
        if not self.bucket_name:
            raise ValueError("GCP_BUCKET_NAME environment variable is required")
            
        # Initialize GCS client
        self.client = None
        self.bucket = None
        
        # CSV header for solar sensor data
        self.csv_headers = [
            'timestamp',
            'panel_id',
            'location_id', 
            'location_name',
            'latitude',
            'longitude',
            'timezone',
            'power_output',
            'unit_power',
            'temperature',
            'unit_temp',
            'irradiance',
            'unit_irradiance',
            'voltage',
            'unit_voltage',
            'current',
            'unit_current',
            'inverter_status',
            'kafka_timestamp',
            'kafka_topic',
            'kafka_partition',
            'kafka_offset'
        ]
    
    def setup(self):
        """Initialize the GCS client and validate connection."""
        try:
            if self.credentials_json:
                # Use service account from JSON string
                import json
                from google.oauth2 import service_account
                
                credentials_dict = json.loads(self.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(credentials_dict)
                self.client = storage.Client(credentials=credentials)
            else:
                # Use default credentials (ADC)
                self.client = storage.Client()
            
            # Test connection by getting bucket info
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Verify bucket exists and we can access it
            if not self.bucket.exists():
                raise ValueError(f"Bucket '{self.bucket_name}' does not exist or is not accessible")
                
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to setup GCS client: {e}")
            raise
    
    def _parse_solar_data(self, item):
        """Parse solar sensor data from the Kafka message."""
        try:
            # Debug: Print raw message structure
            logger.debug(f"Raw message value: {item.value}")
            
            # The message structure from schema analysis shows the actual solar data 
            # is in a JSON string within the 'value' field of the message
            message_data = item.value
            
            # If the value field contains a JSON string, parse it
            if isinstance(message_data, dict) and 'value' in message_data:
                solar_data_json = message_data['value']
                if isinstance(solar_data_json, str):
                    solar_data = json.loads(solar_data_json)
                else:
                    solar_data = solar_data_json
            else:
                # If the data is already parsed or in a different structure
                solar_data = message_data
            
            # Convert timestamp to readable format
            timestamp = solar_data.get('timestamp')
            if timestamp:
                # Convert from nanoseconds to seconds if needed
                if timestamp > 1e12:  # Likely nanoseconds
                    timestamp = timestamp / 1e9
                formatted_timestamp = datetime.fromtimestamp(timestamp).isoformat()
            else:
                formatted_timestamp = datetime.now().isoformat()
            
            # Create CSV row
            csv_row = [
                formatted_timestamp,
                solar_data.get('panel_id', ''),
                solar_data.get('location_id', ''),
                solar_data.get('location_name', ''),
                solar_data.get('latitude', ''),
                solar_data.get('longitude', ''),
                solar_data.get('timezone', ''),
                solar_data.get('power_output', ''),
                solar_data.get('unit_power', ''),
                solar_data.get('temperature', ''),
                solar_data.get('unit_temp', ''),
                solar_data.get('irradiance', ''),
                solar_data.get('unit_irradiance', ''),
                solar_data.get('voltage', ''),
                solar_data.get('unit_voltage', ''),
                solar_data.get('current', ''),
                solar_data.get('unit_current', ''),
                solar_data.get('inverter_status', ''),
                item.timestamp,
                item.topic,
                item.partition,
                item.offset
            ]
            
            return csv_row
            
        except Exception as e:
            logger.error(f"Error parsing solar data: {e}")
            logger.error(f"Raw item value: {item.value}")
            # Return empty row with metadata for debugging
            return [''] * 18 + [item.timestamp, item.topic, item.partition, item.offset]

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar sensor data to GCS as CSV.
        
        Creates a CSV file for each batch and uploads it to GCS with a timestamp.
        """
        attempts_remaining = 3
        
        while attempts_remaining:
            try:
                # Debug: Print batch info
                logger.info(f"Processing batch with {len(batch)} items from topic {batch.topic}, partition {batch.partition}")
                
                # Create CSV content in memory
                csv_buffer = io.StringIO()
                csv_writer = csv.writer(csv_buffer)
                
                # Write header
                csv_writer.writerow(self.csv_headers)
                
                # Process each item in the batch
                for item in batch:
                    try:
                        csv_row = self._parse_solar_data(item)
                        csv_writer.writerow(csv_row)
                    except Exception as e:
                        logger.error(f"Error processing individual item: {e}")
                        continue
                
                # Get CSV content
                csv_content = csv_buffer.getvalue()
                csv_buffer.close()
                
                if not csv_content.strip():
                    logger.warning("No valid data to write in this batch")
                    return
                
                # Generate filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
                filename = f"{self.csv_file_prefix}_{timestamp}_{batch.topic}_p{batch.partition}.csv"
                
                # Upload to GCS
                blob = self.bucket.blob(filename)
                blob.upload_from_string(csv_content, content_type='text/csv')
                
                logger.info(f"Successfully uploaded {filename} to GCS bucket {self.bucket_name}")
                logger.info(f"File contains {len(batch)} records")
                
                return
                
            except GoogleCloudError as e:
                logger.error(f"Google Cloud error: {e}")
                if "quota" in str(e).lower() or "rate" in str(e).lower():
                    # Rate limiting or quota exceeded
                    raise SinkBackpressureError(
                        retry_after=60.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                # For other GCS errors, retry with backoff
                attempts_remaining -= 1
                if attempts_remaining:
                    wait_time = (4 - attempts_remaining) * 5  # Exponential backoff: 5s, 10s, 15s
                    logger.warning(f"GCS error, retrying in {wait_time}s. Attempts remaining: {attempts_remaining}")
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"Unexpected error writing to GCS: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    wait_time = (4 - attempts_remaining) * 3  # 3s, 6s, 9s
                    logger.warning(f"Retrying in {wait_time}s. Attempts remaining: {attempts_remaining}")
                    time.sleep(wait_time)
        
        raise Exception(f"Failed to write batch to GCS after all retry attempts")


def main():
    """Set up our Application to sink solar sensor data to GCP Storage as CSV."""
    
    # Setup necessary objects
    app = Application(
        consumer_group="solar_data_csv_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize the GCP Storage CSV sink
    gcp_csv_sink = GCPStorageCSVSink()
    
    # Get input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Debug: Print raw messages to understand structure
    def debug_message(row):
        logger.info(f"Raw message structure: {row}")
        logger.info(f"Message keys: {list(row.keys()) if isinstance(row, dict) else 'Not a dict'}")
        return row

    # Process the data stream
    sdf = sdf.apply(debug_message)
    
    # Sink the processed data to GCP Storage
    sdf.sink(gcp_csv_sink)

    # Run the application
    logger.info("Starting solar data CSV sink application...")
    
    # Check if we're in test mode
    test_mode = os.environ.get('QUIX_TEST_MODE', '').lower() == 'true'
    if test_mode:
        logger.info("Running in test mode - processing only 10 messages with 20s timeout")
        app.run(count=10, timeout=20)
    else:
        app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()