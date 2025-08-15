from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import json
import csv
import io
import logging
import time
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class GoogleCloudStorageCSVSink(BatchingSink):
    """
    A sink that writes solar sensor data to CSV files in Google Cloud Storage.
    """
    
    def __init__(self):
        super().__init__()
        
        # Get environment variables
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME")
        self.gcp_credentials_json = os.environ.get("GCP_CREDENTIALS_JSON")
        self.csv_file_prefix = os.environ.get("CSV_FILE_PREFIX", "solar_data")
        
        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable is required")
        if not self.gcp_credentials_json:
            raise ValueError("GCP_CREDENTIALS_JSON environment variable is required")
            
        self.storage_client = None
        self.bucket = None
        
        logger.info(f"Initialized GCS CSV Sink for bucket: {self.bucket_name}")
    
    def setup(self):
        """Setup GCS client and validate bucket access"""
        try:
            # Parse credentials JSON from environment variable
            credentials_info = json.loads(self.gcp_credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            
            # Initialize GCS client
            self.storage_client = storage.Client(credentials=credentials)
            self.bucket = self.storage_client.bucket(self.bucket_name)
            
            # Test bucket access
            self.bucket.reload()
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to setup GCS client: {e}")
            raise
    
    def _parse_solar_data(self, item):
        """Parse solar sensor data from Kafka message"""
        try:
            # Debug: Print raw message structure
            logger.debug(f"Raw message: {item}")
            
            # Get the message value
            message_data = item.value
            
            # The schema shows that the actual data is in the 'value' field as a JSON string
            if isinstance(message_data, dict) and 'value' in message_data:
                solar_data_json = message_data['value']
                
                # Parse the JSON string to get the actual solar data
                if isinstance(solar_data_json, str):
                    solar_data = json.loads(solar_data_json)
                else:
                    solar_data = solar_data_json
                
                # Convert Unix timestamp to readable datetime
                timestamp = solar_data.get('timestamp')
                if timestamp:
                    # Convert from nanoseconds to seconds and create datetime
                    dt = datetime.fromtimestamp(timestamp / 1_000_000_000)
                    solar_data['datetime'] = dt.isoformat()
                
                # Add Kafka metadata
                solar_data['kafka_topic'] = message_data.get('topicName', 'unknown')
                solar_data['kafka_partition'] = item.partition
                solar_data['kafka_offset'] = item.offset
                solar_data['kafka_timestamp'] = datetime.fromtimestamp(item.timestamp / 1000).isoformat()
                
                return solar_data
            else:
                logger.error(f"Unexpected message structure: {message_data}")
                return None
                
        except Exception as e:
            logger.error(f"Error parsing solar data: {e}")
            return None
    
    def _create_csv_content(self, solar_data_list):
        """Create CSV content from solar data"""
        if not solar_data_list:
            return None
            
        # Define CSV headers based on the solar data schema
        headers = [
            'datetime', 'panel_id', 'location_id', 'location_name', 
            'latitude', 'longitude', 'timezone', 'power_output', 'unit_power',
            'temperature', 'unit_temp', 'irradiance', 'unit_irradiance',
            'voltage', 'unit_voltage', 'current', 'unit_current', 
            'inverter_status', 'timestamp', 'kafka_topic', 'kafka_partition', 
            'kafka_offset', 'kafka_timestamp'
        ]
        
        # Create CSV content
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        
        for data in solar_data_list:
            writer.writerow(data)
        
        return output.getvalue()
    
    def write(self, batch: SinkBatch):
        """Write batch of solar data to GCS as CSV"""
        attempts_remaining = 3
        
        while attempts_remaining:
            try:
                # Parse all solar data from the batch
                solar_data_list = []
                for item in batch:
                    solar_data = self._parse_solar_data(item)
                    if solar_data:
                        solar_data_list.append(solar_data)
                
                if not solar_data_list:
                    logger.warning("No valid solar data found in batch")
                    return
                
                logger.info(f"Processing {len(solar_data_list)} solar sensor records")
                
                # Create CSV content
                csv_content = self._create_csv_content(solar_data_list)
                if not csv_content:
                    logger.warning("No CSV content generated")
                    return
                
                # Generate unique filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
                filename = f"{self.csv_file_prefix}_{timestamp}_{batch.partition}_{batch.offset}.csv"
                
                # Upload to GCS
                blob = self.bucket.blob(filename)
                blob.upload_from_string(csv_content, content_type='text/csv')
                
                logger.info(f"Successfully uploaded {filename} to GCS bucket {self.bucket_name}")
                return
                
            except Exception as e:
                attempts_remaining -= 1
                logger.error(f"Error writing to GCS (attempts remaining: {attempts_remaining}): {e}")
                
                if attempts_remaining:
                    time.sleep(3)
                else:
                    if "timeout" in str(e).lower() or "503" in str(e):
                        # Server is busy, use backpressure
                        raise SinkBackpressureError(
                            retry_after=30.0,
                            topic=batch.topic,
                            partition=batch.partition,
                        )
                    else:
                        # Other error, re-raise
                        raise


def main():
    """Setup and run the solar data to GCS CSV sink application"""
    
    logger.info("Starting Solar Data to GCS CSV Sink Application")
    
    # Setup necessary objects
    app = Application(
        consumer_group="solar_gcs_csv_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Initialize the GCS CSV sink
    gcs_csv_sink = GoogleCloudStorageCSVSink()
    gcs_csv_sink.setup()
    
    # Setup input topic
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Debug: Print raw messages to understand structure
    sdf = sdf.apply(lambda row: print(f"Raw message: {row}") or row)
    
    # Sink data to GCS CSV
    sdf.sink(gcs_csv_sink)

    # Run the application for testing (process 10 messages max)
    logger.info("Starting to process messages...")
    app.run(count=10, timeout=20)
    logger.info("Application completed")


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()