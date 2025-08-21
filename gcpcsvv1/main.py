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
from google.cloud import storage
from google.oauth2 import service_account

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GCSolarDataSink(BatchingSink):
    """
    Custom sink for writing solar panel sensor data to Google Cloud Storage.
    
    This sink processes JSON messages containing solar panel telemetry data
    and writes them to a GCS bucket as JSON files.
    """
    
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.bucket_name = os.environ.get('GCS_BUCKET_NAME')
        self.project_id = os.environ.get('GCS_PROJECT_ID')
        self.credentials_json = os.environ.get('GCS_CREDENTIALS_JSON')
        self.file_prefix = os.environ.get('GCS_FILE_PREFIX', 'solar-data')
        
        if not all([self.bucket_name, self.project_id, self.credentials_json]):
            raise ValueError("Missing required GCS environment variables: GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_JSON")
            
        self.client = None
        self.bucket = None
        self._initialize_gcs_client()
    
    def _initialize_gcs_client(self):
        """Initialize the Google Cloud Storage client"""
        try:
            # Parse the credentials JSON
            credentials_info = json.loads(self.credentials_json)
            credentials = service_account.Credentials.from_service_account_info(
                credentials_info,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            
            # Create the GCS client
            self.client = storage.Client(credentials=credentials, project=self.project_id)
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test bucket access
            if not self.bucket.exists():
                logger.error(f"Bucket {self.bucket_name} does not exist or is not accessible")
                raise Exception(f"Bucket {self.bucket_name} not found")
            
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise
    
    def _parse_solar_data(self, item):
        """
        Parse the solar data from the message.
        Based on schema analysis, the data structure is:
        - The message has a 'value' field that contains JSON string
        - We need to parse this JSON to get the actual solar panel data
        """
        try:
            # Print raw message for debugging
            print(f"Raw message: {item}")
            
            # Extract the value - it should already be parsed as a dict by quixstreams
            if hasattr(item, 'value') and item.value:
                data = item.value
                
                # If the value is still a JSON string, parse it
                if isinstance(data, str):
                    data = json.loads(data)
                
                # Validate that we have solar panel data
                required_fields = ['panel_id', 'location_id', 'power_output', 'timestamp']
                for field in required_fields:
                    if field not in data:
                        logger.warning(f"Missing required field: {field} in message: {data}")
                        return None
                
                # Add processing timestamp
                data['processed_at'] = datetime.utcnow().isoformat()
                
                # Add kafka metadata
                data['kafka_topic'] = item.topic if hasattr(item, 'topic') else None
                data['kafka_partition'] = item.partition if hasattr(item, 'partition') else None
                data['kafka_offset'] = item.offset if hasattr(item, 'offset') else None
                data['kafka_key'] = str(item.key) if hasattr(item, 'key') and item.key else None
                
                return data
            else:
                logger.warning(f"No value field found in message: {item}")
                return None
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from message: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing solar data: {e}")
            return None
    
    def _write_to_gcs(self, data_batch):
        """Write a batch of solar data to GCS"""
        try:
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
            filename = f"{self.file_prefix}/batch_{timestamp}.json"
            
            # Create blob and upload data
            blob = self.bucket.blob(filename)
            
            # Convert batch to JSON format
            json_data = {
                'batch_timestamp': datetime.utcnow().isoformat(),
                'batch_size': len(data_batch),
                'data': data_batch
            }
            
            # Upload to GCS
            blob.upload_from_string(
                json.dumps(json_data, indent=2),
                content_type='application/json'
            )
            
            logger.info(f"Successfully wrote batch of {len(data_batch)} records to {filename}")
            print(f"✅ Successfully wrote {len(data_batch)} solar panel records to GCS: gs://{self.bucket_name}/{filename}")
            
        except Exception as e:
            logger.error(f"Failed to write batch to GCS: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar panel data to Google Cloud Storage.
        
        This method processes each message in the batch, parses the solar data,
        and writes the batch to GCS as a JSON file.
        """
        attempts_remaining = 3
        
        # Parse all items in the batch
        parsed_data = []
        for item in batch:
            solar_data = self._parse_solar_data(item)
            if solar_data:
                parsed_data.append(solar_data)
            
        if not parsed_data:
            logger.warning("No valid solar data found in batch, skipping")
            return
        
        logger.info(f"Processing batch with {len(parsed_data)} valid solar panel records")
        
        while attempts_remaining:
            try:
                return self._write_to_gcs(parsed_data)
            except ConnectionError as e:
                # Network connectivity issues
                logger.warning(f"Connection error writing to GCS: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except Exception as e:
                # For quota exceeded or temporary GCS issues
                if "quota" in str(e).lower() or "rate" in str(e).lower():
                    logger.warning(f"GCS rate limit/quota exceeded: {e}")
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                else:
                    # Re-raise other exceptions
                    raise
                    
        raise Exception("Failed to write solar data to GCS after multiple attempts")


def main():
    """ Here we will set up our Application for processing solar panel data. """
    
    logger.info("Starting Solar Panel Data GCS Sink Application")
    
    try:
        # Setup necessary objects
        app = Application(
            consumer_group="solar_gcs_sink",
            auto_create_topics=True,
            auto_offset_reset="earliest",
            commit_interval=5.0  # Commit every 5 seconds for better reliability
        )
        
        # Initialize the GCS sink
        gcs_sink = GCSolarDataSink()
        
        # Set up input topic - expecting JSON serialized data
        input_topic = app.topic(
            name=os.environ["input"],
            value_deserializer="json"  # Automatically parse JSON
        )
        
        sdf = app.dataframe(topic=input_topic)
        
        # Add some debug logging to see what data we're receiving
        sdf = sdf.apply(lambda row: {
            **row,
            "debug_info": f"Received solar data for panel: {row.get('panel_id', 'unknown')}"
        }).print(metadata=True)
        
        # Sink the data to GCS
        sdf.sink(gcs_sink)
        
        logger.info("Application configured successfully, starting message processing...")
        
        # Run the application with limits for testing
        app.run(count=10, timeout=20)
        
        logger.info("Application completed successfully")
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()