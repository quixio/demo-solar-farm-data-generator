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
from google.auth.exceptions import GoogleAuthError
from google.oauth2 import service_account

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SolarDataGCSCSVSink(BatchingSink):
    """
    A sink that writes solar sensor data to CSV files in Google Cloud Storage.
    
    This sink processes solar panel data messages, extracts the nested JSON payload,
    and writes the data to CSV files in a GCS bucket with proper error handling
    and retry logic.
    """
    
    def __init__(self):
        super().__init__()
        # Initialize GCS client
        self.bucket_name = os.environ.get("GCS_BUCKET_NAME")
        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable is required")
        
        self.csv_file_path = os.environ.get("GCS_CSV_FILE_PATH", "solar_data/solar_sensor_data.csv")
        self.service_account_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        
        self.client = None
        self.bucket = None
        
        # CSV fieldnames based on the schema analysis
        self.csv_fieldnames = [
            'timestamp_ms',
            'datetime_iso',
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
            'kafka_key',
            'kafka_topic',
            'kafka_partition',
            'kafka_offset'
        ]
        
    def setup(self):
        """Initialize GCS client and validate bucket access."""
        try:
            if self.service_account_json:
                logger.info("Service account credentials found in environment variable")
                # Parse the JSON content and create credentials
                service_account_info = json.loads(self.service_account_json)
                credentials = service_account.Credentials.from_service_account_info(service_account_info)
                self.client = storage.Client(credentials=credentials)
                logger.info("Successfully initialized GCS client with service account credentials")
            else:
                logger.info("No service account credentials provided, attempting to use Application Default Credentials")
                # Use default credentials (ADC)
                self.client = storage.Client()
                logger.info("Successfully initialized GCS client with Application Default Credentials")
            
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test bucket access
            if not self.bucket.exists():
                raise ValueError(f"GCS bucket '{self.bucket_name}' does not exist")
            
            logger.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse service account JSON: {e}")
            raise
        except GoogleAuthError as e:
            logger.error(f"Authentication failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise

    def _parse_solar_message(self, item):
        """
        Parse the solar sensor message structure.
        
        Based on schema analysis, the message has a nested JSON string in the 'value' field.
        """
        try:
            # Debug: Print raw message structure
            logger.debug(f"Raw message item: {item}")
            logger.debug(f"Item value type: {type(item.value)}")
            logger.debug(f"Item value: {item.value}")
            
            message_value = item.value
            
            # Check if we have the expected nested structure
            if isinstance(message_value, dict):
                if 'value' in message_value and isinstance(message_value['value'], str):
                    # Parse the nested JSON string
                    solar_data = json.loads(message_value['value'])
                    logger.debug(f"Parsed nested JSON: {solar_data}")
                elif 'panel_id' in message_value:
                    # Data is already parsed at the top level
                    solar_data = message_value
                else:
                    logger.warning(f"Unexpected message structure: {message_value}")
                    return None
            elif isinstance(message_value, str):
                # Value might be a JSON string itself
                solar_data = json.loads(message_value)
            else:
                logger.warning(f"Unexpected message value type: {type(message_value)}")
                return None
            
            # Convert timestamp to readable format
            timestamp_ms = solar_data.get('timestamp', int(time.time() * 1000))
            datetime_iso = datetime.fromtimestamp(timestamp_ms / 1000).isoformat()
            
            # Create CSV row
            csv_row = {
                'timestamp_ms': timestamp_ms,
                'datetime_iso': datetime_iso,
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
                'kafka_key': str(item.key) if item.key else '',
                'kafka_topic': item.topic if hasattr(item, 'topic') else '',
                'kafka_partition': item.partition if hasattr(item, 'partition') else '',
                'kafka_offset': item.offset if hasattr(item, 'offset') else ''
            }
            
            return csv_row
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON in message: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing solar message: {e}")
            return None

    def _write_csv_to_gcs(self, csv_data):
        """Write CSV data to Google Cloud Storage."""
        try:
            # Create CSV content in memory
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=self.csv_fieldnames)
            
            # Check if file exists to determine if we need headers
            blob = self.bucket.blob(self.csv_file_path)
            file_exists = blob.exists()
            
            if not file_exists:
                writer.writeheader()
                logger.info(f"Creating new CSV file: {self.csv_file_path}")
            
            # Write data rows
            for row in csv_data:
                writer.writerow(row)
            
            csv_content = output.getvalue()
            output.close()
            
            if file_exists:
                # Append to existing file
                existing_content = blob.download_as_text()
                csv_content = existing_content + csv_content
            
            # Upload to GCS
            blob.upload_from_string(csv_content, content_type='text/csv')
            logger.info(f"Successfully wrote {len(csv_data)} records to {self.csv_file_path}")
            
        except Exception as e:
            logger.error(f"Failed to write CSV to GCS: {e}")
            raise

    def write(self, batch: SinkBatch):
        """
        Write a batch of solar sensor data to CSV file in GCS.
        
        Implements retry logic and proper error handling for GCS operations.
        """
        attempts_remaining = 3
        retry_delay = 3
        
        try:
            # Parse all messages in the batch
            csv_data = []
            for item in batch:
                # Debug: Print raw message for first few items
                if len(csv_data) < 3:
                    logger.info(f"Processing message item: {item}")
                
                parsed_row = self._parse_solar_message(item)
                if parsed_row:
                    csv_data.append(parsed_row)
                else:
                    logger.warning("Skipping unparseable message")
            
            if not csv_data:
                logger.warning("No valid data to write in this batch")
                return
            
            logger.info(f"Parsed {len(csv_data)} solar sensor records from batch")
            
            # Retry logic for GCS operations
            while attempts_remaining:
                try:
                    self._write_csv_to_gcs(csv_data)
                    return  # Success
                
                except GoogleAuthError as e:
                    logger.error(f"Authentication error: {e}")
                    raise  # Don't retry auth errors
                
                except Exception as e:
                    logger.error(f"Failed to write to GCS (attempt {4 - attempts_remaining}): {e}")
                    attempts_remaining -= 1
                    
                    if attempts_remaining:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        # Check if this is a temporary issue that should trigger backpressure
                        if "timeout" in str(e).lower() or "temporarily unavailable" in str(e).lower():
                            raise SinkBackpressureError(
                                retry_after=30.0,
                                topic=batch.topic,
                                partition=batch.partition,
                            )
                        else:
                            raise Exception(f"Failed to write to GCS after all retry attempts: {e}")
                            
        except Exception as e:
            logger.error(f"Error in write method: {e}")
            raise


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="solar_gcs_csv_sink",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    # Create and setup the solar data GCS CSV sink
    solar_sink = SolarDataGCSCSVSink()
    solar_sink.setup()
    
    # Get input topic from environment
    input_topic_name = os.environ.get("input")
    if not input_topic_name:
        raise ValueError("input environment variable is required")
    
    input_topic = app.topic(name=input_topic_name)
    sdf = app.dataframe(topic=input_topic)

    # Add debugging to see raw message structure
    def debug_message(row):
        logger.info(f"Received raw message: {row}")
        logger.info(f"Message type: {type(row)}")
        return row

    # Apply debugging transformation and print metadata
    # sdf = sdf.apply(debug_message).print(metadata=True)

    # Sink the data to GCS CSV
    sdf.sink(solar_sink)

    try:
        logger.info("Starting solar data GCS CSV sink application...")
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()