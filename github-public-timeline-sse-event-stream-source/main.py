import os
import json
import time
import logging
import requests
from sseclient import SSEClient
from quixstreams import Application
from quixstreams.sources import Source

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GitHubSSESource(Source):
    """
    A Quix Streams Source that reads from GitHub public timeline SSE event stream
    and produces messages to a Kafka topic.
    """
    
    def __init__(self, name: str = "github-sse-source"):
        super().__init__(name=name)
        self.api_url = os.environ.get('GITHUB_API_URL', 'http://github-firehose.libraries.io')
        self.sse_path = os.environ.get('GITHUB_SSE_PATH', '/events')
        self.timeout = int(os.environ.get('EVENT_STREAM_TIMEOUT', '30'))
        self.repo_filter = os.environ.get('GITHUB_REPO_FILTER', 'all-events')
        self.auth_token = os.environ.get('GITHUB_AUTH_TOKEN', '')
        self.full_url = self.api_url.rstrip('/') + self.sse_path
        self.message_count = 0
        self.max_messages = 100  # Stop after 100 messages for testing
        
    def setup(self):
        """
        Test the connection to the GitHub SSE stream before starting.
        """
        try:
            headers = self._get_headers()
            response = requests.get(self.full_url, headers=headers, stream=False, timeout=5)
            response.raise_for_status()
            logger.info(f"Successfully connected to GitHub SSE stream: {self.full_url}")
            if hasattr(self, 'on_client_connect_success') and self.on_client_connect_success:
                self.on_client_connect_success()
        except Exception as e:
            logger.error(f"Failed to connect to GitHub SSE stream: {e}")
            if hasattr(self, 'on_client_connect_failure') and self.on_client_connect_failure:
                self.on_client_connect_failure(e)
            raise
    
    def _get_headers(self):
        """
        Prepare headers for SSE connection.
        """
        headers = {
            'Accept': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'User-Agent': 'Quix-Streams-GitHub-SSE-Source/1.0'
        }
        
        if self.auth_token:
            headers['Authorization'] = f'token {self.auth_token}'
            
        return headers
    
    def run(self):
        """
        Main loop that reads from GitHub SSE stream and produces messages to Kafka.
        """
        logger.info(f"Starting GitHub SSE source - connecting to: {self.full_url}")
        logger.info(f"Repository filter: {self.repo_filter}")
        logger.info(f"Timeout: {self.timeout} seconds")
        
        while self.running and self.message_count < self.max_messages:
            try:
                self._connect_and_process()
            except requests.exceptions.Timeout:
                logger.warning(f"Connection timed out after {self.timeout} seconds, retrying...")
                time.sleep(5)
                continue
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {e}, retrying in 10 seconds...")
                time.sleep(10)
                continue
            except Exception as e:
                logger.error(f"Unexpected error in GitHub SSE source: {e}")
                time.sleep(5)
                continue
        
        logger.info(f"GitHub SSE source stopped after processing {self.message_count} messages")
    
    def _connect_and_process(self):
        """
        Connect to SSE stream and process events.
        """
        headers = self._get_headers()
        
        try:
            response = requests.get(
                self.full_url, 
                headers=headers, 
                stream=True, 
                timeout=self.timeout
            )
            response.raise_for_status()
            
            logger.info(f"Connected to GitHub SSE stream (Status: {response.status_code})")
            
            client = SSEClient(response)
            
            for event in client.events():
                if not self.running or self.message_count >= self.max_messages:
                    break
                
                try:
                    self._process_event(event)
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    continue
                    
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in SSE connection: {e}")
            raise
    
    def _process_event(self, event):
        """
        Process a single SSE event and produce it to Kafka.
        """
        if not event.data or event.data.strip() == '':
            return
        
        try:
            # Parse the event data as JSON
            event_data = json.loads(event.data)
            
            # Extract key information for Kafka message key
            message_key = self._extract_message_key(event_data)
            
            # Enhance the event data with metadata
            enhanced_event = {
                'id': event_data.get('id'),
                'type': event_data.get('type'),
                'actor': event_data.get('actor', {}),
                'repo': event_data.get('repo', {}),
                'payload': event_data.get('payload', {}),
                'public': event_data.get('public', True),
                'created_at': event_data.get('created_at'),
                'org': event_data.get('org'),
                'source_metadata': {
                    'source': 'github-public-timeline-sse',
                    'processed_at': time.time(),
                    'event_id': event.id,
                    'event_type': event.event or 'message'
                }
            }
            
            # Serialize and produce the message
            serialized_msg = self.serialize(
                key=message_key,
                value=enhanced_event
            )
            
            self.produce(
                key=serialized_msg.key,
                value=serialized_msg.value
            )
            
            self.message_count += 1
            
            if self.message_count % 10 == 0:
                logger.info(f"Processed {self.message_count} GitHub events")
                
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse event data as JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing GitHub event: {e}")
            raise
    
    def _extract_message_key(self, event_data):
        """
        Extract an appropriate message key from the event data.
        Using repo name as key to ensure related events are in the same partition.
        """
        repo = event_data.get('repo', {})
        if repo and repo.get('name'):
            return repo['name']
        
        actor = event_data.get('actor', {})
        if actor and actor.get('login'):
            return f"actor:{actor['login']}"
        
        event_id = event_data.get('id')
        if event_id:
            return f"event:{event_id}"
        
        return "unknown"

def main():
    """
    Main function to set up and run the GitHub SSE source application.
    """
    logger.info("Starting GitHub Public Timeline SSE Event Stream Source")
    
    # Create the Quix Streams application
    app = Application(
        consumer_group="github-sse-source",
        auto_create_topics=True
    )
    
    # Create the GitHub SSE source
    github_source = GitHubSSESource(name="github-public-timeline-sse")
    
    # Get the output topic from environment variable
    output_topic_name = os.environ.get("output", "output")
    output_topic = app.topic(name=output_topic_name)
    
    # Create streaming dataframe from the source
    sdf = app.dataframe(topic=output_topic, source=github_source)
    
    # Add processing to print messages for monitoring
    sdf = sdf.update(lambda value: logger.info(f"Processing GitHub event: {value.get('type', 'unknown')} from repo: {value.get('repo', {}).get('name', 'unknown')}"))
    
    # Print messages with metadata for debugging
    sdf.print(metadata=True)
    
    # Write to the output topic
    sdf.to_topic(output_topic)
    
    logger.info(f"GitHub SSE source configured to write to topic: {output_topic_name}")
    logger.info("Starting application...")
    
    try:
        # Run the application
        app.run()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        logger.info("GitHub SSE source application stopped")

if __name__ == "__main__":
    main()