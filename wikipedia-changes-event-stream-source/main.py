import os
import json
import logging
import signal
import sys
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from quixstreams import Application
from quixstreams.sources.base import Source

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WikipediaChangesSource(Source):
    def __init__(self, name: str = "wikipedia-changes-source", shutdown_event: Optional[object] = None):
        super().__init__(name=name)
        self.url = 'https://stream.wikimedia.org/v2/stream/recentchange'
        self.shutdown_event = shutdown_event
        self.messages_processed = 0
        self.max_messages = 100
        
    def setup(self):
        try:
            import requests
            response = requests.get(self.url, stream=True, timeout=5)
            if response.status_code == 200:
                logger.info(f"Successfully connected to Wikipedia changes stream at {self.url}")
                if hasattr(self, 'on_client_connect_success'):
                    self.on_client_connect_success()
            else:
                logger.error(f"Failed to connect to Wikipedia changes stream. Status code: {response.status_code}")
                if hasattr(self, 'on_client_connect_failure'):
                    self.on_client_connect_failure()
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            if hasattr(self, 'on_client_connect_failure'):
                self.on_client_connect_failure()
    
    def run(self):
        try:
            from requests_sse import EventSource
        except ImportError:
            logger.error("requests-sse library is required. Install it with: pip install requests-sse")
            return
            
        logger.info(f"Starting Wikipedia changes source from {self.url}")
        
        while self.running and self.messages_processed < self.max_messages:
            try:
                with EventSource(self.url, timeout=30) as stream:
                    logger.info("Connected to Wikipedia EventStream")
                    
                    for event in stream:
                        if not self.running or self.messages_processed >= self.max_messages:
                            break
                            
                        if event.type == 'message':
                            try:
                                change = json.loads(event.data)
                                
                                if change.get('meta', {}).get('domain') == 'canary':
                                    continue
                                
                                if change.get('type') != 'edit':
                                    continue
                                
                                kafka_message = {
                                    'timestamp': change.get('meta', {}).get('dt', ''),
                                    'wiki': change.get('wiki', ''),
                                    'pageTitle': change.get('title', ''),
                                    'user': change.get('user', ''),
                                    'editType': change.get('type', ''),
                                    'namespace': change.get('namespace', 0),
                                    'comment': change.get('comment', ''),
                                    'oldRevisionId': change.get('revision', {}).get('old', 0),
                                    'newRevisionId': change.get('revision', {}).get('new', 0),
                                    'botEdit': change.get('bot', False),
                                    'minorEdit': change.get('minor', False),
                                    'pageId': change.get('id', 0),
                                    'serverName': change.get('server_name', '')
                                }
                                
                                key = f"{change.get('wiki', 'unknown')}_{change.get('id', 0)}"
                                
                                msg = self.serialize(
                                    key=key,
                                    value=kafka_message
                                )
                                
                                self.produce(
                                    key=msg.key,
                                    value=msg.value
                                )
                                
                                self.messages_processed += 1
                                
                                if self.messages_processed % 10 == 0:
                                    logger.info(f"Processed {self.messages_processed} Wikipedia edit events")
                                
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse JSON: {e}")
                                continue
                            except Exception as e:
                                logger.error(f"Error processing event: {e}")
                                continue
                                
            except ConnectionError as e:
                logger.error(f"Connection error: {e}")
                if self.running and self.messages_processed < self.max_messages:
                    logger.info("Attempting to reconnect in 5 seconds...")
                    import time
                    time.sleep(5)
                    continue
            except Exception as e:
                logger.error(f"Unexpected error in source: {e}")
                if self.running and self.messages_processed < self.max_messages:
                    logger.info("Attempting to restart in 5 seconds...")
                    import time
                    time.sleep(5)
                    continue
                    
        logger.info(f"Wikipedia changes source stopped. Total messages processed: {self.messages_processed}")
        if self.shutdown_event:
            self.shutdown_event.set()


def signal_handler(sig, frame):
    logger.info("Received shutdown signal, stopping application...")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    app = Application(
        consumer_group="wikipedia-changes-consumer",
        auto_offset_reset="latest"
    )
    
    output_topic_name = os.environ.get("output", "wikipedia-data")
    topic = app.topic(output_topic_name)
    
    source = WikipediaChangesSource(
        name="wikipedia-changes-source"
    )
    
    sdf = app.dataframe(topic=topic, source=source)
    
    sdf.print(metadata=True)
    
    logger.info(f"Starting Wikipedia changes source application")
    logger.info(f"Output topic: {output_topic_name}")
    logger.info(f"Processing up to 100 messages for testing")
    
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()