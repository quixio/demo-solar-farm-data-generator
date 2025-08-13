import os
import json
import logging
import sys
from datetime import datetime
from typing import Optional, Dict, Any

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
    """Source for reading Wikipedia changes event stream"""

    def __init__(self, name: str = "wikipedia-changes-source", max_messages: int = 100):
        super().__init__(name=name)
        self.url = "https://stream.wikimedia.org/v2/stream/recentchange"
        self.messages_processed = 0
        self.max_messages = max_messages
        self._event_source = None

    def _headers(self) -> Dict[str, str]:
        """Get headers for the SSE connection"""
        user_agent = os.environ.get(
            "USER_AGENT",
            "quix-wikipedia-changes-source/1.0 (+https://quix.io; contact: support@quix.io)"
        )
        return {
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "User-Agent": user_agent,
        }

    def setup(self):
        """Setup phase - prepare for connection"""
        logger.info(f"Preparing to connect to Wikipedia changes stream at {self.url}")

    def run(self):
        """Main loop to read events from Wikipedia stream"""
        from requests_sse import EventSource

        try:
            headers = self._headers()
            logger.info(f"Opening SSE connection to {self.url}")

            # Open the SSE connection using the context manager
            with EventSource(self.url, timeout=30, headers=headers) as es:
                self._event_source = es
                logger.info("Successfully connected to Wikipedia changes stream")
                logger.info("Starting to read Wikipedia changes events")

                for event in es:
                    if not self.running:
                        break

                    # Skip non-message events
                    if getattr(event, "type", None) != "message":
                        continue
                    if not getattr(event, "data", None):
                        continue

                    try:
                        change = json.loads(event.data)

                        # Skip canary events
                        if change.get("meta", {}).get("domain") == "canary":
                            continue
                        
                        # Filter for edit events only (page edits)
                        if change.get("type") != "edit":
                            continue

                        # Transform to Kafka message format with basic metadata
                        kafka_message = self._transform_to_kafka_format(change)
                        if not kafka_message:
                            continue

                        # Serialize and produce the message
                        serialized = self.serialize(
                            key=kafka_message.get("page_id", ""),
                            value=kafka_message
                        )
                        self.produce(key=serialized.key, value=serialized.value)

                        self.messages_processed += 1
                        if self.messages_processed % 10 == 0:
                            logger.info(f"Processed {self.messages_processed} messages")

                        # Stop condition for testing
                        if self.max_messages and self.messages_processed >= self.max_messages:
                            logger.info(f"Reached maximum message limit of {self.max_messages}")
                            break

                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
                        continue

        except Exception as e:
            logger.error(f"Error in Wikipedia source run loop: {e}")
            raise
        finally:
            logger.info(f"Wikipedia source stopped. Total messages processed: {self.messages_processed}")
            self._event_source = None

    def _transform_to_kafka_format(self, change: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform Wikipedia change event to Kafka message format with basic metadata"""
        try:
            # Extract basic metadata about page edits
            kafka_message = {
                "timestamp": change.get("meta", {}).get("dt", datetime.utcnow().isoformat()),
                "wiki": change.get("wiki", ""),
                "page_title": change.get("title", ""),
                "user": change.get("user", ""),
                "edit_type": change.get("type", ""),
                "namespace": change.get("namespace", 0),
                "comment": (change.get("comment", "") or "")[:500],  # Truncate long comments
                "old_revision_id": change.get("revision", {}).get("old", 0),
                "new_revision_id": change.get("revision", {}).get("new", 0),
                "bot_edit": change.get("bot", False),
                "minor_edit": change.get("minor", False),
                "page_id": str(change.get("id", 0)),
                "server_name": change.get("server_name", ""),
            }
            return kafka_message
        except Exception as e:
            logger.error(f"Error transforming message: {e}")
            return None

    def stop(self):
        """Clean up resources when stopping"""
        if self._event_source:
            try:
                self._event_source.close()
            except Exception as e:
                logger.warning(f"Error closing event source: {e}")
        super().stop()


def main():
    """Main application entry point"""
    try:
        # Initialize Quix Streams application
        app = Application(
            consumer_group="wikipedia-changes-consumer",
            auto_offset_reset="latest",
        )

        # Get output topic from environment variable
        output_topic_name = os.environ.get("output", "wikipedia-data")
        logger.info(f"Output topic: {output_topic_name}")

        # Create topic
        topic = app.topic(output_topic_name)

        # Create Wikipedia changes source
        source = WikipediaChangesSource(name="wikipedia-changes-source", max_messages=100)

        # Create streaming dataframe with source
        sdf = app.dataframe(topic=topic, source=source)
        
        # Print messages to help users see data being produced
        sdf.print(metadata=True)

        # Start the application
        logger.info("Starting Wikipedia changes source application")
        app.run()

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)
    finally:
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()