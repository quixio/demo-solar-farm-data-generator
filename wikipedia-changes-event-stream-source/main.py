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

    def setup(self):
        """Setup connection to Wikipedia event stream"""
        try:
            from requests_sse import EventSource
            logger.info(f"Connecting to Wikipedia changes stream at {self.url}")
            self._event_source = EventSource(self.url, timeout=30)
            logger.info("Successfully connected to Wikipedia changes stream")
        except Exception as e:
            logger.error(f"Failed to connect to Wikipedia stream: {e}")
            raise

    def run(self):
        """Main loop to read events from Wikipedia stream"""
        try:
            if self._event_source is None:
                self.setup()

            logger.info("Starting to read Wikipedia changes events")

            for event in self._event_source:
                if not self.running:
                    break

                if event.type != "message":
                    continue

                try:
                    change = json.loads(event.data)

                    # Skip canary and non-edit events
                    if change.get("meta", {}).get("domain") == "canary":
                        continue
                    if change.get("type") != "edit":
                        continue

                    kafka_message = self._transform_to_kafka_format(change)
                    if not kafka_message:
                        continue

                    serialized = self.serialize(
                        key=kafka_message.get("page_id", ""),
                        value=kafka_message
                    )
                    self.produce(key=serialized.key, value=serialized.value)

                    self.messages_processed += 1
                    if self.messages_processed % 10 == 0:
                        logger.info(f"Processed {self.messages_processed} messages")

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

    def _transform_to_kafka_format(self, change: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform Wikipedia change event to Kafka message format"""
        try:
            kafka_message = {
                "timestamp": change.get("meta", {}).get("dt", datetime.utcnow().isoformat()),
                "wiki": change.get("wiki", ""),
                "page_title": change.get("title", ""),
                "user": change.get("user", ""),
                "edit_type": change.get("type", ""),
                "namespace": change.get("namespace", 0),
                "comment": (change.get("comment", "") or "")[:500],
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
        app = Application(
            consumer_group="wikipedia-changes-consumer",
            auto_offset_reset="latest",
        )

        output_topic_name = os.environ.get("output", "wikipedia-data")
        logger.info(f"Output topic: {output_topic_name}")

        topic = app.topic(output_topic_name)

        source = WikipediaChangesSource(name="wikipedia-changes-source", max_messages=100)

        sdf = app.dataframe(topic=topic, source=source)
        sdf.print(metadata=True)

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