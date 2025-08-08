import os
import json
import time
import logging
import requests
from sseclient import SSEClient
from quixstreams import Application
from quixstreams.sources import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GitHubSSESource(Source):
    """
    A Quix Streams Source that reads from GitHub public timeline SSE event
    stream and produces messages to a Kafka topic.
    """

    def __init__(self, name: str = "github-sse-source"):
        super().__init__(name=name)
        self.api_url = os.environ.get(
            "GITHUB_API_URL", "http://github-firehose.libraries.io"
        )
        self.sse_path = os.environ.get("GITHUB_SSE_PATH", "/events")
        self.timeout = int(os.environ.get("EVENT_STREAM_TIMEOUT", "30"))
        self.repo_filter = os.environ.get("GITHUB_REPO_FILTER", "all-events")
        self.auth_token = os.environ.get("GITHUB_AUTH_TOKEN", "")
        self.full_url = self.api_url.rstrip("/") + self.sse_path
        self.message_count = 0
        self.max_messages = 100  # Stop after 100 messages for testing

    def setup(self):
        """
        Lightweight connectivity test to the SSE endpoint.
        We only check the status code – we do NOT try to read the body.
        """
        headers = self._get_headers()
        try:
            with requests.get(
                self.full_url,
                headers=headers,
                stream=True,
                timeout=5,
            ) as response:
                response.raise_for_status()

            logger.info(
                f"Successfully connected to GitHub SSE stream: {self.full_url}"
            )

        except Exception as e:
            logger.error(f"Failed to connect to GitHub SSE stream: {e}")
            raise

    def _get_headers(self):
        headers = {
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "User-Agent": "Quix-Streams-GitHub-SSE-Source/1.0",
        }
        if self.auth_token:
            headers["Authorization"] = f"token {self.auth_token}"
        return headers

    def run(self):
        logger.info(f"Starting GitHub SSE source - connecting to: {self.full_url}")
        logger.info(f"Repository filter: {self.repo_filter}")
        logger.info(f"Timeout: {self.timeout} seconds")

        while self.running and self.message_count < self.max_messages:
            try:
                self._connect_and_process()
            except requests.exceptions.Timeout:
                logger.warning(
                    f"Connection timed out after {self.timeout} seconds, retrying..."
                )
                time.sleep(5)
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {e}, retrying in 10 seconds...")
                time.sleep(10)
            except Exception as e:
                logger.error(f"Unexpected error in GitHub SSE source: {e}")
                time.sleep(5)

        logger.info(
            f"GitHub SSE source stopped after processing {self.message_count} messages"
        )

    def _connect_and_process(self):
        headers = self._get_headers()
        with requests.get(
            self.full_url, headers=headers, stream=True, timeout=self.timeout
        ) as response:
            response.raise_for_status()
            logger.info(f"Connected to GitHub SSE stream (Status: {response.status_code})")
            client = SSEClient(response)

            for event in client.events():
                if not self.running or self.message_count >= self.max_messages:
                    break
                self._process_event(event)

    def _process_event(self, event):
        if not event.data or event.data.strip() == "":
            return
        try:
            event_data = json.loads(event.data)
            message_key = self._extract_message_key(event_data)
            enhanced_event = {
                "id": event_data.get("id"),
                "type": event_data.get("type"),
                "actor": event_data.get("actor", {}),
                "repo": event_data.get("repo", {}),
                "payload": event_data.get("payload", {}),
                "public": event_data.get("public", True),
                "created_at": event_data.get("created_at"),
                "org": event_data.get("org"),
                "source_metadata": {
                    "source": "github-public-timeline-sse",
                    "processed_at": time.time(),
                    "event_id": event.id,
                    "event_type": event.event or "message",
                },
            }
            serialized_msg = self.serialize(key=message_key, value=enhanced_event)
            self.produce(key=serialized_msg.key, value=serialized_msg.value)
            self.message_count += 1
            if self.message_count % 10 == 0:
                logger.info(f"Processed {self.message_count} GitHub events")
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse event data as JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing GitHub event: {e}")
            raise

    def _extract_message_key(self, event_data):
        repo = event_data.get("repo", {})
        if repo and repo.get("name"):
            return repo["name"]
        actor = event_data.get("actor", {})
        if actor and actor.get("login"):
            return f"actor:{actor['login']}"
        event_id = event_data.get("id")
        if event_id:
            return f"event:{event_id}"
        return "unknown"


def main():
    logger.info("Starting GitHub Public Timeline SSE Event Stream Source")
    app = Application(consumer_group="github-sse-source", auto_create_topics=True)

    github_source = GitHubSSESource(name="github-public-timeline-sse")

    output_topic_name = os.environ.get("output", "output")
    output_topic = app.topic(name=output_topic_name)

    sdf = app.dataframe(topic=output_topic, source=github_source)
    sdf = sdf.update(
        lambda value: logger.info(
            f"Processing GitHub event: {value.get('type', 'unknown')} "
            f"from repo: {value.get('repo', {}).get('name', 'unknown')}"
        )
    )
    sdf.print(metadata=True)
    sdf.to_topic(output_topic)

    logger.info(f"GitHub SSE source configured to write to topic: {output_topic_name}")
    logger.info("Starting application...")
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    finally:
        logger.info("GitHub SSE source application stopped")


if __name__ == "__main__":
    main()