import requests
import json

# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sinks, see https://quix.io/docs/quix-streams/connectors/sinks/index.html
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBatch, SinkBackpressureError

import os
import time
from typing import List, Dict, Any

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# API endpoint configuration
API_BASE_URL = "https://gateway-demo-joinsdemo-prod.demo.quix.io"


class MyApiSink(BatchingSink):
    """
    A custom sink that sends data to an HTTP API endpoint.
    
    This sink takes messages from a Kafka topic and sends them to the specified API endpoint.
    If a 'location_id' field is present in the data, it will be used as the key in the API URL.
    """
    def _send_to_api(self, data: List[Dict[str, Any]]) -> None:
        """
        Send data to the API endpoint.
        
        Args:
            data: List of records to send to the API
        """
        headers = {"Content-Type": "application/json"}
        
        for record in data:
            try:
                # Use location_id as the key if available, otherwise send without a key
                location_id = record.get("location_id")
                url = f"{API_BASE_URL}/data/{location_id}" if location_id else f"{API_BASE_URL}/data/"
                
                response = requests.post(
                    url=url,
                    headers=headers,
                    data=json.dumps(record),
                    timeout=10  # 10 second timeout
                )
                response.raise_for_status()  # Raise an exception for HTTP errors
                
            except requests.exceptions.RequestException as e:
                print(f"Error sending data to API: {e}")
                raise ConnectionError(f"Failed to send data to API: {e}")

    def write(self, batch: SinkBatch):
        """
        Every Sink requires a .write method.

        Here is where we attempt to send batches of data (multiple consumed messages,
        for the sake of efficiency/speed) to our API endpoint.

        Sinks have sanctioned patterns around retrying and handling connections.

        See https://quix.io/docs/quix-streams/connectors/sinks/custom-sinks.html for
        more details.
        """
        attempts_remaining = 3
        data = [item.value for item in batch]
        while attempts_remaining:
            try:
                return self._send_to_api(data)
            except ConnectionError:
                # Maybe we just failed to connect, do a short wait and try again
                # We can't repeat forever; the consumer will eventually time out
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except TimeoutError:
                # Maybe the server is busy, do a sanctioned extended pause
                # Always safe to do, but will require re-consuming the data.
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                )
        raise Exception("Error while sending data to API")


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(
        consumer_group="my_api_destination",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    my_api_sink = MyApiSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    # Do SDF operations/transformations
    sdf = sdf.apply(lambda row: row).print(metadata=True)

    # Finish by calling StreamingDataFrame.sink()
    sdf.sink(my_api_sink)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()