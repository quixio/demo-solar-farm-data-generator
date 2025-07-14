```python
from quixstreams import Application
import os
import json

def process_message(message):
    """
    Process incoming Kafka message.
    Extracts nested JSON from value and handles data processing.
    """
    payload = json.loads(message.value)
    panel_id = payload.get('panel_id')
    location_id = payload.get('location_id')
    power_output = payload.get('power_output')
    
    # Example processing: print the extracted information
    print(f"Panel ID: {panel_id}, Location ID: {location_id}, Power Output: {power_output}W")
    
def main():
    """
    Main function to process Kafka messages from a given topic.
    Processes 10 messages and stops.
    """
    app = Application(
        consumer_group="solar_data_processor",
        auto_create_topics=False,
        auto_offset_reset="earliest" 
    )
    input_topic = app.topic(name=os.environ["input_topic_name"])
    
    consumer = app.consume(input_topic)
    
    for i in range(10):
        message = consumer.next()
        if message:
            process_message(message)
    
    app.run(count=10)

if __name__ == "__main__":
    main()
```