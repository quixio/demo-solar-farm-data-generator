```python
from quixstreams import Application
import json
import os

def process_message(message):
    try:
        # Parse the nested JSON in the 'value' field
        value_json = json.loads(message['value'])
        # Extract and process necessary data
        panel_id = value_json['panel_id']
        power_output = value_json['power_output']
        temperature = value_json['temperature']
        # ... (Other fields can be processed if required)
        
        # Example processing logic (can be customized)
        print(f"Panel ID: {panel_id}, Power Output: {power_output}W, Temperature: {temperature}°C")

    except Exception as e:
        print("Error processing message:", e)

def main():
    app = Application(
        consumer_group="solar_data_processor",
        auto_create_topics=False,
        auto_offset_reset="earliest"
    )

    input_topic = app.topic(name=os.environ.get("INPUT_TOPIC"))
    sdf = app.dataframe(topic=input_topic)

    # Configure the processing for each message
    sdf.process(lambda message: process_message(message))

    # Run the application, limiting to 10 messages
    app.run(count=10)


if __name__ == "__main__":
    main()
```