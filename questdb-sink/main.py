import os
import json
from datetime import datetime
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

# Test version - just read messages without database connection
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-test-reader"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "10")),
    commit_interval=float(os.environ.get("BUFFER_TIMEOUT", "1.0")),
)

input_topic = app.topic(os.environ.get("INPUT_TOPIC"))
sdf = app.dataframe(input_topic)

# Debug: Print raw message structure
print("=== Raw Message Structure ===")
sdf.print(metadata=True)

# Process and display message content
def process_message(message):
    print(f"\n=== Processing Message ===")
    print(f"Message type: {type(message)}")
    print(f"Message keys: {list(message.keys()) if hasattr(message, 'keys') else 'No keys'}")
    
    # Try to extract the actual data
    try:
        # If message has a 'value' field containing JSON string
        if 'value' in message and isinstance(message['value'], str):
            data = json.loads(message['value'])
            print(f"Parsed JSON from value field:")
        elif isinstance(message, str):
            data = json.loads(message)
            print(f"Parsed JSON from string message:")
        else:
            data = message
            print(f"Direct message data:")
            
        print(f"Data keys: {list(data.keys()) if hasattr(data, 'keys') else 'No data keys'}")
        
        # Show some sample fields
        if hasattr(data, 'get'):
            print(f"panel_id: {data.get('panel_id')}")
            print(f"location_name: {data.get('location_name')}")
            print(f"power_output: {data.get('power_output')}")
            print(f"timestamp: {data.get('timestamp')}")
            
            # Test timestamp conversion
            timestamp_ns = data.get('timestamp', 0)
            if timestamp_ns:
                timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
                print(f"Converted timestamp: {timestamp_dt}")
        
        print(f"Full data: {data}")
        
    except Exception as e:
        print(f"Error processing message: {e}")
        print(f"Raw message: {message}")
    
    return message

sdf = sdf.apply(process_message)

print("=== Starting Message Reader Test ===")
print(f"Input topic: {os.environ.get('INPUT_TOPIC')}")
print(f"Consumer group: {os.environ.get('CONSUMER_GROUP_NAME', 'questdb-test-reader')}")

if __name__ == "__main__":
    print("Running for 20 messages or 30 seconds...")
    app.run(count=20, timeout=30)