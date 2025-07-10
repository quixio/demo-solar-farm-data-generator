import os
import json
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink

# Initialize Quix Application
app = Application(
    broker_address=os.environ.get("Quix__Broker__Address"),
    consumer_group="timescaledb-sink-consumer-group",
    auto_offset_reset="latest"
)

# Debug: Print environment variables
print(f"DEBUG - TIMESCALE_PASSWORD value: '{os.environ.get('TIMESCALE_PASSWORD', 'NOT_FOUND')}'")
print(f"DEBUG - Available env vars containing TIMESCALE: {[k for k in os.environ.keys() if 'TIMESCALE' in k]}")

# Configure TimescaleDB/PostgreSQL connection
postgresql_sink = PostgreSQLSink(
    host=os.environ.get("TIMESCALE_HOST", "timescaledb"),
    port=int(os.environ.get("TIMESCALE_PORT", "5432")),
    dbname=os.environ.get("TIMESCALE_DATABASE", "metrics"),
    user=os.environ.get("TIMESCALE_USERNAME", "tsadmin"),
    password=os.environ.get("TIMESCALE_PASSWORD", "password"),  # Quix Secret
    table_name=os.environ.get("TIMESCALE_TABLE", "solar_panel_data")
)

# Define the input topic
input_topic = app.topic(os.environ.get("INPUT_TOPIC", "solar-data"))

# Create streaming dataframe
sdf = app.dataframe(input_topic)

# Parse the JSON value from the Kafka message
def parse_solar_data(message):
    """Parse the solar panel data from the Kafka message value"""
    try:
        # The actual data is in the 'value' field as a JSON string
        data = json.loads(message["value"])
        
        # Add processing timestamp for TimescaleDB
        data["processed_at"] = message.get("dateTime")
        
        # Convert timestamp from nanoseconds to seconds for better TimescaleDB compatibility
        if "timestamp" in data:
            data["timestamp_seconds"] = data["timestamp"] / 1e9
            
        return data
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing message: {e}")
        return None

# Apply data transformation
sdf = sdf.apply(parse_solar_data)

# Filter out any None values from parsing errors
sdf = sdf.filter(lambda x: x is not None)

# Sink to TimescaleDB
sdf.sink(postgresql_sink)

if __name__ == "__main__":
    print("Starting TimescaleDB sink for solar panel data...")
    print(f"Reading from topic: {os.environ.get('INPUT_TOPIC', 'solar-data')}")
    print(f"Writing to TimescaleDB table: {os.environ.get('TIMESCALE_TABLE', 'solar_panel_data')}")
    print(f"Database: {os.environ.get('TIMESCALE_DATABASE', 'metrics')} on {os.environ.get('TIMESCALE_HOST', 'timescaledb')}:{os.environ.get('TIMESCALE_PORT', '5432')}")
    
    # Run the application
    app.run()