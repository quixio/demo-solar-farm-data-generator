import os
import json
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from dotenv import load_dotenv

load_dotenv()

# Initialize TimescaleDB Sink (using PostgreSQL sink as TimescaleDB is PostgreSQL-compatible)
timescale_sink = PostgreSQLSink(
    host=os.environ.get("TIMESCALEDB_HOST"),
    port=int(os.environ.get("TIMESCALEDB_PORT", "5432")),
    dbname=os.environ.get("TIMESCALEDB_DATABASE"),
    user=os.environ.get("TIMESCALEDB_USER"),
    password=os.environ.get("TIMESCALEDB_PASSWORD"),
    table_name=os.environ.get("TIMESCALEDB_TABLE", "solar_data"),
    schema_name=os.environ.get("TIMESCALEDB_SCHEMA", "public"),
    schema_auto_update=os.environ.get("SCHEMA_AUTO_UPDATE", "true").lower() == "true",
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescale-sink-group"),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

def process_solar_data(message):
    # Debug: Print raw message structure
    print(f"Raw message: {message}")
    
    # Parse the JSON value field to get the actual solar data
    try:
        if hasattr(message, 'value') and message.value:
            # The value field contains JSON-encoded string
            solar_data = json.loads(message.value)
            
            # Create a record with all the solar panel metrics
            processed_data = {
                "topic_id": getattr(message, 'topicId', None),
                "topic_name": getattr(message, 'topicName', None),
                "stream_id": getattr(message, 'streamId', None),
                "message_type": getattr(message, 'type', None),
                "message_datetime": getattr(message, 'dateTime', None),
                "partition": getattr(message, 'partition', None),
                "offset": getattr(message, 'offset', None),
                "panel_id": solar_data.get('panel_id'),
                "location_id": solar_data.get('location_id'),
                "location_name": solar_data.get('location_name'),
                "latitude": solar_data.get('latitude'),
                "longitude": solar_data.get('longitude'),
                "timezone": solar_data.get('timezone'),
                "power_output": solar_data.get('power_output'),
                "unit_power": solar_data.get('unit_power'),
                "temperature": solar_data.get('temperature'),
                "unit_temp": solar_data.get('unit_temp'),
                "irradiance": solar_data.get('irradiance'),
                "unit_irradiance": solar_data.get('unit_irradiance'),
                "voltage": solar_data.get('voltage'),
                "unit_voltage": solar_data.get('unit_voltage'),
                "current": solar_data.get('current'),
                "unit_current": solar_data.get('unit_current'),
                "inverter_status": solar_data.get('inverter_status'),
                "timestamp": solar_data.get('timestamp')
            }
            
            return processed_data
            
        else:
            # If no value field, try to access data directly
            return {
                "topic_id": getattr(message, 'topicId', None),
                "topic_name": getattr(message, 'topicName', None),
                "stream_id": getattr(message, 'streamId', None),
                "message_type": getattr(message, 'type', None),
                "message_datetime": getattr(message, 'dateTime', None),
                "partition": getattr(message, 'partition', None),
                "offset": getattr(message, 'offset', None),
                "panel_id": getattr(message, 'panel_id', None),
                "location_id": getattr(message, 'location_id', None),
                "location_name": getattr(message, 'location_name', None),
                "latitude": getattr(message, 'latitude', None),
                "longitude": getattr(message, 'longitude', None),
                "timezone": getattr(message, 'timezone', None),
                "power_output": getattr(message, 'power_output', None),
                "unit_power": getattr(message, 'unit_power', None),
                "temperature": getattr(message, 'temperature', None),
                "unit_temp": getattr(message, 'unit_temp', None),
                "irradiance": getattr(message, 'irradiance', None),
                "unit_irradiance": getattr(message, 'unit_irradiance', None),
                "voltage": getattr(message, 'voltage', None),
                "unit_voltage": getattr(message, 'unit_voltage', None),
                "current": getattr(message, 'current', None),
                "unit_current": getattr(message, 'unit_current', None),
                "inverter_status": getattr(message, 'inverter_status', None),
                "timestamp": getattr(message, 'timestamp', None)
            }
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from value field: {e}")
        return message
    except Exception as e:
        print(f"Error processing message: {e}")
        return message

# Apply the transformation
sdf = sdf.apply(process_solar_data)

# Sink the processed data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)