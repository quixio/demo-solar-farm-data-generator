import os
import json
from quixstreams import Application
from quixstreams.sinks.community.timescaledb import TimescaleDBSink
from dotenv import load_dotenv

load_dotenv()

# Initialize TimescaleDB Sink
timescale_sink = TimescaleDBSink(
    host=os.environ.get('TIMESCALEDB_HOST'),
    port=int(os.environ.get('TIMESCALEDB_PORT', '5432')),
    dbname=os.environ.get('TIMESCALEDB_DBNAME'),
    user=os.environ.get('TIMESCALEDB_USER'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data'),
    schema_name=os.environ.get('TIMESCALEDB_SCHEMA', 'public'),
    schema_auto_update=os.environ.get('SCHEMA_AUTO_UPDATE', 'true').lower() == 'true',
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'solar-data-consumer'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get('BATCH_TIMEOUT', '1')),
    commit_every=int(os.environ.get('BATCH_SIZE', '1000'))
)

# Define the input topic
input_topic = app.topic(os.environ.get('input', 'solar-data'), key_deserializer="string")

# Create the dataframe
sdf = app.dataframe(input_topic)

def process_solar_data(message):
    """Process solar data message and extract the value JSON"""
    # Debug: Print raw message structure
    print(f'Raw message: {message}')
    
    # Parse the JSON string in the 'value' field
    try:
        if isinstance(message.get('value'), str):
            parsed_value = json.loads(message['value'])
        else:
            parsed_value = message.get('value', {})
        
        # Map the parsed value to the expected database schema
        processed_data = {
            'timestamp': parsed_value.get('timestamp'),
            'panel_id': parsed_value.get('panel_id'),
            'location_id': parsed_value.get('location_id'),
            'location_name': parsed_value.get('location_name'),
            'latitude': parsed_value.get('latitude'),
            'longitude': parsed_value.get('longitude'),
            'timezone': parsed_value.get('timezone'),
            'power_output': parsed_value.get('power_output'),
            'unit_power': parsed_value.get('unit_power'),
            'temperature': parsed_value.get('temperature'),
            'unit_temp': parsed_value.get('unit_temp'),
            'irradiance': parsed_value.get('irradiance'),
            'unit_irradiance': parsed_value.get('unit_irradiance'),
            'voltage': parsed_value.get('voltage'),
            'unit_voltage': parsed_value.get('unit_voltage'),
            'current': parsed_value.get('current'),
            'unit_current': parsed_value.get('unit_current'),
            'inverter_status': parsed_value.get('inverter_status'),
            'dateTime': message.get('dateTime'),
            'streamId': message.get('streamId'),
            'partition': message.get('partition'),
            'offset': message.get('offset')
        }
        
        return processed_data
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON value: {e}")
        return None
    except Exception as e:
        print(f"Error processing message: {e}")
        return None

# Process the data
sdf = sdf.apply(process_solar_data)

# Filter out None values (failed processing)
sdf = sdf.filter(lambda x: x is not None)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)