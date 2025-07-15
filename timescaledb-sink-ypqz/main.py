import os
import json
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from dotenv import load_dotenv

load_dotenv()

# Initialize TimescaleDB Sink (using PostgreSQL sink as TimescaleDB is PostgreSQL-compatible)
timescale_sink = PostgreSQLSink(
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
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'solar-data-consumerv2'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get('BATCH_TIMEOUT', '1')),
    commit_every=int(os.environ.get('BATCH_SIZE', '1000'))
)

# Define the input topic
input_topic = app.topic(os.environ.get('input', 'solar-data'), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

def process_solar_data(item):
    # Debug: Print raw message structure
    print(f'Raw message: {item}')
    
    # Parse the JSON string in the 'value' field
    try:
        # The schema shows that the main data is in the 'value' field as a JSON string
        value_data = json.loads(item['value'])
        
        # Map the parsed data to the table schema
        processed_data = {
            'panel_id': value_data.get('panel_id'),
            'location_id': value_data.get('location_id'),
            'location_name': value_data.get('location_name'),
            'latitude': value_data.get('latitude'),
            'longitude': value_data.get('longitude'),
            'timezone': value_data.get('timezone'),
            'power_output': value_data.get('power_output'),
            'unit_power': value_data.get('unit_power'),
            'temperature': value_data.get('temperature'),
            'unit_temp': value_data.get('unit_temp'),
            'irradiance': value_data.get('irradiance'),
            'unit_irradiance': value_data.get('unit_irradiance'),
            'voltage': value_data.get('voltage'),
            'unit_voltage': value_data.get('unit_voltage'),
            'current': value_data.get('current'),
            'unit_current': value_data.get('unit_current'),
            'inverter_status': value_data.get('inverter_status'),
            'timestamp': value_data.get('timestamp'),
            'topic_id': item.get('topicId'),
            'topic_name': item.get('topicName'),
            'stream_id': item.get('streamId'),
            'date_time': item.get('dateTime'),
            'partition': item.get('partition'),
            'offset': item.get('offset')
        }
        
        return processed_data
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        print(f'Error processing message: {e}')
        return None

# Apply the processing function
sdf = sdf.apply(process_solar_data)

# Filter out None values (failed processing)
sdf = sdf.filter(lambda x: x is not None)

# Sink data to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)