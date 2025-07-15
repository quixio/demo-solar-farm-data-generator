import os
import json
import psycopg2
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from dotenv import load_dotenv

load_dotenv()

# Initialize TimescaleDB Sink (using PostgreSQL driver)
timescale_sink = PostgreSQLSink(
    host=os.environ.get('TIMESCALEDB_HOST'),
    port=int(os.environ.get('TIMESCALEDB_PORT', 5432)),
    dbname=os.environ.get('TIMESCALEDB_DATABASE'),
    user=os.environ.get('TIMESCALEDB_USER'),
    password=os.environ.get('TIMESCALEDB_PASSWORD'),
    table_name=os.environ.get('TIMESCALEDB_TABLE', 'solar_data'),
    schema_name=os.environ.get('TIMESCALEDB_SCHEMA', 'public'),
    schema_auto_update=os.environ.get('SCHEMA_AUTO_UPDATE', 'true').lower() == 'true',
)

# Initialize the application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'timescale-sink-group'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get('BATCH_TIMEOUT', '1')),
    commit_every=int(os.environ.get('BATCH_SIZE', '1000'))
)

# Define the input topic
input_topic = app.topic(os.environ.get('input', 'solar-data'), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

def create_table_if_not_exists():
    """Create the table if it doesn't exist"""
    conn = psycopg2.connect(
        host=os.environ.get('TIMESCALEDB_HOST'),
        port=int(os.environ.get('TIMESCALEDB_PORT', 5432)),
        database=os.environ.get('TIMESCALEDB_DATABASE'),
        user=os.environ.get('TIMESCALEDB_USER'),
        password=os.environ.get('TIMESCALEDB_PASSWORD')
    )
    
    cursor = conn.cursor()
    table_name = os.environ.get('TIMESCALEDB_TABLE', 'solar_data')
    schema_name = os.environ.get('TIMESCALEDB_SCHEMA', 'public')
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        panel_id VARCHAR(50),
        location_id VARCHAR(50),
        location_name VARCHAR(100),
        latitude FLOAT,
        longitude FLOAT,
        timezone INTEGER,
        power_output FLOAT,
        unit_power VARCHAR(10),
        temperature FLOAT,
        unit_temp VARCHAR(10),
        irradiance FLOAT,
        unit_irradiance VARCHAR(10),
        voltage FLOAT,
        unit_voltage VARCHAR(10),
        current FLOAT,
        unit_current VARCHAR(10),
        inverter_status VARCHAR(20)
    );
    """
    
    cursor.execute(create_table_query)
    
    # Create hypertable if it doesn't exist (TimescaleDB extension)
    try:
        cursor.execute(f"SELECT create_hypertable('{schema_name}.{table_name}', 'timestamp', if_not_exists => TRUE);")
    except Exception as e:
        print(f"Note: Could not create hypertable (this is normal if TimescaleDB extension is not installed): {e}")
    
    conn.commit()
    cursor.close()
    conn.close()

def process_message(message):
    """Process the incoming message and extract the solar data"""
    print(f'Raw message: {message}')
    
    # Parse the value field which contains the actual solar data as JSON string
    try:
        if hasattr(message, 'value') and message.value:
            # The value field contains a JSON string, parse it
            solar_data = json.loads(message.value)
            
            # Map the parsed data to the table schema
            processed_data = {
                'timestamp': solar_data.get('timestamp'),
                'panel_id': solar_data.get('panel_id'),
                'location_id': solar_data.get('location_id'),
                'location_name': solar_data.get('location_name'),
                'latitude': solar_data.get('latitude'),
                'longitude': solar_data.get('longitude'),
                'timezone': solar_data.get('timezone'),
                'power_output': solar_data.get('power_output'),
                'unit_power': solar_data.get('unit_power'),
                'temperature': solar_data.get('temperature'),
                'unit_temp': solar_data.get('unit_temp'),
                'irradiance': solar_data.get('irradiance'),
                'unit_irradiance': solar_data.get('unit_irradiance'),
                'voltage': solar_data.get('voltage'),
                'unit_voltage': solar_data.get('unit_voltage'),
                'current': solar_data.get('current'),
                'unit_current': solar_data.get('unit_current'),
                'inverter_status': solar_data.get('inverter_status')
            }
            
            return processed_data
        else:
            print("No value field found in message")
            return None
            
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from value field: {e}")
        return None
    except Exception as e:
        print(f"Error processing message: {e}")
        return None

# Apply the processing function
sdf = sdf.apply(process_message)

# Filter out None values
sdf = sdf.filter(lambda x: x is not None)

# Sink the processed data
sdf.sink(timescale_sink)

if __name__ == "__main__":
    # Create table if it doesn't exist
    create_table_if_not_exists()
    
    # Run the application with limit of 10 messages
    app.run(count=10, timeout=20)