import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()

def create_table_if_not_exists():
    """Create the solar_data table if it doesn't exist"""
    conn = psycopg2.connect(
        host=os.environ.get('TIMESCALEDB_HOST'),
        port=int(os.environ.get('TIMESCALEDB_PORT', 5432)),
        database=os.environ.get('TIMESCALEDB_DATABASE'),
        user=os.environ.get('TIMESCALEDB_USER'),
        password=os.environ.get('TIMESCALEDB_PASSWORD')
    )
    
    cursor = conn.cursor()
    
    # Create table with appropriate schema for solar data
    create_table_query = """
    CREATE TABLE IF NOT EXISTS solar_data (
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
        unit_irradiance VARCHAR(20),
        voltage FLOAT,
        unit_voltage VARCHAR(10),
        current FLOAT,
        unit_current VARCHAR(10),
        inverter_status VARCHAR(20),
        stream_id VARCHAR(100),
        PRIMARY KEY (timestamp, panel_id)
    );
    """
    
    cursor.execute(create_table_query)
    
    # Create hypertable if TimescaleDB extension is available
    try:
        cursor.execute("SELECT create_hypertable('solar_data', 'timestamp', if_not_exists => TRUE);")
    except Exception as e:
        print(f"Warning: Could not create hypertable (TimescaleDB extension may not be installed): {e}")
    
    conn.commit()
    cursor.close()
    conn.close()

def insert_solar_data(data):
    """Insert solar data into TimescaleDB"""
    conn = psycopg2.connect(
        host=os.environ.get('TIMESCALEDB_HOST'),
        port=int(os.environ.get('TIMESCALEDB_PORT', 5432)),
        database=os.environ.get('TIMESCALEDB_DATABASE'),
        user=os.environ.get('TIMESCALEDB_USER'),
        password=os.environ.get('TIMESCALEDB_PASSWORD')
    )
    
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO solar_data (
        timestamp, panel_id, location_id, location_name, latitude, longitude,
        timezone, power_output, unit_power, temperature, unit_temp, irradiance,
        unit_irradiance, voltage, unit_voltage, current, unit_current,
        inverter_status, stream_id
    ) VALUES (
        to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) ON CONFLICT (timestamp, panel_id) DO UPDATE SET
        location_id = EXCLUDED.location_id,
        location_name = EXCLUDED.location_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        timezone = EXCLUDED.timezone,
        power_output = EXCLUDED.power_output,
        unit_power = EXCLUDED.unit_power,
        temperature = EXCLUDED.temperature,
        unit_temp = EXCLUDED.unit_temp,
        irradiance = EXCLUDED.irradiance,
        unit_irradiance = EXCLUDED.unit_irradiance,
        voltage = EXCLUDED.voltage,
        unit_voltage = EXCLUDED.unit_voltage,
        current = EXCLUDED.current,
        unit_current = EXCLUDED.unit_current,
        inverter_status = EXCLUDED.inverter_status,
        stream_id = EXCLUDED.stream_id;
    """
    
    cursor.execute(insert_query, (
        data['timestamp'],
        data['panel_id'],
        data['location_id'],
        data['location_name'],
        data['latitude'],
        data['longitude'],
        data['timezone'],
        data['power_output'],
        data['unit_power'],
        data['temperature'],
        data['unit_temp'],
        data['irradiance'],
        data['unit_irradiance'],
        data['voltage'],
        data['unit_voltage'],
        data['current'],
        data['unit_current'],
        data['inverter_status'],
        data['stream_id']
    ))
    
    conn.commit()
    cursor.close()
    conn.close()

def process_message(message):
    """Process incoming message and extract solar data"""
    print(f'Raw message: {message}')
    
    try:
        # Parse the JSON string in the value field
        if hasattr(message, 'value') and message.value:
            if isinstance(message.value, str):
                solar_data = json.loads(message.value)
            else:
                solar_data = message.value
        else:
            print("Warning: Message has no value field")
            return None
        
        # Extract stream_id from message metadata if available
        stream_id = getattr(message, 'streamId', None) or getattr(message, 'key', None) or 'unknown'
        
        # Map the message value schema to the table schema
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
            'inverter_status': solar_data.get('inverter_status'),
            'stream_id': stream_id
        }
        
        # Insert data into TimescaleDB
        insert_solar_data(processed_data)
        print(f"Successfully inserted solar data for panel {processed_data['panel_id']}")
        
        return processed_data
        
    except Exception as e:
        print(f"Error processing message: {e}")
        return None

# Initialize the application
app = Application(
    consumer_group=os.environ.get('CONSUMER_GROUP_NAME', 'solar-data-sink'),
    auto_offset_reset="earliest",
    commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
    commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
)

# Create table if it doesn't exist
create_table_if_not_exists()

# Define the input topic
input_topic = app.topic(os.environ.get('input', 'solar-data'), key_deserializer="string")

# Process data
sdf = app.dataframe(input_topic)
sdf = sdf.apply(process_message)

if __name__ == "__main__":
    app.run(count=10, timeout=20)