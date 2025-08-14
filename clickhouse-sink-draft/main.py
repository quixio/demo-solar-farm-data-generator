# DEPENDENCIES:
# pip install quixstreams
# pip install clickhouse-driver
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import logging
from datetime import datetime
from quixstreams import Application
from clickhouse_driver import Client
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ClickHouse connection parameters
try:
    clickhouse_port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
except ValueError:
    clickhouse_port = 9000

clickhouse_client = Client(
    host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
    port=clickhouse_port,
    user=os.environ.get('CLICKHOUSE_USER', 'default'),
    password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
    database=os.environ.get('CLICKHOUSE_DATABASE', 'default')
)

# Create table if it doesn't exist
table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_data')
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    panel_id String,
    location_id String,
    location_name String,
    latitude Float64,
    longitude Float64,
    timezone Int32,
    power_output Int32,
    unit_power String,
    temperature Float64,
    unit_temp String,
    irradiance Int32,
    unit_irradiance String,
    voltage Float64,
    unit_voltage String,
    current Int32,
    unit_current String,
    inverter_status String,
    timestamp DateTime64(3),
    message_datetime DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (panel_id, timestamp)
"""

try:
    clickhouse_client.execute(create_table_sql)
    logger.info(f"Table {table_name} created or already exists")
except Exception as e:
    logger.error(f"Failed to create table: {e}")
    raise

def process_solar_data(message):
    """Process solar data message and insert into ClickHouse"""
    try:
        print(f'Raw message: {message}')
        
        # Parse the JSON string from the value field
        if isinstance(message.value, str):
            solar_data = json.loads(message.value)
        else:
            solar_data = message.value
        
        # Convert timestamp from nanoseconds to datetime
        timestamp_ns = solar_data.get('timestamp', 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
        
        # Parse message datetime
        message_datetime = datetime.fromisoformat(message.timestamp.replace('Z', '+00:00'))
        
        # Prepare data for insertion
        insert_data = {
            'panel_id': solar_data.get('panel_id', ''),
            'location_id': solar_data.get('location_id', ''),
            'location_name': solar_data.get('location_name', ''),
            'latitude': solar_data.get('latitude', 0.0),
            'longitude': solar_data.get('longitude', 0.0),
            'timezone': solar_data.get('timezone', 0),
            'power_output': solar_data.get('power_output', 0),
            'unit_power': solar_data.get('unit_power', ''),
            'temperature': solar_data.get('temperature', 0.0),
            'unit_temp': solar_data.get('unit_temp', ''),
            'irradiance': solar_data.get('irradiance', 0),
            'unit_irradiance': solar_data.get('unit_irradiance', ''),
            'voltage': solar_data.get('voltage', 0.0),
            'unit_voltage': solar_data.get('unit_voltage', ''),
            'current': solar_data.get('current', 0),
            'unit_current': solar_data.get('unit_current', ''),
            'inverter_status': solar_data.get('inverter_status', ''),
            'timestamp': timestamp_dt,
            'message_datetime': message_datetime
        }
        
        # Insert data into ClickHouse
        insert_sql = f"""
        INSERT INTO {table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
        ) VALUES
        """
        
        clickhouse_client.execute(insert_sql, [insert_data])
        logger.info(f"Inserted data for panel {insert_data['panel_id']} at {timestamp_dt}")
        
        return message
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        raise

# Create Quix application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "clickhouse-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "100")),
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1")),
)

input_topic = app.topic(os.environ.get("input", "solar-data"))
sdf = app.dataframe(input_topic)

# Process and sink data
sdf = sdf.apply(process_solar_data)

if __name__ == "__main__":
    try:
        logger.info("Starting ClickHouse sink application")
        app.run(count=10, timeout=20)
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        try:
            clickhouse_client.disconnect()
        except:
            pass