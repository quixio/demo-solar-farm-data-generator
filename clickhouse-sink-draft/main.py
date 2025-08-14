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
    port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
except ValueError:
    port = 9000

clickhouse_client = Client(
    host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
    port=port,
    user=os.environ.get('CLICKHOUSE_USER', 'default'),
    password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
    database=os.environ.get('CLICKHOUSE_DATABASE', 'default')
)

# Table name
table_name = os.environ.get('CLICKHOUSE_TABLE', 'solar_data')

# Create table if it doesn't exist
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
    timestamp DateTime64(9),
    message_datetime DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (panel_id, timestamp)
"""

try:
    clickhouse_client.execute(create_table_sql)
    clickhouse_client.execute("COMMIT")
    logger.info(f"Table {table_name} created or already exists")
except Exception as e:
    logger.error(f"Error creating table: {e}")
    raise

app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "clickhouse-sink"),
    auto_offset_reset="earliest",
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)

def process_solar_data(message):
    try:
        print(f'Raw message: {message}')
        
        # Parse the JSON string from the value field
        if isinstance(message.get('value'), str):
            data = json.loads(message['value'])
        else:
            data = message.get('value', {})
        
        # Convert timestamp from nanoseconds to datetime
        timestamp_ns = data.get('timestamp', 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
        
        # Convert message datetime
        message_datetime = datetime.fromisoformat(message.get('dateTime', '').replace('Z', '+00:00'))
        
        # Map data to table schema
        row_data = {
            'panel_id': data.get('panel_id', ''),
            'location_id': data.get('location_id', ''),
            'location_name': data.get('location_name', ''),
            'latitude': float(data.get('latitude', 0.0)),
            'longitude': float(data.get('longitude', 0.0)),
            'timezone': int(data.get('timezone', 0)),
            'power_output': int(data.get('power_output', 0)),
            'unit_power': data.get('unit_power', ''),
            'temperature': float(data.get('temperature', 0.0)),
            'unit_temp': data.get('unit_temp', ''),
            'irradiance': int(data.get('irradiance', 0)),
            'unit_irradiance': data.get('unit_irradiance', ''),
            'voltage': float(data.get('voltage', 0.0)),
            'unit_voltage': data.get('unit_voltage', ''),
            'current': int(data.get('current', 0)),
            'unit_current': data.get('unit_current', ''),
            'inverter_status': data.get('inverter_status', ''),
            'timestamp': timestamp_dt,
            'message_datetime': message_datetime
        }
        
        # Insert data into ClickHouse
        insert_sql = f"""
        INSERT INTO {table_name} VALUES
        """
        
        clickhouse_client.execute(insert_sql, [row_data])
        logger.info(f"Inserted data for panel {row_data['panel_id']}")
        
        return message
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return message

def clickhouse_sink(message):
    return process_solar_data(message)

sdf = sdf.apply(clickhouse_sink)

if __name__ == "__main__":
    logger.info("Starting ClickHouse sink application")
    app.run(count=10, timeout=20)