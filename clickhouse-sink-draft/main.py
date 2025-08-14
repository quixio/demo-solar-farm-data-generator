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
    print(f'Raw message: {message}')
    
    try:
        # Parse the JSON string in the value field
        if isinstance(message.get('value'), str):
            solar_data = json.loads(message['value'])
        else:
            solar_data = message.get('value', {})
        
        # Convert timestamp from nanoseconds to datetime
        timestamp_ns = solar_data.get('timestamp', 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
        
        # Convert message datetime
        message_dt = datetime.fromisoformat(message.get('dateTime', '').replace('Z', '+00:00'))
        
        # Prepare data for insertion
        row_data = (
            solar_data.get('panel_id', ''),
            solar_data.get('location_id', ''),
            solar_data.get('location_name', ''),
            float(solar_data.get('latitude', 0.0)),
            float(solar_data.get('longitude', 0.0)),
            int(solar_data.get('timezone', 0)),
            int(solar_data.get('power_output', 0)),
            solar_data.get('unit_power', ''),
            float(solar_data.get('temperature', 0.0)),
            solar_data.get('unit_temp', ''),
            int(solar_data.get('irradiance', 0)),
            solar_data.get('unit_irradiance', ''),
            float(solar_data.get('voltage', 0.0)),
            solar_data.get('unit_voltage', ''),
            int(solar_data.get('current', 0)),
            solar_data.get('unit_current', ''),
            solar_data.get('inverter_status', ''),
            timestamp_dt,
            message_dt
        )
        
        # Insert into ClickHouse
        insert_sql = f"""
        INSERT INTO {table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
        ) VALUES
        """
        
        clickhouse_client.execute(insert_sql, [row_data])
        logger.info(f"Successfully inserted data for panel {solar_data.get('panel_id', 'unknown')}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        logger.error(f"Message content: {message}")
        raise

# Create ClickHouse sink
def clickhouse_sink(row):
    process_solar_data(row)

sdf.sink(clickhouse_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)