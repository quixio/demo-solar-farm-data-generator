# import Utility modules
import os

# import vendor-specific modules
from quixstreams import Application
from quixstreams.sinks.core.influxdb3 import InfluxDB3Sink

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()


tag_keys = keys.split(",") if (keys := os.environ.get("INFLUXDB_TAG_KEYS")) else []
field_keys = keys.split(",") if (keys := os.environ.get("INFLUXDB_FIELD_KEYS")) else []
measurement_name = os.environ.get("INFLUXDB_MEASUREMENT_NAME", "measurement1")
time_setter = col if (col := os.environ.get("TIMESTAMP_COLUMN")) else None

influxdb_v3_sink = InfluxDB3Sink(
    token=os.environ["INFLUXDB_TOKEN"],
    host=os.environ["INFLUXDB_HOST"],
    organization_id=os.environ["INFLUXDB_ORG"],
    tags_keys=tag_keys,
    fields_keys=field_keys,
    time_setter=time_setter,
    database=os.environ["INFLUXDB_DATABASE"],
    measurement=measurement_name,
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
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "influxdb-data-writer"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("BUFFER_DELAY", "1")),
)
input_topic = app.topic(os.environ["input"])

def process_solar_data(message):
    try:
        print(f'Raw message: {message}')
        
        # Parse the JSON string from the value field
        if isinstance(message.get('value'), str):
            data = json.loads(message['value'])
        else:
            data = message.get('value', {})
        
        # Convert timestamp to datetime
        timestamp_ns = data.get('timestamp', 0)
        timestamp_dt = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
        
        # Convert message datetime
        message_dt = datetime.fromisoformat(message.get('dateTime', '').replace('Z', '+00:00'))
        
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
            'message_datetime': message_dt
        }
        
        # Insert into ClickHouse
        insert_sql = f"""
        INSERT INTO {table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status, timestamp, message_datetime
        ) VALUES
        """
        
        clickhouse_client.execute(insert_sql, [row_data])
        logger.info(f"Inserted data for panel {row_data['panel_id']}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        logger.error(f"Message content: {message}")

def clickhouse_sink(message):
    process_solar_data(message)
    return message


if __name__ == "__main__":
    logger.info("Starting ClickHouse sink application")
    app.run(count=10, timeout=20)