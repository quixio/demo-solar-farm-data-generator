import os
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from datetime import datetime
import json
import time
# Load environment variables from a .env file for local development
from dotenv import load_dotenv
load_dotenv()

# Initialize TimescaleDB Sink (TimescaleDB is PostgreSQL-compatible)
timescale_sink = PostgreSQLSink(
    host=os.getenv("TIMESCALE_HOST", "timescaledb"),
    port=int(os.getenv("TIMESCALE_PORT", "5432")),
    dbname=os.getenv("TIMESCALE_DBNAME", "metrics"),
    user=os.getenv("TIMESCALE_USER", "tsadmin"),
    password=os.getenv("TIMESCALE_PASSWORD"),
    table_name=os.getenv("TIMESCALE_TABLE", "solar_data"),
    schema_name=os.getenv("TIMESCALE_SCHEMA", "public"),
    schema_auto_update=os.getenv("SCHEMA_AUTO_UPDATE", "true").lower() == "true",
)

# Initialize the application with a time-based consumer group for production
app = Application(
    consumer_group=os.getenv("CONSUMER_GROUP_NAME", "solar-timescale-sink"),
    auto_offset_reset="earliest",
    commit_interval=float(os.getenv("BATCH_TIMEOUT", "1")),
    commit_every=int(os.getenv("BATCH_SIZE", "1000"))
)

# Define the input topic
input_topic = app.topic(os.getenv("input", "solar-data"), key_deserializer="string")

# Process and sink data
sdf = app.dataframe(input_topic)

# Transform the data to prepare for TimescaleDB
def transform_solar_data(row):
    """Transform solar panel data for TimescaleDB storage"""
    try:
        # The data is already parsed as a dict, no need to parse JSON
        data = row
        
        # Convert timestamp from nanoseconds to datetime
        timestamp_ns = data.get('timestamp', 0)
        timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
        
        print(f"Processing solar data for panel {data.get('panel_id')} at {timestamp}")
        
        # Return structured data for TimescaleDB
        return {
            'timestamp': timestamp,
            'panel_id': data.get('panel_id'),
            'location_id': data.get('location_id'),
            'location_name': data.get('location_name'),
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            'timezone': data.get('timezone'),
            'power_output': data.get('power_output'),
            'unit_power': data.get('unit_power'),
            'temperature': data.get('temperature'),
            'unit_temp': data.get('unit_temp'),
            'irradiance': data.get('irradiance'),
            'unit_irradiance': data.get('unit_irradiance'),
            'voltage': data.get('voltage'),
            'unit_voltage': data.get('unit_voltage'),
            'current': data.get('current'),
            'unit_current': data.get('unit_current'),
            'inverter_status': data.get('inverter_status')
        }
    except Exception as e:
        print(f"Error processing solar data: {e}")
        return None

# Apply transformation and filter out None values
sdf = sdf.apply(transform_solar_data).filter(lambda x: x is not None)

# Sink to TimescaleDB
sdf.sink(timescale_sink)

if __name__ == "__main__":
    # Production deployment - no stop conditions for continuous operation
    app.run(sdf)