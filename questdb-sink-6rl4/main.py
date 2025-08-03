# DEPENDENCIES:
# pip install psycopg2-binary
# pip install python-dotenv
# END_DEPENDENCIES

import os
import logging
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass

from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- helpers -------------------------------------------------
def env_int(var_name: str, default: int) -> int:
    """
    Read an environment variable and return it as int.
    Returns `default` when the variable is missing, empty,
    or cannot be parsed as an integer.
    """
    val = os.getenv(var_name)
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def env_float(var_name: str, default: float) -> float:
    """
    Same as env_int but for floats.
    """
    val = os.getenv(var_name)
    try:
        return float(val)
    except (TypeError, ValueError):
        return default
# ------------------------------------------------------------

@dataclass
class SolarDataRecord:
    panel_id: str
    location_id: str
    location_name: str
    latitude: float
    longitude: float
    timezone: int
    power_output: float
    temperature: float
    irradiance: float
    voltage: float
    current: float
    inverter_status: str
    timestamp: int
    message_timestamp: datetime

class QuestDBSink(BatchingSink):
    def __init__(self, 
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 database: str = "qdb",
                 table_name: str = "solar_data",
                 **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table_name = table_name
        self._connection = None
        self._table_created = False

    def setup(self):
        """Setup the QuestDB connection and create table if needed"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
                connect_timeout=10
            )
            logger.info(f"Connected to QuestDB at {self.host}:{self.port}")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            raise

    def _create_table_if_not_exists(self):
        """Create the solar_data table if it doesn't exist"""
        if self._table_created:
            return
            
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            panel_id STRING,
            location_id STRING,  
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output DOUBLE,
            temperature DOUBLE,
            irradiance DOUBLE,
            voltage DOUBLE,
            current DOUBLE,
            inverter_status STRING,
            timestamp TIMESTAMP,
            message_timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self._connection.commit()
                logger.info(f"Table {self.table_name} created or already exists")
                self._table_created = True
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def write(self, batch: SinkBatch):
        """Write a batch of records to QuestDB"""
        if not self._connection:
            raise RuntimeError("QuestDB connection not established")

        records = []
        for item in batch:
            try:
                # Parse the message value
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert timestamps
                message_ts = datetime.fromisoformat(item.headers.get('dateTime', '').replace('Z', '+00:00')) if item.headers.get('dateTime') else datetime.utcnow()
                
                # Convert custom timestamp (assuming it's nanoseconds since epoch)
                custom_timestamp = data.get('timestamp', 0)
                if custom_timestamp:
                    # Convert from nanoseconds to seconds and create datetime
                    timestamp_dt = datetime.fromtimestamp(custom_timestamp / 1_000_000_000)
                else:
                    timestamp_dt = message_ts

                record = SolarDataRecord(
                    panel_id=data.get('panel_id', ''),
                    location_id=data.get('location_id', ''),
                    location_name=data.get('location_name', ''),
                    latitude=float(data.get('latitude', 0.0)),
                    longitude=float(data.get('longitude', 0.0)),
                    timezone=int(data.get('timezone', 0)),
                    power_output=float(data.get('power_output', 0.0)),
                    temperature=float(data.get('temperature', 0.0)),
                    irradiance=float(data.get('irradiance', 0.0)),
                    voltage=float(data.get('voltage', 0.0)),
                    current=float(data.get('current', 0.0)),
                    inverter_status=data.get('inverter_status', ''),
                    timestamp=int(custom_timestamp),
                    message_timestamp=message_ts
                )
                records.append(record)
                
            except Exception as e:
                logger.error(f"Failed to parse record: {e}")
                continue

        if not records:
            logger.warning("No valid records to write")
            return

        # Insert records
        insert_sql = f"""
        INSERT INTO {self.table_name} 
        (panel_id, location_id, location_name, latitude, longitude, timezone, 
         power_output, temperature, irradiance, voltage, current, inverter_status, 
         timestamp, message_timestamp)
        VALUES %s
        """
        
        values = [
            (r.panel_id, r.location_id, r.location_name, r.latitude, r.longitude, 
             r.timezone, r.power_output, r.temperature, r.irradiance, r.voltage,
             r.current, r.inverter_status, r.timestamp, r.message_timestamp)
            for r in records
        ]

        try:
            with self._connection.cursor() as cursor:
                execute_values(cursor, insert_sql, values)
                self._connection.commit()
                logger.info(f"Successfully wrote {len(records)} records to QuestDB")
                
        except Exception as e:
            logger.error(f"Failed to write batch to QuestDB: {e}")
            self._connection.rollback()
            raise

    def close(self):
        """Close the QuestDB connection"""
        if self._connection:
            self._connection.close()
            logger.info("QuestDB connection closed")

# Create QuestDB sink
try:
    port = env_int("QUESTDB_PORT", 8812)
except ValueError:
    port = 8812

questdb_sink = QuestDBSink(
    host=os.environ.get("QUESTDB_HOST", "localhost"),
    port=port,
    username=os.environ.get("QUESTDB_USERNAME", "admin"),
    password=os.environ.get("QUESTDB_PASSWORD", "quest"),
    database=os.environ.get("QUESTDB_DATABASE", "qdb"),
    table_name=os.environ.get("QUESTDB_TABLE", "solar_data")
)

# Create Quix Streams application
app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "questdb-data-writer"),
    auto_offset_reset="earliest",
    commit_every=env_int("BUFFER_SIZE", 1000),
    commit_interval=env_float("BUFFER_DELAY", 1.0),
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)

# Add debug logging to see raw messages
def debug_message(row):
    print(f"Raw message: {row}")
    return row

sdf = sdf.apply(debug_message)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)