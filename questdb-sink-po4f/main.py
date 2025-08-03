# DEPENDENCIES:
# pip install quixstreams
# pip install psycopg2-binary
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
import psycopg2
from psycopg2.extras import execute_values

from dotenv import load_dotenv
load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, database, table, timestamp_column=None, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        try:
            self.port = int(os.environ.get('QDB_PORT', '8812'))
        except ValueError:
            self.port = 8812
        self.database = database
        self.table = table
        self.timestamp_column = timestamp_column
        self.token = os.environ.get('QDB_TOKEN_KEY')
        self._connection = None
        self._table_created = False

    def setup(self):
        connection_params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': 'admin',
            'password': self.token,
            'options': '-c search_path=public'
        }
        
        self._connection = psycopg2.connect(**connection_params)
        self._connection.autocommit = False
        
        # Create table if it doesn't exist
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        if self._table_created:
            return
            
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            panel_id SYMBOL,
            location_id SYMBOL,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output DOUBLE,
            unit_power STRING,
            temperature DOUBLE,
            unit_temp STRING,
            irradiance DOUBLE,
            unit_irradiance STRING,
            voltage DOUBLE,
            unit_voltage STRING,
            current DOUBLE,
            unit_current STRING,
            inverter_status STRING,
            message_timestamp TIMESTAMP,
            record_timestamp TIMESTAMP
        ) TIMESTAMP(record_timestamp) PARTITION BY DAY;
        """
        
        cursor = self._connection.cursor()
        cursor.execute(create_table_sql)
        self._connection.commit()
        cursor.close()
        self._table_created = True

    def write(self, batch: SinkBatch):
        if not self._connection:
            self.setup()
            
        records = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Parse the value field which contains JSON string
            if hasattr(item, 'value') and isinstance(item.value, str):
                data = json.loads(item.value)
            elif hasattr(item, 'value') and isinstance(item.value, dict):
                data = item.value
            else:
                # Handle case where data might be directly in the item
                data = item if isinstance(item, dict) else {}
            
            # Convert timestamp to datetime if present
            message_timestamp = None
            if 'timestamp' in data:
                try:
                    # Convert nanosecond timestamp to datetime
                    timestamp_ns = data['timestamp']
                    timestamp_seconds = timestamp_ns / 1_000_000_000
                    message_timestamp = datetime.fromtimestamp(timestamp_seconds)
                except (ValueError, TypeError):
                    message_timestamp = None
            
            record = (
                data.get('panel_id'),
                data.get('location_id'),
                data.get('location_name'),
                data.get('latitude'),
                data.get('longitude'),
                data.get('timezone'),
                data.get('power_output'),
                data.get('unit_power'),
                data.get('temperature'),
                data.get('unit_temp'),
                data.get('irradiance'),
                data.get('unit_irradiance'),
                data.get('voltage'),
                data.get('unit_voltage'),
                data.get('current'),
                data.get('unit_current'),
                data.get('inverter_status'),
                message_timestamp,
                datetime.utcnow()
            )
            records.append(record)
        
        if records:
            cursor = self._connection.cursor()
            insert_sql = f"""
            INSERT INTO {self.table} (
                panel_id, location_id, location_name, latitude, longitude,
                timezone, power_output, unit_power, temperature, unit_temp,
                irradiance, unit_irradiance, voltage, unit_voltage, current,
                unit_current, inverter_status, message_timestamp, record_timestamp
            ) VALUES %s
            """
            execute_values(cursor, insert_sql, records)
            self._connection.commit()
            cursor.close()

app = Application(
    consumer_group=os.environ.get("QDB_CONSUMER_GROUP_NAME", "questdb-sink"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("QDB_BUFFER_SIZE", "1000")),
    commit_interval=float(os.environ.get("QDB_BUFFER_TIMEOUT", "5.0")),
)

input_topic = app.topic(os.environ["input"])

questdb_sink = QuestDBSink(
    host=os.environ.get('QDB_HOST'),
    database=os.environ.get('QDB_DATABASE'),
    table=os.environ.get('QDB_TABLE'),
    timestamp_column=os.environ.get('QDB_TIMESTAMP_COLUMN')
)

sdf = app.dataframe(input_topic)
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)