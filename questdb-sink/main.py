import os
import json
import psycopg2
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

load_dotenv()

class QuestDBSink(BatchingSink):
    def __init__(self, host, username, token, database, table_name):
        super().__init__()
        self.host = host
        self.username = username
        self.token = token
        self.database = database
        self.table_name = table_name
        self.connection = None
        
    def setup(self):
        """Setup the QuestDB connection and create table if needed"""
        try:
            port = int(os.environ.get('QDB_PORT', '8812'))
        except ValueError:
            port = 8812
            
        self.connection = psycopg2.connect(
            host=self.host,
            port=port,
            user=self.username,
            password=self.token,
            database=self.database
        )
        
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            panel_id SYMBOL,
            location_id SYMBOL,
            location_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone INT,
            power_output INT,
            unit_power SYMBOL,
            temperature DOUBLE,
            unit_temp SYMBOL,
            irradiance INT,
            unit_irradiance SYMBOL,
            voltage DOUBLE,
            unit_voltage SYMBOL,
            current INT,
            unit_current SYMBOL,
            inverter_status SYMBOL,
            timestamp TIMESTAMP
        ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """
        
        cursor = self.connection.cursor()
        cursor.execute(create_table_sql)
        self.connection.commit()
        cursor.close()
        
    def write(self, batch: SinkBatch):
        """Write batch of messages to QuestDB"""
        if not self.connection:
            self.setup()
            
        cursor = self.connection.cursor()
        
        for item in batch:
            # Parse the JSON value string
            if isinstance(item.value, str):
                data = json.loads(item.value)
            else:
                data = item.value
                
            # Convert epoch timestamp to datetime
            timestamp_epoch = data.get('timestamp', 0)
            if timestamp_epoch:
                # Convert from nanoseconds to seconds
                timestamp_dt = datetime.fromtimestamp(timestamp_epoch / 1_000_000_000)
            else:
                timestamp_dt = datetime.now()
            
            insert_sql = f"""
            INSERT INTO {self.table_name} VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_sql, (
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
                timestamp_dt
            ))
        
        self.connection.commit()
        cursor.close()

# Initialize the application
app = Application(
    consumer_group=os.environ.get("QDB_CONSUMER_GROUP_NAME", "questdb-sink-group"),
    auto_offset_reset="earliest",
    commit_every=int(os.environ.get("QDB_BUFFER_SIZE", "100")),
    commit_interval=float(os.environ.get("QDB_BUFFER_TIMEOUT", "5.0")),
)

# Create input topic
input_topic = app.topic(os.environ.get("QDB_INPUT_TOPIC"))

# Create QuestDB sink
questdb_sink = QuestDBSink(
    host=os.environ.get("QDB_HOST"),
    username=os.environ.get("QDB_USERNAME"),
    token=os.environ.get("QDB_TOKEN_KEY"),
    database=os.environ.get("QDB_DATABASE"),
    table_name=os.environ.get("QDB_TABLE_NAME")
)

# Create streaming dataframe
sdf = app.dataframe(input_topic)

# Print incoming messages for debugging
sdf.print(metadata=True)

# Sink data to QuestDB
sdf.sink(questdb_sink)

if __name__ == "__main__":
    app.run(count=10, timeout=20)