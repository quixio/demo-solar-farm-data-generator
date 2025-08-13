# DEPENDENCIES:
# pip install clickhouse-connect
# END_DEPENDENCIES

from quixstreams import Application
from quixstreams.sinks.base.sink import BatchingSink, SinkBatch, SinkBackpressureError
import clickhouse_connect
import os
import time
import json
from datetime import datetime

class ClickHouseSink(BatchingSink):
    def __init__(self, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self._client = None
        self._table_created = False

    def setup(self):
        try:
            port = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
        except ValueError:
            port = 8123
            
        self._client = clickhouse_connect.get_client(
            host=os.environ.get('CLICKHOUSE_HOST'),
            port=port,
            database=os.environ.get('CLICKHOUSE_DATABASE'),
            username=os.environ.get('CLICKHOUSE_USER'),
            password=os.environ.get('CLICKHOUSE_PASSWORD')
        )
        
        # Test connection
        self._client.ping()
        
        # Create table if it doesn't exist
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        if not self._table_created:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS kafka_messages (
                timestamp DateTime64(3),
                topic String,
                partition UInt32,
                offset UInt64,
                key String,
                value String,
                headers String
            ) ENGINE = MergeTree()
            ORDER BY (timestamp, topic, partition, offset)
            """
            self._client.command(create_table_sql)
            self._table_created = True

    def write(self, batch: SinkBatch):
        attempts_remaining = 3
        
        # Prepare data for insertion
        data = []
        for item in batch:
            print(f'Raw message: {item}')
            
            # Convert timestamp to datetime
            timestamp = datetime.fromtimestamp(item.timestamp / 1000.0)
            
            # Handle key - convert bytes to string if needed
            key = item.key
            if isinstance(key, bytes):
                key = key.decode('utf-8', errors='ignore')
            elif key is None:
                key = ''
            else:
                key = str(key)
            
            # Handle value - convert to JSON string if it's a dict
            value = item.value
            if isinstance(value, dict):
                value = json.dumps(value)
            elif isinstance(value, bytes):
                value = value.decode('utf-8', errors='ignore')
            elif value is None:
                value = ''
            else:
                value = str(value)
            
            # Handle headers
            headers = item.headers if item.headers else []
            headers_str = json.dumps(dict(headers)) if headers else '{}'
            
            data.append([
                timestamp,
                item.topic,
                item.partition,
                item.offset,
                key,
                value,
                headers_str
            ])
        
        while attempts_remaining:
            try:
                self._client.insert(
                    'kafka_messages',
                    data,
                    column_names=['timestamp', 'topic', 'partition', 'offset', 'key', 'value', 'headers']
                )
                return
            except Exception as e:
                if 'timeout' in str(e).lower():
                    raise SinkBackpressureError(
                        retry_after=30.0,
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
                else:
                    raise Exception(f"Error while writing to ClickHouse: {e}")

def main():
    app = Application(
        consumer_group="clickhouse_sink_destination",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    
    clickhouse_sink = ClickHouseSink()
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)
    
    sdf = sdf.apply(lambda row: row).print(metadata=True)
    sdf.sink(clickhouse_sink)
    
    app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()