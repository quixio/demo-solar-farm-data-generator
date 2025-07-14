```python
from quixstreams import Application
import os
import psycopg2
import json

class TimescaleDBSink:
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=os.environ.get('TIMESCALEDB_NAME'),
            user=os.environ.get('TIMESCALEDB_USER'),
            password=os.environ.get('TIMESCALEDB_PASSWORD'),
            host=os.environ.get('TIMESCALEDB_HOST'),
            port=os.environ.get('TIMESCALEDB_PORT')
        )
        self.cursor = self.connection.cursor()

    def insert_data(self, data):
        insert_query = """
            INSERT INTO solar_data (topic_id, topic_name, stream_id, value, date_time, partition, offset, panel_id, location_id, location_name, latitude, longitude, timezone, power_output, temperature, irradiance, voltage, current, inverter_status, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(insert_query, data)
        self.connection.commit()

def process_message(message, sink):
    value_json = json.loads(message.value)
    data = (
        message.topicId,
        message.topicName,
        message.streamId,
        message.value,
        message.dateTime,
        message.partition,
        message.offset,
        value_json['panel_id'],
        value_json['location_id'],
        value_json['location_name'],
        value_json['latitude'],
        value_json['longitude'],
        value_json['timezone'],
        value_json['power_output'],
        value_json['temperature'],
        value_json['irradiance'],
        value_json['voltage'],
        value_json['current'],
        value_json['inverter_status'],
        value_json['timestamp']
    )
    sink.insert_data(data)

def main():
    app = Application(
        consumer_group="timescaledb_sink",
        auto_create_topics=False,
        auto_offset_reset="earliest"
    )
    input_topic = app.topic(name=os.environ["input"])
    sdf = app.dataframe(topic=input_topic)

    sink = TimescaleDBSink()

    sdf = sdf.apply(lambda row: process_message(row, sink))

    app.run(count=10)

if __name__ == "__main__":
    main()
```