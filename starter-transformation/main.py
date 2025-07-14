import os
import json
from quixstreams import Application, KafkaStreamingClient
from quixstreams.sinks.base import BatchingSink, SinkBatch, SinkBackpressureError
from datetime import datetime
import psycopg2

class TimescaleDBSink(BatchingSink):
    def __init__(self):
        super().__init__()
        self.connection = None

    def setup(self):
        try:
            self.connection = psycopg2.connect(
                dbname=os.environ.get('TIMESCALEDB_NAME'),
                user=os.environ.get('TIMESCALEDB_USER'),
                password=os.environ.get('TIMESCALEDB_PASSWORD'),
                host=os.environ.get('TIMESCALEDB_HOST'),
                port=os.environ.get('TIMESCALEDB_PORT'),
            )
            print("Connected to TimescaleDB")
        except Exception as e:
            print(f"Failed to connect to TimescaleDB: {e}")

    def write(self, batch: SinkBatch):
        cursor = self.connection.cursor()
        for item in batch:
            data = json.loads(item.value.decode('utf-8'))
            try:
                cursor.execute(
                    """
                    INSERT INTO solar_data (
                        panel_id, location_id, location_name, latitude, longitude, 
                        timezone, power_output, temperature, irradiance, voltage, 
                        current, inverter_status, timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        data['panel_id'], data['location_id'], data['location_name'], 
                        data['latitude'], data['longitude'], data['timezone'], 
                        data['power_output'], data['temperature'], data['irradiance'], 
                        data['voltage'], data['current'], data['inverter_status'], 
                        datetime.utcfromtimestamp(data['timestamp'] / 1e9)
                    )
                )
                self.connection.commit()
            except Exception as e:
                print(f"Failed to write to TimescaleDB: {e}")
                raise SinkBackpressureError(batch.topic, batch.partition, retry_after=30.0)
        cursor.close()

def main():
    app = Application(consumer_group="timescaledb_sink", auto_create_topics=True)

    input_topic = os.environ["INPUT_TOPIC"]

    kafka_client = KafkaStreamingClient(consumer_group="timescaledb_sink")
    stream = kafka_client.consume(input_topic)
    sink = TimescaleDBSink()

    stream.sink(sink)

    app.run(count=10)

if __name__ == "__main__":
    main()