import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

class TimescaleDBSink(BatchingSink):
    def __init__(self, connection_params, table_name):
        super().__init__()
        self.connection_params = connection_params
        self.table_name = table_name
        self.connection = None
        self.table_created = False

    def setup(self):
        """Setup the database connection and create table if needed"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = False
            self._create_table_if_not_exists()
            self.connection.commit()
            self.table_created = True
        except Exception as e:
            if self.connection:
                self.connection.close()
            raise e

    def _create_table_if_not_exists(self):
        """Create the table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            panel_id VARCHAR(255),
            location_id VARCHAR(255),
            location_name VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            timezone INTEGER,
            power_output INTEGER,
            unit_power VARCHAR(50),
            temperature FLOAT,
            unit_temp VARCHAR(50),
            irradiance INTEGER,
            unit_irradiance VARCHAR(50),
            voltage FLOAT,
            unit_voltage VARCHAR(50),
            current INTEGER,
            unit_current VARCHAR(50),
            inverter_status VARCHAR(50),
            timestamp_epoch BIGINT,
            timestamp_datetime TIMESTAMP,
            message_datetime TIMESTAMP,
            stream_id VARCHAR(255),
            topic_id VARCHAR(255),
            topic_name VARCHAR(255),
            partition_num INTEGER,
            offset_num BIGINT
        );
        """
        
        # Create hypertable for TimescaleDB if not exists
        hypertable_sql = f"""
        SELECT create_hypertable('{self.table_name}', 'timestamp_datetime', if_not_exists => TRUE);
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            try:
                cursor.execute(hypertable_sql)
            except Exception as e:
                # If hypertable creation fails, continue as regular table
                print(f"Warning: Could not create hypertable: {e}")

    def write(self, batch: SinkBatch):
        """Write batch data to TimescaleDB"""
        if not self.connection or not self.table_created:
            raise Exception("Database connection not established")
        
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            panel_id, location_id, location_name, latitude, longitude, timezone,
            power_output, unit_power, temperature, unit_temp, irradiance, unit_irradiance,
            voltage, unit_voltage, current, unit_current, inverter_status,
            timestamp_epoch, timestamp_datetime, message_datetime, stream_id,
            topic_id, topic_name, partition_num, offset_num
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            with self.connection.cursor() as cursor:
                for item in batch:
                    # Parse the JSON value from the message
                    if isinstance(item.value, str):
                        data = json.loads(item.value)
                    else:
                        data = item.value
                    
                    # Convert epoch timestamp to datetime
                    timestamp_epoch = data.get('timestamp', 0)
                    timestamp_datetime = datetime.fromtimestamp(timestamp_epoch / 1_000_000_000) if timestamp_epoch else None
                    
                    # Parse message datetime
                    message_datetime = datetime.fromisoformat(item.headers.get('dateTime', '').replace('Z', '+00:00')) if item.headers.get('dateTime') else None
                    
                    # Prepare row data
                    row_data = (
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
                        timestamp_epoch,
                        timestamp_datetime,
                        message_datetime,
                        item.headers.get('streamId'),
                        item.headers.get('topicId'),
                        item.headers.get('topicName'),
                        item.partition,
                        item.offset
                    )
                    
                    cursor.execute(insert_sql, row_data)
                
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e

    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()

def main():
    # Environment variables for connection
    try:
        port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
    except ValueError:
        port = 5432
    
    connection_params = {
        'host': os.environ.get('TIMESCALEDB_HOST', 'timescaledb'),
        'port': port,
        'user': os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin'),
        'password': os.environ.get('TIMESCALEDB_PASSWORD', 'password'),
        'database': os.environ.get('TIMESCALEDB_DBNAME', 'metrics')
    }
    
    table_name = os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav9')
    
    # Initialize the Quix application
    app = Application(consumer_group="timescaledb-sink-consumer")
    
    # Use environment variable for input topic
    input_topic_name = os.environ.get('input', 'solar-data')
    input_topic = app.topic(input_topic_name, value_deserializer='json')
    
    # Create the TimescaleDB sink
    timescale_sink = TimescaleDBSink(connection_params, table_name)
    
    # Create streaming dataframe
    sdf = app.dataframe(input_topic)
    
    # Add processing to extract and debug the message structure
    def process_message(message):
        print(f'Raw message: {message}')
        
        # The message structure from schema shows value is a JSON string
        # Parse the JSON value if it's a string
        if isinstance(message, str):
            try:
                parsed_data = json.loads(message)
                return parsed_data
            except json.JSONDecodeError:
                print(f"Failed to parse JSON: {message}")
                return message
        return message
    
    # Process and sink the data
    sdf = sdf.apply(process_message)
    sdf.sink(timescale_sink)
    
    # Run the application for 10 messages
    app.run(count=10, timeout=20)

if __name__ == "__main__":
    main()